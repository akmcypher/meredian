// Serve manifest (E-6): resolves the published Gold datasets a verified subject may query into
// DatasetRefs with 5-minute read URLs, so the DataFusion service loads checksummed Parquet itself
// instead of an operator copying it. The tenant rule lives in serve_manifest (service-only).
import { manifestNames, policyDecision, serveManifestRequest, toDatasetRef } from "./dataset-ref";
import { readJsonCapped, RequestLimitError } from "./request-limits.server";

const MAX_BODY = 16 * 1024;

export async function handleServeManifest(request: Request) {
  const auth = request.headers.get("authorization") ?? "";
  if (!auth.startsWith("Bearer ")) return Response.json({ error: "unauthorized" }, { status: 401 });
  // Callers are engines holding an engine-bound token; a user's Supabase session
  // never reaches an engine, so it is not accepted here either.
  const { verifyEngineToken } = await import("./engine-token.server");
  const claims = await verifyEngineToken(auth.slice(7));
  if (!claims) return Response.json({ error: "unauthorized" }, { status: 401 });
  const { supabaseAdmin: db } = await import("@/integrations/supabase/client.server");
  // The engine must be registered and enabled, and usable by the token's subject.
  const engine = await db
    .from("execution_engines")
    .select("owner_id,organization_id,enabled")
    .eq("id", claims.engineId)
    .maybeSingle();
  if (engine.error || !engine.data?.enabled)
    return Response.json({ error: "unauthorized" }, { status: 401 });
  if (engine.data.organization_id || engine.data.owner_id !== claims.userId) {
    const member = engine.data.organization_id
      ? await db
          .from("org_members")
          .select("user_id")
          .eq("org_id", engine.data.organization_id)
          .eq("user_id", claims.userId)
          .maybeSingle()
      : { data: null };
    if (!member.data) return Response.json({ error: "forbidden" }, { status: 403 });
  }
  let input;
  try {
    input = serveManifestRequest.parse(await readJsonCapped(request, MAX_BODY));
  } catch (error) {
    if (error instanceof RequestLimitError)
      return Response.json({ error: "request_limit" }, { status: error.status });
    return Response.json({ error: "invalid_request" }, { status: 400 });
  }
  if (input.bronze) {
    // Bronze is an explicit completed-run snapshot, not an implicit change to Gold SQL lookup.
    // The service-only RPC permits the dataset owner only; organization sharing/masks are not
    // implied by handing an arbitrary external engine raw-object read URLs.
    const selected = input.bronze;
    const result = await db.rpc("serve_connect_bronze", {
      _subject: claims.userId,
      _engine: claims.engineId,
      _stream: selected.streamId,
      _run: selected.runId,
      _after: selected.afterSequence,
    });
    if (result.error?.code === "42501")
      return Response.json({ error: "unauthorized" }, { status: 401 });
    if (result.error) return Response.json({ error: "manifest_unavailable" }, { status: 503 });
    if (!result.data.length) return Response.json({ error: "not_found" }, { status: 404 });
    const parts = [];
    try {
      for (const row of result.data.slice(0, 25)) {
        const signed = await db.storage.from(row.bucket).createSignedUrl(row.object_key, 300);
        if (!signed.data) return Response.json({ error: "manifest_unavailable" }, { status: 503 });
        parts.push({ ...toDatasetRef(row, signed.data.signedUrl), sequence: row.sequence });
      }
    } catch {
      return Response.json({ error: "manifest_unavailable" }, { status: 503 });
    }
    return Response.json({
      bronze: {
        streamId: selected.streamId,
        runId: selected.runId,
        datasetId: parts[0]!.datasetId,
        parts,
        nextSequence: result.data.length > 25 ? parts.at(-1)!.sequence : null,
      },
    });
  }
  const names = manifestNames(input.names);
  const rows = await db.rpc("serve_manifest", { _subject: claims.userId, _names: names });
  if (rows.error?.code === "42501")
    return Response.json({ error: "unauthorized" }, { status: 401 });
  if (rows.error) return Response.json({ error: "manifest_unavailable" }, { status: 503 });
  const datasets = [];
  for (const row of rows.data) {
    // The decision travels with the read URL; a dataset without a valid one is never handed out.
    const policy = policyDecision.safeParse(row.policy);
    if (!policy.success || !policy.data.allow) continue;
    const signed = await db.storage.from(row.bucket).createSignedUrl(row.object_key, 300);
    if (!signed.data) return Response.json({ error: "manifest_unavailable" }, { status: 503 });
    datasets.push({ ...toDatasetRef(row, signed.data.signedUrl), policy: policy.data });
  }
  return Response.json({ datasets });
}
