// Disposable, extracted-package acceptance only. Never prints account credentials.
import { createClient } from "@supabase/supabase-js";
import { readFileSync, writeFileSync } from "node:fs";
import { execFileSync } from "node:child_process";
const url = process.env.SUPABASE_URL;
if (!url || !["127.0.0.1", "localhost", "[::1]"].includes(new URL(url).hostname))
  throw new Error("Acceptance requires the isolated local backend");
const admin = createClient(url, process.env.SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false },
});
const client = createClient(url, process.env.SUPABASE_PUBLISHABLE_KEY, {
  auth: { persistSession: false },
});
const path = new URL("../.meridian/acceptance.json", import.meta.url);
function checked(result) {
  if (result.error) throw new Error("Independent acceptance operation failed");
  return result.data;
}
if (process.argv[2] === "seed") {
  const email = `package-${crypto.randomUUID()}@example.test`,
    password = `Package-A9-${crypto.randomUUID()}`;
  const user = checked(
    await admin.auth.admin.createUser({ email, password, email_confirm: true }),
  ).user;
  checked(await client.auth.signInWithPassword({ email, password }));
  const roles = checked(await client.from("user_roles").select("role").eq("user_id", user.id));
  if (roles.length) throw new Error("Ordinary user unexpectedly has global privileges");
  if (
    checked(
      await client.rpc("write_native_table", {
        _name: "recovery_fixture",
        _rows: [{ id: 1, value: "persistent" }],
        _columns: [],
        _operation: "acceptance",
      }),
    ) !== 1
  )
    throw new Error("Unexpected initial version");
  const table = checked(
    await client
      .from("user_tables")
      .select("id,organization_id")
      .eq("name", "recovery_fixture")
      .single(),
  );
  const object = `${user.id}/acceptance/fixture.txt`;
  checked(
    await client.storage.from("ingest-files").upload(object, new Blob(["persistent object"])),
  );
  execFileSync(process.execPath, ["scripts/self-host.mjs", "bootstrap-admin", email], {
    stdio: ["ignore", "pipe", "pipe"],
  });
  const elevated = checked(await client.from("user_roles").select("role").eq("user_id", user.id));
  if (!elevated.some((r) => r.role === "admin"))
    throw new Error("Controlled administrator bootstrap failed");
  writeFileSync(
    path,
    JSON.stringify({
      email,
      password,
      userId: user.id,
      workspaceId: table.organization_id,
      tableId: table.id,
      object,
    }),
    {
      mode: 0o600,
    },
  );
  console.log(
    "Independent package fixture created; ordinary privileges and controlled admin bootstrap verified.",
  );
} else if (process.argv[2] === "verify") {
  const fixture = JSON.parse(readFileSync(path, "utf8"));
  const login = checked(
    await client.auth.signInWithPassword({ email: fixture.email, password: fixture.password }),
  );
  if (login.user.id !== fixture.userId || !fixture.workspaceId)
    throw new Error("Recovered user identity differs");
  checked(await client.from("organizations").select("id").eq("id", fixture.workspaceId).single());
  checked(
    await client
      .from("org_members")
      .select("org_id")
      .eq("org_id", fixture.workspaceId)
      .eq("user_id", fixture.userId)
      .single(),
  );
  const roles = checked(
    await client.from("user_roles").select("role").eq("user_id", fixture.userId),
  );
  if (!roles.some((role) => role.role === "admin"))
    throw new Error("Recovered administrator privileges differ");
  const table = checked(
    await client.from("user_tables").select("rows").eq("id", fixture.tableId).single(),
  );
  if (JSON.stringify(table.rows) !== JSON.stringify([{ id: 1, value: "persistent" }]))
    throw new Error("Recovered rows differ");
  const history = checked(
    await client.from("table_versions").select("version").eq("table_id", fixture.tableId),
  );
  if (history.length !== 1 || history[0].version !== 1)
    throw new Error("Recovered version history differs");
  const object = checked(await client.storage.from("ingest-files").download(fixture.object));
  if ((await object.text()) !== "persistent object") throw new Error("Recovered object differs");
  console.log("Sign-in, workspace-backed table/version and private object persisted and verified.");
} else if (process.argv[2] === "realtime") {
  const fixture = JSON.parse(readFileSync(path, "utf8"));
  checked(
    await client.auth.signInWithPassword({ email: fixture.email, password: fixture.password }),
  );
  const nonce = crypto.randomUUID();
  const channel = client.channel(`recovery-${nonce}`, {
    config: { broadcast: { self: true, ack: true } },
  });
  let received;
  channel.on("broadcast", { event: "recovery" }, ({ payload }) => {
    if (payload.nonce === nonce) received = true;
  });
  try {
    await new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("Realtime subscription timed out")), 15000);
      channel.subscribe((status) => {
        if (status === "SUBSCRIBED") {
          clearTimeout(timer);
          resolve();
        } else if (status === "CHANNEL_ERROR" || status === "TIMED_OUT") {
          clearTimeout(timer);
          reject(new Error("Realtime subscription failed"));
        }
      });
    });
    if ((await channel.send({ type: "broadcast", event: "recovery", payload: { nonce } })) !== "ok")
      throw new Error("Realtime delivery failed");
    const deadline = Date.now() + 15000;
    while (!received && Date.now() < deadline)
      await new Promise((resolve) => setTimeout(resolve, 50));
    if (!received) throw new Error("Realtime message was not received");
    console.log("Authenticated Realtime subscription and message round trip verified.");
  } finally {
    await client.removeChannel(channel);
  }
} else throw new Error("Use seed, verify or realtime");
await client.auth.signOut();
