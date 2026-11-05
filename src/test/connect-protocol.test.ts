import { describe, it, expect } from "vitest";
import {
  agentRequest,
  canonicalEnvelopes,
  deadLetterMessage,
  streamSchema,
} from "@/lib/connect-protocol";
describe("Connect protocol", () => {
  it("rejects arbitrary agent actions", () =>
    expect(agentRequest.safeParse({ action: "execute", code: "code" }).success).toBe(false));
  it("preserves raw payload and explicit provenance", () => {
    const record = {
      type: "RECORD" as const,
      record: {
        stream: "customers",
        namespace: "public",
        data: { id: "9007199254740993", name: "Customer" },
        emitted_at: 123,
        primary_key: "9007199254740993",
      },
    };
    const rows = canonicalEnvelopes("connection", "run", "stream", [record], { version: 1 });
    expect(rows[0]?.data).toEqual(record.record.data);
    expect(rows[0]?.run_id).toBe("run");
    expect(rows[0]?.extracted_at).toBe(123);
  });
  it("does not accept raw connector errors", () =>
    expect(
      agentRequest.safeParse({
        action: "finish",
        runId: crypto.randomUUID(),
        token: crypto.randomUUID(),
        ok: false,
        code: "password=leak",
      }).success,
    ).toBe(false));
});
describe("Connect protocol v1 additive fields", () => {
  const ids = { runId: crypto.randomUUID(), token: crypto.randomUUID() };
  const commit = (extra: Record<string, unknown>) => ({
    action: "commit",
    ...ids,
    streamId: crypto.randomUUID(),
    sequence: 0,
    records: [],
    state: { type: "STATE", state: { version: 1 } },
    ...extra,
  });
  it("keeps single-column v1 catalogs valid and accepts composite keys", () => {
    const stream = { name: "pairs", namespace: "public", json_schema: {} };
    expect(streamSchema.safeParse({ ...stream, primary_key: "id" }).success).toBe(true);
    expect(streamSchema.safeParse({ ...stream, primary_key: ["a", "b"] }).success).toBe(true);
    expect(streamSchema.safeParse({ ...stream, primary_key: [] }).success).toBe(false);
    expect(streamSchema.safeParse({ ...stream, primary_key: ["a;drop"] }).success).toBe(false);
  });
  it("carries deletes into the bronze envelope and defaults to upsert", () => {
    const base = { stream: "t", namespace: "k", data: {}, emitted_at: 1, primary_key: "1" };
    const rows = canonicalEnvelopes(
      "c",
      "r",
      "s",
      [
        { type: "RECORD", record: base },
        { type: "RECORD", record: { ...base, operation: "delete" } },
      ],
      {},
    );
    expect(rows.map((r) => r.operation)).toEqual(["upsert", "delete"]);
  });
  it("accepts per-partition positions and dead letters, and rejects malformed ones", () => {
    const dead = {
      type: "DEAD_LETTER",
      dead_letter: {
        stream: "orders",
        namespace: "kafka",
        position: "0:7",
        error_code: "schema_not_found",
        payload: { value_b64: "AA==", partition: 0, offset: 7 },
      },
    };
    const ok = commit({
      dead_letters: [dead],
      state: { type: "STATE", state: { version: 1, positions: { "0": 8, "2": 0 } } },
    });
    expect(agentRequest.safeParse(ok).success).toBe(true);
    expect(
      agentRequest.safeParse(
        commit({ state: { type: "STATE", state: { version: 1, positions: { "0": -1 } } } }),
      ).success,
    ).toBe(false);
    expect(
      agentRequest.safeParse(
        commit({
          dead_letters: [{ ...dead, dead_letter: { ...dead.dead_letter, error_code: "x" } }],
        }),
      ).success,
    ).toBe(false);
    expect(deadLetterMessage.safeParse(dead).success).toBe(true);
  });
});
describe("Connect protocol offset gaps", () => {
  const ids = { runId: crypto.randomUUID(), token: crypto.randomUUID() };
  it("accepts a positions-only gap report and rejects anything else in it", () => {
    const gap = { partition: 0, offset: 5, low: 10, gap: 5 };
    const report = { action: "report", ...ids, code: "offset_out_of_range", detail: [gap] };
    expect(agentRequest.safeParse(report).success).toBe(true);
    expect(agentRequest.safeParse({ ...report, detail: [] }).success).toBe(false);
    expect(agentRequest.safeParse({ ...report, code: "schema_drift" }).success).toBe(false);
    // Unknown fields are refused outright, so a report cannot smuggle record data.
    expect(
      agentRequest.safeParse({ ...report, detail: [{ ...gap, value: "row data" }] }).success,
    ).toBe(false);
    expect(
      agentRequest.safeParse({ ...report, code: "source_changed", detail: [{ reason: "x" }] })
        .success,
    ).toBe(false);
  });
  it("lets finish name offset_out_of_range and commits carry accepted gaps", () => {
    expect(
      agentRequest.safeParse({ action: "finish", ...ids, ok: false, code: "offset_out_of_range" })
        .success,
    ).toBe(true);
    const commit = {
      action: "commit",
      ...ids,
      streamId: crypto.randomUUID(),
      sequence: 0,
      records: [],
      state: { type: "STATE", state: { version: 1, gaps: [{ partition: 0, from: 5, to: 10 }] } },
    };
    expect(agentRequest.safeParse(commit).success).toBe(true);
  });
});
describe("Connect protocol lossless offsets", () => {
  const commit = (positions: Record<string, unknown>) => ({
    action: "commit",
    runId: crypto.randomUUID(),
    token: crypto.randomUUID(),
    streamId: crypto.randomUUID(),
    sequence: 0,
    records: [],
    state: { type: "STATE", state: { version: 1, positions } },
  });
  it("keeps offsets at and past 2^53 exactly as decimal strings", () => {
    for (const o of [
      "9007199254740991",
      "9007199254740992",
      "9007199254740993",
      "18446744073709551615",
    ]) {
      const parsed = agentRequest.parse(JSON.parse(JSON.stringify(commit({ "0": o }))));
      expect(parsed.action === "commit" && parsed.state.state.positions?.["0"]).toBe(o);
    }
  });
  it("still reads small integer offsets from older checkpoints, but nothing else", () => {
    expect(agentRequest.safeParse(commit({ "0": 42 })).success).toBe(true);
    expect(agentRequest.safeParse(commit({ "0": "-1" })).success).toBe(false);
    expect(agentRequest.safeParse(commit({ "0": "1e3" })).success).toBe(false);
    expect(agentRequest.safeParse(commit({ "0": 1.5 })).success).toBe(false);
    expect(agentRequest.safeParse(commit({ "0": Number.MAX_SAFE_INTEGER })).success).toBe(true);
    expect(agentRequest.safeParse(commit({ "0": 9007199254740992 })).success).toBe(false);
    expect(agentRequest.safeParse(commit({ "0": JSON.parse("9007199254740993") })).success).toBe(
      false,
    );
  });
});
describe("Connect protocol source_changed report", () => {
  it("accepts Kafka's base64 cluster and topic ids", () => {
    const report = {
      action: "report",
      runId: crypto.randomUUID(),
      token: crypto.randomUUID(),
      code: "source_changed",
      streamId: crypto.randomUUID(),
      detail: [{ reason: "topic_id", saved: "AAAAAGAJS0n/////jUZwBA", current: "q+Wz/9a_Lk-0==" }],
    };
    expect(agentRequest.safeParse(report).success).toBe(true);
    expect(
      agentRequest.safeParse({ ...report, detail: [{ reason: "topic_id", saved: "a b" }] }).success,
    ).toBe(false);
  });
});
