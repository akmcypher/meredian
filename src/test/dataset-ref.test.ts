import { describe, expect, it } from "vitest";
import {
  DATASET_REF_VERSION,
  manifestNames,
  policyDecision,
  toDatasetRef,
} from "@/lib/dataset-ref";

const row = {
  dataset_id: "6b1f0c1e-3a55-4a8e-9d55-0d9d0d6a1a01",
  name: "chain_gold",
  layer: "gold",
  version_id: "6b1f0c1e-3a55-4a8e-9d55-0d9d0d6a1a02",
  sha256: "a".repeat(64),
  byte_count: 1024,
  row_count: 3,
  schema: [{ name: "bucket", type: "int64" }],
  produced_by: {
    product: "transform",
    run_id: "6b1f0c1e-3a55-4a8e-9d55-0d9d0d6a1a03",
    task_id: "6b1f0c1e-3a55-4a8e-9d55-0d9d0d6a1a04",
    attempt: 1,
  },
  evidence_class: "worker_attested",
  created_at: "2026-11-02T00:00:00Z",
};

describe("DatasetRef v1", () => {
  it("preserves Connect Bronze format and provenance instead of relabelling it Parquet", () => {
    const bronze = {
      ...row,
      namespace: "connect",
      layer: "bronze",
      format: "ndjson",
      produced_by: { ...row.produced_by, product: "ingest" },
    };
    expect(toDatasetRef(bronze)).toMatchObject({
      namespace: "connect",
      format: "ndjson",
      layer: "bronze",
      producedBy: { product: "ingest" },
    });
    expect(() => toDatasetRef({ ...bronze, format: "parquet" })).toThrow();
    expect(
      toDatasetRef({ ...bronze, produced_by: { ...bronze.produced_by, attempt: null } }),
    ).toMatchObject({ producedBy: { attempt: null } });
    expect(() =>
      toDatasetRef({ ...row, produced_by: { ...row.produced_by, attempt: null } }),
    ).toThrow();
  });
  it("maps a registry row to the versioned contract", () => {
    const ref = toDatasetRef(row, "http://127.0.0.1:54321/storage/v1/object/sign/x");
    expect(ref).toMatchObject({
      contract: DATASET_REF_VERSION,
      name: "chain_gold",
      layer: "gold",
      format: "parquet",
      location: { sha256: "a".repeat(64), bytes: 1024 },
      producedBy: { product: "transform", attempt: 1 },
      evidenceClass: "worker_attested",
    });
  });

  it("rejects rows that would break the contract", () => {
    expect(() => toDatasetRef({ ...row, sha256: "not-a-digest" })).toThrow();
    expect(() => toDatasetRef({ ...row, name: "x; DROP" })).toThrow();
    expect(() => toDatasetRef({ ...row, evidence_class: "certified" })).toThrow();
  });

  it("accepts only structured policy decisions", () => {
    expect(policyDecision.parse({ allow: false })).toEqual({ allow: false });
    const filtered = {
      allow: true,
      rows: "filtered",
      rowFilters: [{ column: "bucket", op: ">=", value: "1" }],
      masks: [{ column: "customers", mode: "full" }],
    };
    expect(policyDecision.parse(filtered)).toEqual(filtered);
    expect(() =>
      policyDecision.parse({ ...filtered, rowFilters: [{ column: "x", op: "LIKE", value: "%" }] }),
    ).toThrow();
    expect(() => policyDecision.parse({ allow: true, rows: "all" })).toThrow();
  });

  it("forwards only possible dataset identifiers, lowercased and deduplicated", () => {
    expect(
      manifestNames(["Chain_Gold", "chain_gold", "1bad", "a-b", "x".repeat(64), "orders"]),
    ).toEqual(["chain_gold", "orders"]);
  });
});
