import { describe, it, expect } from "vitest";
import { CONNECTORS, filterConnectors } from "@/lib/connector-registry";
describe("Kafka certified preview availability", () => {
  it("advertises the implemented certified actor while preserving production limits", () => {
    const kafka = CONNECTORS.find((c) => c.id === "kafka")!;
    expect(kafka.status).toBe("Available — Certified Preview");
    expect(kafka.adapter).toBe("independent-worker");
    // Secured clusters are certified (K1); managed services and authorization are not yet.
    expect(kafka.capabilities).toMatch(/SCRAM-SHA-512/);
    expect(kafka.capabilities).toMatch(/mutual TLS/);
    expect(kafka.limitations).toMatch(/managed-service runs/);
    expect(kafka.limitations).toMatch(/ACL authorization/);
    expect(kafka.capabilities).toMatch(/backpressure/);
    expect(kafka.modes).toContain("streaming");
    expect(kafka.capabilities).toMatch(/topic discovery and multi-partition ingestion/);
    expect(kafka.capabilities).toMatch(/crash recovery without loss or duplicate positions/);
    expect(kafka.limitations).not.toMatch(/topic discovery/);
  });
  it("includes certified previews in Available and exact-status filters", () => {
    expect(
      filterConnectors("Apache Kafka", "Streaming / CDC", "Available").map((c) => c.id),
    ).toEqual(["kafka"]);
    expect(
      filterConnectors("Apache Kafka", "all", "Available — Certified Preview").map((c) => c.id),
    ).toEqual(["kafka"]);
  });
});
