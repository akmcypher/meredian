import { describe, it, expect } from "vitest";
import { CONNECTORS, filterConnectors } from "@/lib/connector-registry";
describe("Kafka certified preview availability", () => {
  it("advertises the implemented certified actor while preserving production limits", () => {
    const kafka = CONNECTORS.find((c) => c.id === "kafka")!;
    expect(kafka.status).toBe("Available — Certified Preview");
    expect(kafka.adapter).toBe("independent-worker");
    expect(kafka.limitations).toMatch(/consumer-group scaling/);
    expect(kafka.limitations).toMatch(/SASL\/TLS/);
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
