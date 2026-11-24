import { describe, expect, it } from "vitest";
import { render, screen } from "@testing-library/react";
import {
  buildConfig,
  initialSetup,
  PUBLIC_CONFIG_KEYS,
  setupFromStored,
  setupProblems,
  SECRET_PLACEHOLDER,
} from "@/lib/connect-setup";
import { CredentialsStep } from "@/components/connect/wizard-steps";
import { sourceKind } from "@/components/connect/connect-view";

const setup = () => ({
  ...initialSetup("oracle"),
  name: "Oracle orders",
  credentialRef: "oracle-orders",
  host: "db.example.test",
  database: "ORCLPDB1",
  user: "MERIDIAN_READER",
});
describe("Oracle setup", () => {
  it("builds a worker-local password template on the TCPS port with the service name", () => {
    const s = setup();
    expect(setupProblems(s)).toEqual([]);
    const config = buildConfig(s);
    expect(config.publicConfig).toEqual({
      host: "db.example.test",
      port: 2484,
      service_name: "ORCLPDB1",
      user: "MERIDIAN_READER",
    });
    expect(config.agentFile).toEqual({ ...config.publicConfig, password: SECRET_PLACEHOLDER });
    expect(sourceKind("oracle-v1")).toBe("Oracle");
  });
  it("round trips schema and CA through stored settings", () => {
    const stored = buildConfig({
      ...setup(),
      schema: "SALES",
      sslCa: "oracle-ca.pem",
    }).publicConfig;
    expect(Object.keys(stored).every((k) => PUBLIC_CONFIG_KEYS.has(k))).toBe(true);
    const s = setupFromStored("oracle", "Oracle orders", "oracle-orders", stored);
    expect(buildConfig(s).agentFile).toEqual({
      host: "db.example.test",
      port: 2484,
      service_name: "ORCLPDB1",
      user: "MERIDIAN_READER",
      schema: "SALES",
      ssl_ca: "oracle-ca.pem",
      password: SECRET_PLACEHOLDER,
    });
  });
  it.each(["(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)))", "reader@db", "db.example.test/ORCLPDB1"])(
    "refuses connect descriptors and EZConnect strings as the host (%s)",
    (host) => {
      expect(setupProblems({ ...setup(), host }).join(" ")).toMatch(/connect descriptor/);
    },
  );
  it("refuses CDC, bad service names and bad schemas", () => {
    expect(setupProblems({ ...setup(), cdc: true }).join(" ")).toMatch(/Oracle CDC/);
    expect(setupProblems({ ...setup(), database: "a b" }).join(" ")).toMatch(/Service name/);
    expect(setupProblems({ ...setup(), schema: "1SALES" }).join(" ")).toMatch(/Schema/);
  });
  it("offers mandatory verified TLS, service name and schema fields", () => {
    render(<CredentialsStep setup={setup()} update={() => {}} />);
    expect(screen.getByLabelText("Port")).toHaveValue("2484");
    expect(screen.getByLabelText("Service name")).toHaveValue("ORCLPDB1");
    expect(screen.getByLabelText("Schema (optional)")).toBeTruthy();
    expect(screen.getByText(/host name verification is required/i)).toBeTruthy();
    expect(screen.queryByLabelText(/CDC/)).toBeNull();
  });
});
