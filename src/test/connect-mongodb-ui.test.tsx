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
  ...initialSetup("mongodb"),
  name: "Mongo orders",
  credentialRef: "mongo-orders",
  host: "m1.example.test,m2.example.test",
  database: "shop",
  user: "reader",
});
describe("MongoDB setup", () => {
  it("builds a worker-local password template with port 27017 and admin auth", () => {
    const s = setup();
    expect(setupProblems(s)).toEqual([]);
    const config = buildConfig(s);
    expect(config.publicConfig).toEqual({
      hosts: "m1.example.test,m2.example.test",
      port: 27017,
      database: "shop",
      user: "reader",
      auth_source: "admin",
    });
    expect(config.agentFile).toEqual({ ...config.publicConfig, password: SECRET_PLACEHOLDER });
    expect(sourceKind("mongodb-v1")).toBe("MongoDB");
  });
  it("round trips replica set, auth database and CA through stored settings", () => {
    const stored = {
      ...buildConfig({ ...setup(), replicaSet: "rs0", authSource: "shop", sslCa: "mongo-ca.pem" })
        .publicConfig,
    };
    expect(Object.keys(stored).every((k) => PUBLIC_CONFIG_KEYS.has(k))).toBe(true);
    const s = setupFromStored("mongodb", "Mongo orders", "mongo-orders", stored);
    expect(buildConfig(s).agentFile).toEqual({
      hosts: "m1.example.test,m2.example.test",
      port: 27017,
      database: "shop",
      user: "reader",
      auth_source: "shop",
      replica_set: "rs0",
      ssl_ca: "mongo-ca.pem",
      password: SECRET_PLACEHOLDER,
    });
  });
  it.each(["reader:pw@m1.example.test", "m1.example.test/shop", "m1.example.test?tls=false"])(
    "refuses credentials or options in hosts (%s)",
    (host) => {
      expect(setupProblems({ ...setup(), host }).join(" ")).toMatch(/without credentials/);
    },
  );
  it("allows change streams only with a replica set name", () => {
    expect(setupProblems({ ...setup(), cdc: true }).join(" ")).toMatch(/replica set/i);
    expect(setupProblems({ ...setup(), cdc: true, replicaSet: "rs0" })).toEqual([]);
  });
  it("offers mandatory verified TLS, replica set and auth database fields", () => {
    render(<CredentialsStep setup={setup()} update={() => {}} />);
    expect(screen.getByLabelText("Port")).toHaveValue("27017");
    expect(screen.getByLabelText("Hosts")).toHaveValue("m1.example.test,m2.example.test");
    expect(screen.getByLabelText("Replica set (optional)")).toBeTruthy();
    expect(screen.getByLabelText("Authentication database")).toHaveValue("admin");
    expect(screen.getByText(/certificate and hostname verification is required/i)).toBeTruthy();
    expect(screen.getByLabelText(/Read changes from change streams/)).toBeTruthy();
  });
});
