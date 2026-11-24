import { describe, expect, it } from "vitest";
import { render, screen } from "@testing-library/react";
import {
  buildConfig,
  initialSetup,
  setupFromStored,
  setupProblems,
  SECRET_PLACEHOLDER,
} from "@/lib/connect-setup";
import { CredentialsStep } from "@/components/connect/wizard-steps";
import { sourceKind } from "@/components/connect/connect-view";

const setup = () => ({
  ...initialSetup("sqlserver"),
  name: "SQL orders",
  credentialRef: "sql-orders",
  host: "sql.example.test",
  database: "orders",
  user: "reader",
});
describe("SQL Server setup", () => {
  it("builds a worker-local password template with port 1433 and system trust", () => {
    const s = setup();
    expect(setupProblems(s)).toEqual([]);
    const config = buildConfig(s);
    expect(config.publicConfig).toEqual({
      host: "sql.example.test",
      port: 1433,
      database: "orders",
      user: "reader",
    });
    expect(config.agentFile).toEqual({ ...config.publicConfig, password: SECRET_PLACEHOLDER });
    expect(sourceKind("sqlserver-v1")).toBe("SQL Server");
  });
  it("round trips the optional worker-local CA and ignores obsolete publication metadata", () => {
    const s = setupFromStored("sqlserver", "SQL orders", "sql-orders", {
      host: "sql.example.test",
      database: "orders",
      user: "reader",
      ssl_ca: "sql-ca.pem",
      publication: "unrelated",
    });
    expect(s.cdc).toBe(false);
    expect(buildConfig(s).agentFile).toEqual({
      host: "sql.example.test",
      port: 1433,
      database: "orders",
      user: "reader",
      ssl_ca: "sql-ca.pem",
      password: SECRET_PLACEHOLDER,
    });
  });
  it("refuses CDC instead of registering an unsupported SQL Server actor", () => {
    expect(setupProblems({ ...setup(), cdc: true }).join(" ")).toMatch(/CDC.*not.*available/i);
  });
  it("offers mandatory verified TLS and no CDC or insecure TLS switch", () => {
    render(<CredentialsStep setup={setup()} update={() => {}} />);
    expect(screen.getByLabelText("Port")).toHaveValue("1433");
    expect(screen.getByLabelText("CA file (optional)")).toBeTruthy();
    expect(screen.getByText(/full-session TLS.*hostname/i)).toBeTruthy();
    expect(screen.queryByLabelText(/Read changes/)).toBeNull();
    expect(screen.queryByLabelText(/sslmode/)).toBeNull();
  });
});
