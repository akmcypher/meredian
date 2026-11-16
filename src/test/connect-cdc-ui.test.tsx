// CDC in the Connect UI (deploy/connect/README.md, "Change data capture"): the credentials step offers change
// data capture for databases, PostgreSQL asks for the publication, and a CDC connection's streams
// are saved in sync mode `cdc` with no cursor or mode to pick.
import { describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen } from "@testing-library/react";
import { useState } from "react";
import { CredentialsStep } from "@/components/connect/wizard-steps";
import { StreamPicker } from "@/components/connect/StreamPicker";
import { formatBytes, isCdc, sourceKind } from "@/components/connect/connect-view";
import { initialSetup, type SetupState } from "@/lib/connect-setup";

function Credentials({ kind }: { kind: "postgres" | "mysql" }) {
  const [setup, setSetup] = useState<SetupState>(initialSetup(kind));
  return (
    <>
      <CredentialsStep setup={setup} update={(p) => setSetup((s) => ({ ...s, ...p }))} />
      <output data-testid="state">
        {JSON.stringify({ cdc: setup.cdc, pub: setup.publication })}
      </output>
    </>
  );
}

describe("CDC in the Connect UI", () => {
  it("PostgreSQL: the CDC choice reveals the publication field", () => {
    render(<Credentials kind="postgres" />);
    expect(screen.queryByLabelText("Publication")).toBeNull();
    fireEvent.click(screen.getByLabelText("Read changes from the database log (CDC)"));
    expect(screen.getByTestId("state").textContent).toBe('{"cdc":true,"pub":"meridian"}');
    fireEvent.change(screen.getByLabelText("Publication"), { target: { value: "orders_pub" } });
    expect(screen.getByTestId("state").textContent).toBe('{"cdc":true,"pub":"orders_pub"}');
    expect(screen.getByText(/wal_level=logical/)).toBeTruthy();
  });

  it("MySQL: the CDC choice names the binlog settings and asks for no publication", () => {
    render(<Credentials kind="mysql" />);
    fireEvent.click(screen.getByLabelText("Read changes from the database log (CDC)"));
    expect(screen.queryByLabelText("Publication")).toBeNull();
    expect(screen.getByText(/binlog_row_metadata=FULL/)).toBeTruthy();
  });

  it("a CDC connection's streams are saved in cdc mode, keyed by the first key column", () => {
    const onSave = vi.fn();
    render(
      <StreamPicker
        catalog={[
          {
            namespace: "public",
            name: "orders",
            primary_key: ["region", "id"],
            json_schema: { properties: { region: {}, id: {}, updated_at: {} } },
          },
        ]}
        kafka={false}
        cdc
        configured={[]}
        onSave={onSave}
      />,
    );
    fireEvent.click(screen.getByRole("checkbox"));
    expect(screen.queryByLabelText("Mode for public.orders")).toBeNull();
    expect(screen.getByText(/every insert, update and delete/)).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: /Save streams/ }));
    expect(onSave).toHaveBeenCalledWith([
      {
        namespace: "public",
        name: "orders",
        primaryKey: ["region", "id"],
        cursor: "region",
        mode: "cdc",
      },
    ]);
  });

  it("labels CDC connections and shows their lag in bytes", () => {
    expect(isCdc("postgres-cdc-v1")).toBe(true);
    expect(isCdc("postgres-v1")).toBe(false);
    expect(sourceKind("mysql-cdc-v1")).toBe("MySQL CDC");
    expect(sourceKind("postgres-v1")).toBe("PostgreSQL");
    expect(formatBytes(512)).toBe("512 B");
    expect(formatBytes(3 * 1024 * 1024)).toBe("3.0 MB");
  });
});
