import type { ReactNode } from "react";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Checkbox } from "@/components/ui/checkbox";
import {
  KAFKA_PROFILES,
  mechanismChoices,
  SELF_MANAGED,
  setupWarnings,
  SOURCE_KINDS,
  visibleFields,
  initialSetup,
  type SetupState,
} from "@/lib/connect-setup";

type Update = (patch: Partial<SetupState>) => void;

function Choice({
  selected,
  onSelect,
  title,
  summary,
  name,
}: {
  selected: boolean;
  onSelect: () => void;
  title: string;
  summary: string;
  name: string;
}) {
  return (
    <label
      className={`flex cursor-pointer gap-2 rounded-md border p-3 text-sm ${selected ? "border-primary ring-1 ring-primary" : ""}`}
    >
      <input type="radio" name={name} checked={selected} onChange={onSelect} className="mt-1" />
      <span>
        <span className="block font-medium">{title}</span>
        <span className="text-muted-foreground">{summary}</span>
      </span>
    </label>
  );
}

export function SourceTypeStep({
  setup,
  onChange,
}: {
  setup: SetupState;
  onChange: (s: SetupState) => void;
}) {
  return (
    <fieldset className="grid gap-2 sm:grid-cols-3">
      <legend className="mb-2 text-sm font-medium">Source type</legend>
      {SOURCE_KINDS.map((k) => (
        <Choice
          key={k.id}
          name="connect-source-kind"
          selected={setup.kind === k.id}
          onSelect={() =>
            onChange({
              ...initialSetup(k.id),
              name: setup.name,
              credentialRef: setup.credentialRef,
            })
          }
          title={k.label}
          summary={k.summary}
        />
      ))}
    </fieldset>
  );
}

export function KafkaSecurityStep({ setup, update }: { setup: SetupState; update: Update }) {
  const m = setup.kafkaMode;
  return (
    <div className="space-y-4">
      <fieldset className="grid gap-2 sm:grid-cols-2">
        <legend className="mb-2 text-sm font-medium">Managed service</legend>
        {KAFKA_PROFILES.map((p) => (
          <Choice
            key={p.id}
            name="kafka-security"
            selected={m.type === "profile" && m.profile === p.id}
            onSelect={() =>
              update({
                kafkaMode: { type: "profile", profile: p.id },
                saslMechanism: p.id === "redpanda_cloud" ? "SCRAM-SHA-256" : "SCRAM-SHA-512",
              })
            }
            title={p.label}
            summary={p.summary}
          />
        ))}
      </fieldset>
      <fieldset className="grid gap-2 sm:grid-cols-2">
        <legend className="mb-2 text-sm font-medium">Self-managed cluster</legend>
        {SELF_MANAGED.map((o) => (
          <Choice
            key={o.id}
            name="kafka-security"
            selected={m.type === "self" && m.security === o.id}
            onSelect={() =>
              update({
                kafkaMode: { type: "self", security: o.id },
                saslMechanism: "SCRAM-SHA-512",
              })
            }
            title={o.label}
            summary={o.summary}
          />
        ))}
      </fieldset>
    </div>
  );
}

function Field({
  id,
  label,
  hint,
  children,
}: {
  id: string;
  label: string;
  hint?: string | undefined;
  children: ReactNode;
}) {
  return (
    <div className="space-y-1">
      <Label htmlFor={id}>{label}</Label>
      {children}
      {hint && <p className="text-xs text-muted-foreground">{hint}</p>}
    </div>
  );
}

function Text({
  id,
  label,
  hint,
  value,
  placeholder,
  onChange,
}: {
  id: string;
  label: string;
  hint?: string | undefined;
  value: string;
  placeholder?: string;
  onChange: (v: string) => void;
}) {
  return (
    <Field id={id} label={label} hint={hint}>
      <Input
        id={id}
        value={value}
        placeholder={placeholder}
        autoComplete="off"
        onChange={(e) => onChange(e.target.value)}
      />
    </Field>
  );
}

/** Only the non-secret fields the chosen source and security need. */
export function CredentialsStep({ setup, update }: { setup: SetupState; update: Update }) {
  const show = visibleFields(setup);
  const profile =
    setup.kafkaMode.type === "profile"
      ? KAFKA_PROFILES.find((p) => p.id === (setup.kafkaMode as { profile: string }).profile)
      : null;
  const mechanisms = mechanismChoices(setup);
  return (
    <div className="space-y-4">
      <div className="grid gap-3 sm:grid-cols-2">
        <Text
          id="cw-name"
          label="Connection name"
          value={setup.name}
          onChange={(name) => update({ name })}
        />
        <Text
          id="cw-ref"
          label="Credential reference"
          hint="Names the agent's private <reference>.json that holds the secrets."
          value={setup.credentialRef}
          placeholder="orders-kafka"
          onChange={(credentialRef) => update({ credentialRef })}
        />
      </div>
      {show.database ? (
        <div className="grid gap-3 sm:grid-cols-2">
          <Text
            id="cw-host"
            label={setup.kind === "mongodb" ? "Hosts" : "Host"}
            hint={
              setup.kind === "mongodb"
                ? "One host, or replica set members as host1,host2 (no credentials or options)."
                : undefined
            }
            value={setup.host}
            onChange={(host) => update({ host })}
          />
          <Text
            id="cw-port"
            label="Port"
            value={setup.port}
            onChange={(port) => update({ port })}
          />
          <Text
            id="cw-db"
            label={setup.kind === "oracle" ? "Service name" : "Database"}
            hint={setup.kind === "oracle" ? "For example FREEPDB1 or ORCLPDB1." : undefined}
            value={setup.database}
            onChange={(database) => update({ database })}
          />
          <Text
            id="cw-user"
            label="User"
            value={setup.user}
            onChange={(user) => update({ user })}
          />
          {setup.kind === "postgres" ? (
            <Field id="cw-sslmode" label="TLS (sslmode)">
              <select
                id="cw-sslmode"
                className="w-full border bg-background p-2"
                value={setup.sslmode}
                onChange={(e) => update({ sslmode: e.target.value })}
              >
                {["verify-full", "verify-ca", "require", "prefer", "disable"].map((m) => (
                  <option key={m}>{m}</option>
                ))}
              </select>
            </Field>
          ) : (
            <Text
              id="cw-sslca"
              label="CA file (optional)"
              hint="File name in the agent's secrets directory."
              value={setup.sslCa}
              onChange={(sslCa) => update({ sslCa })}
            />
          )}
          {setup.kind === "oracle" && (
            <Text
              id="cw-schema"
              label="Schema (optional)"
              hint="Owner of the tables to offer; defaults to the user's own schema."
              value={setup.schema}
              onChange={(schema) => update({ schema })}
            />
          )}
          {setup.kind === "mongodb" && (
            <>
              <Text
                id="cw-replica-set"
                label="Replica set (optional)"
                value={setup.replicaSet}
                onChange={(replicaSet) => update({ replicaSet })}
              />
              <Text
                id="cw-auth-source"
                label="Authentication database"
                value={setup.authSource}
                onChange={(authSource) => update({ authSource })}
              />
            </>
          )}
          {setup.kind === "mongodb" ? (
            <div className="space-y-2 sm:col-span-2">
              <p className="text-xs text-muted-foreground">
                TLS with certificate and hostname verification is required. Uses system trust when
                no CA file is supplied. Documents land whole as Extended JSON.
              </p>
              <div className="flex items-center gap-2">
                <Checkbox
                  id="cw-cdc"
                  checked={setup.cdc}
                  onCheckedChange={(v) => update({ cdc: v === true })}
                />
                <Label htmlFor="cw-cdc">Read changes from change streams (CDC)</Label>
              </div>
              <p className="text-xs text-muted-foreground">
                Captures inserts, updates, replaces and deletes. Needs a replica set or sharded
                cluster and a user with the read role (which includes changeStream) on the database.
              </p>
            </div>
          ) : setup.kind === "oracle" ? (
            <p className="text-xs text-muted-foreground sm:col-span-2">
              TLS (TCPS, usually port 2484) with certificate and host name verification is required;
              connect by the DNS name on the server certificate. Uses system trust when no CA file
              is supplied. Full refresh and cursor-based incremental only; CDC is not available yet.
            </p>
          ) : setup.kind === "sqlserver" ? (
            <p className="text-xs text-muted-foreground sm:col-span-2">
              Full-session TLS with hostname validation is required. Uses system trust when no CA
              file is supplied. Full refresh and cursor-based incremental only; CDC is not available
              yet.
            </p>
          ) : (
            <div className="space-y-2 sm:col-span-2">
              <div className="flex items-center gap-2">
                <Checkbox
                  id="cw-cdc"
                  checked={setup.cdc}
                  onCheckedChange={(v) => update({ cdc: v === true })}
                />
                <Label htmlFor="cw-cdc">Read changes from the database log (CDC)</Label>
              </div>
              <p className="text-xs text-muted-foreground">
                {setup.kind === "postgres"
                  ? "Captures inserts, updates and deletes. Needs wal_level=logical, a user with the REPLICATION attribute, and a publication listing the tables (CREATE PUBLICATION meridian FOR TABLE ...)."
                  : "Captures inserts, updates and deletes. Needs binlog_format=ROW, binlog_row_image=FULL, binlog_row_metadata=FULL, and a user with REPLICATION SLAVE and REPLICATION CLIENT."}
              </p>
              {setup.cdc && setup.kind === "postgres" && (
                <Text
                  id="cw-publication"
                  label="Publication"
                  hint="An existing publication that includes every table you will sync."
                  value={setup.publication}
                  onChange={(publication) => update({ publication })}
                />
              )}
            </div>
          )}
        </div>
      ) : (
        <div className="grid gap-3 sm:grid-cols-2">
          <div className="sm:col-span-2">
            <Text
              id="cw-bootstrap"
              label="Bootstrap servers"
              hint="Comma-separated host:port."
              value={setup.bootstrapServers}
              placeholder={profile?.bootstrapHint ?? "broker-1:9092,broker-2:9092"}
              onChange={(bootstrapServers) => update({ bootstrapServers })}
            />
          </div>
          {show.aivenAuth && (
            <Field id="cw-aiven" label="Aiven authentication">
              <select
                id="cw-aiven"
                className="w-full border bg-background p-2"
                value={setup.aivenAuth}
                onChange={(e) =>
                  update({
                    aivenAuth: e.target.value as SetupState["aivenAuth"],
                    saslMechanism: "SCRAM-SHA-256",
                  })
                }
              >
                <option value="SSL">Client certificate (SSL)</option>
                <option value="SASL_SSL">SCRAM over SASL_SSL</option>
              </select>
            </Field>
          )}
          {show.mechanism && (
            <Field id="cw-mech" label="SASL mechanism">
              <select
                id="cw-mech"
                className="w-full border bg-background p-2"
                value={setup.saslMechanism}
                onChange={(e) =>
                  update({ saslMechanism: e.target.value as SetupState["saslMechanism"] })
                }
              >
                {mechanisms.map((m) => (
                  <option key={m}>{m}</option>
                ))}
              </select>
            </Field>
          )}
          {show.awsRegion && (
            <Text
              id="cw-region"
              label="AWS region"
              value={setup.awsRegion}
              placeholder="eu-west-1"
              onChange={(awsRegion) => update({ awsRegion })}
            />
          )}
          {show.mskIamCredentials && (
            <Field id="cw-iam" label="AWS credentials">
              <select
                id="cw-iam"
                className="w-full border bg-background p-2"
                value={setup.mskIamCredentials}
                onChange={(e) =>
                  update({ mskIamCredentials: e.target.value as SetupState["mskIamCredentials"] })
                }
              >
                <option value="default_chain">
                  Agent's default chain (IRSA, instance or container role, environment)
                </option>
                <option value="role">Assume an IAM role</option>
                <option value="keys">Access keys in the agent file</option>
              </select>
            </Field>
          )}
          {show.awsRoleArn && (
            <Text
              id="cw-role"
              label="IAM role ARN"
              value={setup.awsRoleArn}
              placeholder="arn:aws:iam::123456789012:role/meridian-connect"
              onChange={(awsRoleArn) => update({ awsRoleArn })}
            />
          )}
          {show.caFile && (
            <Text
              id="cw-ca"
              label="CA file (optional)"
              hint="Private CA, in the agent's secrets directory. Empty = system trust store."
              value={setup.caFile}
              placeholder="ca.pem"
              onChange={(caFile) => update({ caFile })}
            />
          )}
          {show.clientCertificate && (
            <>
              <Text
                id="cw-cert"
                label="Client certificate file"
                value={setup.certificateFile}
                placeholder="client.pem"
                onChange={(certificateFile) => update({ certificateFile })}
              />
              <Text
                id="cw-key"
                label="Client key file"
                value={setup.keyFile}
                placeholder="client.key"
                onChange={(keyFile) => update({ keyFile })}
              />
              <label className="flex items-center gap-2 text-sm">
                <Checkbox
                  checked={setup.keyHasPassword}
                  onCheckedChange={(v) => update({ keyHasPassword: !!v })}
                />
                The key is password-protected
              </label>
            </>
          )}
          <Text
            id="cw-registry"
            label="Schema registry URL (optional, Avro)"
            value={setup.schemaRegistryUrl}
            placeholder="https://registry.example.com"
            onChange={(schemaRegistryUrl) => update({ schemaRegistryUrl })}
          />
          {setup.schemaRegistryUrl.trim() && (
            <label className="flex items-center gap-2 self-end text-sm">
              <Checkbox
                checked={setup.schemaRegistryAuth}
                onCheckedChange={(v) => update({ schemaRegistryAuth: !!v })}
              />
              The registry needs a login
            </label>
          )}
        </div>
      )}
      {setupWarnings(setup).map((w) => (
        <p key={w} className="text-sm text-amber-700 dark:text-amber-400">
          {w}
        </p>
      ))}
    </div>
  );
}
