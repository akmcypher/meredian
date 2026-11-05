export type Field = { name: string; type: string; nullable: boolean };
export type SchemaVersion = { version: number; createdAt: string; note: string; fields: Field[] };
export type Dataset = {
  id: string;
  name: string;
  lineageNodeId?: string;
  versions: SchemaVersion[];
};

export type ChangeKind = "added" | "removed" | "type_changed" | "nullability_changed";
export type Change = {
  kind: ChangeKind;
  field: string;
  from?: string;
  to?: string;
  breaking: boolean;
};

export function diffSchemas(prev: Field[], next: Field[]): Change[] {
  const p = new Map(prev.map((f) => [f.name, f]));
  const n = new Map(next.map((f) => [f.name, f]));
  const out: Change[] = [];
  for (const f of next) {
    const o = p.get(f.name);
    if (!o) {
      out.push({ kind: "added", field: f.name, to: f.type, breaking: !f.nullable });
      continue;
    }
    if (o.type !== f.type)
      out.push({ kind: "type_changed", field: f.name, from: o.type, to: f.type, breaking: true });
    if (o.nullable !== f.nullable)
      out.push({
        kind: "nullability_changed",
        field: f.name,
        from: o.nullable ? "nullable" : "required",
        to: f.nullable ? "nullable" : "required",
        breaking: !f.nullable,
      });
  }
  for (const f of prev)
    if (!n.has(f.name)) out.push({ kind: "removed", field: f.name, from: f.type, breaking: true });
  return out;
}

/** Parse "name: type" lines; a trailing "?" on the type marks nullable. */
export function parseSchemaText(text: string): Field[] {
  return text
    .split("\n")
    .map((l) => l.trim())
    .filter(Boolean)
    .map((l) => {
      const [name, rawType = "string"] = l.split(":").map((s) => s.trim());
      const nullable = rawType.endsWith("?");
      return { name: name ?? "", type: rawType.replace(/\?$/, "") || "string", nullable };
    });
}

export const fieldsToText = (fields: Field[]) =>
  fields.map((f) => `${f.name}: ${f.type}${f.nullable ? "?" : ""}`).join("\n");

/** Infer a schema from CSV rows. */
export function inferSchema(rows: Record<string, string>[]): Field[] {
  if (!rows.length) return [];
  return Object.keys(rows[0]!).map((name) => {
    const vals = rows.map((r) => (r[name] ?? "").trim());
    const present = vals.filter(Boolean);
    let type = "string";
    if (present.length && present.every((v) => /^-?\d+$/.test(v))) type = "integer";
    else if (present.length && present.every((v) => !isNaN(Number(v)))) type = "decimal";
    else if (present.length && present.every((v) => /^(true|false)$/i.test(v))) type = "boolean";
    else if (present.length && present.every((v) => /^\d{4}-\d{2}-\d{2}/.test(v))) type = "date";
    return { name, type, nullable: present.length < vals.length };
  });
}
