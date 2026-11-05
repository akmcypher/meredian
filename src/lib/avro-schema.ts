// Avro schema evolution: parsing, writer→reader resolution (Avro 1.11 spec, "Schema Resolution")
// and registry-style compatibility modes. Pure; used by the Avro file reader and the registry.

export type Primitive =
  "null" | "boolean" | "int" | "long" | "float" | "double" | "bytes" | "string";
export type AvroField = {
  name: string;
  type: AvroNode;
  aliases: string[];
  hasDefault: boolean;
  default?: unknown;
};
export type AvroNode =
  | { t: Primitive }
  | { t: "record"; name: string; aliases: string[]; fields: AvroField[] }
  | { t: "enum"; name: string; aliases: string[]; symbols: string[]; default?: string }
  | { t: "fixed"; name: string; aliases: string[]; size: number }
  | { t: "array"; items: AvroNode }
  | { t: "map"; values: AvroNode }
  | { t: "union"; branches: AvroNode[] };

const PRIMITIVES = new Set<string>([
  "null",
  "boolean",
  "int",
  "long",
  "float",
  "double",
  "bytes",
  "string",
]);
const NAME = /^[A-Za-z_][A-Za-z0-9_]*$/;
const short = (fullname: string) => fullname.slice(fullname.lastIndexOf(".") + 1);

/** Parses an Avro schema (JSON text or value) into a resolved tree; named types are shared nodes. */
export function parseAvroSchema(input: unknown): AvroNode {
  const json = typeof input === "string" ? JSON.parse(input) : input;
  const named = new Map<string, AvroNode>();
  const fullname = (name: string, ns: string) => {
    for (const part of name.split("."))
      if (!NAME.test(part)) throw new Error(`Invalid Avro name "${name}"`);
    return name.includes(".") || !ns ? name : `${ns}.${name}`;
  };
  const define = (name: string, node: AvroNode) => {
    if (named.has(name) || PRIMITIVES.has(name)) throw new Error(`Avro type "${name}" redefined`);
    named.set(name, node);
  };
  const walk = (s: unknown, ns: string): AvroNode => {
    if (typeof s === "string") {
      if (PRIMITIVES.has(s)) return { t: s as Primitive };
      const found = named.get(s) ?? named.get(ns ? `${ns}.${s}` : s);
      if (!found) throw new Error(`Unknown Avro type "${s}"`);
      return found;
    }
    if (Array.isArray(s)) {
      const branches = s.map((b) => walk(b, ns));
      const seen = new Set<string>();
      for (const b of branches) {
        if (b.t === "union") throw new Error("Avro unions may not contain unions");
        const id = "name" in b ? b.name : b.t;
        if (seen.has(id)) throw new Error(`Avro union repeats ${id}`);
        seen.add(id);
      }
      return { t: "union", branches };
    }
    if (!s || typeof s !== "object") throw new Error("Invalid Avro schema");
    const o = s as Record<string, unknown>;
    const type = o["type"];
    if (typeof type !== "string" || PRIMITIVES.has(type)) {
      if (typeof type === "string") return { t: type as Primitive }; // logical types read as base
      return walk(type, ns);
    }
    const aliasesOf = (space: string) =>
      Array.isArray(o["aliases"]) ? (o["aliases"] as string[]).map((a) => fullname(a, space)) : [];
    if (type === "record" || type === "error" || type === "enum" || type === "fixed") {
      if (typeof o["name"] !== "string") throw new Error(`Avro ${type} needs a name`);
      const space = typeof o["namespace"] === "string" ? (o["namespace"] as string) : ns;
      const name = fullname(o["name"], space);
      const inner = name.includes(".") ? name.slice(0, name.lastIndexOf(".")) : "";
      if (type === "enum") {
        const symbols = o["symbols"];
        if (!Array.isArray(symbols) || symbols.some((x) => typeof x !== "string" || !NAME.test(x)))
          throw new Error(`Avro enum ${name} needs valid symbols`);
        if (new Set(symbols).size !== symbols.length)
          throw new Error(`Avro enum ${name} repeats a symbol`);
        const d = o["default"];
        if (d !== undefined && !symbols.includes(d))
          throw new Error(`Avro enum ${name} default is not a symbol`);
        const node: AvroNode = {
          t: "enum",
          name,
          aliases: aliasesOf(inner),
          symbols: symbols as string[],
          ...(typeof d === "string" ? { default: d } : {}),
        };
        define(name, node);
        return node;
      }
      if (type === "fixed") {
        const size = o["size"];
        if (!Number.isInteger(size) || (size as number) < 0)
          throw new Error(`Avro fixed ${name} needs a size`);
        const node: AvroNode = {
          t: "fixed",
          name,
          aliases: aliasesOf(inner),
          size: size as number,
        };
        define(name, node);
        return node;
      }
      const node: AvroNode & { t: "record" } = {
        t: "record",
        name,
        aliases: aliasesOf(inner),
        fields: [],
      };
      define(name, node);
      if (!Array.isArray(o["fields"])) throw new Error(`Avro record ${name} needs fields`);
      const names = new Set<string>();
      for (const f of o["fields"] as Record<string, unknown>[]) {
        if (typeof f?.["name"] !== "string" || !NAME.test(f["name"]))
          throw new Error(`Avro record ${name} has an invalid field name`);
        if (names.has(f["name"])) throw new Error(`Avro record ${name} repeats ${f["name"]}`);
        names.add(f["name"]);
        const fieldType = walk(f["type"], inner);
        const hasDefault = "default" in f;
        if (hasDefault) defaultValue(fieldType, f["default"], `${name}.${f["name"]}`);
        node.fields.push({
          name: f["name"],
          type: fieldType,
          aliases: Array.isArray(f["aliases"]) ? (f["aliases"] as string[]) : [],
          hasDefault,
          ...(hasDefault ? { default: f["default"] } : {}),
        });
      }
      return node;
    }
    if (type === "array") return { t: "array", items: walk(o["items"], ns) };
    if (type === "map") return { t: "map", values: walk(o["values"], ns) };
    return walk(type, ns);
  };
  return walk(json, "");
}

/** Converts a JSON field default into the runtime value for `node`, or throws if it does not fit. */
export function defaultValue(node: AvroNode, json: unknown, path = "default"): unknown {
  const bad = () => new Error(`Default for ${path} does not match its Avro type`);
  switch (node.t) {
    case "null":
      if (json !== null) throw bad();
      return null;
    case "boolean":
      if (typeof json !== "boolean") throw bad();
      return json;
    case "int":
    case "long":
      if (!Number.isInteger(json)) throw bad();
      return json;
    case "float":
    case "double":
      if (typeof json !== "number") throw bad();
      return json;
    case "string":
      if (typeof json !== "string") throw bad();
      return json;
    case "bytes":
    case "fixed": {
      if (typeof json !== "string") throw bad();
      const bytes = Uint8Array.from(json, (c) => c.charCodeAt(0) & 0xff);
      if (node.t === "fixed" && bytes.length !== node.size) throw bad();
      return bytes;
    }
    case "enum":
      if (typeof json !== "string" || !node.symbols.includes(json)) throw bad();
      return json;
    case "array":
      if (!Array.isArray(json)) throw bad();
      return json.map((v, i) => defaultValue(node.items, v, `${path}[${i}]`));
    case "map":
      if (!json || typeof json !== "object" || Array.isArray(json)) throw bad();
      return Object.fromEntries(
        Object.entries(json).map(([k, v]) => [k, defaultValue(node.values, v, `${path}.${k}`)]),
      );
    case "union":
      // The spec ties a union default to its first branch.
      return defaultValue(node.branches[0]!, json, path);
    case "record": {
      if (!json || typeof json !== "object" || Array.isArray(json)) throw bad();
      const o = json as Record<string, unknown>;
      return Object.fromEntries(
        node.fields.map((f) => {
          if (f.name in o) return [f.name, defaultValue(f.type, o[f.name], `${path}.${f.name}`)];
          if (f.hasDefault) return [f.name, defaultValue(f.type, f.default, `${path}.${f.name}`)];
          throw bad();
        }),
      );
    }
  }
}

/** How to turn a value written with one schema into the reader's shape. */
export type ResolutionPlan =
  | { k: "prim"; from: Primitive; to: Primitive }
  | {
      k: "record";
      // In writer field order: read into a reader field, or decode with the writer type and drop.
      steps: ({ field: string; plan: ResolutionPlan } | { skip: AvroNode })[];
      defaults: { field: string; value: unknown }[];
      order: string[];
    }
  | { k: "enum"; symbols: string[]; map: (string | null)[] }
  | { k: "fixed"; size: number }
  | { k: "array"; items: ResolutionPlan }
  | { k: "map"; values: ResolutionPlan }
  | { k: "writer-union"; branches: (ResolutionPlan | { error: string })[] };

export type SchemaIssue = { path: string; message: string };

const PROMOTIONS: Record<string, Primitive[]> = {
  int: ["long", "float", "double"],
  long: ["float", "double"],
  float: ["double"],
  string: ["bytes"],
  bytes: ["string"],
};
const namesOf = (n: { name: string; aliases: string[] }) => [
  short(n.name),
  ...n.aliases.map(short),
];
const describe = (n: AvroNode) => ("name" in n ? `${n.t} ${n.name}` : n.t);

/**
 * Plans reading data written with `writer` using `reader`. Every rule the spec makes an error is
 * returned as an issue; an empty list means every value the writer can produce is readable.
 */
export function resolveSchemas(
  writer: AvroNode,
  reader: AvroNode,
): { plan: ResolutionPlan; issues: SchemaIssue[] } {
  const memo = new Map<string, ResolutionPlan>();
  const issues: SchemaIssue[] = [];
  const plan = resolve(writer, reader, "$", issues, memo);
  return { plan, issues };
}

function resolve(
  w: AvroNode,
  r: AvroNode,
  path: string,
  issues: SchemaIssue[],
  memo: Map<string, ResolutionPlan>,
): ResolutionPlan {
  if (w.t === "union") {
    return {
      k: "writer-union",
      branches: w.branches.map((branch, i) => {
        const local: SchemaIssue[] = [];
        const p = resolve(branch, r, `${path}<${describe(branch)}>`, local, memo);
        issues.push(...local);
        return local.length ? { error: local[0]!.message } : p;
      }),
    };
  }
  if (r.t === "union") {
    // The first reader branch that accepts the writer type wins (spec).
    for (const branch of r.branches) {
      const local: SchemaIssue[] = [];
      resolve(w, branch, path, local, new Map(memo));
      if (!local.length) return resolve(w, branch, path, issues, memo);
    }
    issues.push({ path, message: `no branch of the reader union accepts ${describe(w)}` });
    return { k: "prim", from: "null", to: "null" };
  }
  const fail = (message: string): ResolutionPlan => {
    issues.push({ path, message });
    return { k: "prim", from: "null", to: "null" };
  };
  if (PRIMITIVES.has(w.t) && PRIMITIVES.has(r.t)) {
    const from = w.t as Primitive,
      to = r.t as Primitive;
    if (from === to || PROMOTIONS[from]?.includes(to)) return { k: "prim", from, to };
    return fail(`${from} cannot be read as ${to}`);
  }
  if (w.t !== r.t) return fail(`${describe(w)} cannot be read as ${describe(r)}`);
  switch (w.t) {
    case "array":
      return {
        k: "array",
        items: resolve(w.items, (r as typeof w).items, `${path}[]`, issues, memo),
      };
    case "map":
      return {
        k: "map",
        values: resolve(w.values, (r as typeof w).values, `${path}{}`, issues, memo),
      };
    case "fixed": {
      const rf = r as typeof w;
      if (!namesOf(rf).includes(short(w.name)))
        return fail(`fixed ${w.name} cannot be read as fixed ${rf.name}`);
      if (rf.size !== w.size) return fail(`fixed size changed from ${w.size} to ${rf.size}`);
      return { k: "fixed", size: w.size };
    }
    case "enum": {
      const re = r as typeof w;
      if (!namesOf(re).includes(short(w.name)))
        return fail(`enum ${w.name} cannot be read as enum ${re.name}`);
      const map = w.symbols.map((s) => (re.symbols.includes(s) ? s : (re.default ?? null)));
      w.symbols.forEach((s, i) => {
        if (map[i] === null)
          issues.push({
            path,
            message: `enum symbol ${s} is missing and the reader has no default`,
          });
      });
      return { k: "enum", symbols: w.symbols, map };
    }
    case "record": {
      const rr = r as typeof w;
      if (!namesOf(rr).includes(short(w.name)))
        return fail(`record ${w.name} cannot be read as record ${rr.name}`);
      const key = `${w.name}|${rr.name}`;
      const cached = memo.get(key);
      if (cached) return cached; // recursive types: the plan object is filled in below
      const plan: ResolutionPlan & { k: "record" } = {
        k: "record",
        steps: [],
        defaults: [],
        order: rr.fields.map((f) => f.name),
      };
      memo.set(key, plan);
      const readerFor = (name: string) =>
        rr.fields.find((f) => f.name === name || f.aliases.includes(name));
      const used = new Set<string>();
      for (const wf of w.fields) {
        const rf = readerFor(wf.name);
        if (!rf || used.has(rf.name)) {
          plan.steps.push({ skip: wf.type });
          continue;
        }
        used.add(rf.name);
        plan.steps.push({
          field: rf.name,
          plan: resolve(wf.type, rf.type, `${path}.${rf.name}`, issues, memo),
        });
      }
      for (const rf of rr.fields) {
        if (used.has(rf.name)) continue;
        if (!rf.hasDefault) {
          issues.push({
            path: `${path}.${rf.name}`,
            message: `field ${rf.name} is new and has no default`,
          });
          continue;
        }
        plan.defaults.push({ field: rf.name, value: defaultValue(rf.type, rf.default) });
      }
      return plan;
    }
  }
  return fail(`${describe(w)} cannot be read as ${describe(r)}`);
}

/** Converts a primitive value decoded as `from` into `to` (Avro promotions). */
export function promote(value: unknown, from: Primitive, to: Primitive): unknown {
  if (from === to) return value;
  if (from === "string" && to === "bytes") return new TextEncoder().encode(value as string);
  if (from === "bytes" && to === "string") return new TextDecoder().decode(value as Uint8Array);
  return Number(value);
}

export type CompatibilityMode =
  | "NONE"
  | "BACKWARD"
  | "BACKWARD_TRANSITIVE"
  | "FORWARD"
  | "FORWARD_TRANSITIVE"
  | "FULL"
  | "FULL_TRANSITIVE";
export const COMPATIBILITY_MODES: CompatibilityMode[] = [
  "BACKWARD",
  "BACKWARD_TRANSITIVE",
  "FORWARD",
  "FORWARD_TRANSITIVE",
  "FULL",
  "FULL_TRANSITIVE",
  "NONE",
];

/**
 * Checks a candidate schema against earlier versions (oldest first), with the same meaning as a
 * schema registry: BACKWARD = the candidate can read data written with the latest version,
 * FORWARD = the latest version can read the candidate's data, FULL = both; _TRANSITIVE checks
 * every earlier version instead of only the latest.
 */
export function checkCompatibility(
  candidate: AvroNode,
  history: AvroNode[],
  mode: CompatibilityMode,
): { compatible: boolean; issues: (SchemaIssue & { version: number; direction: string })[] } {
  if (mode === "NONE" || !history.length) return { compatible: true, issues: [] };
  const transitive = mode.endsWith("_TRANSITIVE");
  const base = mode.replace("_TRANSITIVE", "");
  const versions = history
    .map((schema, i) => ({ schema, version: i + 1 }))
    .slice(transitive ? 0 : -1);
  const issues: (SchemaIssue & { version: number; direction: string })[] = [];
  for (const { schema, version } of versions) {
    if (base === "BACKWARD" || base === "FULL")
      for (const issue of resolveSchemas(schema, candidate).issues)
        issues.push({ ...issue, version, direction: "new schema reading old data" });
    if (base === "FORWARD" || base === "FULL")
      for (const issue of resolveSchemas(candidate, schema).issues)
        issues.push({ ...issue, version, direction: "old schema reading new data" });
  }
  return { compatible: issues.length === 0, issues };
}

/** Flattens a record schema to the column list the schema diff view uses. */
export function avroColumns(node: AvroNode): { name: string; type: string; nullable: boolean }[] {
  const typeName = (n: AvroNode): string =>
    n.t === "union"
      ? n.branches
          .filter((b) => b.t !== "null")
          .map(typeName)
          .join("|")
      : n.t === "array"
        ? `array<${typeName(n.items)}>`
        : n.t === "map"
          ? `map<${typeName(n.values)}>`
          : "name" in n
            ? short(n.name)
            : n.t;
  if (node.t !== "record") return [{ name: "value", type: typeName(node), nullable: false }];
  return node.fields.map((f) => ({
    name: f.name,
    type: typeName(f.type),
    nullable:
      f.type.t === "null" || (f.type.t === "union" && f.type.branches.some((b) => b.t === "null")),
  }));
}
