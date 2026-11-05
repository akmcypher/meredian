import { z } from "zod";
import {
  demoStats,
  planPartitions,
  type Strategy,
  type ColumnStats,
  type PlannedPartition,
} from "./partitioning";

const column = z.string().regex(/^[A-Za-z_][A-Za-z0-9_]{0,62}$/);
const count = z.number().int().min(1).max(2000);
const strategySchema = z.discriminatedUnion("kind", [
  z.object({ kind: z.literal("range"), column, n: count }),
  z.object({ kind: z.literal("hash"), column, n: count }),
  z.object({ kind: z.literal("modulo"), column, n: count }),
  z.object({ kind: z.literal("date"), column, grain: z.enum(["day", "month", "quarter"]) }),
  z.object({ kind: z.literal("key"), column, values: z.array(z.string().max(500)).max(1000) }),
]);

/** Never persist caller-supplied predicate fragments. Custom SQL requires a future parsed/authorized grammar. */
export function authoritativePartitions(
  plan: Record<string, unknown>,
  total: number,
  demo: boolean,
  statsInput?: unknown,
): PlannedPartition[] {
  const strategy = strategySchema.parse(plan["strategy"]) as Strategy;
  let stats: ColumnStats;
  if (demo && "column" in strategy) stats = demoStats(strategy.column, total);
  else {
    stats = z
      .object({
        min: z.number().finite().nullable(),
        max: z.number().finite().nullable(),
        nullFraction: z.number().min(0).max(1),
        distinct: z.number().int().nonnegative(),
        indexed: z.boolean(),
        minDate: z.string().nullable().optional(),
        maxDate: z.string().nullable().optional(),
      })
      .parse(statsInput) as ColumnStats;
  }
  const parts = planPartitions(strategy, stats);
  if (!parts.length) throw new Error("No valid partitions");
  return parts.map((p) => ({ ...p, estRows: Math.round(total / parts.length) }));
}
