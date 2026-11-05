import type { Row } from "@/lib/rows";

export function ResultTable({
  columns,
  rows,
  masked = [],
}: {
  columns: string[];
  rows: Row[];
  masked?: string[];
}) {
  if (!rows.length) return <div className="p-3 text-sm text-muted-foreground">No rows.</div>;
  return (
    <div className="max-h-72 overflow-auto border">
      <table className="w-full font-mono text-xs">
        <thead className="sticky top-0 bg-muted">
          <tr>
            {columns.map((c) => (
              <th key={c} className="px-2 py-1 text-left font-semibold">
                {c}
                {masked.includes(c) && <span className="ml-1 text-warning">masked</span>}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.slice(0, 200).map((r, i) => (
            <tr key={i} className="border-t">
              {columns.map((c) => (
                <td key={c} className="px-2 py-1">
                  {String(r[c] ?? "")}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
