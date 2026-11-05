/** Row shapes shared by every product: a row, a named set of tables, and a query result. */
export type Row = Record<string, unknown>;
export type Warehouse = Record<string, Row[]>;
export type QueryResult = { columns: string[]; rows: Row[]; ms: number };
