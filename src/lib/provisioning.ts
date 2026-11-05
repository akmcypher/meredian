/** Connector catalog: what each external system needs and how to stand it up. */
export type FieldDef = { key: string; label: string; placeholder?: string; secret?: boolean };
export type ConnectorKind = {
  kind: string;
  label: string;
  group: "compute" | "bi" | "storage" | "semantic" | "warehouse" | "database";
  desc: string;
  fields: FieldDef[];
  template: { file: string; body: string };
  /** Credentials come from the platform's account connection (set up once by an admin), not this form. */
  managed?: boolean;
  /** Can run SQL cells / jobs. */
  sql?: boolean;
};

const LIVY_FIELDS: FieldDef[] = [
  { key: "url", label: "Livy endpoint", placeholder: "http://emr-master:8998" },
  { key: "session_kind", label: "Session kind", placeholder: "pyspark" },
  { key: "username", label: "Username (optional)" },
  { key: "password", label: "Password (optional)", secret: true },
];

export const CONNECTORS: ConnectorKind[] = [
  {
    kind: "emr",
    label: "AWS EMR (via Livy)",
    group: "compute",
    sql: true,
    desc: "Runs notebook cells and jobs on an EMR cluster through Apache Livy on the master node.",
    fields: LIVY_FIELDS,
    template: {
      file: "emr.tf",
      body: `provider "aws" { region = var.region }
variable "region"   { default = "us-east-1" }
variable "subnet_id" {}
variable "key_name" {}

resource "aws_emr_cluster" "meridian" {
  name          = "meridian-spark"
  release_label = "emr-7.2.0"
  applications  = ["Spark", "Livy", "Hadoop", "JupyterEnterpriseGateway"]
  service_role  = "EMR_DefaultRole"

  ec2_attributes {
    subnet_id        = var.subnet_id
    key_name         = var.key_name
    instance_profile = "EMR_EC2_DefaultRole"
  }
  master_instance_group { instance_type = "m5.xlarge" }
  core_instance_group {
    instance_type  = "m5.xlarge"
    instance_count = 2
  }
  configurations_json = jsonencode([
    { Classification = "spark-defaults", Properties = {
        "spark.sql.catalog.glue"              = "org.apache.iceberg.spark.SparkCatalog"
        "spark.sql.catalog.glue.catalog-impl" = "org.apache.iceberg.aws.glue.GlueCatalog"
        "spark.sql.extensions"                = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    } },
    { Classification = "iceberg-defaults", Properties = { "iceberg.enabled" = "true" } }
  ])
}

# Open 8998 (Livy) only to the IP range your Meridian server calls from.
output "livy_url" { value = "http://\${aws_emr_cluster.meridian.master_public_dns}:8998" }`,
    },
  },
  {
    kind: "livy",
    label: "Self-hosted Spark + Livy",
    group: "compute",
    sql: true,
    desc: "Any Spark cluster (standalone, YARN, Kubernetes) exposed through Apache Livy.",
    fields: LIVY_FIELDS,
    template: {
      file: "docker-compose.spark.yml",
      body: `services:
  spark-master:
    image: bitnami/spark:3.5
    environment: [SPARK_MODE=master]
    ports: ["7077:7077", "8080:8080"]
  spark-worker:
    image: bitnami/spark:3.5
    environment: [SPARK_MODE=worker, SPARK_MASTER_URL=spark://spark-master:7077, SPARK_WORKER_CORES=2, SPARK_WORKER_MEMORY=2g]
    deploy: { replicas: 2 }
  livy:
    image: apache/livy:0.8.0-incubating
    environment:
      - SPARK_MASTER=spark://spark-master:7077
      - LIVY_SPARK_DEPLOY_MODE=client
    ports: ["8998:8998"]
    depends_on: [spark-master]`,
    },
  },
  {
    kind: "databricks",
    label: "Databricks SQL",
    group: "warehouse",
    sql: true,
    desc: "Runs SQL on a Databricks SQL warehouse via the Statement Execution API.",
    fields: [
      { key: "url", label: "Workspace URL", placeholder: "https://dbc-xxxx.cloud.databricks.com" },
      { key: "warehouse_id", label: "SQL warehouse ID" },
      { key: "token", label: "Personal access token", secret: true },
    ],
    template: {
      file: "databricks.tf",
      body: `resource "databricks_sql_endpoint" "meridian" {
  name             = "meridian"
  cluster_size     = "2X-Small"
  max_num_clusters = 1
  auto_stop_mins   = 10
}
output "warehouse_id" { value = databricks_sql_endpoint.meridian.id }`,
    },
  },
  {
    kind: "superset",
    label: "Apache Superset",
    group: "bi",
    desc: "Embeds Superset dashboards using guest tokens issued by the server.",
    fields: [
      { key: "url", label: "Superset URL", placeholder: "https://superset.example.com" },
      { key: "dashboard_ids", label: "Embedded dashboard UUIDs (comma separated)" },
      {
        key: "rls_clause",
        label: "Row-level security clause (required; {user_id} and {org_id} are filled in)",
        placeholder: "organization_id = '{org_id}'",
      },
      { key: "username", label: "Service account username" },
      { key: "password", label: "Service account password", secret: true },
    ],
    template: {
      file: "superset_config.py",
      body: `# docker run -p 8088:8088 -v ./superset_config.py:/app/pythonpath/superset_config.py apache/superset:4.0.2
FEATURE_FLAGS = {"EMBEDDED_SUPERSET": True}
GUEST_ROLE_NAME = "Gamma"
GUEST_TOKEN_JWT_EXP_SECONDS = 300
TALISMAN_ENABLED = False          # or set frame-ancestors to your Meridian domain
ENABLE_CORS = True
CORS_OPTIONS = {"origins": ["https://YOUR-MERIDIAN-DOMAIN"], "supports_credentials": True}
# Then: Dashboard → ... → Embed dashboard → add your domain → copy the UUID.`,
    },
  },
  {
    kind: "cube",
    label: "Cube (semantic layer)",
    group: "semantic",
    desc: "Sends metric-layer queries to a running Cube API instead of the built-in engine.",
    fields: [
      { key: "url", label: "Cube API URL", placeholder: "https://cube.example.com/cubejs-api" },
      { key: "token", label: "API token (JWT)", secret: true },
    ],
    template: {
      file: "docker-compose.cube.yml",
      body: `services:
  cube:
    image: cubejs/cube:latest
    ports: ["4000:4000"]
    environment:
      - CUBEJS_DB_TYPE=postgres
      - CUBEJS_DB_HOST=warehouse
      - CUBEJS_API_SECRET=change-me
      - CUBEJS_DEV_MODE=false
    volumes: ["./model:/cube/conf/model"]   # paste the YAML from "Metric layer → Copy Cube model"`,
    },
  },
  {
    kind: "iceberg_rest",
    label: "Iceberg REST catalog",
    group: "storage",
    desc: "Reads table metadata and snapshots from an Iceberg REST catalog (Polaris, Tabular, Nessie, Glue REST).",
    fields: [
      { key: "url", label: "Catalog URI", placeholder: "https://catalog.example.com/api/catalog" },
      { key: "warehouse", label: "Warehouse", placeholder: "s3://lake/warehouse" },
      { key: "token", label: "Bearer token", secret: true },
    ],
    template: {
      file: "docker-compose.iceberg.yml",
      body: `services:
  minio:
    image: minio/minio
    command: server /data --console-address ":9001"
    ports: ["9000:9000", "9001:9001"]
  iceberg-rest:
    image: tabulario/iceberg-rest
    ports: ["8181:8181"]
    environment:
      - CATALOG_WAREHOUSE=s3://warehouse/
      - CATALOG_IO__IMPL=org.apache.iceberg.aws.s3.S3FileIO
      - CATALOG_S3_ENDPOINT=http://minio:9000`,
    },
  },
  {
    kind: "s3",
    label: "Amazon S3 data lake",
    group: "storage",
    managed: true,
    desc: "Browse a bucket, preview Parquet / Avro / CSV / JSON files and load them as lakehouse tables. Uses the company AWS account connected by an admin.",
    fields: [{ key: "prefix", label: "Default folder (optional)", placeholder: "raw/sales/" }],
    template: {
      file: "s3-readonly-policy.json",
      body: `{
  "Version": "2012-10-17",
  "Statement": [
    { "Effect": "Allow", "Action": ["s3:ListBucket"], "Resource": "arn:aws:s3:::YOUR-BUCKET" },
    { "Effect": "Allow", "Action": ["s3:GetObject"], "Resource": "arn:aws:s3:::YOUR-BUCKET/*" }
  ]
}
// Attach to a dedicated IAM user, then connect it on this page.`,
    },
  },
  {
    kind: "redshift",
    label: "Amazon Redshift",
    group: "warehouse",
    managed: true,
    sql: true,
    desc: "Runs notebook SQL and scheduled jobs on Redshift (Serverless or provisioned) through the Redshift Data API.",
    fields: [],
    template: {
      file: "redshift-readonly.sql",
      body: `CREATE USER meridian_ro PASSWORD DISABLE;
GRANT USAGE ON SCHEMA analytics TO meridian_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO meridian_ro;
-- IAM: redshift-data:ExecuteStatement, DescribeStatement, GetStatementResult
--      + redshift-serverless:GetCredentials (serverless) or redshift:GetClusterCredentials`,
    },
  },
  {
    kind: "athena",
    label: "Amazon Athena",
    group: "warehouse",
    managed: true,
    sql: true,
    desc: "Queries Parquet/Avro/ORC/Iceberg tables in S3 through the Glue catalog with Athena SQL.",
    fields: [
      { key: "database", label: "Glue database", placeholder: "lake_db" },
      { key: "workgroup", label: "Workgroup (optional)", placeholder: "primary" },
      {
        key: "output_location",
        label: "Results location (optional)",
        placeholder: "s3://bucket/athena-results/",
      },
    ],
    template: {
      file: "athena-table.sql",
      body: `CREATE EXTERNAL TABLE lake_db.orders (
  order_id string, customer_id string, amount double, ts timestamp
)
STORED AS PARQUET              -- or AVRO / ORC
LOCATION 's3://YOUR-BUCKET/orders/';
-- Iceberg: CREATE TABLE ... TBLPROPERTIES ('table_type'='ICEBERG')`,
    },
  },
  {
    kind: "snowflake",
    label: "Snowflake",
    group: "warehouse",
    managed: true,
    sql: true,
    desc: "Runs SQL on a Snowflake warehouse via the SQL REST API.",
    fields: [
      { key: "warehouse", label: "Warehouse", placeholder: "COMPUTE_WH" },
      { key: "database", label: "Database (optional)" },
      { key: "schema", label: "Schema (optional)" },
    ],
    template: {
      file: "snowflake-role.sql",
      body: `CREATE ROLE meridian_ro;
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE meridian_ro;
GRANT USAGE ON DATABASE ANALYTICS TO ROLE meridian_ro;
GRANT SELECT ON ALL TABLES IN DATABASE ANALYTICS TO ROLE meridian_ro;`,
    },
  },
  {
    kind: "bigquery",
    label: "Google BigQuery",
    group: "warehouse",
    managed: true,
    sql: true,
    desc: "Runs standard SQL in BigQuery. Every query is capped at 1 GB billed by default.",
    fields: [
      { key: "project_id", label: "Billing project ID", placeholder: "my-gcp-project" },
      { key: "max_bytes", label: "Max bytes billed per query", placeholder: "1073741824" },
    ],
    template: {
      file: "bigquery-roles.txt",
      body: `Grant the connected account:
  roles/bigquery.jobUser       on the billing project
  roles/bigquery.dataViewer    on the datasets you want to read`,
    },
  },
  {
    kind: "postgres",
    label: "PostgreSQL",
    group: "database",
    desc: "Private databases are read by a small worker you run next to them, which pushes rows to Meridian. Set it up on the Ingestion page.",
    fields: [],
    template: {
      file: "README.txt",
      body: "Open Ingestion → Database source → Postgres to get the worker script and a push token.",
    },
  },
  {
    kind: "mysql",
    label: "MySQL",
    group: "database",
    desc: "Same as Postgres: a worker you run next to the database pushes full or incremental loads. Set it up on the Ingestion page.",
    fields: [],
    template: {
      file: "README.txt",
      body: "Open Ingestion → Database source → MySQL to get the worker script and a push token.",
    },
  },
];

export const connectorOf = (kind: string) => CONNECTORS.find((c) => c.kind === kind);

/** Split a form into the public config and the secret part (secrets never go to the browser again). */
export function splitConfig(kind: string, values: Record<string, string>) {
  const def = connectorOf(kind);
  const config: Record<string, string> = {},
    secrets: Record<string, string> = {};
  for (const f of def?.fields ?? []) {
    const v = (values[f.key] ?? "").trim();
    if (!v) continue;
    (f.secret ? secrets : config)[f.key] = v;
  }
  return { config, secrets };
}

/** Kinds a notebook cell or job can run on. */
export const RUNNABLE_KINDS = CONNECTORS.filter((c) => c.sql).map((c) => c.kind);
/** Warehouses that only take SQL (no PySpark). */
export const SQL_ONLY_KINDS = ["databricks", "redshift", "athena", "snowflake", "bigquery"];
