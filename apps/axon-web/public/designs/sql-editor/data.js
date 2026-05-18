// Axon-native data model — Delta Lake catalog, snapshots, capabilities, fallback reasons.
// Mirrors the shapes from crates/query-contract/src/lib.rs.

// ─── Capability matrix ──────────────────────────────────────────────────────
// CapabilityKey × CapabilityState, per execution target.
const CAPABILITIES = {
  ChangeDataFeed: {
    browser: 'native_only',
    native: 'supported',
    label: 'Change Data Feed',
    desc: 'Read CDF version range',
  },
  ColumnMapping: {
    browser: 'supported',
    native: 'supported',
    label: 'Column Mapping',
    desc: 'Delta column-mapping id/name modes',
  },
  DeletionVectors: {
    browser: 'native_only',
    native: 'supported',
    label: 'Deletion Vectors',
    desc: 'DV-encoded row deletions',
  },
  MultiPartitionExecution: {
    browser: 'experimental',
    native: 'supported',
    label: 'Multi-partition Scan',
    desc: 'Execute across >1 partition concurrently',
  },
  ProxyAccess: {
    browser: 'supported',
    native: 'supported',
    label: 'Proxy Access',
    desc: 'Route reads through control plane',
  },
  RangeReads: {
    browser: 'supported',
    native: 'supported',
    label: 'HTTP Range Reads',
    desc: 'Validated byte-range object reads',
  },
  SignedUrlAccess: {
    browser: 'supported',
    native: 'native_only',
    label: 'Signed URL Access',
    desc: 'Read via pre-signed object URLs',
  },
  TimeTravel: {
    browser: 'supported',
    native: 'supported',
    label: 'Time Travel',
    desc: 'Pin snapshot_version or timestamp',
  },
  TimestampNtz: {
    browser: 'experimental',
    native: 'supported',
    label: 'Timestamp NTZ',
    desc: 'TIMESTAMP without time zone',
  },
  UnknownProtocolFeatures: {
    browser: 'unsupported',
    native: 'experimental',
    label: 'Unknown Protocol',
    desc: 'Fall back when protocol unknown',
  },
};
const CAPABILITY_ORDER = [
  'RangeReads',
  'SignedUrlAccess',
  'TimeTravel',
  'ColumnMapping',
  'TimestampNtz',
  'MultiPartitionExecution',
  'ChangeDataFeed',
  'DeletionVectors',
  'ProxyAccess',
  'UnknownProtocolFeatures',
];

const FALLBACK_REASONS = {
  access_denied: { label: 'Access denied', kind: 'danger' },
  browser_runtime_constraint: { label: 'Browser runtime constraint', kind: 'warning' },
  native_required: { label: 'Native required', kind: 'info' },
  network_failure: { label: 'Network failure', kind: 'danger' },
  range_read_unavailable: { label: 'Range reads unavailable', kind: 'warning' },
  security_policy: { label: 'Security policy', kind: 'danger' },
  signed_url_expired: { label: 'Signed URL expired', kind: 'warning' },
  capability_gate: { label: 'Capability gate', kind: 'info' },
};

// ─── Catalog of Delta tables ───────────────────────────────────────────────
const CATALOG = {
  name: 'axon-prod',
  region: 'us-central1',
  storage: 'gs://axon-prod-fixtures',
  tables: [
    {
      name: 'events',
      uri: 'gs://axon-prod-fixtures/events',
      kind: 'delta',
      snapshot: 142,
      created: '2025-09-04',
      last_commit: '2026-05-16 12:18:09Z',
      size_bytes: 184_120_400_281,
      row_count: 9_248_192_310,
      file_count: 8421,
      row_group_count: 41_882,
      partition_columns: [
        { name: 'day', type: 'date', pruning: 'stats' },
        { name: 'country', type: 'string', pruning: 'stats' },
      ],
      protocol: {
        minReaderVersion: 3,
        minWriterVersion: 7,
        features: ['columnMapping', 'timestampNtz', 'deletionVectors'],
      },
      columns: [
        { name: 'event_id', type: 'uuid', pk: true },
        { name: 'user_id', type: 'long' },
        { name: 'session_id', type: 'uuid' },
        { name: 'name', type: 'string' },
        { name: 'properties', type: 'struct' },
        { name: 'occurred_at', type: 'timestamp' },
        { name: 'received_at', type: 'timestamp_ntz' },
        { name: 'day', type: 'date', part: true },
        { name: 'country', type: 'string', part: true },
      ],
    },
    {
      name: 'orders',
      uri: 'gs://axon-prod-fixtures/orders',
      kind: 'delta',
      snapshot: 318,
      created: '2024-01-12',
      last_commit: '2026-05-16 14:08:21Z',
      size_bytes: 38_220_440_112,
      row_count: 1_284_932,
      file_count: 412,
      row_group_count: 2_106,
      partition_columns: [{ name: 'placed_month', type: 'string', pruning: 'stats' }],
      protocol: { minReaderVersion: 2, minWriterVersion: 5, features: ['columnMapping'] },
      columns: [
        { name: 'id', type: 'long', pk: true },
        { name: 'customer_id', type: 'long', fk: 'customers.id' },
        { name: 'status', type: 'string' },
        { name: 'subtotal_cents', type: 'integer' },
        { name: 'tax_cents', type: 'integer' },
        { name: 'total_cents', type: 'integer' },
        { name: 'currency', type: 'string' },
        { name: 'placed_at', type: 'timestamp' },
        { name: 'shipped_at', type: 'timestamp', nullable: true },
        { name: 'metadata', type: 'struct', nullable: true },
        { name: 'placed_month', type: 'string', part: true },
      ],
    },
    {
      name: 'customers',
      uri: 'gs://axon-prod-fixtures/customers',
      kind: 'delta',
      snapshot: 87,
      created: '2024-01-12',
      last_commit: '2026-05-16 11:02:47Z',
      size_bytes: 1_120_004_881,
      row_count: 184_221,
      file_count: 24,
      row_group_count: 96,
      partition_columns: [],
      protocol: { minReaderVersion: 2, minWriterVersion: 5, features: ['columnMapping'] },
      columns: [
        { name: 'id', type: 'long', pk: true },
        { name: 'email', type: 'string' },
        { name: 'full_name', type: 'string' },
        { name: 'country', type: 'string' },
        { name: 'plan', type: 'string' },
        { name: 'created_at', type: 'timestamp' },
        { name: 'deleted_at', type: 'timestamp', nullable: true },
      ],
    },
    {
      name: 'order_items',
      uri: 'gs://axon-prod-fixtures/order_items',
      kind: 'delta',
      snapshot: 318,
      created: '2024-01-12',
      last_commit: '2026-05-16 14:08:21Z',
      size_bytes: 14_882_300_120,
      row_count: 4_112_903,
      file_count: 184,
      row_group_count: 894,
      partition_columns: [{ name: 'placed_month', type: 'string' }],
      protocol: { minReaderVersion: 2, minWriterVersion: 5, features: ['columnMapping'] },
      columns: [
        { name: 'id', type: 'long', pk: true },
        { name: 'order_id', type: 'long', fk: 'orders.id' },
        { name: 'sku', type: 'string' },
        { name: 'quantity', type: 'integer' },
        { name: 'unit_price_cents', type: 'integer' },
        { name: 'placed_month', type: 'string', part: true },
      ],
    },
    {
      name: 'products',
      uri: 'gs://axon-prod-fixtures/products',
      kind: 'delta',
      snapshot: 41,
      created: '2024-01-12',
      last_commit: '2026-05-12 22:31:08Z',
      size_bytes: 14_882_991,
      row_count: 8_294,
      file_count: 4,
      row_group_count: 12,
      partition_columns: [],
      protocol: { minReaderVersion: 2, minWriterVersion: 5, features: [] },
      columns: [
        { name: 'sku', type: 'string', pk: true },
        { name: 'title', type: 'string' },
        { name: 'category', type: 'string' },
        { name: 'price_cents', type: 'integer' },
        { name: 'inventory', type: 'integer' },
      ],
    },
    {
      name: 'shipments',
      uri: 'gs://axon-prod-fixtures/shipments',
      kind: 'delta',
      snapshot: 204,
      created: '2024-02-04',
      last_commit: '2026-05-16 13:44:11Z',
      size_bytes: 8_281_004_991,
      row_count: 1_102_441,
      file_count: 122,
      row_group_count: 488,
      partition_columns: [],
      protocol: { minReaderVersion: 3, minWriterVersion: 7, features: ['deletionVectors'] },
      columns: [
        { name: 'id', type: 'long', pk: true },
        { name: 'order_id', type: 'long', fk: 'orders.id' },
        { name: 'carrier', type: 'string' },
        { name: 'tracking', type: 'string' },
        { name: 'shipped_at', type: 'timestamp' },
        { name: 'delivered_at', type: 'timestamp', nullable: true },
      ],
    },
    {
      name: 'sessions',
      uri: 'gs://axon-prod-fixtures/sessions',
      kind: 'delta',
      snapshot: 92,
      created: '2025-01-08',
      last_commit: '2026-05-16 13:58:02Z',
      size_bytes: 19_220_004_122,
      row_count: 3_294_112,
      file_count: 802,
      row_group_count: 3_104,
      partition_columns: [{ name: 'day', type: 'date' }],
      protocol: {
        minReaderVersion: 3,
        minWriterVersion: 7,
        features: ['columnMapping', 'timestampNtz'],
      },
      columns: [
        { name: 'session_id', type: 'uuid', pk: true },
        { name: 'user_id', type: 'long' },
        { name: 'started_at', type: 'timestamp' },
        { name: 'ended_at', type: 'timestamp' },
        { name: 'device', type: 'string' },
        { name: 'day', type: 'date', part: true },
      ],
    },
  ],
};

// ─── Sample queries ─────────────────────────────────────────────────────────
const SAMPLE_QUERY = `-- top customers by revenue, last 30 days
-- runs in browser (WASM) — partition + footer-stat pruning enabled
SELECT
  c.id,
  c.full_name,
  c.country,
  COUNT(o.id)                          AS orders,
  SUM(o.total_cents) / 100.0           AS revenue_usd,
  MAX(o.placed_at)                     AS last_order_at
FROM orders o
JOIN customers c ON c.id = o.customer_id
WHERE o.placed_at >= TIMESTAMP '2026-04-16 00:00:00'
  AND o.placed_month IN ('2026-04','2026-05')
  AND o.status = 'fulfilled'
GROUP BY c.id, c.full_name, c.country
HAVING SUM(o.total_cents) > 50000
ORDER BY revenue_usd DESC
LIMIT 50;`;

const SECONDARY_QUERY = `-- fulfilment funnel for Q2 (browser-safe)
WITH base AS (
  SELECT
    date_trunc('week', placed_at) AS week,
    status,
    COUNT(*) AS n
  FROM orders
  WHERE placed_month BETWEEN '2026-04' AND '2026-06'
  GROUP BY 1, 2
)
SELECT week,
       SUM(n) FILTER (WHERE status = 'placed')     AS placed,
       SUM(n) FILTER (WHERE status = 'fulfilled')  AS fulfilled,
       SUM(n) FILTER (WHERE status = 'cancelled')  AS cancelled
FROM base
GROUP BY week
ORDER BY week;`;

const THIRD_QUERY = `-- CDF — reads change_data_feed (NativeOnly capability)
SELECT _change_type, sku, title, inventory, _commit_version
FROM table_changes('products', 38, 41)
WHERE _change_type IN ('update_preimage','update_postimage')
ORDER BY _commit_version DESC;`;

// ─── Mock result for the primary query ─────────────────────────────────────
const RESULT_COLUMNS = [
  { name: 'id', type: 'long' },
  { name: 'full_name', type: 'string' },
  { name: 'country', type: 'string' },
  { name: 'orders', type: 'long' },
  { name: 'revenue_usd', type: 'double' },
  { name: 'last_order_at', type: 'timestamp' },
];

const RESULT_ROWS = [
  [10284, 'Mira Okafor', 'NG', 38, 18420.5, '2026-05-14 09:21:08+00'],
  [10412, 'Daniel Vasquez', 'MX', 27, 14210.1, '2026-05-15 17:02:44+00'],
  [10583, 'Yuki Tanaka', 'JP', 31, 12980.0, '2026-05-15 02:11:19+00'],
  [10629, 'Aria Lindqvist', 'SE', 22, 11920.75, '2026-05-13 11:46:01+00'],
  [10744, 'Omar Haddad', 'AE', 19, 10840.2, '2026-05-12 22:03:33+00'],
  [10812, 'Priya Raghunathan', 'IN', 41, 9982.4, '2026-05-15 14:18:50+00'],
  [10977, 'Lucas Moreau', 'FR', 17, 9420.0, '2026-05-14 06:42:11+00'],
  [11023, 'Sofia Bianchi', 'IT', 14, 8810.99, '2026-05-11 19:31:28+00'],
  [11108, 'Ethan Cole', 'US', 23, 8204.55, '2026-05-15 20:55:09+00'],
  [11240, 'Nadia Petrov', 'BG', 16, 7920.1, '2026-05-10 08:14:42+00'],
  [11317, 'Felix Becker', 'DE', 12, 7610.0, '2026-05-14 13:27:36+00'],
  [11422, 'Camila Reyes', 'CL', 18, 7220.65, '2026-05-13 23:09:54+00'],
  [11580, "Hannah O'Connor", 'IE', 11, 6840.2, '2026-05-12 04:01:17+00'],
  [11633, 'Kenji Mori', 'JP', 20, 6510.45, '2026-05-15 11:38:02+00'],
  [11804, 'Isabel Costa', 'PT', 9, 6210.0, '2026-05-11 16:48:21+00'],
  [11912, 'Marcus Bell', 'GB', 13, 5980.3, '2026-05-14 22:17:39+00'],
  [12085, 'Ada Nwosu', 'NG', 8, 5740.1, '2026-05-10 12:55:48+00'],
  [12217, 'Theo Lambert', 'BE', 15, 5510.0, '2026-05-13 07:23:14+00'],
  [12344, 'Selin Yıldız', 'TR', 10, 5320.85, '2026-05-15 08:09:27+00'],
  [12498, 'Rafael Pinto', 'BR', 22, 5180.4, '2026-05-14 18:51:03+00'],
];

// ─── Query metrics (matches QueryMetricsSummary) ────────────────────────────
const METRICS = {
  bytes_fetched: 3_842_104, // bytes scanned
  duration_ms: 312,
  files_touched: 12,
  files_skipped: 398,
  row_groups_touched: 34,
  row_groups_skipped: 1_842,
  footer_reads: 12,
  rows_emitted: 20,
  snapshot_bootstrap_duration_ms: 38,
  access_mode: 'browser_safe_http',
  // breakdown
  phases: [
    { name: 'snapshot_bootstrap', ms: 38 },
    { name: 'plan', ms: 4 },
    { name: 'partition_prune', ms: 2 },
    { name: 'row_group_prune', ms: 14 },
    { name: 'scan', ms: 218 },
    { name: 'aggregate + sort', ms: 36 },
  ],
};

// ─── Files touched / skipped for the Plan view ─────────────────────────────
const PLAN_FILES = [
  {
    path: 'placed_month=2026-05/part-00012-c4e9c2.parquet',
    status: 'touched',
    size: 412_004,
    rows: 88_201,
    row_groups: 4,
    partitions: { placed_month: '2026-05' },
  },
  {
    path: 'placed_month=2026-05/part-00013-c4e9c2.parquet',
    status: 'touched',
    size: 398_882,
    rows: 84_104,
    row_groups: 4,
    partitions: { placed_month: '2026-05' },
  },
  {
    path: 'placed_month=2026-05/part-00014-c4e9c2.parquet',
    status: 'touched',
    size: 401_180,
    rows: 86_421,
    row_groups: 4,
    partitions: { placed_month: '2026-05' },
  },
  {
    path: 'placed_month=2026-05/part-00015-c4e9c2.parquet',
    status: 'touched',
    size: 412_440,
    rows: 88_882,
    row_groups: 4,
    partitions: { placed_month: '2026-05' },
  },
  {
    path: 'placed_month=2026-04/part-00009-a1b2c3.parquet',
    status: 'touched',
    size: 388_120,
    rows: 81_402,
    row_groups: 4,
    partitions: { placed_month: '2026-04' },
  },
  {
    path: 'placed_month=2026-04/part-00010-a1b2c3.parquet',
    status: 'touched',
    size: 394_280,
    rows: 82_104,
    row_groups: 4,
    partitions: { placed_month: '2026-04' },
  },
  {
    path: 'placed_month=2026-03/part-00007-7f8e2a.parquet',
    status: 'row_group_skipped',
    size: 412_004,
    rows: 88_201,
    row_groups: 4,
    partitions: { placed_month: '2026-03' },
    reason: 'row-group max(placed_at) < filter lo',
  },
  {
    path: 'placed_month=2026-02/part-00004-21bcd9.parquet',
    status: 'partition_skipped',
    size: 401_180,
    rows: 86_421,
    row_groups: 4,
    partitions: { placed_month: '2026-02' },
    reason: 'partition outside IN list',
  },
  {
    path: 'placed_month=2026-01/part-00001-93ad11.parquet',
    status: 'partition_skipped',
    size: 388_120,
    rows: 81_402,
    row_groups: 4,
    partitions: { placed_month: '2026-01' },
    reason: 'partition outside IN list',
  },
  {
    path: 'placed_month=2025-12/part-00012-b9ee10.parquet',
    status: 'partition_skipped',
    size: 412_440,
    rows: 88_882,
    row_groups: 4,
    partitions: { placed_month: '2025-12' },
    reason: 'partition outside IN list',
  },
];

const PLAN_TREE = `SortPreservingMerge: revenue_usd DESC, limit=50
└─ Limit: 50
   └─ Sort: [revenue_usd DESC NULLS LAST]
      └─ AggregateExec: HashAggregate group=[c.id, c.full_name, c.country]
                       agg=[count(o.id), sum(o.total_cents), max(o.placed_at)]
         filter: sum(o.total_cents) > 50000
         └─ HashJoinExec: c.id = o.customer_id
            ├─ CoalesceBatchesExec(8192)
            │  └─ ParquetExec[customers]
            │      files: 24 · row_groups: 96 · bytes: 1.1 GB
            └─ CoalesceBatchesExec(8192)
               └─ ParquetExec[orders]
                   files: 6 of 412 (pruned 398 partition, 8 row-group)
                   filter: placed_month IN ('2026-04','2026-05')
                         · placed_at >= '2026-04-16'
                         · status = 'fulfilled'
                   pruning: stats predicate on (placed_at, status)
                   row_groups: 34 of 2106 (pruned 1842 partition, 230 stats)`;

// ─── Snapshot timeline ─────────────────────────────────────────────────────
const COMMITS = [
  {
    v: 318,
    ts: '2026-05-16 14:08:21Z',
    op: 'MERGE',
    author: 'etl-pipeline',
    adds: 2,
    removes: 0,
    current: true,
    note: 'incremental ingest · 84k rows',
  },
  {
    v: 317,
    ts: '2026-05-16 13:08:01Z',
    op: 'MERGE',
    author: 'etl-pipeline',
    adds: 2,
    removes: 0,
    note: 'incremental ingest · 81k rows',
  },
  {
    v: 316,
    ts: '2026-05-16 12:08:08Z',
    op: 'WRITE',
    author: 'ops@axon',
    adds: 1,
    removes: 0,
    note: 'manual backfill 2026-05-15',
  },
  {
    v: 315,
    ts: '2026-05-16 11:08:14Z',
    op: 'MERGE',
    author: 'etl-pipeline',
    adds: 2,
    removes: 0,
    note: 'incremental ingest · 79k rows',
  },
  {
    v: 314,
    ts: '2026-05-16 10:08:09Z',
    op: 'OPTIMIZE',
    author: 'compactor',
    adds: 1,
    removes: 4,
    checkpoint: true,
    note: 'Z-ORDER (customer_id) · 18MB → 4MB',
  },
  {
    v: 313,
    ts: '2026-05-16 09:08:02Z',
    op: 'MERGE',
    author: 'etl-pipeline',
    adds: 2,
    removes: 0,
    note: 'incremental ingest · 92k rows',
  },
  {
    v: 312,
    ts: '2026-05-16 08:08:11Z',
    op: 'MERGE',
    author: 'etl-pipeline',
    adds: 2,
    removes: 0,
    note: 'incremental ingest · 78k rows',
  },
  {
    v: 311,
    ts: '2026-05-16 07:08:04Z',
    op: 'DELETE',
    author: 'ops@axon',
    adds: 1,
    removes: 1,
    note: 'GDPR erasure · 412 rows',
  },
];

// ─── History ───────────────────────────────────────────────────────────────
const HISTORY = [
  {
    time: '14:08:21',
    ms: 312,
    rows: 50,
    status: 'ok',
    target: 'browser_wasm',
    fallback: null,
    sql: 'SELECT c.id, c.full_name, ...',
  },
  {
    time: '14:04:02',
    ms: 103,
    rows: 812,
    status: 'ok',
    target: 'browser_wasm',
    fallback: null,
    sql: "SELECT * FROM events WHERE day = '2026-05-16'",
  },
  {
    time: '14:01:55',
    ms: 1240,
    rows: 0,
    status: 'error',
    target: 'native',
    fallback: 'browser_runtime_constraint',
    sql: "SELECT _change_type FROM table_changes('products', 38, 41)",
  },
  {
    time: '13:58:12',
    ms: 62,
    rows: 24,
    status: 'ok',
    target: 'browser_wasm',
    fallback: null,
    sql: 'SELECT sku, title, inventory FROM products WHERE inventory < 20',
  },
  {
    time: '13:44:30',
    ms: 208,
    rows: 1024,
    status: 'ok',
    target: 'native',
    fallback: 'capability_gate',
    sql: "WITH base AS ( SELECT date_trunc('week', ...) ...",
  },
  {
    time: '13:30:11',
    ms: 12,
    rows: 1,
    status: 'ok',
    target: 'browser_wasm',
    fallback: null,
    sql: 'SELECT NOW()',
  },
  {
    time: '13:14:48',
    ms: 482,
    rows: 2814,
    status: 'ok',
    target: 'native',
    fallback: 'range_read_unavailable',
    sql: 'SELECT customer_id, COUNT(*) FROM orders ...',
  },
];

const SAVED = [
  { name: 'Top customers · 30d', owner: 'you', edited: '2h', target: 'browser_wasm' },
  { name: 'Fulfilment funnel · Q2', owner: 'you', edited: 'yesterday', target: 'browser_wasm' },
  { name: 'CDF · product price changes', owner: 'ops team', edited: '3d', target: 'native' },
  { name: 'MRR cohort retention', owner: 'analytics', edited: '1w', target: 'native' },
  { name: 'Failed payments by gateway', owner: 'you', edited: '2w', target: 'browser_wasm' },
];

// ─── Engine status ─────────────────────────────────────────────────────────
const ENGINE_STATUS = {
  bundle: 'browser-engine-worker.wasm',
  bundle_tier: 'single-threaded baseline',
  available_tiers: ['baseline', 'simd', 'threaded', 'simd+threaded'],
  active_tier: 'baseline',
  wasm_size_kb: 2_410,
  cold_start_ms: 184,
  worker_mem_mb: 34.2,
  cache: {
    opfs_used_mb: 124.4,
    opfs_budget_mb: 512,
    memory_mb: 48.0,
    extents: 2_018,
    hit_ratio: 0.86,
  },
  router: { browser_supported: true, signed_urls_minted_by: 'delta-control-plane' },
  proto: 'PostgreSQL-compatible SQL · DataFusion 41',
};

window.AXON_DATA = {
  CATALOG,
  CAPABILITIES,
  CAPABILITY_ORDER,
  FALLBACK_REASONS,
  SAMPLE_QUERY,
  SECONDARY_QUERY,
  THIRD_QUERY,
  RESULT_COLUMNS,
  RESULT_ROWS,
  METRICS,
  PLAN_FILES,
  PLAN_TREE,
  COMMITS,
  HISTORY,
  SAVED,
  ENGINE_STATUS,
};
