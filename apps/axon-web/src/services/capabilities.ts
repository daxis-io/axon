import type { CapabilityKey, CapabilityMatrixRow, CapabilityState } from './types.ts';

// Static capability matrix sourced from the spec in crates/query-contract/src/lib.rs.
// At runtime, a QueryResponse.capabilities map can override the browser column for
// the currently-loaded snapshot — see overlayCapabilityReport().

const BROWSER: Partial<Record<CapabilityKey, CapabilityState>> = {
  change_data_feed: 'native_only',
  column_mapping: 'supported',
  deletion_vectors: 'native_only',
  multi_partition_execution: 'experimental',
  proxy_access: 'supported',
  range_reads: 'supported',
  signed_url_access: 'supported',
  time_travel: 'supported',
  timestamp_ntz: 'experimental',
  unknown_protocol_features: 'unsupported',
};

const NATIVE: Partial<Record<CapabilityKey, CapabilityState>> = {
  change_data_feed: 'supported',
  column_mapping: 'supported',
  deletion_vectors: 'supported',
  multi_partition_execution: 'supported',
  proxy_access: 'supported',
  range_reads: 'supported',
  signed_url_access: 'native_only',
  time_travel: 'supported',
  timestamp_ntz: 'supported',
  unknown_protocol_features: 'experimental',
};

const META: Record<CapabilityKey, { label: string; desc: string }> = {
  range_reads: { label: 'HTTP Range Reads', desc: 'Validated byte-range object reads' },
  signed_url_access: { label: 'Signed URL Access', desc: 'Read via pre-signed object URLs' },
  time_travel: { label: 'Time Travel', desc: 'Pin snapshot_version or timestamp' },
  column_mapping: { label: 'Column Mapping', desc: 'Delta column-mapping id/name modes' },
  timestamp_ntz: { label: 'Timestamp NTZ', desc: 'TIMESTAMP without time zone' },
  multi_partition_execution: {
    label: 'Multi-partition Scan',
    desc: 'Execute across >1 partition concurrently',
  },
  change_data_feed: { label: 'Change Data Feed', desc: 'Read CDF version range' },
  deletion_vectors: { label: 'Deletion Vectors', desc: 'DV-encoded row deletions' },
  proxy_access: { label: 'Proxy Access', desc: 'Route reads through control plane' },
  unknown_protocol_features: {
    label: 'Unknown Protocol',
    desc: 'Fall back when protocol unknown',
  },
};

export const CAPABILITY_ORDER: CapabilityKey[] = [
  'range_reads',
  'signed_url_access',
  'time_travel',
  'column_mapping',
  'timestamp_ntz',
  'multi_partition_execution',
  'change_data_feed',
  'deletion_vectors',
  'proxy_access',
  'unknown_protocol_features',
];

export function defaultCapabilityMatrix(): CapabilityMatrixRow[] {
  return CAPABILITY_ORDER.map((key) => ({
    key,
    label: META[key].label,
    desc: META[key].desc,
    browser: BROWSER[key] ?? 'unsupported',
    native: NATIVE[key] ?? 'unsupported',
  }));
}

// When a query returns a CapabilityReport (snapshot-required capabilities), overlay
// those states onto the browser column so the UI reflects what the active table needs.
export function overlayCapabilityReport(
  base: CapabilityMatrixRow[],
  report: Partial<Record<CapabilityKey, CapabilityState>> | undefined,
): CapabilityMatrixRow[] {
  if (!report) return base;
  return base.map((row) => {
    const override = report[row.key];
    return override ? { ...row, browser: override } : row;
  });
}
