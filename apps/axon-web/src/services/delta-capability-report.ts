import { create } from '@bufbuild/protobuf';
import {
  CapabilityEntrySchema,
  CapabilityKey,
  CapabilityReportSchema,
  CapabilityState,
  type CapabilityReport,
} from '../generated/contracts/protobuf/axon/dataaccess/v1/dataaccess_pb.ts';

// Capability report shape emitted by the WASM snapshot resolver
// (crates/wasm-delta-snapshot ResolvedSnapshotDescriptor) for browser_compatibility and
// required_capabilities.
export type ResolvedCapabilityKey =
  | 'change_data_feed'
  | 'column_mapping'
  | 'deletion_vectors'
  | 'multi_partition_execution'
  | 'proxy_access'
  | 'range_reads'
  | 'signed_url_access'
  | 'time_travel'
  | 'timestamp_ntz'
  | 'unknown_protocol_features';

export type ResolvedCapabilityState = 'supported' | 'native_only' | 'unsupported' | 'experimental';

export type ResolvedCapabilityReport = {
  capabilities?: Partial<Record<ResolvedCapabilityKey, ResolvedCapabilityState>>;
};

// Converts a resolver capability report into the generated contract. Unknown keys or states
// raise the caller's typed error instead of being dropped.
export function generatedCapabilityReport(
  report: ResolvedCapabilityReport | undefined,
  invalidReport: () => Error,
): CapabilityReport {
  return create(CapabilityReportSchema, {
    capabilities: Object.entries(report?.capabilities ?? {}).flatMap(([key, state]) => {
      const generatedKey = generatedCapabilityKey(key as ResolvedCapabilityKey);
      const generatedState = generatedCapabilityState(state);
      if (generatedKey === undefined || generatedState === undefined) {
        throw invalidReport();
      }
      return [
        create(CapabilityEntrySchema, {
          key: generatedKey,
          state: generatedState,
        }),
      ];
    }),
  });
}

function generatedCapabilityKey(value: ResolvedCapabilityKey): CapabilityKey | undefined {
  switch (value) {
    case 'change_data_feed':
      return CapabilityKey.CHANGE_DATA_FEED;
    case 'column_mapping':
      return CapabilityKey.COLUMN_MAPPING;
    case 'deletion_vectors':
      return CapabilityKey.DELETION_VECTORS;
    case 'multi_partition_execution':
      return CapabilityKey.MULTI_PARTITION_EXECUTION;
    case 'proxy_access':
      return CapabilityKey.PROXY_ACCESS;
    case 'range_reads':
      return CapabilityKey.RANGE_READS;
    case 'signed_url_access':
      return CapabilityKey.SIGNED_URL_ACCESS;
    case 'time_travel':
      return CapabilityKey.TIME_TRAVEL;
    case 'timestamp_ntz':
      return CapabilityKey.TIMESTAMP_NTZ;
    case 'unknown_protocol_features':
      return CapabilityKey.UNKNOWN_PROTOCOL_FEATURES;
  }
}

function generatedCapabilityState(
  value: ResolvedCapabilityState | undefined,
): CapabilityState | undefined {
  switch (value) {
    case 'supported':
      return CapabilityState.SUPPORTED;
    case 'native_only':
      return CapabilityState.NATIVE_ONLY;
    case 'unsupported':
      return CapabilityState.UNSUPPORTED;
    case 'experimental':
      return CapabilityState.EXPERIMENTAL;
    case undefined:
      return undefined;
  }
}
