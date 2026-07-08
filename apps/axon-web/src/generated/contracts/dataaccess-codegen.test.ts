import { create, fromJson, toJson, type JsonValue } from '@bufbuild/protobuf';
import { describe, expect, it } from 'vitest';
import {
  BlockedReadAccessPlanSchema,
  BrokeredDeltaAccessMode,
  BrokeredDeltaReadAccessPlanSchema,
  BrokeredObjectAccessSchema,
  BrokeredPolicyAuthoritySchema,
  BrowserHttpFileDescriptorSchema,
  DirectExternalEngineReadSupport,
  ObjectGrantBatchSignResponseSchema,
  ObjectGrantListResponseSchema,
  PartitionValueSchema,
  PolicyAuthorityKind,
  ReadAccessPlanSchema,
  ReadAccessPlanReason,
} from './protobuf/axon/dataaccess/v1/dataaccess_pb.ts';

describe('dataaccess contract codegen', () => {
  it('normalizes legacy read access plan JSON into protobuf oneof JSON', () => {
    const legacyBrokeredDelta = {
      plan_type: 'brokered_delta',
      tableId: 'tbl-123',
      fullName: 'main.sales.orders',
      tableRoot: 's3://prod-bucket/tables/orders',
      grantId: 'grant-456',
      expiresAtEpochMs: 1_800_000_000_000,
      deltaAccessMode: 'delta_log',
      policyAuthority: {
        authority: 'unity_catalog',
        directExternalEngineRead: 'confirmed',
      },
      objectAccess: {
        list: true,
        head: true,
        get: false,
        rangeGet: true,
        batchSign: true,
        proxyRange: true,
      },
    } satisfies LegacyReadAccessPlan;

    const normalized = legacyReadAccessPlanToProtobufJson(legacyBrokeredDelta);
    const decoded = fromJson(ReadAccessPlanSchema, normalized);
    const protobufJson = toJson(ReadAccessPlanSchema, decoded);

    expect(protobufJson).toEqual(normalized);
    expect(protobufJson).toMatchObject({
      brokeredDelta: {
        tableId: 'tbl-123',
        fullName: 'main.sales.orders',
        grantId: 'grant-456',
        expiresAtEpochMs: '1800000000000',
        deltaAccessMode: 'BROKERED_DELTA_ACCESS_MODE_DELTA_LOG',
        policyAuthority: {
          authority: 'POLICY_AUTHORITY_KIND_UNITY_CATALOG',
          directExternalEngineRead: 'DIRECT_EXTERNAL_ENGINE_READ_SUPPORT_CONFIRMED',
        },
      },
    });
  });

  it('normalizes nullable legacy partition values through explicit protobuf partition values', () => {
    const legacyFile = {
      path: 'event_date=2026-07-08/part-000.parquet',
      url: 'https://signed.example.test/part-000.parquet?sig=redacted',
      size_bytes: 512,
      partition_values: {
        event_date: '2026-07-08',
        region: null,
      },
      stats: '{"numRecords":42}',
    };

    const normalized = legacyBrowserHttpFileDescriptorToProtobufJson(legacyFile);
    const decoded = fromJson(BrowserHttpFileDescriptorSchema, normalized);
    const protobufJson = toJson(BrowserHttpFileDescriptorSchema, decoded);

    expect(protobufJson).toEqual(normalized);
    expect(protobufJson).toMatchObject({
      size_bytes: '512',
      partition_values: {
        event_date: { stringValue: '2026-07-08' },
        region: { nullValue: null },
      },
    });
  });

  it('preserves legacy object grant camelCase JSON field names', () => {
    const list = create(ObjectGrantListResponseSchema, {
      objects: [
        {
          path: 'part-000.parquet',
          sizeBytes: 512n,
          etag: 'etag-1',
        },
      ],
    });
    const batchSign = create(ObjectGrantBatchSignResponseSchema, {
      signedUrls: [
        {
          path: 'part-000.parquet',
          url: 'https://signed.example.test/part-000.parquet?sig=redacted',
          expiresAtEpochMs: 1_800_000_000_000n,
        },
      ],
    });

    expect(toJson(ObjectGrantListResponseSchema, list)).toEqual({
      objects: [
        {
          path: 'part-000.parquet',
          sizeBytes: '512',
          etag: 'etag-1',
        },
      ],
    });
    expect(toJson(ObjectGrantBatchSignResponseSchema, batchSign)).toEqual({
      signedUrls: [
        {
          path: 'part-000.parquet',
          url: 'https://signed.example.test/part-000.parquet?sig=redacted',
          expiresAtEpochMs: '1800000000000',
        },
      ],
    });
  });
});

type LegacyReadAccessPlan = {
  plan_type: 'brokered_delta' | 'blocked';
  tableId: string;
  fullName: string;
  tableRoot?: string;
  grantId?: string;
  expiresAtEpochMs?: number;
  deltaAccessMode?: 'delta_log' | 'presigned_files';
  policyAuthority?: {
    authority: 'unity_catalog' | 'delta_sharing' | 'mock_broker';
    directExternalEngineRead: 'confirmed' | 'not_confirmed';
  };
  objectAccess?: {
    list: boolean;
    head: boolean;
    get: boolean;
    rangeGet: boolean;
    batchSign: boolean;
    proxyRange: boolean;
  };
  reason?: 'row_filter' | 'unknown_policy_state';
  message?: string;
};

function legacyReadAccessPlanToProtobufJson(plan: LegacyReadAccessPlan): JsonValue {
  switch (plan.plan_type) {
    case 'brokered_delta': {
      const brokeredDelta = create(BrokeredDeltaReadAccessPlanSchema, {
        tableId: plan.tableId,
        fullName: plan.fullName,
        tableRoot: plan.tableRoot,
        grantId: plan.grantId,
        expiresAtEpochMs: BigInt(plan.expiresAtEpochMs ?? 0),
        deltaAccessMode: normalizeDeltaAccessMode(plan.deltaAccessMode),
        policyAuthority: create(BrokeredPolicyAuthoritySchema, {
          authority: normalizePolicyAuthority(plan.policyAuthority?.authority),
          directExternalEngineRead: normalizeDirectExternalEngineRead(
            plan.policyAuthority?.directExternalEngineRead,
          ),
        }),
        objectAccess: create(BrokeredObjectAccessSchema, plan.objectAccess),
      });
      return toJson(
        ReadAccessPlanSchema,
        create(ReadAccessPlanSchema, {
          plan: {
            case: 'brokeredDelta',
            value: brokeredDelta,
          },
        }),
      );
    }
    case 'blocked': {
      const blocked = create(BlockedReadAccessPlanSchema, {
        tableId: plan.tableId,
        fullName: plan.fullName,
        reason: normalizeReadAccessPlanReason(plan.reason),
        message: plan.message,
      });
      return toJson(
        ReadAccessPlanSchema,
        create(ReadAccessPlanSchema, {
          plan: {
            case: 'blocked',
            value: blocked,
          },
        }),
      );
    }
  }
}

type LegacyBrowserHttpFileDescriptor = {
  path: string;
  url: string;
  size_bytes: number;
  partition_values: Record<string, string | null>;
  stats?: string;
};

function legacyBrowserHttpFileDescriptorToProtobufJson(
  file: LegacyBrowserHttpFileDescriptor,
): JsonValue {
  return toJson(
    BrowserHttpFileDescriptorSchema,
    create(BrowserHttpFileDescriptorSchema, {
      path: file.path,
      url: file.url,
      sizeBytes: BigInt(file.size_bytes),
      partitionValues: Object.fromEntries(
        Object.entries(file.partition_values).map(([key, value]) => [
          key,
          create(
            PartitionValueSchema,
            value === null
              ? { value: { case: 'nullValue', value: 0 } }
              : { value: { case: 'stringValue', value } },
          ),
        ]),
      ),
      stats: file.stats,
    }),
  );
}

function normalizeDeltaAccessMode(
  value: LegacyReadAccessPlan['deltaAccessMode'],
): BrokeredDeltaAccessMode {
  switch (value) {
    case 'delta_log':
      return BrokeredDeltaAccessMode.DELTA_LOG;
    case 'presigned_files':
      return BrokeredDeltaAccessMode.PRESIGNED_FILES;
    default:
      return BrokeredDeltaAccessMode.UNSPECIFIED;
  }
}

function normalizePolicyAuthority(
  value: NonNullable<LegacyReadAccessPlan['policyAuthority']>['authority'] | undefined,
): PolicyAuthorityKind {
  switch (value) {
    case 'unity_catalog':
      return PolicyAuthorityKind.UNITY_CATALOG;
    case 'delta_sharing':
      return PolicyAuthorityKind.DELTA_SHARING;
    case 'mock_broker':
      return PolicyAuthorityKind.MOCK_BROKER;
    default:
      return PolicyAuthorityKind.UNSPECIFIED;
  }
}

function normalizeDirectExternalEngineRead(
  value: NonNullable<LegacyReadAccessPlan['policyAuthority']>['directExternalEngineRead'] | undefined,
): DirectExternalEngineReadSupport {
  switch (value) {
    case 'confirmed':
      return DirectExternalEngineReadSupport.CONFIRMED;
    case 'not_confirmed':
      return DirectExternalEngineReadSupport.NOT_CONFIRMED;
    default:
      return DirectExternalEngineReadSupport.UNSPECIFIED;
  }
}

function normalizeReadAccessPlanReason(value: LegacyReadAccessPlan['reason']): ReadAccessPlanReason {
  switch (value) {
    case 'row_filter':
      return ReadAccessPlanReason.ROW_FILTER;
    case 'unknown_policy_state':
      return ReadAccessPlanReason.UNKNOWN_POLICY_STATE;
    default:
      return ReadAccessPlanReason.UNSPECIFIED;
  }
}
