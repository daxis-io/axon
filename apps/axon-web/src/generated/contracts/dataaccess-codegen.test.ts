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
  ObjectGrantAuditAction,
  ObjectGrantAuditEventSchema,
  ObjectGrantAuditOutcome,
  ObjectGrantAuditRangeSchema,
  ObjectGrantHeadResponseSchema,
  ObjectGrantListResponseSchema,
  ObjectGrantRangeRequestSchema,
  ObjectGrantRangeResponseSchema,
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

  it('preserves object grant head response JSON without a protobuf-only envelope', () => {
    const head = create(ObjectGrantHeadResponseSchema, {
      path: 'part-000.parquet',
      sizeBytes: 512n,
      etag: '"part-000-v7"',
    });

    expect(toJson(ObjectGrantHeadResponseSchema, head)).toEqual({
      path: 'part-000.parquet',
      sizeBytes: '512',
      etag: '"part-000-v7"',
    });
  });

  it('documents proxy range bytes as an explicit protobuf envelope', () => {
    const request = create(ObjectGrantRangeRequestSchema, {
      path: 'part-000.parquet',
      start: 0n,
      end: 16n,
    });
    const range = create(ObjectGrantRangeResponseSchema, {
      data: new Uint8Array([1, 2, 3, 4]),
      contentRange: 'bytes 0-3/512',
      etag: '"part-000-v7"',
      sizeBytes: 512n,
    });

    expect(toJson(ObjectGrantRangeRequestSchema, request)).toEqual({
      path: 'part-000.parquet',
      start: '0',
      end: '16',
    });
    expect(toJson(ObjectGrantRangeResponseSchema, range)).toEqual({
      data: 'AQIDBA==',
      contentRange: 'bytes 0-3/512',
      etag: '"part-000-v7"',
      sizeBytes: '512',
    });
  });

  it('normalizes legacy lowercase object grant audit enums into protobuf enum JSON', () => {
    const legacyAudit = {
      eventId: 'audit-123',
      eventType: 'object_grant_access',
      occurredAtEpochMs: 1_800_000_000_000,
      tenantId: 'tenant-123',
      workspaceId: 'workspace-123',
      userSubject: 'user:analyst@example.test',
      tableId: 'tbl-123',
      fullName: 'main.sales.orders',
      grantId: 'grant-456',
      queryId: 'query-789',
      requestId: 'req-123',
      correlationId: 'corr-123',
      action: 'range',
      objectPath: 'part-000.parquet',
      range: { start: 0, end: 16 },
      outcome: 'allowed',
    } satisfies LegacyObjectGrantAuditEvent;

    const normalized = legacyObjectGrantAuditEventToProtobufJson(legacyAudit);
    const decoded = fromJson(ObjectGrantAuditEventSchema, normalized);
    const protobufJson = toJson(ObjectGrantAuditEventSchema, decoded);

    expect(protobufJson).toEqual(normalized);
    expect(protobufJson).toMatchObject({
      action: 'OBJECT_GRANT_AUDIT_ACTION_RANGE',
      outcome: 'OBJECT_GRANT_AUDIT_OUTCOME_ALLOWED',
      range: { start: '0', end: '16' },
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

type LegacyObjectGrantAuditEvent = {
  eventId: string;
  eventType: 'object_grant_access';
  occurredAtEpochMs: number;
  tenantId: string;
  workspaceId: string;
  userSubject: string;
  tableId: string;
  fullName: string;
  grantId: string;
  queryId: string;
  requestId: string;
  correlationId: string;
  action: 'list' | 'head' | 'batch_sign' | 'range';
  objectPath: string;
  range?: {
    start: number;
    end: number;
  };
  outcome: 'allowed' | 'denied';
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

function legacyObjectGrantAuditEventToProtobufJson(event: LegacyObjectGrantAuditEvent): JsonValue {
  return toJson(
    ObjectGrantAuditEventSchema,
    create(ObjectGrantAuditEventSchema, {
      eventId: event.eventId,
      eventType: event.eventType,
      occurredAtEpochMs: BigInt(event.occurredAtEpochMs),
      tenantId: event.tenantId,
      workspaceId: event.workspaceId,
      userSubject: event.userSubject,
      tableId: event.tableId,
      fullName: event.fullName,
      grantId: event.grantId,
      queryId: event.queryId,
      requestId: event.requestId,
      correlationId: event.correlationId,
      action: normalizeObjectGrantAuditAction(event.action),
      objectPath: event.objectPath,
      range: event.range
        ? create(ObjectGrantAuditRangeSchema, {
            start: BigInt(event.range.start),
            end: BigInt(event.range.end),
          })
        : undefined,
      outcome: normalizeObjectGrantAuditOutcome(event.outcome),
    }),
  );
}

function normalizeObjectGrantAuditAction(
  value: LegacyObjectGrantAuditEvent['action'],
): ObjectGrantAuditAction {
  switch (value) {
    case 'list':
      return ObjectGrantAuditAction.LIST;
    case 'head':
      return ObjectGrantAuditAction.HEAD;
    case 'batch_sign':
      return ObjectGrantAuditAction.BATCH_SIGN;
    case 'range':
      return ObjectGrantAuditAction.RANGE;
  }
}

function normalizeObjectGrantAuditOutcome(
  value: LegacyObjectGrantAuditEvent['outcome'],
): ObjectGrantAuditOutcome {
  switch (value) {
    case 'allowed':
      return ObjectGrantAuditOutcome.ALLOWED;
    case 'denied':
      return ObjectGrantAuditOutcome.DENIED;
  }
}
