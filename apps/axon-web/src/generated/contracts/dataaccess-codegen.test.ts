import { create, fromJson, toJson, type JsonValue } from '@bufbuild/protobuf';
import { timestampFromMs } from '@bufbuild/protobuf/wkt';
import { describe, expect, it } from 'vitest';
import {
  CanonicalResourceRefSchema,
  ProviderErrorCode,
  ProviderErrorSchema,
  ResourceKind,
} from './protobuf/axon/common/v1/common_pb.ts';
import {
  BlockedReadAccessPlanSchema,
  BrowserAccessClass,
  BrokeredDeltaAccessMode,
  BrokeredDeltaReadAccessPlanSchema,
  BrokeredObjectAccessSchema,
  BrokeredPolicyAuthoritySchema,
  BrowserHttpParquetDatasetDescriptorSchema,
  BrowserHttpSnapshotDescriptorSchema,
  BrowserReadDescriptorSchema,
  CapabilityEntrySchema,
  CapabilityKey,
  CapabilityReportSchema,
  CapabilityState,
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
  ReadDeniedSchema,
  ReadResolutionReason,
  ReadResolutionSchema,
  RemoteRequiredSchema,
  ResolvedBrowserReadSchema,
  ResolutionProvenanceSchema,
  type CapabilityReport,
  type ResolvedBrowserRead,
  ReadAccessPlanReason,
} from './protobuf/axon/dataaccess/v1/dataaccess_pb.ts';

describe('dataaccess contract codegen', () => {
  it('round-trips all four disjoint read-resolution outcomes', () => {
    expect(ReadResolutionSchema.oneofs[0]?.fields.map((field) => field.name)).toEqual([
      'browser_read',
      'remote_required',
      'denied',
      'error',
    ]);
    expect(ResolvedBrowserReadSchema.fields.map((field) => field.name)).toEqual([
      'resource',
      'descriptor',
      'access_class',
      'not_after',
      'correlation_id',
      'provenance',
    ]);

    const resource = create(CanonicalResourceRefSchema, {
      connectionId: 'connection-public-gcs',
      providerNamespace: 'axon.public-gcs/v1',
      kind: ResourceKind.TABLE,
      identity: {
        case: 'canonicalLocator',
        value: 'gs://axon-fixtures/tables/events',
      },
    });
    const descriptor = create(BrowserReadDescriptorSchema, {
      descriptor: {
        case: 'snapshot',
        value: create(BrowserHttpSnapshotDescriptorSchema, {
          tableUri: 'gs://axon-fixtures/tables/events',
        }),
      },
    });
    const browserRead = create(ResolvedBrowserReadSchema, {
      resource,
      descriptor,
      accessClass: BrowserAccessClass.PUBLIC,
      correlationId: 'correlation-001',
      provenance: create(ResolutionProvenanceSchema, {
        resolverId: 'public-object-storage',
        resolutionId: 'resolution-001',
      }),
    });
    const outcomes = [
      create(ReadResolutionSchema, {
        outcome: { case: 'browserRead', value: browserRead },
      }),
      create(ReadResolutionSchema, {
        outcome: {
          case: 'remoteRequired',
          value: create(RemoteRequiredSchema, {
            resource,
            enforcementOwner: 'unity-catalog-host',
            reason: ReadResolutionReason.POLICY_ENFORCEMENT_REQUIRED,
            message: 'row filter must be enforced by the host',
          }),
        },
      }),
      create(ReadResolutionSchema, {
        outcome: {
          case: 'denied',
          value: create(ReadDeniedSchema, {
            resource,
            reason: ReadResolutionReason.ACCESS_DENIED,
            message: 'table access denied',
          }),
        },
      }),
      create(ReadResolutionSchema, {
        outcome: {
          case: 'error',
          value: create(ProviderErrorSchema, {
            code: ProviderErrorCode.SESSION_EXPIRED,
            message: 'session expired',
            correlationId: 'correlation-001',
          }),
        },
      }),
    ];

    expect(outcomes.map((resolution) => resolution.outcome.case)).toEqual([
      'browserRead',
      'remoteRequired',
      'denied',
      'error',
    ]);
    for (const resolution of outcomes) {
      const json = toJson(ReadResolutionSchema, resolution);
      expect(toJson(ReadResolutionSchema, fromJson(ReadResolutionSchema, json))).toEqual(json);
    }
    expect(toJson(ReadResolutionSchema, outcomes[0]!)).toMatchObject({
      browserRead: {
        resource: {
          providerNamespace: 'axon.public-gcs/v1',
          canonicalLocator: 'gs://axon-fixtures/tables/events',
        },
        accessClass: 'BROWSER_ACCESS_CLASS_PUBLIC',
        correlationId: 'correlation-001',
        provenance: {
          resolverId: 'public-object-storage',
          resolutionId: 'resolution-001',
        },
      },
    });
  });

  it('uses one directly openable descriptor and typed unique capability entries', () => {
    expect(BrowserReadDescriptorSchema.oneofs[0]?.fields.map((field) => field.name)).toEqual([
      'snapshot',
      'parquet_dataset',
    ]);

    const report = create(CapabilityReportSchema, {
      capabilities: [
        create(CapabilityEntrySchema, {
          key: CapabilityKey.RANGE_READS,
          state: CapabilityState.SUPPORTED,
        }),
        create(CapabilityEntrySchema, {
          key: CapabilityKey.DELETION_VECTORS,
          state: CapabilityState.UNSUPPORTED,
        }),
      ],
    });
    expect(validatedCapabilityStates(report)).toEqual(
      new Map([
        [CapabilityKey.RANGE_READS, CapabilityState.SUPPORTED],
        [CapabilityKey.DELETION_VECTORS, CapabilityState.UNSUPPORTED],
      ]),
    );
    expect(() =>
      validatedCapabilityStates(
        create(CapabilityReportSchema, {
          capabilities: [
            create(CapabilityEntrySchema, {
              key: CapabilityKey.RANGE_READS,
              state: CapabilityState.SUPPORTED,
            }),
            create(CapabilityEntrySchema, {
              key: CapabilityKey.RANGE_READS,
              state: CapabilityState.NATIVE_ONLY,
            }),
          ],
        }),
      ),
    ).toThrow('duplicate capability key');
    expect(() =>
      validatedCapabilityStates(
        create(CapabilityReportSchema, {
          capabilities: [
            create(CapabilityEntrySchema, {
              state: CapabilityState.SUPPORTED,
            }),
          ],
        }),
      ),
    ).toThrow('unspecified capability key');
    expect(() =>
      validatedCapabilityStates(
        create(CapabilityReportSchema, {
          capabilities: [
            create(CapabilityEntrySchema, {
              key: CapabilityKey.RANGE_READS,
            }),
          ],
        }),
      ),
    ).toThrow('unspecified capability state');

    const parquet = create(BrowserReadDescriptorSchema, {
      descriptor: {
        case: 'parquetDataset',
        value: create(BrowserHttpParquetDatasetDescriptorSchema, {
          tableUri: 's3://axon-fixtures/parquet/events',
          browserCompatibility: report,
        }),
      },
    });
    expect(parquet.descriptor.case).toBe('parquetDataset');
  });

  it('requires the earliest finite expiry for capability-bearing browser access', () => {
    const base = create(ResolvedBrowserReadSchema, {
      resource: create(CanonicalResourceRefSchema, {
        connectionId: 'connection-signed',
        providerNamespace: 'axon.signed-object/v1',
        kind: ResourceKind.TABLE,
        identity: { case: 'providerObjectId', value: 'table-123' },
      }),
      descriptor: create(BrowserReadDescriptorSchema, {
        descriptor: {
          case: 'snapshot',
          value: create(BrowserHttpSnapshotDescriptorSchema, {
            tableUri: 'https://signed.example.test/table-123',
          }),
        },
      }),
      accessClass: BrowserAccessClass.SIGNED_URL,
      correlationId: 'correlation-002',
      provenance: create(ResolutionProvenanceSchema, {
        resolverId: 'signed-object-resolver',
        resolutionId: 'resolution-002',
      }),
    });
    const nowMs = 1_800_000_000_000;

    expect(() => validateBrowserRead(base, nowMs, [nowMs + 120_000])).toThrow(
      'capability-bearing browser access requires not_after',
    );
    expect(() =>
      validateBrowserRead(
        create(ResolvedBrowserReadSchema, {
          ...base,
          notAfter: timestampFromMs(nowMs),
        }),
        nowMs,
        [nowMs],
      ),
    ).toThrow('browser access is expired');

    const valid = create(ResolvedBrowserReadSchema, {
      ...base,
      notAfter: timestampFromMs(nowMs + 60_000),
    });
    expect(validateBrowserRead(valid, nowMs, [nowMs + 120_000, nowMs + 60_000])).toBe(valid);
    expect(() =>
      validateBrowserRead(
        create(ResolvedBrowserReadSchema, {
          ...base,
          notAfter: timestampFromMs(nowMs + 120_000),
        }),
        nowMs,
        [nowMs + 120_000, nowMs + 60_000],
      ),
    ).toThrow('not_after must equal the earliest finite capability expiry');
    expect(() => validateBrowserRead(valid, nowMs, [Number.POSITIVE_INFINITY])).toThrow(
      'capability-bearing browser access requires a finite expiry',
    );

    const publicRead = create(ResolvedBrowserReadSchema, {
      ...base,
      accessClass: BrowserAccessClass.PUBLIC,
    });
    expect(validateBrowserRead(publicRead, nowMs, [])).toBe(publicRead);
  });

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
      executionId: 'execution-789',
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
      executionId: 'execution-789',
      action: 'OBJECT_GRANT_AUDIT_ACTION_RANGE',
      outcome: 'OBJECT_GRANT_AUDIT_OUTCOME_ALLOWED',
      range: { start: '0', end: '16' },
    });
    expect(ObjectGrantAuditEventSchema.fields.find((field) => field.number === 10)?.name).toBe(
      'execution_id',
    );
    expect(
      ObjectGrantAuditEventSchema.fields.some(
        (field) => field.name === 'query_id' || field.name === 'request_id',
      ),
    ).toBe(false);
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
  executionId: string;
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
      executionId: event.executionId,
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

function validatedCapabilityStates(
  report: CapabilityReport,
): ReadonlyMap<CapabilityKey, CapabilityState> {
  const states = new Map<CapabilityKey, CapabilityState>();
  for (const capability of report.capabilities) {
    if (capability.key === CapabilityKey.UNSPECIFIED) {
      throw new TypeError('unspecified capability key');
    }
    if (capability.state === CapabilityState.UNSPECIFIED) {
      throw new TypeError('unspecified capability state');
    }
    if (states.has(capability.key)) {
      throw new TypeError('duplicate capability key');
    }
    states.set(capability.key, capability.state);
  }
  return states;
}

function validateBrowserRead(
  read: ResolvedBrowserRead,
  nowMs: number,
  capabilityExpiriesMs: readonly number[],
): ResolvedBrowserRead {
  const capabilityBearing = read.accessClass !== BrowserAccessClass.PUBLIC;
  if (capabilityBearing && !read.notAfter) {
    throw new TypeError('capability-bearing browser access requires not_after');
  }
  if (read.notAfter) {
    const notAfterMs = Number(read.notAfter.seconds) * 1000 + read.notAfter.nanos / 1_000_000;
    if (!Number.isFinite(notAfterMs)) {
      throw new TypeError('browser access not_after must be finite');
    }
    if (notAfterMs <= nowMs) {
      throw new TypeError('browser access is expired');
    }
    if (capabilityBearing) {
      const finiteExpiries = capabilityExpiriesMs.filter(Number.isFinite);
      if (finiteExpiries.length === 0) {
        throw new TypeError('capability-bearing browser access requires a finite expiry');
      }
      if (notAfterMs !== Math.min(...finiteExpiries)) {
        throw new TypeError('not_after must equal the earliest finite capability expiry');
      }
    }
  }
  return read;
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
