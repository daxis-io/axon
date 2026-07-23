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
  BrowserAccessClass,
  BrowserHttpParquetDatasetDescriptorSchema,
  BrowserHttpSnapshotDescriptorSchema,
  BrowserReadDescriptorSchema,
  CapabilityEntrySchema,
  CapabilityKey,
  CapabilityReportSchema,
  CapabilityState,
  BrowserHttpFileDescriptorSchema,
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
  ReadDeniedSchema,
  ReadResolutionReason,
  ReadResolutionSchema,
  RemoteRequiredSchema,
  ResolvedBrowserReadSchema,
  ResolutionProvenanceSchema,
  file_axon_dataaccess_v1_dataaccess,
  type CapabilityReport,
  type ResolvedBrowserRead,
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

  it('rejects incomplete browser-read identity, descriptor, access, and provenance', () => {
    const valid = create(ResolvedBrowserReadSchema, {
      resource: create(CanonicalResourceRefSchema, {
        connectionId: 'connection-public-gcs',
        providerNamespace: 'axon.public-gcs/v1',
        kind: ResourceKind.TABLE,
        identity: {
          case: 'canonicalLocator',
          value: 'gs://axon-fixtures/tables/events',
        },
      }),
      descriptor: create(BrowserReadDescriptorSchema, {
        descriptor: {
          case: 'snapshot',
          value: create(BrowserHttpSnapshotDescriptorSchema, {
            tableUri: 'gs://axon-fixtures/tables/events',
          }),
        },
      }),
      accessClass: BrowserAccessClass.PUBLIC,
      correlationId: 'correlation-001',
      provenance: create(ResolutionProvenanceSchema, {
        resolverId: 'public-object-storage',
        resolutionId: 'resolution-001',
      }),
    });
    const nowMs = 1_800_000_000_000;
    expect(validateBrowserRead(valid, nowMs, [])).toBe(valid);

    expect(() =>
      validateBrowserRead(
        create(ResolvedBrowserReadSchema, { ...valid, resource: undefined }),
        nowMs,
        [],
      ),
    ).toThrow('resource');
    expect(() =>
      validateBrowserRead(
        create(ResolvedBrowserReadSchema, {
          ...valid,
          resource: create(CanonicalResourceRefSchema, {
            ...valid.resource!,
            connectionId: '',
          }),
        }),
        nowMs,
        [],
      ),
    ).toThrow('canonical resource');
    expect(() =>
      validateBrowserRead(
        create(ResolvedBrowserReadSchema, { ...valid, descriptor: undefined }),
        nowMs,
        [],
      ),
    ).toThrow('descriptor');
    expect(() =>
      validateBrowserRead(
        create(ResolvedBrowserReadSchema, {
          ...valid,
          descriptor: create(BrowserReadDescriptorSchema, {}),
        }),
        nowMs,
        [],
      ),
    ).toThrow('descriptor arm');
    expect(() =>
      validateBrowserRead(
        create(ResolvedBrowserReadSchema, {
          ...valid,
          accessClass: BrowserAccessClass.UNSPECIFIED,
        }),
        nowMs,
        [],
      ),
    ).toThrow('access_class');
    expect(() =>
      validateBrowserRead(
        create(ResolvedBrowserReadSchema, { ...valid, correlationId: '' }),
        nowMs,
        [],
      ),
    ).toThrow('correlation_id');
    expect(() =>
      validateBrowserRead(
        create(ResolvedBrowserReadSchema, { ...valid, provenance: undefined }),
        nowMs,
        [],
      ),
    ).toThrow('provenance');
    expect(() =>
      validateBrowserRead(
        create(ResolvedBrowserReadSchema, {
          ...valid,
          provenance: create(ResolutionProvenanceSchema, {
            resolverId: '',
            resolutionId: 'resolution-001',
          }),
        }),
        nowMs,
        [],
      ),
    ).toThrow('resolver_id');
    expect(() =>
      validateBrowserRead(
        create(ResolvedBrowserReadSchema, {
          ...valid,
          descriptor: create(BrowserReadDescriptorSchema, {
            descriptor: {
              case: 'snapshot',
              value: create(BrowserHttpSnapshotDescriptorSchema, {
                tableUri: 'gs://axon-fixtures/tables/events',
                browserCompatibility: create(CapabilityReportSchema, {
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
              }),
            },
          }),
        }),
        nowMs,
        [],
      ),
    ).toThrow('duplicate capability key');
  });

  it('removes the temporary resolution-plan compatibility hierarchy', () => {
    const messageNames = file_axon_dataaccess_v1_dataaccess.messages.map(
      (message) => message.name,
    );
    const enumNames = file_axon_dataaccess_v1_dataaccess.enums.map((value) => value.name);

    expect(messageNames).not.toEqual(
      expect.arrayContaining([
        'BrokeredObjectAccess',
        'BrokeredPolicyAuthority',
        'BrokeredDeltaReadAccessPlan',
        'DeltaSharingReadAccessPlan',
        'SqlFallbackRequiredReadAccessPlan',
        'BlockedReadAccessPlan',
        'ReadAccessPlan',
        'TableReadResolution',
      ]),
    );
    expect(enumNames).not.toEqual(
      expect.arrayContaining([
        'ReadAccessPlanReason',
        'BrokeredDeltaAccessMode',
        'PolicyAuthorityKind',
        'DirectExternalEngineReadSupport',
      ]),
    );
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
  if (!read.resource) {
    throw new TypeError('browser_read resource is required');
  }
  validateCanonicalResource(read.resource);
  if (!read.descriptor) {
    throw new TypeError('browser_read descriptor is required');
  }
  const descriptor = read.descriptor.descriptor;
  if (!descriptor.case) {
    throw new TypeError('exactly one browser_read descriptor arm is required');
  }
  if (!descriptor.value.tableUri.trim()) {
    throw new TypeError('browser_read descriptor table_uri is required');
  }
  validatedOptionalCapabilityReport(descriptor.value.browserCompatibility);
  validatedOptionalCapabilityReport(descriptor.value.requiredCapabilities);
  if (read.accessClass === BrowserAccessClass.UNSPECIFIED) {
    throw new TypeError('browser_read access_class is required');
  }
  if (!read.correlationId.trim()) {
    throw new TypeError('browser_read correlation_id is required');
  }
  if (!read.provenance) {
    throw new TypeError('browser_read provenance is required');
  }
  if (!read.provenance.resolverId.trim()) {
    throw new TypeError('browser_read provenance resolver_id is required');
  }
  if (!read.provenance.resolutionId.trim()) {
    throw new TypeError('browser_read provenance resolution_id is required');
  }
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

function validateCanonicalResource(
  resource: NonNullable<ResolvedBrowserRead['resource']>,
): void {
  if (!resource.connectionId.trim()) {
    throw new TypeError('canonical resource connection_id is required');
  }
  if (!resource.providerNamespace.trim()) {
    throw new TypeError('canonical resource provider_namespace is required');
  }
  if (resource.kind === ResourceKind.UNSPECIFIED) {
    throw new TypeError('canonical resource kind is required');
  }
  if (!resource.identity.case || !resource.identity.value.trim()) {
    throw new TypeError('canonical resource identity is required');
  }
}

function validatedOptionalCapabilityReport(report: CapabilityReport | undefined): void {
  if (report) validatedCapabilityStates(report);
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
