// Connect-Catalog modal — 3-step workflow:
//   1. Pick a source (Local · Object storage · Unity Catalog · Delta Sharing)
//   2. Configure (varies by source) + Test connection
//   3. Review & connect (alias + included schemas/tables)

import {
  Fragment,
  useCallback,
  useEffect,
  useMemo,
  useState,
  type ChangeEvent,
  type DragEvent,
  type MouseEvent,
  type ReactNode,
} from 'react';
import { IconChevR, IconClose, IconKey, IconTable } from '../components/icons.tsx';
import {
  LOCAL_DISCOVERY,
  OBJECT_STORE_PROVIDERS,
  SOURCES,
  availabilityForSource,
  type DiscoveryPayload,
  type ObjectStoreProvider,
  type SourceId,
} from './data.ts';
import { IconCheck, IconFolder, IconLock, IconShareNode, IconWarn } from './icons.tsx';
import type { ConnectForm, ConnectResult, SchemaSelection, TestState } from './types.ts';
import type { ConnectorFeatureFlags } from '../../services/connector-features.ts';

type Props = {
  initialStep?: 1 | 2 | 3;
  initialSource?: SourceId | null;
  serverFallbackEnabled: boolean;
  connectorFeatures: ConnectorFeatureFlags;
  onClose: () => void;
  onConnect: (result: ConnectResult) => void;
};

const DEFAULT_FORM: ConnectForm = {
  path: '',
  detected: null,
  provider: 'gcs',
  uri: '',
  region: '',
  endpoint: '',
  uc_mode: 'databricks',
  uc_host: '',
  uc_bff_url: '',
  uc_session_label: '',
  uc_catalog: '',
  uc_schema_filter: '',
  ds_mode: 'profile',
  ds_profile_name: '',
  ds_endpoint: '',
  ds_share: '',
};

const SOURCE_LABEL_DEFAULT: Record<SourceId, string> = {
  local: 'local-orders',
  object_store: 'acme-lake',
  unity_catalog: 'acme-uc',
  delta_share: 'acme-partner',
};

export function ConnectModal({
  initialStep = 1,
  initialSource = null,
  serverFallbackEnabled,
  connectorFeatures,
  onClose,
  onConnect,
}: Props) {
  const initialSourceEnabled =
    initialSource == null || availabilityForSource(initialSource, connectorFeatures).enabled;
  const [step, setStep] = useState<1 | 2 | 3>(initialSourceEnabled ? initialStep : 1);
  const [source, setSource] = useState<SourceId | null>(
    initialSourceEnabled ? initialSource : null,
  );
  const [form, setForm] = useState<ConnectForm>(DEFAULT_FORM);
  const [testState, setTestState] = useState<TestState>(null);
  const [alias, setAlias] = useState('');
  const [selection, setSelection] = useState<Record<string, SchemaSelection>>({});

  // ─── ESC closes the modal ─────────────────────────
  useEffect(() => {
    const h = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose();
    };
    window.addEventListener('keydown', h);
    return () => window.removeEventListener('keydown', h);
  }, [onClose]);

  // ─── Pre-fill sample defaults on first arrival to step 2 ─
  useEffect(() => {
    if (step === 2 && source === 'object_store' && !form.uri) {
      setForm((f) => ({
        ...f,
        uri: 'gs://acme-lake/silver',
        region: 'us-central1',
        endpoint: 'browser-local',
      }));
    }
    if (step === 2 && source === 'unity_catalog' && !form.uc_host) {
      setForm((f) => ({
        ...f,
        uc_host: 'https://acme-prod.cloud.databricks.com',
        uc_bff_url: '/api/uc/read-access-plan',
        uc_session_label: 'Signed in through Axon BFF',
      }));
    }
  }, [step, source, form.uri, form.uc_host]);

  // ─── Seed alias + selection when entering step 3 ───
  // Intentionally only fires on step transition; alias is read but not a dep
  // because the user may have already typed something.
  const seededRef = useState({ done: false })[0];
  useEffect(() => {
    if (step !== 3 || !source || seededRef.done) return;
    if (!alias) setAlias(SOURCE_LABEL_DEFAULT[source]);
    const sel: Record<string, SchemaSelection> = {};
    const discovery = discoveryForSource(source);
    if (!discovery) return;
    discovery.schemas.forEach((s) => {
      sel[s.name] = s.included ? 'all' : 'none';
    });
    setSelection(sel);
    seededRef.done = true;
  }, [step, source, alias, seededRef]);

  const runTest = useCallback(() => {
    setTestState('running');
    setTestState('err');
  }, []);

  const next = () => {
    if (step === 1 && source && availabilityForSource(source, connectorFeatures).enabled) {
      setStep(2);
      setTestState(null);
    } else if (step === 2) {
      if (testState !== 'ok') {
        runTest();
        return;
      }
      setStep(3);
    } else if (step === 3 && source) {
      const discovered = discoveryForSource(source);
      if (!discovered) return;
      onConnect({
        source,
        form,
        alias,
        selection,
        discovered,
      });
    }
  };
  const back = () => {
    if (step === 1) return onClose();
    setStep((s) => (s === 3 ? 2 : 1));
  };

  const canNext =
    step === 1
      ? !!source && availabilityForSource(source, connectorFeatures).enabled
      : step === 2
        ? source === 'local'
          ? !!form.path
          : source === 'object_store'
            ? form.uri.length > 8
            : source === 'unity_catalog'
              ? form.uc_host.length > 8 && form.uc_bff_url.length > 0
              : source === 'delta_share'
                ? form.ds_mode === 'profile'
                  ? !!form.ds_profile_name
                  : form.ds_endpoint.length > 8
                : false
        : step === 3
          ? !!alias
          : true;

  const titles: Record<1 | 2 | 3, { t: string; s: string }> = {
    1: { t: 'Connect a Delta source', s: 'Choose where your Delta tables live.' },
    2: { t: titleForConfig(source), s: subtitleForConfig(source) },
    3: { t: 'Review & name catalog', s: 'Pick what to include and label this connection.' },
  };

  return (
    <div
      className="cc-overlay"
      onClick={(e) => {
        if (e.target === e.currentTarget) onClose();
      }}
    >
      <div className="cc-modal" role="dialog" aria-labelledby="cc-modal-title">
        <header className="cc-head">
          <div>
            <h3 id="cc-modal-title">{titles[step].t}</h3>
            <div className="sub">{titles[step].s}</div>
          </div>
          <div className="cc-steps">
            {[
              { n: 1 as const, label: 'Source' },
              { n: 2 as const, label: 'Configure' },
              { n: 3 as const, label: 'Review' },
            ].map((p, i) => (
              <Fragment key={p.n}>
                <span className={'cc-step ' + (step === p.n ? 'active' : step > p.n ? 'done' : '')}>
                  <span className="n">{step > p.n ? <IconCheck size={9} /> : p.n}</span>
                  {p.label}
                </span>
                {i < 2 && <span className="cc-bar" />}
              </Fragment>
            ))}
          </div>
          <button className="cc-x" onClick={onClose} title="Close (Esc)">
            <IconClose size={13} />
          </button>
        </header>

        <div className="cc-body">
          {step === 1 && (
            <SourcePicker
              value={source}
              connectorFeatures={connectorFeatures}
              onChange={setSource}
            />
          )}
          {step === 2 && source === 'local' && (
            <ConfigLocal form={form} setForm={setForm} testState={testState} />
          )}
          {step === 2 && source === 'object_store' && (
            <ConfigObjectStore
              form={form}
              setForm={setForm}
              serverFallbackEnabled={serverFallbackEnabled}
              testState={testState}
            />
          )}
          {step === 2 && source === 'unity_catalog' && (
            <ConfigUnityCatalog form={form} setForm={setForm} testState={testState} />
          )}
          {step === 2 && source === 'delta_share' && (
            <ConfigDeltaShare form={form} setForm={setForm} testState={testState} />
          )}
          {step === 3 && source && (
            <Discover
              sourceId={source}
              discovered={discoveryForSource(source) ?? LOCAL_DISCOVERY}
              alias={alias}
              setAlias={setAlias}
              selection={selection}
              setSelection={setSelection}
            />
          )}
        </div>

        <footer className="cc-foot">
          <button className="cc-btn" onClick={back}>
            {step === 1 ? 'Cancel' : 'Back'}
          </button>
          {step === 2 && (
            <button
              className="cc-btn lg"
              onClick={runTest}
              disabled={!canNext || testState === 'running'}
            >
              {testState === 'running' ? <span className="cc-spin" /> : <IconCheck size={12} />}
              Test connection
            </button>
          )}
          <span className="cc-hint">
            {step === 2 && testState === 'ok' && (
              <span
                style={{
                  color: 'var(--success)',
                  display: 'inline-flex',
                  alignItems: 'center',
                  gap: 6,
                }}
              >
                <IconCheck size={11} /> source check passed
              </span>
            )}
            {step === 3 && (
              <>
                Catalog will be available as{' '}
                <code style={{ font: '11.5px var(--mono)' }}>{alias || 'alias'}</code>
              </>
            )}
          </span>
          <span className="cc-spacer" />
          <button
            className="cc-btn primary lg"
            disabled={!canNext || (step === 2 && testState !== 'ok')}
            onClick={next}
          >
            {step === 1 && (
              <>
                Continue <IconChevR size={11} />
              </>
            )}
            {step === 2 && (
              <>
                Discover tables <IconChevR size={11} />
              </>
            )}
            {step === 3 && (
              <>
                Connect catalog <IconCheck size={11} />
              </>
            )}
          </button>
        </footer>
      </div>
    </div>
  );
}

// ─── Source picker (step 1) ─────────────────────────────
function SourcePicker({
  value,
  connectorFeatures,
  onChange,
}: {
  value: SourceId | null;
  connectorFeatures: ConnectorFeatureFlags;
  onChange: (id: SourceId) => void;
}) {
  return (
    <>
      <p className="cc-intro">
        Connect Delta Lake sources through browser-local snapshot reconstruction.{' '}
        <span className="k">Governed catalogs can still use brokered contracts when needed.</span>
      </p>

      <div className="cc-source-grid">
        {SOURCES.map((s) => {
          const availability = availabilityForSource(s, connectorFeatures);
          const disabled = !availability.enabled;
          const tags = availability.label ? [...s.tags, availability.label] : s.tags;

          return (
            <div
              key={s.id}
              aria-disabled={disabled ? 'true' : undefined}
              className={
                'cc-source-card ' +
                (value === s.id ? 'selected ' : '') +
                (disabled ? 'disabled' : '')
              }
              onClick={() => {
                if (!disabled) onChange(s.id);
              }}
              title={disabled ? `${s.title}: ${availability.reason}` : undefined}
            >
              <span className="pick-ind">
                <IconCheck size={10} />
              </span>
              <div className="tags">
                {tags.map((t) => (
                  <span key={t} className="tag">
                    {t}
                  </span>
                ))}
              </div>
              <div className={'glyph ' + s.glyphTone}>{s.glyph}</div>
              <div className="title">{s.title}</div>
              <div className="blurb">{s.blurb}</div>
              <div className="owner-map" aria-label={`${s.title} runtime ownership`}>
                <div>
                  <span>Access</span>
                  <b>{s.owners.access}</b>
                </div>
                <div>
                  <span>Snapshot</span>
                  <b>{s.owners.snapshot}</b>
                </div>
                <div>
                  <span>Query</span>
                  <b>{s.owners.query}</b>
                </div>
              </div>
              {disabled && (
                <div className="cc-source-status">
                  <b>{availability.label}</b>
                  <span>{availability.reason}</span>
                </div>
              )}
              <div className="examples">{s.examples}</div>
            </div>
          );
        })}
      </div>

      <div className="cc-section-head">What you&apos;ll need</div>
      <div className="cc-need-list">
        <div className="cc-need">
          <span className="ico">
            <IconFolder size={14} />
          </span>
          <div>
            <b>Local files</b>
            <br />A browser file handle for a Delta table directory containing a{' '}
            <code style={{ font: '11px var(--mono)' }}>_delta_log/</code>.
          </div>
        </div>
        <div className="cc-need">
          <span className="ico">
            <IconLock size={14} />
          </span>
          <div>
            <b>Object storage</b>
            <br />A bucket URI with browser-readable Delta log and Parquet objects.
          </div>
        </div>
        <div className="cc-need">
          <span className="ico">
            <IconKey size={14} />
          </span>
          <div>
            <b>Unity Catalog</b>
            <br />
            An authenticated Axon session backed by a UC read-access-plan broker.
          </div>
        </div>
        <div className="cc-need">
          <span className="ico">
            <IconShareNode size={14} />
          </span>
          <div>
            <b>Delta Sharing</b>
            <br />A provider profile registered with a trusted sharing broker.
          </div>
        </div>
      </div>
    </>
  );
}

// ─── Local config ───────────────────────────────────────
function ConfigLocal({
  form,
  setForm,
  testState,
}: {
  form: ConnectForm;
  setForm: (f: ConnectForm) => void;
  testState: TestState;
}) {
  const [over, setOver] = useState(false);
  const detected = form.detected;

  const dropOrPick = () => {
    setOver(false);
  };

  return (
    <div className="cc-config-grid">
      <div>
        <div
          className={'cc-drop ' + (over ? 'over' : '')}
          onDragOver={(e: DragEvent) => {
            e.preventDefault();
            setOver(true);
          }}
          onDragLeave={() => setOver(false)}
          onDrop={(e: DragEvent) => {
            e.preventDefault();
            dropOrPick();
          }}
          onClick={dropOrPick}
        >
          <div className="glyph">
            <IconFolder size={22} />
          </div>
          <div className="ti">Drag a Delta table folder here</div>
          <div className="sub">
            Browser file-handle discovery is not wired in this editor flow yet.
          </div>
          <button className="browse">
            <IconFolder size={11} /> Browse…
          </button>
        </div>

        <div className="cc-field" style={{ marginTop: 14 }}>
          <label className="cc-label">Folder path</label>
          <div className="cc-input-wrap">
            <input
              className="cc-input mono"
              placeholder="/path/to/your/delta_table"
              value={form.path}
              onChange={(e: ChangeEvent<HTMLInputElement>) =>
                setForm({
                  ...form,
                  path: e.target.value,
                  detected: null,
                })
              }
            />
            {detected && (
              <div className="right">
                <span className="check" title="Detected Delta protocol">
                  <IconCheck size={11} />
                </span>
              </div>
            )}
          </div>
          <div className="cc-help">
            The sandbox runtime can ingest browser-selected local files. The Connect Catalog editor
            will stay blocked until that file-handle path creates a queryable descriptor here.
          </div>
        </div>

        {detected && (
          <div className="cc-detected">
            <div className="row">
              <span className="ico">
                <IconTable size={14} />
              </span>
              <div style={{ minWidth: 0, flex: 1 }}>
                <div className="name">{detected.name}</div>
                <div className="path">
                  {form.path}/_delta_log/00000000000000000{detected.snapshot}.json
                </div>
              </div>
              <span style={{ font: '11.5px var(--mono)', color: 'var(--accent)' }}>
                v{detected.snapshot}
              </span>
            </div>
            <div className="stats">
              <div>
                <div className="lbl">Rows</div>
                <div className="val">{detected.rowsLabel}</div>
              </div>
              <div>
                <div className="lbl">Files</div>
                <div className="val">{detected.files}</div>
              </div>
              <div>
                <div className="lbl">Size</div>
                <div className="val">{detected.size}</div>
              </div>
              <div>
                <div className="lbl">Protocol</div>
                <div className="val">{detected.protocol}</div>
              </div>
            </div>
          </div>
        )}

        <TestResult
          state={testState}
          okText="Delta log parsed · 1 table ready"
          okDetail="Snapshot version 318 · 412 files · last commit 14:08 UTC. Protocol features: columnMapping."
          errText="Editor local file discovery is not wired"
          errDetail="Use the sandbox local import path for now, or wire File System Access handles into a BrowserHttpSnapshotDescriptor before discovery."
        />
      </div>

      <aside className="cc-helper">
        <h5>How local tables work</h5>
        <p>
          The runtime has browser-local file ingestion, but this Connect Catalog wizard does not yet
          pass File System Access handles into a queryable descriptor. It does not claim a local
          catalog is connected until that path is available here.
        </p>
        <hr />
        <h5>Tips</h5>
        <ul>
          <li>
            Point at the table root (where <code>_delta_log/</code> lives), not at an individual{' '}
            <code>.parquet</code> file.
          </li>
          <li>Do not use this editor flow as proof that zipped local tables are connected yet.</li>
          <li>Use Object Storage instead for shared / cloud datasets.</li>
        </ul>
      </aside>
    </div>
  );
}

// ─── Object storage config ──────────────────────────────
function ConfigObjectStore({
  form,
  setForm,
  serverFallbackEnabled,
  testState,
}: {
  form: ConnectForm;
  setForm: (f: ConnectForm) => void;
  serverFallbackEnabled: boolean;
  testState: TestState;
}) {
  const provider = OBJECT_STORE_PROVIDERS.find(
    (p) => p.id === form.provider,
  ) as ObjectStoreProvider;
  const okURI = form.uri.startsWith(provider.scheme);

  return (
    <div className="cc-config-grid">
      <div>
        <div className="cc-field">
          <label className="cc-label">Provider</label>
          <div className="cc-seg">
            {OBJECT_STORE_PROVIDERS.map((p) => (
              <button
                key={p.id}
                className={form.provider === p.id ? 'active' : ''}
                onClick={() => setForm({ ...form, provider: p.id })}
              >
                <span className={'g ' + p.id}>{p.scheme.replace('://', '').toUpperCase()}</span>
                {p.label}
              </button>
            ))}
          </div>
        </div>

        <div className="cc-field">
          <label className="cc-label">Bucket or prefix URI</label>
          <div className="cc-input-wrap">
            <span className="prefix">{provider.scheme}</span>
            <input
              className="cc-input mono has-prefix"
              placeholder={provider.placeholder.replace(provider.scheme, '')}
              value={form.uri.replace(provider.scheme, '')}
              onChange={(e: ChangeEvent<HTMLInputElement>) =>
                setForm({ ...form, uri: provider.scheme + e.target.value })
              }
            />
            {okURI && form.uri.length > provider.scheme.length + 4 && (
              <div className="right">
                <span className="check">
                  <IconCheck size={11} />
                </span>
              </div>
            )}
          </div>
          <div className="cc-help">
            Point at the bucket, a folder containing many Delta tables, or a single table root. Axon
            auto-detects table boundaries by walking for <code>_delta_log/</code>.
          </div>
        </div>

        <div className="cc-row-2">
          <div className="cc-field">
            <label className="cc-label">
              Region <span className="opt">· optional</span>
            </label>
            <select
              className="cc-select"
              value={form.region}
              onChange={(e: ChangeEvent<HTMLSelectElement>) =>
                setForm({ ...form, region: e.target.value })
              }
            >
              <option value="">Auto-detect</option>
              {provider.regions.map((r: string) => (
                <option key={r} value={r}>
                  {r}
                </option>
              ))}
            </select>
          </div>
          <div className="cc-field">
            <label className="cc-label">Browser-local Delta log access</label>
            <input
              className="cc-input mono"
              placeholder="CORS-enabled HTTPS or browser storage adapter"
              value={form.endpoint}
              onChange={(e: ChangeEvent<HTMLInputElement>) =>
                setForm({ ...form, endpoint: e.target.value })
              }
            />
            <div className="cc-help">
              Axon lists and reads <code>_delta_log/</code> in the browser, then builds the
              BrowserHttpSnapshotDescriptor locally.
            </div>
          </div>
        </div>

        <TestResult
          state={testState}
          okText={`${provider.label} Delta log is browser-readable`}
          okDetail="The browser can reconstruct the snapshot and range-read active Parquet files."
          errText="Browser-local storage access not configured"
          errDetail="Configure CORS-enabled object access or a browser storage adapter before discovery."
        />
      </div>

      <aside className="cc-helper">
        <h5>How Axon reads {provider.label}</h5>
        <p>
          The browser reads the Delta log, reconstructs the active snapshot, and then range-reads
          active Parquet files directly through the browser runtime.
          {serverFallbackEnabled
            ? ' Server query fallback can route unsupported table features to your configured query service.'
            : ' If a table requires features this browser build cannot serve, the query stops with a structured browser error.'}
        </p>
        <hr />
        <h5>Required permissions</h5>
        <ul>
          <li>Browser can list or receive a manifest for the table&apos;s Delta log.</li>
          <li>Browser can range-read Delta log and active Parquet objects.</li>
        </ul>
        <hr />
        <h5>Network</h5>
        <p>
          Browser egress is limited to the table objects needed to reconstruct the snapshot and run
          the query. No query service is required for the default path.
        </p>
      </aside>
    </div>
  );
}

// ─── Unity Catalog config ───────────────────────────────
function ConfigUnityCatalog({
  form,
  setForm,
  testState,
}: {
  form: ConnectForm;
  setForm: (f: ConnectForm) => void;
  testState: TestState;
}) {
  return (
    <div className="cc-config-grid">
      <div>
        <div className="cc-field">
          <label className="cc-label">Deployment</label>
          <div className="cc-seg">
            <button
              className={form.uc_mode === 'databricks' ? 'active' : ''}
              onClick={() => setForm({ ...form, uc_mode: 'databricks' })}
            >
              <span className="g" style={{ background: '#7A3CD7' }}>
                UC
              </span>
              Databricks-hosted
            </button>
            <button
              className={form.uc_mode === 'oss' ? 'active' : ''}
              onClick={() => setForm({ ...form, uc_mode: 'oss' })}
            >
              <span className="g" style={{ background: 'var(--ink-2)' }}>
                OSS
              </span>
              Open-source UC server
            </button>
          </div>
        </div>

        <div className="cc-field">
          <label className="cc-label">
            {form.uc_mode === 'databricks' ? 'Workspace URL' : 'UC server endpoint'}
          </label>
          <input
            className="cc-input mono"
            placeholder={
              form.uc_mode === 'databricks'
                ? 'https://acme-prod.cloud.databricks.com'
                : 'https://uc.acme.internal/api/2.1/unity-catalog'
            }
            value={form.uc_host}
            onChange={(e: ChangeEvent<HTMLInputElement>) =>
              setForm({ ...form, uc_host: e.target.value })
            }
          />
          <div className="cc-help">
            The browser sends this as table-planning context only. UC credentials stay in the
            authenticated service session.
          </div>
        </div>

        <div className="cc-row-2">
          <div className="cc-field">
            <label className="cc-label">Read-access broker</label>
            <input
              className="cc-input mono"
              placeholder="/api/uc/read-access-plan"
              value={form.uc_bff_url}
              onChange={(e: ChangeEvent<HTMLInputElement>) =>
                setForm({ ...form, uc_bff_url: e.target.value })
              }
            />
            <div className="cc-help">
              Same-origin BFF route that returns <code>ReadAccessPlan</code> responses.
            </div>
          </div>
          <div className="cc-field">
            <label className="cc-label">
              Session
              <span className="opt">
                <IconLock size={9} /> · service-owned
              </span>
            </label>
            <input
              className="cc-input mono"
              placeholder="Signed in through Axon BFF"
              value={form.uc_session_label}
              onChange={(e: ChangeEvent<HTMLInputElement>) =>
                setForm({ ...form, uc_session_label: e.target.value })
              }
            />
            <div className="cc-help">No browser-owned tokens or cloud keys are accepted here.</div>
          </div>
        </div>

        <div className="cc-row-2">
          <div className="cc-field">
            <label className="cc-label">
              Scope to catalog <span className="opt">· optional</span>
            </label>
            <input
              className="cc-input mono"
              placeholder="main"
              value={form.uc_catalog}
              onChange={(e: ChangeEvent<HTMLInputElement>) =>
                setForm({ ...form, uc_catalog: e.target.value })
              }
            />
            <div className="cc-help">Leave blank to list all reachable catalogs.</div>
          </div>
          <div className="cc-field">
            <label className="cc-label">
              Schema filter <span className="opt">· optional</span>
            </label>
            <input
              className="cc-input mono"
              placeholder="analytics, raw, ml_*"
              value={form.uc_schema_filter}
              onChange={(e: ChangeEvent<HTMLInputElement>) =>
                setForm({ ...form, uc_schema_filter: e.target.value })
              }
            />
            <div className="cc-help">Comma-separated. Glob patterns OK.</div>
          </div>
        </div>

        <TestResult
          state={testState}
          okText="Read-access broker reachable · 3 schemas, 12 tables"
          okDetail={
            <>
              Session-backed broker returned contract-first plans for <code>main.analytics</code>,{' '}
              <code>main.raw</code>, and blocked <code>main.ml_models</code> before descriptor
              handoff.
            </>
          }
        />
      </div>

      <aside className="cc-helper">
        <h5>Governed reads</h5>
        <p>
          Axon consumes UC <code>ReadAccessPlan</code> responses. The service evaluates grants,
          policy, object grants, and SQL fallback before the browser reconstructs or materializes a
          descriptor.
        </p>
        <hr />
        <h5>Permissions used</h5>
        <ul>
          <li>
            <code>USE CATALOG</code> on the target catalog
          </li>
          <li>
            <code>USE SCHEMA</code> on each schema
          </li>
          <li>
            <code>SELECT</code> on each table you want to query
          </li>
          <li>
            <code>EXTERNAL USE SCHEMA</code> for direct cloud reads
          </li>
        </ul>
        <hr />
        <h5>Storage credentials</h5>
        <p>
          The browser receives brokered descriptors, Delta Sharing files, structured fallback
          states, or blocked states. It never owns UC tokens or cloud credential material.
        </p>
      </aside>
    </div>
  );
}

// ─── Delta Sharing config ───────────────────────────────
function ConfigDeltaShare({
  form,
  setForm,
  testState,
}: {
  form: ConnectForm;
  setForm: (f: ConnectForm) => void;
  testState: TestState;
}) {
  const [over, setOver] = useState(false);
  const mode = form.ds_mode;

  const handleProfilePick = () => {
    setOver(false);
    setForm({
      ...form,
      ds_profile_name: 'acme-partner-profile',
    });
  };

  return (
    <div className="cc-config-grid">
      <div>
        <div className="cc-field">
          <label className="cc-label">Connect using</label>
          <div className="cc-seg">
            <button
              className={mode === 'profile' ? 'active' : ''}
              onClick={() => setForm({ ...form, ds_mode: 'profile' })}
            >
              <span className="g" style={{ background: '#0891B2' }}>
                DS
              </span>
              Brokered profile
            </button>
            <button
              className={mode === 'manual' ? 'active' : ''}
              onClick={() => setForm({ ...form, ds_mode: 'manual' })}
            >
              <span className="g" style={{ background: 'var(--ink-2)' }}>
                URL
              </span>
              Provider endpoint
            </button>
          </div>
        </div>

        {mode === 'profile' ? (
          <>
            <div
              className={'cc-drop ' + (over ? 'over' : '')}
              onDragOver={(e: DragEvent) => {
                e.preventDefault();
                setOver(true);
              }}
              onDragLeave={() => setOver(false)}
              onDrop={(e: DragEvent) => {
                e.preventDefault();
                handleProfilePick();
              }}
              onClick={handleProfilePick}
            >
              <div className="glyph">
                <IconKey size={18} />
              </div>
              <div className="ti">Select a brokered provider profile</div>
              <div className="sub">
                The trusted BFF owns provider credentials and returns browser-safe file actions or
                descriptors.
              </div>
              <button className="browse">
                <IconFolder size={11} /> Choose profile
              </button>
            </div>

            {form.ds_profile_name && (
              <div className="cc-detected" style={{ marginTop: 14 }}>
                <div className="row">
                  <span className="ico">
                    <IconKey size={14} />
                  </span>
                  <div style={{ minWidth: 0, flex: 1 }}>
                    <div className="name">{form.ds_profile_name}</div>
                    <div className="path">profile handle · sharing broker · protocol v1</div>
                  </div>
                  <span style={{ font: '11.5px var(--mono)', color: 'var(--success)' }}>
                    selected
                  </span>
                </div>
              </div>
            )}

            <div className="cc-field" style={{ marginTop: 14 }}>
              <label className="cc-label">Profile handle</label>
              <input
                className="cc-input mono"
                placeholder="partner-profile-id"
                value={form.ds_profile_name}
                onChange={(e: ChangeEvent<HTMLInputElement>) =>
                  setForm({ ...form, ds_profile_name: e.target.value })
                }
              />
              <div className="cc-help">
                The browser stores only this handle. Provider profile material remains with the
                trusted sharing broker.
              </div>
            </div>
          </>
        ) : (
          <>
            <div className="cc-field">
              <label className="cc-label">Sharing server endpoint</label>
              <input
                className="cc-input mono"
                placeholder="https://sharing.acme.io/delta-sharing"
                value={form.ds_endpoint}
                onChange={(e: ChangeEvent<HTMLInputElement>) =>
                  setForm({ ...form, ds_endpoint: e.target.value })
                }
              />
              <div className="cc-help">
                Endpoint context for the trusted sharing broker. Browser code does not authenticate
                directly to the provider.
              </div>
            </div>
            <div className="cc-field">
              <label className="cc-label">Sharing broker endpoint</label>
              <input
                className="cc-input mono"
                placeholder="/api/delta-sharing/resolve"
                value={form.ds_profile_name}
                onChange={(e: ChangeEvent<HTMLInputElement>) =>
                  setForm({ ...form, ds_profile_name: e.target.value })
                }
              />
              <div className="cc-help">
                Same-origin BFF route that exchanges the provider context for descriptor contracts.
              </div>
            </div>
          </>
        )}

        <div className="cc-row-2">
          <div className="cc-field">
            <label className="cc-label">
              Scope to share <span className="opt">· optional</span>
            </label>
            <input
              className="cc-input mono"
              placeholder="acme_partner"
              value={form.ds_share}
              onChange={(e: ChangeEvent<HTMLInputElement>) =>
                setForm({ ...form, ds_share: e.target.value })
              }
            />
            <div className="cc-help">Limit discovery to a single share.</div>
          </div>
          <div className="cc-field">
            <label className="cc-label">
              Recipient identity <span className="opt">· read-only</span>
            </label>
            <input
              className="cc-input mono"
              disabled
              value={form.ds_profile_name ? 'acme-analytics@axon' : '—'}
              readOnly
            />
            <div className="cc-help">Resolved from the profile when present.</div>
          </div>
        </div>

        <TestResult
          state={testState}
          okText="Sharing broker returned descriptors"
          okDetail={
            <>Trusted broker returned share metadata and descriptor URLs for allowed tables.</>
          }
        />
      </div>

      <aside className="cc-helper">
        <h5>What is Delta Sharing?</h5>
        <p>
          An open protocol for sharing Delta tables across clouds and organisations. In this browser
          build, provider authentication belongs to the trusted sharing broker.
        </p>
        <hr />
        <h5>You&apos;ll get</h5>
        <ul>
          <li>One catalog per configured sharing broker endpoint</li>
          <li>
            <code>share/schema</code> mapped to Axon schemas
          </li>
          <li>Read-only access — Axon never writes back</li>
          <li>Descriptor refresh is owned by the trusted broker</li>
        </ul>
        <hr />
        <h5>Security</h5>
        <p>
          Repo-owned browser code stores only profile handles and endpoint context. It does not
          persist provider authentication material.
        </p>
      </aside>
    </div>
  );
}

// ─── Test-connection result strip ──────────────────────
function TestResult({
  state,
  okText,
  okDetail,
  errText = 'Connection path not configured',
  errDetail = 'Configure the selected source before discovery.',
}: {
  state: TestState;
  okText: string;
  okDetail: ReactNode;
  errText?: ReactNode;
  errDetail?: ReactNode;
}) {
  if (!state) return null;
  if (state === 'running') {
    return (
      <div className="cc-test-result run">
        <span className="cc-spin" />
        <div className="body">
          <div className="t">Checking connection…</div>
          <div className="d">Verifying that the selected source can be opened for discovery.</div>
        </div>
      </div>
    );
  }
  if (state === 'ok') {
    return (
      <div className="cc-test-result ok">
        <span className="ico">
          <IconCheck size={14} />
        </span>
        <div className="body">
          <div className="t">{okText}</div>
          <div className="d">{okDetail}</div>
        </div>
      </div>
    );
  }
  return (
    <div className="cc-test-result err">
      <span className="ico">
        <IconWarn size={14} />
      </span>
      <div className="body">
        <div className="t">{errText}</div>
        <div className="d">{errDetail}</div>
      </div>
    </div>
  );
}

// ─── Step 3: discover & review ──────────────────────────
function Discover({
  sourceId,
  discovered,
  alias,
  setAlias,
  selection,
  setSelection,
}: {
  sourceId: SourceId;
  discovered: DiscoveryPayload;
  alias: string;
  setAlias: (a: string) => void;
  selection: Record<string, SchemaSelection>;
  setSelection: (s: Record<string, SchemaSelection>) => void;
}) {
  const disc = discovered;
  const included = useMemo(() => countIncluded(disc, selection), [disc, selection]);
  const total = useMemo(() => disc.schemas.reduce((a, s) => a + s.tables.length, 0), [disc]);

  return (
    <>
      <div className="cc-discover-summary">
        <span className="check">
          <IconCheck size={12} />
        </span>
        <div className="text" dangerouslySetInnerHTML={{ __html: disc.summary }} />
        <div className="meta">local parse</div>
      </div>

      <div className="cc-section-head">Pick what to include</div>
      <div className="cc-disc-tree">
        {disc.schemas.map((s) => (
          <SchemaRow
            key={s.name}
            schema={s}
            selection={selection[s.name] ?? (s.included ? 'all' : 'none')}
            onSchemaToggle={(state) => setSelection({ ...selection, [s.name]: state })}
            onTableToggle={(t, on) => {
              const cur = selection[s.name] ?? (s.included ? 'all' : 'none');
              let nextSel: SchemaSelection;
              if (cur === 'all') {
                nextSel = on ? 'all' : { except: [t] };
              } else if (cur === 'none') {
                nextSel = on ? { only: [t] } : 'none';
              } else if ('except' in cur) {
                const exc = on ? cur.except.filter((x) => x !== t) : [...cur.except, t];
                nextSel = exc.length === 0 ? 'all' : { except: exc };
              } else {
                const only = on ? [...cur.only, t] : cur.only.filter((x) => x !== t);
                nextSel = only.length === 0 ? 'none' : { only };
              }
              setSelection({ ...selection, [s.name]: nextSel });
            }}
          />
        ))}
      </div>

      <div className="cc-section-head">Catalog details</div>
      <div className="cc-final-grid">
        <div className="cc-final-card">
          <h6 className="h">Connection</h6>
          <div className="row">
            <span className="l">Source</span>
            <span className="v">{labelForSource(sourceId)}</span>
          </div>
          <div className="row">
            <span className="l">Endpoint</span>
            <span className="v">{endpointFor(sourceId)}</span>
          </div>
          <div className="row">
            <span className="l">Access</span>
            <span className="v">{runtimeOwnersFor(sourceId).access}</span>
          </div>
          <div className="row">
            <span className="l">Snapshot</span>
            <span className="v">{runtimeOwnersFor(sourceId).snapshot}</span>
          </div>
          <div className="row">
            <span className="l">Query</span>
            <span className="v">{runtimeOwnersFor(sourceId).query}</span>
          </div>
        </div>
        <div className="cc-final-card">
          <h6 className="h">In Axon</h6>
          <div className="cc-field" style={{ marginBottom: 8 }}>
            <label className="cc-label">Catalog alias</label>
            <input
              className="cc-input mono"
              value={alias}
              onChange={(e: ChangeEvent<HTMLInputElement>) => setAlias(e.target.value)}
            />
            <div className="cc-help">
              SQL prefix: <code>{alias || 'your-catalog'}.schema.table</code>
            </div>
          </div>
          <div className="row">
            <span className="l">Tables included</span>
            <span className="v">
              {included} of {total}
            </span>
          </div>
          <div className="row">
            <span className="l">Cache strategy</span>
            <span className="v">OPFS · 512 MB budget</span>
          </div>
          <div className="row">
            <span className="l">Refresh</span>
            <span className="v">on first query</span>
          </div>
        </div>
      </div>
    </>
  );
}

function SchemaRow({
  schema,
  selection,
  onSchemaToggle,
  onTableToggle,
}: {
  schema: DiscoveryPayload['schemas'][number];
  selection: SchemaSelection;
  onSchemaToggle: (next: SchemaSelection) => void;
  onTableToggle: (tableName: string, on: boolean) => void;
}) {
  const [open, setOpen] = useState(true);
  const includedAll = selection === 'all';
  const includedNone = selection === 'none';
  const isIncluded = (t: string) => {
    if (selection === 'all') return true;
    if (selection === 'none') return false;
    if ('except' in selection) return !selection.except.includes(t);
    return selection.only.includes(t);
  };
  const cbState: 'on' | 'off' | 'mixed' = includedAll ? 'on' : includedNone ? 'off' : 'mixed';
  const partial = selection !== 'all' && selection !== 'none';

  return (
    <div className={'cc-disc-schema ' + (open ? 'open' : '')}>
      <div className="head" onClick={() => setOpen(!open)}>
        <span className="twist">
          <IconChevR size={9} />
        </span>
        <span
          className={'cb ' + (cbState === 'off' ? '' : cbState)}
          onClick={(e: MouseEvent) => {
            e.stopPropagation();
            onSchemaToggle(includedAll ? 'none' : 'all');
          }}
        >
          {cbState === 'on' && <IconCheck size={10} />}
        </span>
        <span className="name">{schema.name}</span>
        <span className="count">{schema.tables.length} tables</span>
        {schema.governed && <span className="pill">governed</span>}
        {schema.shared && <span className="pill">shared</span>}
        <div className="right">
          {partial && <span style={{ color: 'var(--accent)' }}>partial</span>}
        </div>
      </div>
      {open &&
        schema.tables.map((t) => {
          const on = isIncluded(t.name);
          return (
            <div key={t.name} className={'cc-disc-table ' + (on ? '' : 'excluded')}>
              <span className={'cb ' + (on ? 'on' : '')} onClick={() => onTableToggle(t.name, !on)}>
                {on && <IconCheck size={10} />}
              </span>
              <span className="name">
                <span className="ico">
                  <IconTable size={11} />
                </span>
                {t.name}
              </span>
              <span className="v">v{t.snapshot}</span>
              <span className="num">{compactRows(t.rows)} rows</span>
              <span className="num">{(t.size || '').trim()}</span>
              {t.perm ? (
                <span className="perm">{t.perm}</span>
              ) : (
                <span className="proto">
                  {t.protocol}
                  {t.features ? ' · ' + t.features.length + ' ft' : ''}
                </span>
              )}
            </div>
          );
        })}
    </div>
  );
}

function compactRows(n: number | null | undefined) {
  if (n == null) return '—';
  if (n < 1_000) return n.toString();
  if (n < 1_000_000) return (n / 1_000).toFixed(n < 10_000 ? 1 : 0) + 'k';
  if (n < 1_000_000_000) return (n / 1_000_000).toFixed(n < 10_000_000 ? 1 : 0) + 'M';
  return (n / 1_000_000_000).toFixed(n < 10_000_000_000 ? 1 : 0) + 'B';
}

function countIncluded(disc: DiscoveryPayload, sel: Record<string, SchemaSelection>) {
  let n = 0;
  for (const s of disc.schemas) {
    const v = sel[s.name] ?? (s.included ? 'all' : 'none');
    if (v === 'all') n += s.tables.length;
    else if (v === 'none') continue;
    else if ('except' in v) n += s.tables.length - v.except.length;
    else n += v.only.length;
  }
  return n;
}

function discoveryForSource(source: SourceId): DiscoveryPayload | null {
  return source === 'local' ? LOCAL_DISCOVERY : null;
}

function labelForSource(s: SourceId) {
  return {
    local: 'Local files',
    object_store: 'Object storage (GCS)',
    unity_catalog: 'Unity Catalog (brokered)',
    delta_share: 'Delta Sharing',
  }[s];
}
function endpointFor(s: SourceId) {
  return {
    local: '~/Datasets/acme/silver/orders',
    object_store: 'gs://acme-lake/silver',
    unity_catalog: 'acme-prod.cloud.databricks.com',
    delta_share: 'https://sharing.acme.io/delta-sharing',
  }[s];
}
function runtimeOwnersFor(source: SourceId) {
  return SOURCES.find((candidate) => candidate.id === source)?.owners ?? SOURCES[0].owners;
}

function titleForConfig(s: SourceId | null) {
  if (!s) return 'Configure';
  return {
    local: 'Connect a local Delta folder',
    object_store: 'Connect to object storage',
    unity_catalog: 'Connect to Unity Catalog',
    delta_share: 'Connect a Delta Sharing provider',
  }[s];
}
function subtitleForConfig(s: SourceId | null) {
  if (!s) return '';
  return {
    local: 'Read a Delta table directly from this machine — nothing leaves disk.',
    object_store: 'Bring up an S3, GCS, ADLS, or R2 bucket as a queryable catalog.',
    unity_catalog: 'Use a session-backed broker that returns ReadAccessPlan responses.',
    delta_share: 'Read tables shared by another organisation through the open protocol.',
  }[s];
}
