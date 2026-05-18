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
  AUTH_LABELS,
  DISCOVERED,
  OBJECT_STORE_PROVIDERS,
  SAMPLE_DS_PROFILE,
  SOURCES,
  type AuthMethod,
  type DiscoveryPayload,
  type ObjectStoreProvider,
  type ObjectStoreProviderId,
  type SourceId,
} from './data.ts';
import { IconCheck, IconFolder, IconLock, IconShareNode, IconWarn } from './icons.tsx';
import type { ConnectForm, ConnectResult, SchemaSelection, TestState } from './types.ts';

type Props = {
  initialStep?: 1 | 2 | 3;
  initialSource?: SourceId | null;
  serverFallbackEnabled: boolean;
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
  auth: 'service_account',
  creds: {},
  uc_mode: 'databricks',
  uc_host: '',
  uc_auth: 'pat',
  uc_token: '',
  uc_client_id: '',
  uc_client_secret: '',
  uc_catalog: '',
  uc_schema_filter: '',
  ds_mode: 'profile',
  ds_profile_name: '',
  ds_profile_json: '',
  ds_endpoint: '',
  ds_token: '',
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
  onClose,
  onConnect,
}: Props) {
  const [step, setStep] = useState<1 | 2 | 3>(initialStep);
  const [source, setSource] = useState<SourceId | null>(initialSource);
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
        auth: 'service_account',
      }));
    }
    if (step === 2 && source === 'unity_catalog' && !form.uc_host) {
      setForm((f) => ({
        ...f,
        uc_host: 'https://acme-prod.cloud.databricks.com',
        uc_token: 'dapi••••••••••••••••••••••••••••',
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
    DISCOVERED[source].schemas.forEach((s) => {
      sel[s.name] = s.included ? 'all' : 'none';
    });
    setSelection(sel);
    seededRef.done = true;
  }, [step, source, alias, seededRef]);

  const runTest = useCallback(() => {
    setTestState('running');
    window.setTimeout(() => setTestState('ok'), 900);
  }, []);

  const next = () => {
    if (step === 1 && source) {
      setStep(2);
      setTestState(null);
    } else if (step === 2) {
      if (testState !== 'ok') {
        runTest();
        return;
      }
      setStep(3);
    } else if (step === 3 && source) {
      onConnect({
        source,
        form,
        alias,
        selection,
        discovered: DISCOVERED[source],
      });
    }
  };
  const back = () => {
    if (step === 1) return onClose();
    setStep((s) => (s === 3 ? 2 : 1));
  };

  const canNext =
    step === 1
      ? !!source
      : step === 2
        ? source === 'local'
          ? !!form.path
          : source === 'object_store'
            ? form.uri.length > 8
            : source === 'unity_catalog'
              ? form.uc_host.length > 8
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
          {step === 1 && <SourcePicker value={source} onChange={setSource} />}
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
                <IconCheck size={11} /> connection verified
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
  onChange,
}: {
  value: SourceId | null;
  onChange: (id: SourceId) => void;
}) {
  return (
    <>
      <p className="cc-intro">
        Connect a Delta Lake source to Axon. Axon will read tables in place — nothing is copied.{' '}
        <span className="k">All four sources support the same SQL surface area.</span>
      </p>

      <div className="cc-source-grid">
        {SOURCES.map((s) => (
          <div
            key={s.id}
            className={'cc-source-card ' + (value === s.id ? 'selected' : '')}
            onClick={() => onChange(s.id)}
          >
            <span className="pick-ind">
              <IconCheck size={10} />
            </span>
            <div className="tags">
              {s.tags.map((t) => (
                <span key={t} className="tag">
                  {t}
                </span>
              ))}
            </div>
            <div className={'glyph ' + s.glyphTone}>{s.glyph}</div>
            <div className="title">{s.title}</div>
            <div className="blurb">{s.blurb}</div>
            <div className="examples">{s.examples}</div>
          </div>
        ))}
      </div>

      <div className="cc-section-head">What you&apos;ll need</div>
      <div className="cc-need-list">
        <div className="cc-need">
          <span className="ico">
            <IconFolder size={14} />
          </span>
          <div>
            <b>Local files</b>
            <br />
            The path to a Delta table directory containing a{' '}
            <code style={{ font: '11px var(--mono)' }}>_delta_log/</code>.
          </div>
        </div>
        <div className="cc-need">
          <span className="ico">
            <IconLock size={14} />
          </span>
          <div>
            <b>Object storage</b>
            <br />A bucket URI plus credentials with read access (or anonymous for public data).
          </div>
        </div>
        <div className="cc-need">
          <span className="ico">
            <IconKey size={14} />
          </span>
          <div>
            <b>Unity Catalog</b>
            <br />
            Workspace URL + a personal access token, or a UC-OSS server endpoint.
          </div>
        </div>
        <div className="cc-need">
          <span className="ico">
            <IconShareNode size={14} />
          </span>
          <div>
            <b>Delta Sharing</b>
            <br />A <code style={{ font: '11px var(--mono)' }}>config.share</code> profile file from
            your data provider.
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
    setForm({
      ...form,
      path: '~/Datasets/acme/silver/orders',
      detected: {
        name: 'orders',
        snapshot: 318,
        rowsLabel: '1.28M',
        files: 412,
        size: '38.2 GB',
        protocol: 'r2/w5',
      },
    });
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
            or paste a path — Axon reads{' '}
            <code style={{ fontFamily: 'var(--mono)', fontSize: 11.5 }}>_delta_log/</code> in place
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
                  detected: e.target.value
                    ? {
                        name: 'orders',
                        snapshot: 318,
                        rowsLabel: '1.28M',
                        files: 412,
                        size: '38.2 GB',
                        protocol: 'r2/w5',
                      }
                    : null,
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
            Both files and zipped tables work. Symlinks are resolved. Read-only — Axon will never
            write to local sources.
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
        />
      </div>

      <aside className="cc-helper">
        <h5>How local tables work</h5>
        <p>
          Axon mounts the folder as a single-table catalog under whatever alias you choose. The
          browser engine reads Parquet directly through the File System Access API — files never
          leave your machine.
        </p>
        <hr />
        <h5>Tips</h5>
        <ul>
          <li>
            Point at the table root (where <code>_delta_log/</code> lives), not at an individual{' '}
            <code>.parquet</code> file.
          </li>
          <li>
            For a zipped table, drop the <code>.zip</code> here and we&apos;ll unpack into your OPFS
            cache.
          </li>
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
  const auth = AUTH_LABELS[form.auth];
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
                onClick={() => setForm({ ...form, provider: p.id, auth: p.auths[0] })}
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
            <label className="cc-label">
              Endpoint override <span className="opt">· optional</span>
            </label>
            <input
              className="cc-input mono"
              placeholder="https://s3.example.com"
              value={form.endpoint}
              onChange={(e: ChangeEvent<HTMLInputElement>) =>
                setForm({ ...form, endpoint: e.target.value })
              }
            />
          </div>
        </div>

        <div className="cc-field">
          <label className="cc-label">Authentication</label>
          <div className="cc-auth-grid">
            {provider.auths.map((a: AuthMethod) => (
              <div
                key={a}
                className={'cc-auth-card ' + (form.auth === a ? 'selected' : '')}
                onClick={() => setForm({ ...form, auth: a })}
              >
                <span className="radio" />
                <div>
                  <div className="n">{AUTH_LABELS[a].label}</div>
                  <div className="d">{authBlurb(a)}</div>
                </div>
              </div>
            ))}
          </div>
        </div>

        {auth.fields.length > 0 && (
          <div className="cc-row-2">
            {auth.fields.map((f) => {
              const optional = f.endsWith('?');
              const key = f.replace('?', '');
              const isSecret = /(secret|token|key$|json)/i.test(key);
              const isSingle = auth.fields.length === 1;
              return (
                <div
                  key={key}
                  className="cc-field"
                  style={isSingle ? { gridColumn: 'span 2' } : undefined}
                >
                  <label className="cc-label">
                    {humanize(key)}
                    {optional && <span className="opt">· optional</span>}
                    {isSecret && (
                      <span className="opt">
                        <IconLock size={9} /> · encrypted
                      </span>
                    )}
                  </label>
                  {key === 'sa_json' ? (
                    <textarea
                      className="cc-textarea"
                      placeholder='{"type":"service_account",…}'
                      value={form.creds[key] || ''}
                      onChange={(e: ChangeEvent<HTMLTextAreaElement>) =>
                        setForm({ ...form, creds: { ...form.creds, [key]: e.target.value } })
                      }
                    />
                  ) : (
                    <input
                      className="cc-input mono"
                      type={isSecret ? 'password' : 'text'}
                      placeholder={placeholderFor(key)}
                      value={form.creds[key] || ''}
                      onChange={(e: ChangeEvent<HTMLInputElement>) =>
                        setForm({
                          ...form,
                          creds: { ...form.creds, [key]: e.target.value },
                        })
                      }
                    />
                  )}
                </div>
              );
            })}
          </div>
        )}

        <TestResult
          state={testState}
          okText={`Reached ${provider.label} · 8 tables discovered`}
          okDetail="2 prefixes (silver, bronze) · auth verified · range reads supported · 184 GB scanned"
        />
      </div>

      <aside className="cc-helper">
        <h5>How Axon reads {provider.label}</h5>
        <p>
          The browser engine talks to {provider.label} over HTTPS using validated byte-range reads.
          {serverFallbackEnabled
            ? ' Server query fallback can route unsupported table features to your configured query service.'
            : ' If a table requires features this browser build cannot serve, the query stops with a structured browser error.'}
        </p>
        <hr />
        <h5>Required permissions</h5>
        <ul>
          <li>
            <code>{providerPerm(provider.id, 'list')}</code> on the bucket
          </li>
          <li>
            <code>{providerPerm(provider.id, 'get')}</code> on listed objects
          </li>
        </ul>
        <hr />
        <h5>Network</h5>
        <p>
          Egress: <code>{provider.scheme.replace('://', '')}.region.amazonaws.com</code> (or
          compatible). CORS must allow <code>Range</code> + <code>GET</code> from this origin.
        </p>
      </aside>
    </div>
  );
}

function authBlurb(a: AuthMethod) {
  return {
    access_key: 'AKIA-style key + secret. Stored encrypted.',
    iam_role: 'Assume a role via STS. Best for shared access.',
    env: 'Use AWS_PROFILE / credential file on this host.',
    anonymous: 'No credentials. For public datasets only.',
    service_account: 'Paste a GCP service-account JSON key.',
    workload_identity: 'Federated GCP identity via OIDC.',
    adc: 'Use Application Default Credentials.',
    sas_token: 'Short-lived shared-access signature.',
    service_principal: 'Azure AD app registration.',
    managed_identity: "Use this host's Azure managed identity.",
  }[a];
}

function providerPerm(p: ObjectStoreProviderId, op: 'list' | 'get') {
  return {
    s3: op === 'list' ? 's3:ListBucket' : 's3:GetObject',
    gcs: op === 'list' ? 'storage.objects.list' : 'storage.objects.get',
    abfss: op === 'list' ? 'Storage Blob Data Reader' : 'Storage Blob Data Reader',
    r2: op === 'list' ? 'Object Read (Bucket)' : 'Object Read',
  }[p];
}

function humanize(k: string) {
  return k.replace(/_/g, ' ').replace(/\b\w/g, (m) => m.toUpperCase());
}

function placeholderFor(k: string) {
  return (
    {
      access_key_id: 'AKIAIOSFODNN7EXAMPLE',
      secret_access_key: '••••••••••••',
      role_arn: 'arn:aws:iam::123456789012:role/AxonRead',
      external_id: 'auto-generated',
      audience: '//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/…',
      sa_json: '{"type":"service_account", ...}',
      sas_token: '?sv=2024-…&sr=c&sp=rl&sig=…',
      tenant_id: '11111111-2222-3333-4444-555555555555',
      client_id: 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee',
      client_secret: '••••••••••••',
    }[k] || ''
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

        {form.uc_mode === 'databricks' ? (
          <>
            <div className="cc-field">
              <label className="cc-label">Workspace URL</label>
              <input
                className="cc-input mono"
                placeholder="https://acme-prod.cloud.databricks.com"
                value={form.uc_host}
                onChange={(e: ChangeEvent<HTMLInputElement>) =>
                  setForm({ ...form, uc_host: e.target.value })
                }
              />
              <div className="cc-help">
                The full workspace hostname. Account consoles aren&apos;t supported — use a
                workspace URL.
              </div>
            </div>
            <div className="cc-field">
              <label className="cc-label">
                Authentication
                <span className="opt">
                  <IconLock size={9} /> · encrypted
                </span>
              </label>
              <div className="cc-seg" style={{ marginBottom: 8 }}>
                <button
                  className={form.uc_auth === 'pat' ? 'active' : ''}
                  onClick={() => setForm({ ...form, uc_auth: 'pat' })}
                >
                  Personal access token
                </button>
                <button
                  className={form.uc_auth === 'oauth' ? 'active' : ''}
                  onClick={() => setForm({ ...form, uc_auth: 'oauth' })}
                >
                  OAuth (M2M)
                </button>
              </div>
              {form.uc_auth === 'pat' ? (
                <input
                  className="cc-input mono"
                  type="password"
                  placeholder="dapi••••••••••••••••••••••••••••"
                  value={form.uc_token}
                  onChange={(e: ChangeEvent<HTMLInputElement>) =>
                    setForm({ ...form, uc_token: e.target.value })
                  }
                />
              ) : (
                <div className="cc-row-2">
                  <input
                    className="cc-input mono"
                    placeholder="Client ID"
                    value={form.uc_client_id}
                    onChange={(e: ChangeEvent<HTMLInputElement>) =>
                      setForm({ ...form, uc_client_id: e.target.value })
                    }
                  />
                  <input
                    className="cc-input mono"
                    type="password"
                    placeholder="Client secret"
                    value={form.uc_client_secret}
                    onChange={(e: ChangeEvent<HTMLInputElement>) =>
                      setForm({ ...form, uc_client_secret: e.target.value })
                    }
                  />
                </div>
              )}
            </div>
          </>
        ) : (
          <>
            <div className="cc-field">
              <label className="cc-label">Server endpoint</label>
              <input
                className="cc-input mono"
                placeholder="https://uc.acme.internal/api/2.1/unity-catalog"
                value={form.uc_host}
                onChange={(e: ChangeEvent<HTMLInputElement>) =>
                  setForm({ ...form, uc_host: e.target.value })
                }
              />
              <div className="cc-help">
                Open-source UC servers expose the same REST surface as Databricks. mTLS endpoints
                are supported.
              </div>
            </div>
            <div className="cc-field">
              <label className="cc-label">
                Bearer token <span className="opt">· optional</span>
              </label>
              <input
                className="cc-input mono"
                type="password"
                placeholder="•••• if your UC requires auth"
                value={form.uc_token}
                onChange={(e: ChangeEvent<HTMLInputElement>) =>
                  setForm({ ...form, uc_token: e.target.value })
                }
              />
            </div>
          </>
        )}

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
          okText="Reached Unity Catalog · 3 schemas, 12 tables"
          okDetail={
            <>
              Authenticated as <code>analyst@acme.com</code> · Grants resolved for{' '}
              <code>main.analytics</code>, <code> main.raw</code>. 3 tables in{' '}
              <code>main.ml_models</code> hidden (no read grant).
            </>
          }
        />
      </div>

      <aside className="cc-helper">
        <h5>Governed reads</h5>
        <p>
          Axon honours Unity Catalog grants. Tables you can&apos;t <code>SELECT</code> appear greyed
          out — we never bypass UC&apos;s policy engine to read underlying storage.
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
          UC vends short-lived storage credentials. Axon refreshes them automatically and never
          persists them to disk.
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
      ds_profile_name: 'acme-partner.share',
      ds_profile_json: SAMPLE_DS_PROFILE,
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
              Profile file
            </button>
            <button
              className={mode === 'manual' ? 'active' : ''}
              onClick={() => setForm({ ...form, ds_mode: 'manual' })}
            >
              <span className="g" style={{ background: 'var(--ink-2)' }}>
                URL
              </span>
              Manual endpoint
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
              <div className="ti">
                Drop your{' '}
                <code style={{ fontFamily: 'var(--mono)', fontSize: 11.5 }}>config.share</code> file
              </div>
              <div className="sub">Or paste its JSON below — the file your data provider sent.</div>
              <button className="browse">
                <IconFolder size={11} /> Browse…
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
                    <div className="path">
                      share endpoint · bearer token · expiration · protocol v1
                    </div>
                  </div>
                  <span style={{ font: '11.5px var(--mono)', color: 'var(--success)' }}>
                    parsed
                  </span>
                </div>
              </div>
            )}

            <div className="cc-field" style={{ marginTop: 14 }}>
              <label className="cc-label">
                Profile JSON <span className="opt">· paste</span>
                <span className="opt">
                  <IconLock size={9} /> · token encrypted
                </span>
              </label>
              <textarea
                className="cc-textarea"
                style={{ height: 110 }}
                placeholder='{"shareCredentialsVersion":1, "endpoint":"https://sharing.acme.io/delta-sharing", "bearerToken":"••••"}'
                value={form.ds_profile_json}
                onChange={(e: ChangeEvent<HTMLTextAreaElement>) =>
                  setForm({
                    ...form,
                    ds_profile_json: e.target.value,
                    ds_profile_name:
                      e.target.value && !form.ds_profile_name
                        ? 'pasted-profile'
                        : form.ds_profile_name,
                  })
                }
              />
              <div className="cc-help">
                Profile fields kept: <code>endpoint</code>, <code>bearerToken</code>,{' '}
                <code>expirationTime</code>, <code>shareCredentialsVersion</code>. Nothing else is
                read.
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
                The HTTPS base URL of a Delta Sharing server (REST API v1). Often suffixed with{' '}
                <code>/delta-sharing</code>.
              </div>
            </div>
            <div className="cc-field">
              <label className="cc-label">
                Bearer token
                <span className="opt">
                  <IconLock size={9} /> · encrypted
                </span>
              </label>
              <input
                className="cc-input mono"
                type="password"
                placeholder="••••••••••••••••••••"
                value={form.ds_token}
                onChange={(e: ChangeEvent<HTMLInputElement>) =>
                  setForm({ ...form, ds_token: e.target.value })
                }
              />
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
          okText="Provider reachable · 2 shares, 9 tables visible"
          okDetail={
            <>
              Profile parsed. Bearer token valid for <code>14 days</code>. Storage credentials
              vended via short-lived URLs — nothing persists to disk.
            </>
          }
        />
      </div>

      <aside className="cc-helper">
        <h5>What is Delta Sharing?</h5>
        <p>
          An open protocol for sharing live Delta tables across clouds and organisations. The data
          stays at the provider; Axon reads it through short-lived signed URLs the sharing server
          hands out per query.
        </p>
        <hr />
        <h5>You&apos;ll get</h5>
        <ul>
          <li>One catalog per Delta Sharing endpoint</li>
          <li>
            <code>share/schema</code> mapped to Axon schemas
          </li>
          <li>Read-only access — Axon never writes back</li>
          <li>Automatic refresh of vended URLs as they expire</li>
        </ul>
        <hr />
        <h5>Security</h5>
        <p>
          The profile file contains a bearer token. Axon stores it encrypted at rest and only sends
          it over TLS to the endpoint URL declared in the profile.
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
}: {
  state: TestState;
  okText: string;
  okDetail: ReactNode;
}) {
  if (!state) return null;
  if (state === 'running') {
    return (
      <div className="cc-test-result run">
        <span className="cc-spin" />
        <div className="body">
          <div className="t">Testing connection…</div>
          <div className="d">
            Reading <code>_delta_log/</code>, verifying read access, sampling object metadata.
          </div>
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
        <div className="t">Couldn&apos;t reach the source</div>
        <div className="d">
          CORS preflight failed at <code>HEAD /</code>. Allow <code>Range, Authorization</code>{' '}
          headers from this origin and retry.
        </div>
      </div>
    </div>
  );
}

// ─── Step 3: discover & review ──────────────────────────
function Discover({
  sourceId,
  alias,
  setAlias,
  selection,
  setSelection,
}: {
  sourceId: SourceId;
  alias: string;
  setAlias: (a: string) => void;
  selection: Record<string, SchemaSelection>;
  setSelection: (s: Record<string, SchemaSelection>) => void;
}) {
  const disc = DISCOVERED[sourceId];
  const included = useMemo(() => countIncluded(disc, selection), [disc, selection]);
  const total = useMemo(() => disc.schemas.reduce((a, s) => a + s.tables.length, 0), [disc]);

  return (
    <>
      <div className="cc-discover-summary">
        <span className="check">
          <IconCheck size={12} />
        </span>
        <div className="text" dangerouslySetInnerHTML={{ __html: disc.summary }} />
        <div className="meta">scanned in 1.42 s</div>
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
            <span className="l">Auth</span>
            <span className="v">{authShortFor(sourceId)}</span>
          </div>
          <div className="row">
            <span className="l">Region</span>
            <span className="v">{regionFor(sourceId)}</span>
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

function labelForSource(s: SourceId) {
  return {
    local: 'Local files',
    object_store: 'Object storage (GCS)',
    unity_catalog: 'Unity Catalog (Databricks)',
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
function authShortFor(s: SourceId) {
  return {
    local: 'file:// · read-only',
    object_store: 'Service account JSON',
    unity_catalog: 'PAT · analyst@acme.com',
    delta_share: 'Bearer token · 14 d valid',
  }[s];
}
function regionFor(s: SourceId) {
  return {
    local: '—',
    object_store: 'us-central1',
    unity_catalog: 'auto · us-east-1',
    delta_share: 'provider-vended',
  }[s];
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
    unity_catalog: 'Use UC for governance; Axon honours every grant.',
    delta_share: 'Read tables shared by another organisation through the open protocol.',
  }[s];
}
