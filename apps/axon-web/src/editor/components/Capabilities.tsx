import { useEffect } from 'react';
import type { CapabilityMatrixRow } from '../../services/types.ts';
import { IconBolt, IconClose } from './icons.tsx';

type CapabilitiesProps = {
  matrix: CapabilityMatrixRow[];
  serverFallbackEnabled: boolean;
  onClose: () => void;
  anchorRight?: number;
  anchorTop?: number;
};

export function CapabilityPopover({
  matrix,
  serverFallbackEnabled,
  onClose,
  anchorRight = 12,
  anchorTop = 52,
}: CapabilitiesProps) {
  useEffect(() => {
    const onDoc = (e: globalThis.MouseEvent) => {
      const target = e.target as HTMLElement | null;
      if (!target?.closest('[data-cap-popover]') && !target?.closest('[data-cap-trigger]')) {
        onClose();
      }
    };
    document.addEventListener('mousedown', onDoc);
    return () => document.removeEventListener('mousedown', onDoc);
  }, [onClose]);

  return (
    <div className="popover" data-cap-popover style={{ right: anchorRight, top: anchorTop }}>
      <div className="hdr">
        <IconBolt size={13} />
        <span>Engine capabilities</span>
        <span className="sub">
          {serverFallbackEnabled ? '· browser vs native' : '· browser build'}
        </span>
        <button
          className="iconbtn"
          style={{ marginLeft: 'auto' }}
          onClick={onClose}
          aria-label="Close"
        >
          <IconClose size={11} />
        </button>
      </div>

      <div className={'cap-head ' + (serverFallbackEnabled ? '' : 'browser-only')}>
        <span>Capability</span>
        <span>Browser</span>
        {serverFallbackEnabled && <span>Native</span>}
      </div>

      <div className="cap-matrix">
        {matrix.map((row) => (
          <div className={'cap-row ' + (serverFallbackEnabled ? '' : 'browser-only')} key={row.key}>
            <div>
              <div className="name">{row.label}</div>
              <div className="desc">{row.desc}</div>
            </div>
            <span className={'state state-' + row.browser}>{row.browser.replace('_', ' ')}</span>
            {serverFallbackEnabled && (
              <span className={'state state-' + row.native}>{row.native.replace('_', ' ')}</span>
            )}
          </div>
        ))}
      </div>

      <div className="cap-foot">
        {serverFallbackEnabled ? (
          <>
            Unsupported on the preferred target returns a{' '}
            <b style={{ color: 'var(--ink-2)' }}>structured fallback</b> instead of a wrong answer.
            See <span style={{ fontFamily: 'var(--mono)' }}>FallbackReason</span>.
          </>
        ) : (
          <>
            Unsupported browser features return a{' '}
            <b style={{ color: 'var(--ink-2)' }}>structured error</b> instead of a wrong answer.
          </>
        )}
      </div>
    </div>
  );
}
