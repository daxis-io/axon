import { useEffect } from 'react';
import type { CapabilityMatrixRow } from '../../services/types.ts';
import { IconBolt, IconClose } from './icons.tsx';

type CapabilitiesProps = {
  matrix: CapabilityMatrixRow[];
  onClose: () => void;
  anchorRight?: number;
  anchorTop?: number;
};

export function CapabilityPopover({
  matrix,
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
        <span className="sub">· browser vs native</span>
        <button
          className="iconbtn"
          style={{ marginLeft: 'auto' }}
          onClick={onClose}
          aria-label="Close"
        >
          <IconClose size={11} />
        </button>
      </div>

      <div className="cap-head">
        <span>Capability</span>
        <span>Browser</span>
        <span>Native</span>
      </div>

      <div className="cap-matrix">
        {matrix.map((row) => (
          <div className="cap-row" key={row.key}>
            <div>
              <div className="name">{row.label}</div>
              <div className="desc">{row.desc}</div>
            </div>
            <span className={'state state-' + row.browser}>{row.browser.replace('_', ' ')}</span>
            <span className={'state state-' + row.native}>{row.native.replace('_', ' ')}</span>
          </div>
        ))}
      </div>

      <div className="cap-foot">
        Unsupported on the preferred target returns a{' '}
        <b style={{ color: 'var(--ink-2)' }}>structured fallback</b> instead of a wrong answer. See{' '}
        <span style={{ fontFamily: 'var(--mono)' }}>FallbackReason</span>.
      </div>
    </div>
  );
}
