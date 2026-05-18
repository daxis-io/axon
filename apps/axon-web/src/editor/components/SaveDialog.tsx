import { useEffect, useRef, useState } from 'react';
import { IconClose } from './icons.tsx';

type SaveDialogProps = {
  initialName: string;
  onCancel: () => void;
  onSave: (name: string) => void;
};

export function SaveDialog({ initialName, onCancel, onSave }: SaveDialogProps) {
  const [name, setName] = useState(initialName);
  const inputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    inputRef.current?.focus();
    inputRef.current?.select();
  }, []);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onCancel();
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [onCancel]);

  return (
    <div
      style={{
        position: 'fixed',
        inset: 0,
        background: 'rgba(15, 16, 12, 0.32)',
        display: 'grid',
        placeItems: 'center',
        zIndex: 80,
      }}
      onMouseDown={onCancel}
    >
      <div
        className="popover"
        onMouseDown={(e) => e.stopPropagation()}
        style={{
          position: 'static',
          width: 400,
          padding: 0,
          background: 'var(--paper)',
        }}
      >
        <div className="hdr">
          <span>Save query</span>
          <button
            className="iconbtn"
            style={{ marginLeft: 'auto' }}
            onClick={onCancel}
            aria-label="Close"
          >
            <IconClose size={11} />
          </button>
        </div>
        <form
          onSubmit={(e) => {
            e.preventDefault();
            onSave(name.trim() || initialName);
          }}
          style={{ padding: 16, display: 'flex', flexDirection: 'column', gap: 12 }}
        >
          <label style={{ display: 'flex', flexDirection: 'column', gap: 6, fontSize: 12 }}>
            <span style={{ color: 'var(--ink-3)', fontWeight: 500 }}>Name</span>
            <input
              ref={inputRef}
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="Top customers · 30d"
              style={{
                padding: '6px 10px',
                border: '1px solid var(--line)',
                borderRadius: 'var(--r)',
                background: 'var(--paper)',
                font: '13px var(--ui)',
                outline: 'none',
              }}
            />
          </label>
          <div style={{ display: 'flex', gap: 8, justifyContent: 'flex-end' }}>
            <button type="button" className="btn ghost" onClick={onCancel}>
              Cancel
            </button>
            <button type="submit" className="btn primary">
              Save
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}
