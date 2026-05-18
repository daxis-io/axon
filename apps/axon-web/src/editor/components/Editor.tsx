import { useEffect, useMemo, useRef, useState, type ChangeEvent, type KeyboardEvent } from 'react';
import type { Catalog } from '../../services/types.ts';
import { highlightSql, SQL_KEYWORDS } from '../lib/highlight.ts';

type AutocompleteKind = 'kw' | 'tbl' | 'col';

type AutocompleteItem = {
  label: string;
  kind: AutocompleteKind;
  type?: string;
};

type AutocompleteState = {
  items: AutocompleteItem[];
  word: string;
  sel: number;
};

type EditorProps = {
  value: string;
  catalog: Catalog | undefined;
  running: boolean;
  onChange: (value: string) => void;
  onRun: () => void;
  onFormat: () => void;
};

export function Editor({ value, catalog, running, onChange, onRun, onFormat }: EditorProps) {
  const taRef = useRef<HTMLTextAreaElement>(null);
  const preRef = useRef<HTMLPreElement>(null);
  const gutRef = useRef<HTMLDivElement>(null);
  const scrollRef = useRef<HTMLDivElement>(null);
  const [activeLine, setActiveLine] = useState(1);
  const [autocomp, setAutocomp] = useState<AutocompleteState | null>(null);

  const lines = useMemo(() => value.split('\n'), [value]);
  const html = useMemo(() => highlightSql(value) + ' ', [value]);

  useEffect(() => {
    const s = scrollRef.current;
    if (!s) return;
    const sync = () => {
      if (gutRef.current) gutRef.current.scrollTop = s.scrollTop;
    };
    s.addEventListener('scroll', sync);
    return () => s.removeEventListener('scroll', sync);
  }, []);

  function autocompleteFor(prefix: string): AutocompleteItem[] {
    const out: AutocompleteItem[] = [];
    const tables = catalog?.tables ?? [];
    for (const t of tables) {
      if (t.name.toLowerCase().startsWith(prefix)) {
        out.push({ label: t.name, kind: 'tbl', type: t.kind });
      }
    }
    const seen = new Set<string>();
    for (const t of tables)
      for (const c of t.columns) {
        if (c.name.toLowerCase().startsWith(prefix) && !seen.has(c.name)) {
          out.push({ label: c.name, kind: 'col', type: c.type });
          seen.add(c.name);
        }
      }
    for (const k of SQL_KEYWORDS) {
      if (k.startsWith(prefix)) out.push({ label: k.toUpperCase(), kind: 'kw' });
    }
    return out;
  }

  function handleKeyDown(e: KeyboardEvent<HTMLTextAreaElement>) {
    if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') {
      e.preventDefault();
      onRun();
      return;
    }
    if ((e.metaKey || e.ctrlKey) && e.shiftKey && (e.key === 'f' || e.key === 'F')) {
      e.preventDefault();
      onFormat();
      return;
    }
    if (e.key === 'Tab' && !e.shiftKey) {
      e.preventDefault();
      const ta = taRef.current;
      if (!ta) return;
      const start = ta.selectionStart;
      const end = ta.selectionEnd;
      const next = value.slice(0, start) + '  ' + value.slice(end);
      onChange(next);
      requestAnimationFrame(() => {
        ta.selectionStart = ta.selectionEnd = start + 2;
      });
      return;
    }
    if (e.key === 'Escape' && autocomp) {
      setAutocomp(null);
    }
  }

  function handleChange(e: ChangeEvent<HTMLTextAreaElement>) {
    const v = e.target.value;
    onChange(v);
    const upto = v.slice(0, e.target.selectionStart);
    setActiveLine(upto.split('\n').length);
    const before = upto.slice(-20);
    const m = before.match(/([A-Za-z_][A-Za-z0-9_]*)$/);
    if (m && m[1].length >= 2) {
      const items = autocompleteFor(m[1].toLowerCase());
      if (items.length) {
        setAutocomp({ items, word: m[1], sel: 0 });
        return;
      }
    }
    setAutocomp(null);
  }

  function handleClick() {
    const ta = taRef.current;
    if (!ta) return;
    const upto = value.slice(0, ta.selectionStart);
    setActiveLine(upto.split('\n').length);
    setAutocomp(null);
  }

  return (
    <div className={'editor-wrap ' + (running ? 'running' : '')}>
      <div className="gutter" ref={gutRef}>
        {lines.map((_, i) => (
          <span key={i} className={'ln ' + (i + 1 === activeLine ? 'active' : '')}>
            {i + 1}
          </span>
        ))}
      </div>

      <div className="editor-scroll" ref={scrollRef}>
        <div className="editor-stack">
          <pre className="code" ref={preRef} dangerouslySetInnerHTML={{ __html: html }} />
          <textarea
            ref={taRef}
            className="code-input"
            value={value}
            onChange={handleChange}
            onKeyDown={handleKeyDown}
            onClick={handleClick}
            spellCheck={false}
            autoCorrect="off"
            autoCapitalize="off"
            wrap="off"
          />
        </div>

        {autocomp && (
          <div className="autocomp" style={{ top: 18 + activeLine * 21, left: 60 }}>
            {autocomp.items.slice(0, 7).map((it, i) => (
              <div
                key={it.label}
                className={'item ' + it.kind + (i === autocomp.sel ? ' sel' : '')}
              >
                <span className="ico">
                  {it.kind === 'kw' ? 'kw' : it.kind === 'tbl' ? 'tbl' : 'col'}
                </span>
                <span>{it.label}</span>
                {it.type && <span className="type">{it.type}</span>}
              </div>
            ))}
            <div className="footer">
              <span>
                <span className="kbd">↑↓</span> navigate
              </span>
              <span>
                <span className="kbd">⏎</span> accept
              </span>
              <span>
                <span className="kbd">esc</span> close
              </span>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
