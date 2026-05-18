// SQL Editor — textarea overlaid on a syntax-highlighted <pre>.
// Includes line gutter, autocomplete stub, format button, and run shortcut.

function Editor({ value, onChange, onRun, running, onFormat }) {
  const taRef = React.useRef(null);
  const preRef = React.useRef(null);
  const gutRef = React.useRef(null);
  const scrollRef = React.useRef(null);
  const [activeLine, setActiveLine] = React.useState(1);
  const [autocomp, setAutocomp] = React.useState(null);

  const lines = React.useMemo(() => value.split('\n'), [value]);
  const html = React.useMemo(() => highlightSql(value) + ' ', [value]);

  React.useEffect(() => {
    // keep gutter scrolled with editor
    const s = scrollRef.current;
    if (!s) return;
    const sync = () => {
      if (gutRef.current) gutRef.current.scrollTop = s.scrollTop;
    };
    s.addEventListener('scroll', sync);
    return () => s.removeEventListener('scroll', sync);
  }, []);

  function handleKeyDown(e) {
    // Cmd/Ctrl+Enter → Run
    if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') {
      e.preventDefault();
      onRun();
      return;
    }
    // Cmd/Ctrl+Shift+F → Format
    if ((e.metaKey || e.ctrlKey) && e.shiftKey && (e.key === 'f' || e.key === 'F')) {
      e.preventDefault();
      onFormat();
      return;
    }
    // Tab → insert two spaces
    if (e.key === 'Tab' && !e.shiftKey) {
      e.preventDefault();
      const ta = taRef.current;
      const start = ta.selectionStart,
        end = ta.selectionEnd;
      const next = value.slice(0, start) + '  ' + value.slice(end);
      onChange(next);
      requestAnimationFrame(() => {
        ta.selectionStart = ta.selectionEnd = start + 2;
      });
      return;
    }
    // Escape → close autocomplete
    if (e.key === 'Escape' && autocomp) {
      setAutocomp(null);
    }
  }

  function handleChange(e) {
    const v = e.target.value;
    onChange(v);
    // update active line
    const upto = v.slice(0, e.target.selectionStart);
    setActiveLine(upto.split('\n').length);

    // crude autocomplete trigger
    const before = upto.slice(-20);
    const m = before.match(/([A-Za-z_][A-Za-z0-9_]*)$/);
    if (m && m[1].length >= 2) {
      const word = m[1].toLowerCase();
      const items = autocompleteFor(word, v);
      if (items.length) {
        setAutocomp({ items, word, sel: 0 });
        return;
      }
    }
    setAutocomp(null);
  }

  function handleClick(e) {
    const ta = taRef.current;
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

function autocompleteFor(prefix, _src) {
  const out = [];
  const tables = window.AXON_DATA.CATALOG.tables;
  for (const t of tables) {
    if (t.name.toLowerCase().startsWith(prefix)) {
      out.push({ label: t.name, kind: 'tbl', type: t.kind });
    }
  }
  const seenCols = new Set();
  for (const t of tables)
    for (const c of t.columns || []) {
      if (c.name.toLowerCase().startsWith(prefix) && !seenCols.has(c.name)) {
        out.push({ label: c.name, kind: 'col', type: c.type });
        seenCols.add(c.name);
      }
    }
  for (const k of SQL_KEYWORDS) {
    if (k.startsWith(prefix)) out.push({ label: k.toUpperCase(), kind: 'kw' });
  }
  return out;
}

window.Editor = Editor;
