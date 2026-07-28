// Capabilities matrix popover — shows CapabilityKey × ExecutionTarget states.

function CapabilityPopover({ onClose, anchorRight = 12, anchorTop = 52 }) {
  const caps = window.AXON_DATA.CAPABILITIES;
  const order = window.AXON_DATA.CAPABILITY_ORDER;

  // close on outside click
  React.useEffect(() => {
    const onDoc = (e) => {
      if (!e.target.closest('[data-cap-popover]') && !e.target.closest('[data-cap-trigger]'))
        onClose();
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
        <button className="iconbtn" style={{ marginLeft: 'auto' }} onClick={onClose}>
          <IconClose size={11} />
        </button>
      </div>

      <div className="cap-head">
        <span>Capability</span>
        <span>Browser</span>
        <span>Native</span>
      </div>

      <div className="cap-matrix">
        {order.map((key) => {
          const c = caps[key];
          if (!c) return null;
          return (
            <div className="cap-row" key={key}>
              <div>
                <div className="name">{c.label}</div>
                <div className="desc">{c.desc}</div>
              </div>
              <span className={'state state-' + c.browser}>{c.browser.replace('_', ' ')}</span>
              <span className={'state state-' + c.native}>{c.native.replace('_', ' ')}</span>
            </div>
          );
        })}
      </div>

      <div className="cap-foot">
        Unsupported on the preferred target returns a{' '}
        <b style={{ color: 'var(--ink-2)' }}>structured fallback</b> instead of a wrong answer. See{' '}
        <span style={{ fontFamily: 'var(--mono)' }}>FallbackReason</span>.
      </div>
    </div>
  );
}

window.CapabilityPopover = CapabilityPopover;
