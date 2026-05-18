// Inline SVG icon components for the Axon SQL editor.
// All icons render at 14×14 by default with currentColor stroke.

const Icon = ({
  d,
  size = 14,
  fill = 'none',
  stroke = 'currentColor',
  sw = 1.6,
  viewBox = '0 0 24 24',
  children,
}) => (
  <svg
    width={size}
    height={size}
    viewBox={viewBox}
    fill={fill}
    stroke={stroke}
    strokeWidth={sw}
    strokeLinecap="round"
    strokeLinejoin="round"
    aria-hidden="true"
  >
    {d ? <path d={d} /> : children}
  </svg>
);

const IconSearch = (p) => (
  <Icon {...p}>
    <circle cx="11" cy="11" r="6.5" />
    <path d="m20 20-3.5-3.5" />
  </Icon>
);
const IconChevR = (p) => (
  <Icon {...p}>
    <path d="m9 5 7 7-7 7" />
  </Icon>
);
const IconChevD = (p) => (
  <Icon {...p}>
    <path d="m5 9 7 7 7-7" />
  </Icon>
);
const IconChevUD = (p) => (
  <Icon {...p}>
    <path d="m8 9 4-4 4 4M8 15l4 4 4-4" />
  </Icon>
);
const IconClose = (p) => (
  <Icon {...p}>
    <path d="M6 6l12 12M6 18 18 6" />
  </Icon>
);
const IconPlus = (p) => (
  <Icon {...p}>
    <path d="M12 5v14M5 12h14" />
  </Icon>
);
const IconPlay = (p) => (
  <Icon {...p} fill="currentColor" stroke="none">
    <path d="M7 5v14l12-7z" />
  </Icon>
);
const IconStop = (p) => (
  <Icon {...p} fill="currentColor" stroke="none">
    <rect x="6" y="6" width="12" height="12" rx="1" />
  </Icon>
);
const IconSave = (p) => (
  <Icon {...p}>
    <path d="M5 5h11l3 3v11a1 1 0 0 1-1 1H5a1 1 0 0 1-1-1V6a1 1 0 0 1 1-1z" />
    <path d="M7 5v5h8V5" />
    <rect x="7" y="13" width="10" height="6" />
  </Icon>
);
const IconShare = (p) => (
  <Icon {...p}>
    <circle cx="6" cy="12" r="2.5" />
    <circle cx="18" cy="6" r="2.5" />
    <circle cx="18" cy="18" r="2.5" />
    <path d="m8.2 10.8 7.6-3.6M8.2 13.2l7.6 3.6" />
  </Icon>
);
const IconExport = (p) => (
  <Icon {...p}>
    <path d="M12 4v11" />
    <path d="m7 9 5-5 5 5" />
    <path d="M5 17v2a1 1 0 0 0 1 1h12a1 1 0 0 0 1-1v-2" />
  </Icon>
);
const IconHistory = (p) => (
  <Icon {...p}>
    <path d="M3.5 12a8.5 8.5 0 1 0 2.6-6.1" />
    <path d="M3.5 4v4h4" />
    <path d="M12 7v5l3 2" />
  </Icon>
);
const IconBranch = (p) => (
  <Icon {...p}>
    <circle cx="6" cy="5" r="2" />
    <circle cx="6" cy="19" r="2" />
    <circle cx="18" cy="7" r="2" />
    <path d="M6 7v10" />
    <path d="M18 9c0 4-6 4-6 8" />
  </Icon>
);
const IconDatabase = (p) => (
  <Icon {...p}>
    <ellipse cx="12" cy="6" rx="7" ry="2.5" />
    <path d="M5 6v12c0 1.4 3.1 2.5 7 2.5s7-1.1 7-2.5V6" />
    <path d="M5 12c0 1.4 3.1 2.5 7 2.5s7-1.1 7-2.5" />
  </Icon>
);
const IconSchema = (p) => (
  <Icon {...p}>
    <path d="M4 6h16M4 12h16M4 18h16" />
    <circle cx="7" cy="6" r="1" fill="currentColor" />
    <circle cx="11" cy="12" r="1" fill="currentColor" />
    <circle cx="9" cy="18" r="1" fill="currentColor" />
  </Icon>
);
const IconTable = (p) => (
  <Icon {...p}>
    <rect x="3.5" y="4.5" width="17" height="15" rx="1.5" />
    <path d="M3.5 9.5h17M3.5 14.5h17M9 4.5v15M15 4.5v15" />
  </Icon>
);
const IconView = (p) => (
  <Icon {...p}>
    <path d="M2.5 12s3.5-6.5 9.5-6.5S21.5 12 21.5 12s-3.5 6.5-9.5 6.5S2.5 12 2.5 12z" />
    <circle cx="12" cy="12" r="2.5" />
  </Icon>
);
const IconColumn = (p) => (
  <Icon {...p}>
    <rect x="5" y="3.5" width="14" height="17" rx="1.5" />
    <path d="M5 9.5h14M5 14.5h14" />
  </Icon>
);
const IconKey = (p) => (
  <Icon {...p}>
    <circle cx="8" cy="12" r="3.5" />
    <path d="m11.5 12 7 0M16 12v3M19 12v2" />
  </Icon>
);
const IconSparkle = (p) => (
  <Icon {...p}>
    <path d="M12 4 13.4 9.6 19 11l-5.6 1.4L12 18l-1.4-5.6L5 11l5.6-1.4z" />
  </Icon>
);
const IconFolder = (p) => (
  <Icon {...p}>
    <path d="M3 6.5a1.5 1.5 0 0 1 1.5-1.5h4.2a1 1 0 0 1 .7.3l1.6 1.6a1 1 0 0 0 .7.3h7.8A1.5 1.5 0 0 1 21 8.7V18a1.5 1.5 0 0 1-1.5 1.5h-15A1.5 1.5 0 0 1 3 18z" />
  </Icon>
);
const IconBookmark = (p) => (
  <Icon {...p}>
    <path d="M6 4h12v17l-6-4-6 4z" />
  </Icon>
);
const IconCog = (p) => (
  <Icon {...p}>
    <circle cx="12" cy="12" r="3" />
    <path d="M12 2v3M12 19v3M4.2 4.2l2.1 2.1M17.7 17.7l2.1 2.1M2 12h3M19 12h3M4.2 19.8l2.1-2.1M17.7 6.3l2.1-2.1" />
  </Icon>
);
const IconHelp = (p) => (
  <Icon {...p}>
    <circle cx="12" cy="12" r="9" />
    <path d="M9.5 9a2.5 2.5 0 0 1 5 0c0 1.5-2.5 2-2.5 4" />
    <circle cx="12" cy="17" r=".8" fill="currentColor" />
  </Icon>
);
const IconBolt = (p) => (
  <Icon {...p}>
    <path d="M13 3 5 14h6l-1 7 8-11h-6z" />
  </Icon>
);
const IconFormat = (p) => (
  <Icon {...p}>
    <path d="M4 6h16M4 10h10M4 14h16M4 18h8" />
  </Icon>
);
const IconBeauty = (p) => (
  <Icon {...p}>
    <path d="M4 7h12M4 12h16M4 17h8" />
  </Icon>
);
const IconArrowDown = (p) => (
  <Icon {...p}>
    <path d="M12 5v14m-5-5 5 5 5-5" />
  </Icon>
);
const IconArrowUp = (p) => (
  <Icon {...p}>
    <path d="M12 19V5m-5 5 5-5 5 5" />
  </Icon>
);
const IconChevDownTiny = (p) => (
  <Icon {...p} size={p.size || 10}>
    <path d="m6 9 6 6 6-6" />
  </Icon>
);
const IconDots = (p) => (
  <Icon {...p}>
    <circle cx="6" cy="12" r="1.2" fill="currentColor" />
    <circle cx="12" cy="12" r="1.2" fill="currentColor" />
    <circle cx="18" cy="12" r="1.2" fill="currentColor" />
  </Icon>
);
const IconCopy = (p) => (
  <Icon {...p}>
    <rect x="8" y="8" width="12" height="12" rx="1.5" />
    <path d="M16 8V5a1 1 0 0 0-1-1H5a1 1 0 0 0-1 1v10a1 1 0 0 0 1 1h3" />
  </Icon>
);
const IconRefresh = (p) => (
  <Icon {...p}>
    <path d="M4 12a8 8 0 0 1 14-5.3L21 4" />
    <path d="M21 4v5h-5" />
    <path d="M20 12a8 8 0 0 1-14 5.3L3 20" />
    <path d="M3 20v-5h5" />
  </Icon>
);
const IconPanel = (p) => (
  <Icon {...p}>
    <rect x="3.5" y="4.5" width="17" height="15" rx="1.5" />
    <path d="M3.5 9.5h17" />
  </Icon>
);
const IconPanelSide = (p) => (
  <Icon {...p}>
    <rect x="3.5" y="4.5" width="17" height="15" rx="1.5" />
    <path d="M10 4.5v15" />
  </Icon>
);
const IconCheck = (p) => (
  <Icon {...p}>
    <path d="m4 12 5 5L20 6" />
  </Icon>
);
const IconWarn = (p) => (
  <Icon {...p}>
    <path d="M12 3 2 20h20z" />
    <path d="M12 10v5" />
    <circle cx="12" cy="18" r=".8" fill="currentColor" />
  </Icon>
);
const IconCmd = (p) => (
  <Icon {...p}>
    <path d="M9 6h6v6m0 0h3a2 2 0 1 0-2-2v8m-1-6H6m0 0H3a2 2 0 1 1 2 2v-8m1 6h6" />
  </Icon>
);
const IconLock = (p) => (
  <Icon {...p}>
    <rect x="5" y="11" width="14" height="9" rx="1.5" />
    <path d="M8 11V8a4 4 0 0 1 8 0v3" />
  </Icon>
);
const IconTerm = (p) => (
  <Icon {...p}>
    <rect x="3.5" y="4.5" width="17" height="15" rx="1.5" />
    <path d="m7 10 3 2-3 2M12 14h5" />
  </Icon>
);

Object.assign(window, {
  Icon,
  IconSearch,
  IconChevR,
  IconChevD,
  IconChevUD,
  IconClose,
  IconPlus,
  IconPlay,
  IconStop,
  IconSave,
  IconShare,
  IconExport,
  IconHistory,
  IconBranch,
  IconDatabase,
  IconSchema,
  IconTable,
  IconView,
  IconColumn,
  IconKey,
  IconSparkle,
  IconFolder,
  IconBookmark,
  IconCog,
  IconHelp,
  IconBolt,
  IconFormat,
  IconBeauty,
  IconArrowDown,
  IconArrowUp,
  IconChevDownTiny,
  IconDots,
  IconCopy,
  IconRefresh,
  IconPanel,
  IconPanelSide,
  IconCheck,
  IconWarn,
  IconCmd,
  IconLock,
  IconTerm,
});
