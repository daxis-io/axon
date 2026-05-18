import type { CSSProperties, ReactNode } from 'react';

type IconBaseProps = {
  d?: string;
  size?: number;
  fill?: string;
  stroke?: string;
  sw?: number;
  viewBox?: string;
  children?: ReactNode;
  style?: CSSProperties;
  className?: string;
};

export const Icon = ({
  d,
  size = 14,
  fill = 'none',
  stroke = 'currentColor',
  sw = 1.6,
  viewBox = '0 0 24 24',
  children,
  style,
  className,
}: IconBaseProps) => (
  <svg
    width={size}
    height={size}
    viewBox={viewBox}
    fill={fill}
    stroke={stroke}
    strokeWidth={sw}
    strokeLinecap="round"
    strokeLinejoin="round"
    style={style}
    className={className}
    aria-hidden="true"
  >
    {d ? <path d={d} /> : children}
  </svg>
);

export type IconProps = Omit<IconBaseProps, 'd' | 'children'>;

export const IconSearch = (p: IconProps) => (
  <Icon {...p}>
    <circle cx="11" cy="11" r="6.5" />
    <path d="m20 20-3.5-3.5" />
  </Icon>
);
export const IconChevR = (p: IconProps) => <Icon {...p} d="m9 5 7 7-7 7" />;
export const IconChevD = (p: IconProps) => <Icon {...p} d="m5 9 7 7 7-7" />;
export const IconChevUD = (p: IconProps) => <Icon {...p} d="m8 9 4-4 4 4M8 15l4 4 4-4" />;
export const IconClose = (p: IconProps) => <Icon {...p} d="M6 6l12 12M6 18 18 6" />;
export const IconPlus = (p: IconProps) => <Icon {...p} d="M12 5v14M5 12h14" />;
export const IconPlay = (p: IconProps) => (
  <Icon {...p} fill="currentColor" stroke="none" d="M7 5v14l12-7z" />
);
export const IconStop = (p: IconProps) => (
  <Icon {...p} fill="currentColor" stroke="none">
    <rect x="6" y="6" width="12" height="12" rx="1" />
  </Icon>
);
export const IconSave = (p: IconProps) => (
  <Icon {...p}>
    <path d="M5 5h11l3 3v11a1 1 0 0 1-1 1H5a1 1 0 0 1-1-1V6a1 1 0 0 1 1-1z" />
    <path d="M7 5v5h8V5" />
    <rect x="7" y="13" width="10" height="6" />
  </Icon>
);
export const IconShare = (p: IconProps) => (
  <Icon {...p}>
    <circle cx="6" cy="12" r="2.5" />
    <circle cx="18" cy="6" r="2.5" />
    <circle cx="18" cy="18" r="2.5" />
    <path d="m8.2 10.8 7.6-3.6M8.2 13.2l7.6 3.6" />
  </Icon>
);
export const IconExport = (p: IconProps) => (
  <Icon {...p}>
    <path d="M12 4v11" />
    <path d="m7 9 5-5 5 5" />
    <path d="M5 17v2a1 1 0 0 0 1 1h12a1 1 0 0 0 1-1v-2" />
  </Icon>
);
export const IconHistory = (p: IconProps) => (
  <Icon {...p}>
    <path d="M3.5 12a8.5 8.5 0 1 0 2.6-6.1" />
    <path d="M3.5 4v4h4" />
    <path d="M12 7v5l3 2" />
  </Icon>
);
export const IconBranch = (p: IconProps) => (
  <Icon {...p}>
    <circle cx="6" cy="5" r="2" />
    <circle cx="6" cy="19" r="2" />
    <circle cx="18" cy="7" r="2" />
    <path d="M6 7v10" />
    <path d="M18 9c0 4-6 4-6 8" />
  </Icon>
);
export const IconDatabase = (p: IconProps) => (
  <Icon {...p}>
    <ellipse cx="12" cy="6" rx="7" ry="2.5" />
    <path d="M5 6v12c0 1.4 3.1 2.5 7 2.5s7-1.1 7-2.5V6" />
    <path d="M5 12c0 1.4 3.1 2.5 7 2.5s7-1.1 7-2.5" />
  </Icon>
);
export const IconTable = (p: IconProps) => (
  <Icon {...p}>
    <rect x="3.5" y="4.5" width="17" height="15" rx="1.5" />
    <path d="M3.5 9.5h17M3.5 14.5h17M9 4.5v15M15 4.5v15" />
  </Icon>
);
export const IconColumn = (p: IconProps) => (
  <Icon {...p}>
    <rect x="5" y="3.5" width="14" height="17" rx="1.5" />
    <path d="M5 9.5h14M5 14.5h14" />
  </Icon>
);
export const IconKey = (p: IconProps) => (
  <Icon {...p}>
    <circle cx="8" cy="12" r="3.5" />
    <path d="m11.5 12 7 0M16 12v3M19 12v2" />
  </Icon>
);
export const IconSparkle = (p: IconProps) => (
  <Icon {...p} d="M12 4 13.4 9.6 19 11l-5.6 1.4L12 18l-1.4-5.6L5 11l5.6-1.4z" />
);
export const IconBookmark = (p: IconProps) => <Icon {...p} d="M6 4h12v17l-6-4-6 4z" />;
export const IconBolt = (p: IconProps) => <Icon {...p} d="M13 3 5 14h6l-1 7 8-11h-6z" />;
export const IconFormat = (p: IconProps) => <Icon {...p} d="M4 6h16M4 10h10M4 14h16M4 18h8" />;
export const IconArrowDown = (p: IconProps) => <Icon {...p} d="M12 5v14m-5-5 5 5 5-5" />;
export const IconArrowUp = (p: IconProps) => <Icon {...p} d="M12 19V5m-5 5 5-5 5 5" />;
export const IconChevDownTiny = (p: IconProps) => (
  <Icon {...p} size={p.size ?? 10} d="m6 9 6 6 6-6" />
);
export const IconCopy = (p: IconProps) => (
  <Icon {...p}>
    <rect x="8" y="8" width="12" height="12" rx="1.5" />
    <path d="M16 8V5a1 1 0 0 0-1-1H5a1 1 0 0 0-1 1v10a1 1 0 0 0 1 1h3" />
  </Icon>
);
export const IconRefresh = (p: IconProps) => (
  <Icon {...p}>
    <path d="M4 12a8 8 0 0 1 14-5.3L21 4" />
    <path d="M21 4v5h-5" />
    <path d="M20 12a8 8 0 0 1-14 5.3L3 20" />
    <path d="M3 20v-5h5" />
  </Icon>
);
