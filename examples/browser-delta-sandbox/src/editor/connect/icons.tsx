import { Icon, type IconProps } from '../components/icons.tsx';

export const IconFolder = (p: IconProps) => (
  <Icon {...p}>
    <path d="M3.5 6.5a1 1 0 0 1 1-1H10l2 2h7.5a1 1 0 0 1 1 1V18a1 1 0 0 1-1 1h-15a1 1 0 0 1-1-1z" />
  </Icon>
);

export const IconLock = (p: IconProps) => (
  <Icon {...p}>
    <rect x="5" y="11" width="14" height="9" rx="1.5" />
    <path d="M8 11V8a4 4 0 0 1 8 0v3" />
  </Icon>
);

export const IconWarn = (p: IconProps) => (
  <Icon {...p}>
    <path d="M12 4 2.5 20h19z" />
    <path d="M12 10v4" />
    <circle cx="12" cy="17" r="0.6" fill="currentColor" stroke="none" />
  </Icon>
);

export const IconCheck = (p: IconProps) => <Icon {...p} d="m5 12 5 5 9-11" />;

export const IconDots = (p: IconProps) => (
  <Icon {...p}>
    <circle cx="5" cy="12" r="1.5" fill="currentColor" stroke="none" />
    <circle cx="12" cy="12" r="1.5" fill="currentColor" stroke="none" />
    <circle cx="19" cy="12" r="1.5" fill="currentColor" stroke="none" />
  </Icon>
);

export const IconCog = (p: IconProps) => (
  <Icon {...p}>
    <circle cx="12" cy="12" r="2.5" />
    <path d="M19.4 13.5a8 8 0 0 0 0-3l1.7-1.3-2-3.4-2 .8a8 8 0 0 0-2.6-1.5l-.3-2.1h-4l-.3 2.1a8 8 0 0 0-2.6 1.5l-2-.8-2 3.4 1.7 1.3a8 8 0 0 0 0 3L3.3 14.8l2 3.4 2-.8a8 8 0 0 0 2.6 1.5l.3 2.1h4l.3-2.1a8 8 0 0 0 2.6-1.5l2 .8 2-3.4z" />
  </Icon>
);

export const IconSchema = (p: IconProps) => (
  <Icon {...p}>
    <rect x="3.5" y="4.5" width="9" height="6" rx="1" />
    <rect x="11.5" y="13.5" width="9" height="6" rx="1" />
    <path d="M8 10.5v3M16 10.5v3M8 13.5h8" />
  </Icon>
);

export const IconShareNode = (p: IconProps) => (
  <Icon {...p}>
    <circle cx="6" cy="12" r="2.5" />
    <circle cx="18" cy="6" r="2.5" />
    <circle cx="18" cy="18" r="2.5" />
    <path d="m8.2 10.8 7.6-3.6M8.2 13.2l7.6 3.6" />
  </Icon>
);
