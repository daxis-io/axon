export function formatRows(n: number | null | undefined): string {
  if (n == null) return '';
  if (n < 1_000) return n.toString();
  if (n < 1_000_000) return (n / 1_000).toFixed(n < 10_000 ? 1 : 0) + 'k';
  if (n < 1_000_000_000) return (n / 1_000_000).toFixed(n < 10_000_000 ? 1 : 0) + 'M';
  return (n / 1_000_000_000).toFixed(n < 10_000_000_000 ? 1 : 0) + 'B';
}

export function formatBytes(b: number | null | undefined): string {
  if (b == null) return '—';
  if (b < 1024) return b + ' B';
  if (b < 1024 ** 2) return (b / 1024).toFixed(1) + ' KB';
  if (b < 1024 ** 3) return (b / 1024 ** 2).toFixed(1) + ' MB';
  if (b < 1024 ** 4) return (b / 1024 ** 3).toFixed(1) + ' GB';
  return (b / 1024 ** 4).toFixed(1) + ' TB';
}

export function prettifySql(sql: string): string {
  return sql
    .replace(/\s+\n/g, '\n')
    .replace(/\n{3,}/g, '\n\n')
    .replace(
      /\b(select|from|where|group by|order by|having|limit|join|left join|right join|inner join|on|and|or|with|as|case|when|then|else|end|insert|update|delete|values|distinct|union|all|interval|filter|in|between|like)\b/gi,
      (m) => m.toUpperCase(),
    );
}

export function hexToSoft(hex: string, dark: boolean): string {
  const h = hex.replace('#', '');
  const r = parseInt(h.substring(0, 2), 16);
  const g = parseInt(h.substring(2, 4), 16);
  const b = parseInt(h.substring(4, 6), 16);
  return dark ? `rgba(${r},${g},${b},0.22)` : `rgba(${r},${g},${b},0.10)`;
}
