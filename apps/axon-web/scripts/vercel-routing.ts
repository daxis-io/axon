// Evaluates the repository's vercel.json routing rules the way the production host does.
//
// Vercel matches `source` with path-to-regexp. Only the subset this repository uses is supported:
// literal path text, `/:name` (exactly one segment), `/:name?`, `/:name+`, `/:name*` (any number of
// segments), and parenthesized regular-expression groups. Anything else throws, so a new rule the
// emulation cannot evaluate fails its tests instead of being silently treated as a non-match.

export type VercelRewrite = { source: string; destination: string };
export type VercelHeaderRule = { source: string; headers: { key: string; value: string }[] };
export type VercelRoutingConfig = { rewrites?: VercelRewrite[]; headers?: VercelHeaderRule[] };

const SEGMENT = '[^/#?]+?';

export function vercelSourcePattern(source: string): RegExp {
  if (!source.startsWith('/')) {
    throw new Error(`unsupported Vercel source '${source}': it must start with '/'`);
  }
  let pattern = '';
  for (let index = 0; index < source.length; ) {
    const rest = source.slice(index);
    // A parameter followed by a custom pattern, such as `:id(\d+)`, is not supported.
    const parameter = /^\/:([A-Za-z_]\w*)(?!\w)([*+?]?)(?![(*+?])/.exec(rest);
    if (parameter) {
      const modifier = parameter[2];
      if (modifier === '') pattern += `/(${SEGMENT})`;
      else if (modifier === '?') pattern += `(?:/(${SEGMENT}))?`;
      else if (modifier === '+') pattern += `/(${SEGMENT}(?:/${SEGMENT})*)`;
      else pattern += `(?:/(${SEGMENT}(?:/${SEGMENT})*))?`;
      index += parameter[0].length;
      continue;
    }
    const group = /^\(([^()]*)\)/.exec(rest);
    if (group) {
      pattern += `(${group[1]})`;
      index += group[0].length;
      continue;
    }
    if (/^[A-Za-z0-9._~/-]/.test(rest)) {
      pattern += rest[0].replace(/[.]/g, '\\.');
      index += 1;
      continue;
    }
    throw new Error(`unsupported Vercel source syntax in '${source}' at '${rest}'`);
  }
  // path-to-regexp defaults: case-insensitive, optional trailing slash, anchored at both ends.
  return new RegExp(`^${pattern}/?$`, 'i');
}

/** The destination of the first rewrite whose source matches, or undefined when none does. */
export function vercelRewriteDestination(
  config: VercelRoutingConfig,
  pathname: string,
): string | undefined {
  return (config.rewrites ?? []).find((rewrite) =>
    vercelSourcePattern(rewrite.source).test(pathname),
  )?.destination;
}
