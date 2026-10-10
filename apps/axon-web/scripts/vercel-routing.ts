// Evaluates the repository's vercel.json routing rules the way the production host does.
//
// Vercel matches `source` with path-to-regexp. Only the subset this repository uses is supported:
// literal path text, `/:name` (exactly one segment), `/:name?`, `/:name+`, `/:name*` (any number of
// segments), and parenthesized regular-expression groups. Anything else throws, so a new rule the
// emulation cannot evaluate fails its tests instead of being silently treated as a non-match.

import { statSync } from 'node:fs';
import { resolve, sep } from 'node:path';

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

/** Every header that the matching `headers` rules attach to a response for this path. */
export function vercelResponseHeaders(
  config: VercelRoutingConfig,
  pathname: string,
): [string, string][] {
  return (config.headers ?? [])
    .filter((rule) => vercelSourcePattern(rule.source).test(pathname))
    .flatMap((rule) => rule.headers.map(({ key, value }): [string, string] => [key, value]));
}

type PreviewRequest = { url?: string };
type PreviewResponse = {
  statusCode: number;
  setHeader(name: string, value: string): unknown;
  end(body?: string): unknown;
};

/**
 * Connect middleware for `vite preview` that serves a build the way the production host does: a
 * built file first, then the first matching rewrite, otherwise a plain-text NOT_FOUND instead of
 * Vite's SPA fallback. Matching `headers` rules apply to every response.
 */
export function vercelPreviewMiddleware(config: VercelRoutingConfig, outDir: string) {
  const root = resolve(outDir);
  return (request: PreviewRequest, response: PreviewResponse, next: () => void): void => {
    const url = new URL(request.url ?? '/', 'http://preview.invalid');
    for (const [key, value] of vercelResponseHeaders(config, url.pathname)) {
      response.setHeader(key, value);
    }
    if (url.pathname === '/' || isBuiltFile(root, url.pathname)) {
      next();
      return;
    }
    const destination = vercelRewriteDestination(config, url.pathname);
    if (destination !== undefined) {
      request.url = `${destination}${url.search}`;
      next();
      return;
    }
    response.statusCode = 404;
    response.setHeader('Content-Type', 'text/plain; charset=utf-8');
    response.end('The page could not be found\n\nNOT_FOUND\n');
  };
}

function isBuiltFile(root: string, pathname: string): boolean {
  let decoded: string;
  try {
    decoded = decodeURIComponent(pathname);
  } catch {
    return false;
  }
  const file = resolve(root, `.${decoded}`);
  if (!file.startsWith(`${root}${sep}`)) return false;
  try {
    return statSync(file).isFile();
  } catch {
    return false;
  }
}
