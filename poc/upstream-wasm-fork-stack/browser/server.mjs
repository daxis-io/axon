import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { createServer } from "node:http";
import { dirname, extname, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";
import { gzipSync } from "node:zlib";

const BROWSER_DIR = dirname(fileURLToPath(import.meta.url));
const POC_DIR = resolve(BROWSER_DIR, "..");
const DEFAULT_FIXTURE_ROOT = resolve(POC_DIR, "fixtures");
const DEFAULT_WASM_PKG_ROOT = resolve(POC_DIR, "target", "browser-pkg");
const SAMPLE_OBJECT = Buffer.from("0123456789abcdefghijklmnopqrstuvwxyz");
const EXPOSED_HEADERS = [
  "Accept-Ranges",
  "Content-Encoding",
  "Content-Length",
  "Content-Range",
  "ETag",
].join(", ");

function parseRange(value, size) {
  if (value === undefined) {
    return undefined;
  }
  const match = /^bytes=(\d*)-(\d*)$/.exec(value);
  if (!match || (!match[1] && !match[2])) {
    throw new Error(`unsupported Range header: ${value}`);
  }
  let start;
  let end;
  if (!match[1]) {
    const suffix = Number(match[2]);
    start = Math.max(0, size - suffix);
    end = size;
  } else {
    start = Number(match[1]);
    end = match[2] ? Math.min(size, Number(match[2]) + 1) : size;
  }
  if (!Number.isSafeInteger(start) || !Number.isSafeInteger(end) || start < 0 || start >= end) {
    throw new Error(`invalid Range header for ${size}-byte object: ${value}`);
  }
  return { start, end };
}

function baseHeaders(body, etag) {
  return {
    "accept-ranges": "bytes",
    "cache-control": "no-store",
    "content-encoding": "identity",
    "content-length": String(body.length),
    etag,
  };
}

export function planObjectResponse({ body, etag, headers, scenario, attempt }) {
  const range = parseRange(headers.range, body.length);
  const responseHeaders = baseHeaders(body, etag);

  if (scenario === "encoded" && range) {
    const encodedBody = gzipSync(body);
    return {
      status: 200,
      headers: {
        ...baseHeaders(encodedBody, etag),
        "content-encoding": "gzip",
      },
      body: encodedBody,
    };
  }

  if (
    range &&
    ["fallback", "ordinary-200", "overbound", "missing-length"].includes(scenario)
  ) {
    if (scenario === "missing-length") {
      delete responseHeaders["content-length"];
    }
    return { status: 200, headers: responseHeaders, body };
  }

  if (scenario === "validator-mismatch" && attempt > 1 && range) {
    return {
      status: 200,
      headers: baseHeaders(body, '"changed-validator"'),
      body,
    };
  }

  const response = range
    ? {
        status: 206,
        headers: {
          ...responseHeaders,
          "content-length": String(range.end - range.start),
          "content-range": `bytes ${range.start}-${range.end - 1}/${body.length}`,
        },
        body: body.subarray(range.start, range.end),
      }
    : { status: 200, headers: responseHeaders, body };

  if (
    !range &&
    attempt === 1 &&
    (scenario === "retry" || scenario === "validator-mismatch")
  ) {
    response.truncateAt = Math.max(1, Math.floor(body.length / 2));
  }
  if (scenario === "invalid-length" && range) {
    const invalidLength = Math.max(1, response.body.length - 1);
    response.headers["content-length"] = String(invalidLength);
    response.body = response.body.subarray(0, invalidLength);
  }
  if (scenario === "invalid-content-range" && range) {
    response.headers["content-range"] = `bytes 0-${response.body.length - 1}/${body.length}`;
  }
  return response;
}

function contentType(pathname) {
  switch (extname(pathname)) {
    case ".html":
      return "text/html; charset=utf-8";
    case ".js":
    case ".mjs":
      return "text/javascript; charset=utf-8";
    case ".json":
      return "application/json";
    case ".wasm":
      return "application/wasm";
    case ".parquet":
      return "application/vnd.apache.parquet";
    default:
      return "application/octet-stream";
  }
}

function safePath(root, relativePath) {
  const candidate = resolve(root, relativePath);
  const prefix = `${resolve(root)}${sep}`;
  if (candidate !== resolve(root) && !candidate.startsWith(prefix)) {
    throw new Error(`path escapes server root: ${relativePath}`);
  }
  return candidate;
}

function listen(server, host) {
  return new Promise((resolveListening, reject) => {
    server.once("error", reject);
    server.listen(0, host, () => {
      server.off("error", reject);
      resolveListening(server.address());
    });
  });
}

function closeServer(server) {
  return new Promise((resolveClosed, reject) => {
    server.close((error) => (error ? reject(error) : resolveClosed()));
  });
}

export async function startPocServers({
  host = "127.0.0.1",
  fixtureRoot = DEFAULT_FIXTURE_ROOT,
  wasmPkgRoot = DEFAULT_WASM_PKG_ROOT,
} = {}) {
  const pageServer = createServer(async (request, response) => {
    try {
      const url = new URL(request.url, "http://page.invalid");
      const isWasmPackage = url.pathname.startsWith("/pkg/");
      const relativePath =
        url.pathname === "/"
          ? "index.html"
          : isWasmPackage
            ? url.pathname.slice("/pkg/".length)
            : url.pathname.slice(1);
      const root = isWasmPackage ? wasmPkgRoot : BROWSER_DIR;
      const bytes = await readFile(safePath(root, relativePath));
      response.writeHead(200, {
        "cache-control": "no-store",
        "content-length": String(bytes.length),
        "content-type": contentType(relativePath),
      });
      response.end(request.method === "HEAD" ? undefined : bytes);
    } catch (error) {
      response.writeHead(error?.code === "ENOENT" ? 404 : 500, {
        "content-type": "text/plain; charset=utf-8",
      });
      response.end(error instanceof Error ? error.message : String(error));
    }
  });
  const pageAddress = await listen(pageServer, host);
  const pageOrigin = `http://${host}:${pageAddress.port}`;

  const attempts = new Map();
  const requestLog = new Map();
  const dataServer = createServer(async (request, response) => {
    const url = new URL(request.url, "http://data.invalid");
    if (url.pathname === "/reset") {
      const scenario = url.searchParams.get("scenario");
      for (const key of attempts.keys()) {
        if (!scenario || key.startsWith(`${scenario}:`)) {
          attempts.delete(key);
        }
      }
      if (scenario) {
        requestLog.delete(scenario);
      } else {
        requestLog.clear();
      }
      response.writeHead(204, corsHeaders(pageOrigin));
      response.end();
      return;
    }
    if (url.pathname === "/stats") {
      const scenario = url.searchParams.get("scenario");
      const requests = scenario
        ? (requestLog.get(scenario) ?? [])
        : [...requestLog.values()].flat();
      const payload = Buffer.from(
        JSON.stringify({
          request_count: requests.filter(({ method }) => method !== "OPTIONS").length,
          transferred_bytes: requests.reduce(
            (total, entry) => total + entry.transferred_bytes,
            0,
          ),
          requests,
        }),
      );
      response.writeHead(200, {
        ...corsHeaders(pageOrigin),
        "content-length": String(payload.length),
        "content-type": "application/json",
      });
      response.end(payload);
      return;
    }

    const [, scenario, ...resourceParts] = url.pathname.split("/");
    const resource = resourceParts.join("/");
    const cors = scenario === "nocors" ? {} : corsHeaders(pageOrigin);
    if (request.method === "OPTIONS") {
      recordRequest(requestLog, scenario, {
        attempt: 0,
        method: request.method,
        pathname: url.pathname,
        range: request.headers.range ?? null,
        if_range: request.headers["if-range"] ?? null,
        origin: request.headers.origin ?? null,
        status: 204,
        transferred_bytes: 0,
      });
      response.writeHead(204, cors);
      response.end();
      return;
    }

    try {
      const body =
        resource === "sample.bin"
          ? SAMPLE_OBJECT
          : await readFile(safePath(fixtureRoot, resource));
      const etag = `"${createHash("sha256").update(body).digest("hex")}"`;
      const attemptKey = `${scenario}:${resource}`;
      const attempt = (attempts.get(attemptKey) ?? 0) + 1;
      attempts.set(attemptKey, attempt);
      const plan = planObjectResponse({
        body,
        etag,
        headers: request.headers,
        scenario,
        attempt,
      });
      const transferredBytes =
        request.method === "HEAD" ? 0 : (plan.truncateAt ?? plan.body.length);
      recordRequest(requestLog, scenario, {
        attempt,
        method: request.method,
        pathname: url.pathname,
        range: request.headers.range ?? null,
        if_range: request.headers["if-range"] ?? null,
        origin: request.headers.origin ?? null,
        status: plan.status,
        transferred_bytes: transferredBytes,
      });
      response.writeHead(plan.status, {
        ...plan.headers,
        ...cors,
        "content-type": contentType(resource),
      });
      if (request.method === "HEAD") {
        response.end();
      } else if (plan.truncateAt !== undefined) {
        response.write(plan.body.subarray(0, plan.truncateAt));
        setTimeout(() => response.socket?.destroy(), 10);
      } else {
        response.end(plan.body);
      }
    } catch (error) {
      const payload = Buffer.from(error instanceof Error ? error.message : String(error));
      response.writeHead(error?.code === "ENOENT" ? 404 : 416, {
        ...cors,
        "content-length": String(payload.length),
        "content-type": "text/plain; charset=utf-8",
      });
      response.end(payload);
    }
  });
  const dataAddress = await listen(dataServer, host);
  const dataOrigin = `http://${host}:${dataAddress.port}`;

  return {
    dataOrigin,
    pageOrigin,
    async close() {
      await Promise.all([closeServer(dataServer), closeServer(pageServer)]);
    },
  };
}

function corsHeaders(pageOrigin) {
  return {
    "access-control-allow-headers": "Range, If-Range, Content-Type, User-Agent",
    "access-control-allow-methods": "GET, HEAD, OPTIONS",
    "access-control-allow-origin": pageOrigin,
    "access-control-expose-headers": EXPOSED_HEADERS,
    vary: "Origin",
  };
}

function recordRequest(log, scenario, entry) {
  const entries = log.get(scenario) ?? [];
  entries.push(entry);
  log.set(scenario, entries);
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const servers = await startPocServers();
  process.stdout.write(`${JSON.stringify(servers, ["pageOrigin", "dataOrigin"])}\n`);
  for (const signal of ["SIGINT", "SIGTERM"]) {
    process.once(signal, async () => {
      await servers.close();
      process.exit(0);
    });
  }
}
