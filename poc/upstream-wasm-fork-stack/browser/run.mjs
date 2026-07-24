import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { resolve } from "node:path";
import { brotliCompressSync, gzipSync } from "node:zlib";

import { chromium, firefox } from "playwright";

import { startPocServers } from "./server.mjs";

const BROWSER_DIR = new URL(".", import.meta.url).pathname;
const POC_DIR = resolve(BROWSER_DIR, "..");
const AXON_ROOT = resolve(POC_DIR, "..", "..");
const WASM_PATH = resolve(
  POC_DIR,
  "target/wasm32-unknown-unknown/release/axon_upstream_wasm_browser_harness.wasm",
);
const STACK_LOCK_PATH = resolve(POC_DIR, "stack.lock.toml");
const CARGO_LOCK_PATH = resolve(POC_DIR, "Cargo.lock");

function sha256(bytes) {
  return createHash("sha256").update(bytes).digest("hex");
}

function version(command, args = ["--version"]) {
  return execFileSync(command, args, { encoding: "utf8" }).trim();
}

function round(value) {
  return Math.round(value * 100) / 100;
}

function median(values) {
  const sorted = [...values].sort((left, right) => left - right);
  const middle = Math.floor(sorted.length / 2);
  return sorted.length % 2
    ? sorted[middle]
    : (sorted[middle - 1] + sorted[middle]) / 2;
}

async function evaluateStep(page, browserName, label, expression, timeoutMs = 60_000) {
  process.stderr.write(`[${browserName}] ${label}\n`);
  let timer;
  try {
    return await Promise.race([
      page.evaluate(expression),
      new Promise((_, reject) => {
        timer = setTimeout(
          () => reject(new Error(`${browserName} timed out during ${label}`)),
          timeoutMs,
        );
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

async function runBrowser(config, pageOrigin, dataOrigin, evidenceDir) {
  const browser = await config.type.launch(config.launch);
  const browserVersion = browser.version();
  const context = await browser.newContext();
  const page = await context.newPage();
  const consoleMessages = [];
  page.on("console", (message) => {
    const rendered = `${message.type()}: ${message.text()}`;
    consoleMessages.push(rendered);
    process.stderr.write(`[${config.name}] ${rendered}\n`);
  });
  page.on("pageerror", (error) => {
    consoleMessages.push(`pageerror: ${error.stack ?? error.message}`);
  });

  try {
    const coldStartedAt = performance.now();
    await page.goto(
      `${pageOrigin}/?dataOrigin=${encodeURIComponent(dataOrigin)}`,
      { waitUntil: "load" },
    );
    await evaluateStep(page, config.name, "initialize WASM", () => window.pocReady);
    const cold = await evaluateStep(page, config.name, "cold snapshot/query", () =>
      window.pocHarness.runSnappy(),
    );
    cold.end_to_end_duration_ms = round(performance.now() - coldStartedAt);

    const discardedWarmup = await evaluateStep(
      page,
      config.name,
      "discarded warmup",
      () => window.pocHarness.runSnappy(),
    );
    const warmRuns = [];
    for (let run = 0; run < 5; run += 1) {
      warmRuns.push(
        await evaluateStep(page, config.name, `measured warm run ${run + 1}/5`, () =>
          window.pocHarness.runSnappy(),
        ),
      );
    }
    const protocol = await evaluateStep(page, config.name, "protocol suite", () =>
      window.pocHarness.runProtocolSuite(),
    );
    const zstd = await evaluateStep(page, config.name, "zstd failure boundary", () =>
      window.pocHarness.runZstdFailure(),
    );
    const memoryHighWaterBytes = await page.evaluate(() =>
      window.pocHarness.memoryHighWater(),
    );
    const warmDurations = warmRuns.map(({ duration_ms }) => duration_ms);

    return {
      browser: config.name,
      browser_version: browserVersion,
      cold,
      console: consoleMessages,
      discarded_warmup: discardedWarmup,
      memory_high_water_bytes: memoryHighWaterBytes,
      protocol,
      warm: {
        max_duration_ms: round(Math.max(...warmDurations)),
        median_duration_ms: round(median(warmDurations)),
        runs: warmRuns,
      },
      zstd,
    };
  } catch (error) {
    await mkdir(evidenceDir, { recursive: true });
    await page.screenshot({
      fullPage: true,
      path: resolve(evidenceDir, `${config.name}-failure.png`),
    });
    throw error;
  } finally {
    await context.close();
    await browser.close();
  }
}

const stackLock = await readFile(STACK_LOCK_PATH);
const cargoLock = await readFile(CARGO_LOCK_PATH);
const wasm = await readFile(WASM_PATH);
const stackLockHash = sha256(stackLock);
const evidenceDir =
  process.env.POC_EVIDENCE_DIR ??
  resolve(AXON_ROOT, "target/upstream-wasm-fork-poc-evidence", stackLockHash);
const servers = await startPocServers();

try {
  const browserResults = [];
  for (const config of [
    {
      name: "chrome",
      type: chromium,
      launch: { channel: "chrome", headless: true },
    },
    {
      name: "firefox",
      type: firefox,
      launch: {
        headless: true,
        firefoxUserPrefs: { "permissions.default.loopback-network": 1 },
      },
    },
  ]) {
    browserResults.push(
      await runBrowser(
        config,
        servers.pageOrigin,
        servers.dataOrigin,
        evidenceDir,
      ),
    );
  }

  const [chrome, firefoxResult] = browserResults;
  for (const field of [
    "canonical_rows",
    "content_type",
    "executed_on",
    "ipc_sha256",
    "row_count",
    "snapshot_version",
    "used_native_fallback",
  ]) {
    assert.deepEqual(
      chrome.cold[field],
      firefoxResult.cold[field],
      `Chrome and Firefox disagree on ${field}`,
    );
  }
  assert.equal(chrome.cold.canonical_rows, "alpha=7,beta=10");
  assert.equal(chrome.cold.executed_on, "browser_wasm");
  assert.equal(chrome.cold.used_native_fallback, false);

  const evidence = {
    browsers: browserResults,
    bundle_bytes: {
      brotli: brotliCompressSync(wasm).length,
      gzip: gzipSync(wasm).length,
      raw: wasm.length,
      sha256: sha256(wasm),
    },
    generated_at: new Date().toISOString(),
    locks: {
      cargo_lock_sha256: sha256(cargoLock),
      stack_lock_sha256: stackLockHash,
    },
    network: {
      data_origin: servers.dataOrigin,
      page_origin: servers.pageOrigin,
      two_origin: new URL(servers.dataOrigin).origin !== new URL(servers.pageOrigin).origin,
    },
    toolchain: {
      cargo: version("cargo"),
      node: process.version,
      rustc: version("rustc"),
      wasm_bindgen: version("wasm-bindgen"),
    },
  };
  await mkdir(evidenceDir, { recursive: true });
  const evidencePath = resolve(evidenceDir, "browser-evidence.json");
  await writeFile(evidencePath, `${JSON.stringify(evidence, null, 2)}\n`);
  process.stdout.write(
    `${JSON.stringify(
      {
        browsers: browserResults.map(
          ({ browser, browser_version, cold, memory_high_water_bytes, warm }) => ({
            browser,
            browser_version,
            cold_duration_ms: cold.end_to_end_duration_ms,
            memory_high_water_bytes,
            warm_max_ms: warm.max_duration_ms,
            warm_median_ms: warm.median_duration_ms,
          }),
        ),
        bundle_bytes: evidence.bundle_bytes,
        evidence_path: evidencePath,
      },
      null,
      2,
    )}\n`,
  );
} finally {
  await servers.close();
}
