import init, {
  PocBrowserTable,
  decodeAggregateRows,
  probeRange,
  probeStream,
  validateIpcRows,
} from "/pkg/axon_upstream_wasm_browser_harness.js";

const AGGREGATE_SQL =
  "SELECT category, SUM(value) AS total FROM delta GROUP BY category ORDER BY category";
const EXPECTED_CONTENT_TYPE = "application/vnd.apache.arrow.stream";
const EXPECTED_ROWS = "alpha=7,beta=10";
const dataOrigin = new URLSearchParams(location.search).get("dataOrigin");
const statusElement = document.querySelector("#status");
const resultElement = document.querySelector("#result");
const runButton = document.querySelector("#run");

if (!dataOrigin) {
  throw new Error("the browser harness requires a dataOrigin query parameter");
}

let wasmMemory;
let memoryHighWaterBytes = 0;

function assert(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

function updateMemoryHighWater() {
  memoryHighWaterBytes = Math.max(
    memoryHighWaterBytes,
    wasmMemory?.buffer?.byteLength ?? 0,
  );
  return memoryHighWaterBytes;
}

async function sha256(bytes) {
  const digest = await crypto.subtle.digest("SHA-256", bytes);
  return [...new Uint8Array(digest)]
    .map((byte) => byte.toString(16).padStart(2, "0"))
    .join("");
}

async function reset(scenario) {
  const response = await fetch(
    `${dataOrigin}/reset?scenario=${encodeURIComponent(scenario)}`,
  );
  assert(response.ok, `failed to reset ${scenario}: ${response.status}`);
}

async function stats(scenario) {
  const response = await fetch(
    `${dataOrigin}/stats?scenario=${encodeURIComponent(scenario)}`,
  );
  assert(response.ok, `failed to read ${scenario} stats: ${response.status}`);
  return response.json();
}

async function expectedFailure(operation, expectedFragments) {
  try {
    await operation();
  } catch (error) {
    const message = String(error);
    for (const fragment of expectedFragments) {
      assert(
        message.includes(fragment),
        `expected error to include ${JSON.stringify(fragment)}; received ${message}`,
      );
    }
    updateMemoryHighWater();
    return message;
  }
  throw new Error(
    `expected failure containing ${expectedFragments.map(JSON.stringify).join(", ")}`,
  );
}

async function expectedFailureOneOf(operation, expectedAlternatives) {
  try {
    await operation();
  } catch (error) {
    const message = String(error);
    const matched = expectedAlternatives.some((fragments) =>
      fragments.every((fragment) => message.includes(fragment)),
    );
    assert(
      matched,
      `expected error to match one of ${JSON.stringify(expectedAlternatives)}; received ${message}`,
    );
    updateMemoryHighWater();
    return message;
  }
  throw new Error(
    `expected failure matching one of ${JSON.stringify(expectedAlternatives)}`,
  );
}

async function runSnappy() {
  await reset("normal");
  const startedAt = performance.now();
  const table = await PocBrowserTable.open(
    `${dataOrigin}/normal/snappy/`,
    2 * 1024 * 1024,
  );
  try {
    assert(
      Number(table.snapshotVersion) === 0,
      `expected snapshot version 0; received ${table.snapshotVersion}`,
    );
    const result = await table.query(AGGREGATE_SQL);
    try {
      const bytes = result.ipcBytes;
      assert(bytes instanceof Uint8Array, "Arrow IPC result is not a Uint8Array");
      assert(
        bytes.byteLength === result.ipcByteLength,
        "Arrow IPC Uint8Array is not exact-sized",
      );
      assert(result.rowCount === 2, `expected 2 rows; received ${result.rowCount}`);
      assert(
        validateIpcRows(bytes) === 2,
        "Arrow IPC stream did not decode to two rows",
      );
      const rows = decodeAggregateRows(bytes);
      assert(rows === EXPECTED_ROWS, `unexpected aggregate rows: ${rows}`);
      assert(
        result.contentType === EXPECTED_CONTENT_TYPE,
        `unexpected content type: ${result.contentType}`,
      );
      assert(
        result.executedOn === "browser_wasm",
        `unexpected execution target: ${result.executedOn}`,
      );
      assert(result.usedNativeFallback === false, "native fallback was used");
      const durationMs = performance.now() - startedAt;
      const transport = await stats("normal");
      const output = {
        bytes_fetched: result.bytesFetched,
        canonical_rows: rows,
        content_type: result.contentType,
        duration_ms: durationMs,
        executed_on: result.executedOn,
        ipc_byte_length: bytes.byteLength,
        ipc_sha256: await sha256(bytes),
        request_count: result.requestCount,
        row_count: result.rowCount,
        snapshot_version: Number(result.snapshotVersion),
        transport,
        used_native_fallback: result.usedNativeFallback,
      };
      updateMemoryHighWater();
      return output;
    } finally {
      result.free();
    }
  } finally {
    table.free();
  }
}

async function runZstdFailure() {
  await reset("normal");
  const table = await PocBrowserTable.open(
    `${dataOrigin}/normal/zstd/`,
    2 * 1024 * 1024,
  );
  try {
    assert(
      Number(table.snapshotVersion) === 0,
      "zstd metadata/schema replay did not reach snapshot version 0",
    );
    const error = await expectedFailure(
      () => table.query(AGGREGATE_SQL),
      [
        "cannot create Parquet zstd codec",
        'feature "zstd" is enabled',
        "wasm32-unknown-unknown",
      ],
    );
    return {
      error,
      metadata_schema_replay_succeeded: true,
      snapshot_version: Number(table.snapshotVersion),
      transport: await stats("normal"),
    };
  } finally {
    table.free();
  }
}

async function directCorsRangeProbe() {
  await reset("normal");
  const response = await fetch(`${dataOrigin}/normal/sample.bin`, {
    headers: { Range: "bytes=1-4" },
  });
  const body = new Uint8Array(await response.arrayBuffer());
  assert(response.status === 206, `expected direct CORS 206; received ${response.status}`);
  assert(
    response.headers.get("content-range") === "bytes 1-4/36",
    `unexpected exposed Content-Range: ${response.headers.get("content-range")}`,
  );
  assert(
    response.headers.get("etag")?.startsWith('"'),
    "strong ETag was not exposed through CORS",
  );
  assert(new TextDecoder().decode(body) === "1234", "unexpected direct range body");
  return stats("normal");
}

async function runProtocolSuite() {
  const directCors = await directCorsRangeProbe();

  await reset("normal");
  const normalBytes = await probeRange(`${dataOrigin}/normal/sample.bin`, 0);
  assert(
    new TextDecoder().decode(normalBytes) === "1234",
    "normal object_store range returned unexpected bytes",
  );
  const normal = await stats("normal");

  await reset("retry");
  const retryBytes = await probeStream(`${dataOrigin}/retry/sample.bin`);
  assert(
    new TextDecoder().decode(retryBytes) ===
      "0123456789abcdefghijklmnopqrstuvwxyz",
    "retried stream did not reconstruct the object",
  );
  const retry = await stats("retry");
  const retryGets = retry.requests.filter(({ method }) => method === "GET");
  assert(retryGets.length === 2, `expected two retry GETs; received ${retryGets.length}`);
  assert(retryGets[0].status === 200, "retry did not begin with a full 200 response");
  assert(retryGets[1].status === 206, "retry continuation was not a 206 response");
  assert(retryGets[1].range, "retry continuation omitted Range");
  assert(retryGets[1].if_range, "retry continuation omitted If-Range");

  await reset("validator-mismatch");
  const validatorMismatchError = await expectedFailure(
    () => probeStream(`${dataOrigin}/validator-mismatch/sample.bin`),
    ["did not honor If-Range", "changed-validator"],
  );
  const validatorMismatch = await stats("validator-mismatch");

  await reset("ordinary-200");
  const ordinary200Error = await expectedFailure(
    () => probeRange(`${dataOrigin}/ordinary-200/sample.bin`, 0),
    ["did not honor"],
  );

  await reset("fallback");
  const fallbackBytes = await probeRange(`${dataOrigin}/fallback/sample.bin`, 64);
  assert(
    new TextDecoder().decode(fallbackBytes) === "1234",
    "bounded full-object fallback returned unexpected bytes",
  );
  const fallback = await stats("fallback");

  await reset("overbound");
  const overboundError = await expectedFailure(
    () => probeRange(`${dataOrigin}/overbound/sample.bin`, 4),
    ["36-byte object exceeds configured 4-byte"],
  );

  await reset("missing-length");
  const missingLengthError = await expectedFailure(
    () => probeRange(`${dataOrigin}/missing-length/sample.bin`, 64),
    ["omitted Content-Length"],
  );

  await reset("encoded");
  const encodedError = await expectedFailureOneOf(
    () => probeRange(`${dataOrigin}/encoded/sample.bin`, 0),
    [
      ['Content-Encoding "gzip"', "expected identity"],
      ["browser Fetch failed"],
      ["did not honor the range request"],
    ],
  );

  await reset("invalid-length");
  const invalidLengthError = await expectedFailure(
    () => probeRange(`${dataOrigin}/invalid-length/sample.bin`, 0),
    ["declared 3 bytes", "1..5"],
  );

  await reset("invalid-content-range");
  const invalidContentRangeError = await expectedFailure(
    () => probeRange(`${dataOrigin}/invalid-content-range/sample.bin`, 0),
    ["Requested 1..5", "got 0..4"],
  );

  await reset("nocors");
  const corsError = await expectedFailure(
    () => probeRange(`${dataOrigin}/nocors/sample.bin`, 0),
    ["CORS", "Range", "If-Range", "Content-Range"],
  );

  updateMemoryHighWater();
  return {
    bounded_fallback: fallback,
    cors_error: corsError,
    direct_cors: directCors,
    encoded_error: encodedError,
    invalid_content_range_error: invalidContentRangeError,
    invalid_length_error: invalidLengthError,
    missing_length_error: missingLengthError,
    normal_range: normal,
    ordinary_partial_200_error: ordinary200Error,
    overbound_fallback_error: overboundError,
    retry,
    validator_mismatch: validatorMismatch,
    validator_mismatch_error: validatorMismatchError,
  };
}

async function runInteractive() {
  const output = {
    protocol: await runProtocolSuite(),
    snappy: await runSnappy(),
    zstd: await runZstdFailure(),
  };
  resultElement.textContent = JSON.stringify(output, null, 2);
  statusElement.textContent = "PASS: browser proof completed";
  return output;
}

window.pocReady = (async () => {
  const exports = await init();
  wasmMemory = exports.memory;
  updateMemoryHighWater();
  window.pocHarness = {
    memoryHighWater: () => updateMemoryHighWater(),
    runInteractive,
    runProtocolSuite,
    runSnappy,
    runZstdFailure,
  };
  statusElement.textContent = "Ready";
  runButton.disabled = false;
  runButton.addEventListener("click", () => {
    runButton.disabled = true;
    statusElement.textContent = "Running…";
    runInteractive()
      .catch((error) => {
        statusElement.textContent = "FAIL";
        resultElement.textContent = error?.stack ?? String(error);
      })
      .finally(() => {
        runButton.disabled = false;
      });
  });
})().catch((error) => {
  statusElement.textContent = "FAIL";
  resultElement.textContent = error?.stack ?? String(error);
  throw error;
});
