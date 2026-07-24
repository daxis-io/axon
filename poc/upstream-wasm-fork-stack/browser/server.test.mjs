import assert from "node:assert/strict";
import test from "node:test";
import { gunzipSync } from "node:zlib";

import { planObjectResponse, startPocServers } from "./server.mjs";

const SAMPLE = Buffer.from("0123456789abcdefghijklmnopqrstuvwxyz");

test("normal range responses are exact identity-encoded 206 responses", () => {
  const plan = planObjectResponse({
    body: SAMPLE,
    etag: '"sample"',
    headers: { range: "bytes=1-4" },
    scenario: "normal",
    attempt: 1,
  });

  assert.equal(plan.status, 206);
  assert.equal(plan.headers["content-range"], "bytes 1-4/36");
  assert.equal(plan.headers["content-length"], "4");
  assert.equal(plan.headers.etag, '"sample"');
  assert.deepEqual(plan.body, Buffer.from("1234"));
});

test("bounded-fallback scenarios deliberately return a complete 200 response", () => {
  const plan = planObjectResponse({
    body: SAMPLE,
    etag: '"sample"',
    headers: { range: "bytes=1-4" },
    scenario: "fallback",
    attempt: 1,
  });

  assert.equal(plan.status, 200);
  assert.equal(plan.headers["content-length"], "36");
  assert.deepEqual(plan.body, SAMPLE);
});

test("retry continuation responses preserve the validator and requested range", () => {
  const plan = planObjectResponse({
    body: SAMPLE,
    etag: '"sample"',
    headers: { range: "bytes=18-35", "if-range": '"sample"' },
    scenario: "retry",
    attempt: 2,
  });

  assert.equal(plan.status, 206);
  assert.equal(plan.headers.etag, '"sample"');
  assert.equal(plan.headers["content-range"], "bytes 18-35/36");
  assert.deepEqual(plan.body, SAMPLE.subarray(18));
});

test("encoded range scenario returns a valid encoded full-object fallback", () => {
  const plan = planObjectResponse({
    body: SAMPLE,
    etag: '"sample"',
    headers: { range: "bytes=1-4" },
    scenario: "encoded",
    attempt: 1,
  });

  assert.equal(plan.status, 200);
  assert.equal(plan.headers["content-encoding"], "gzip");
  assert.equal(plan.headers["content-length"], String(plan.body.length));
  assert.equal(plan.headers["content-range"], undefined);
  assert.deepEqual(gunzipSync(plan.body), SAMPLE);
});

test("CORS preflight allows browser adapter request headers", async () => {
  const servers = await startPocServers();
  try {
    const response = await fetch(`${servers.dataOrigin}/normal/sample.bin`, {
      method: "OPTIONS",
      headers: {
        Origin: servers.pageOrigin,
        "Access-Control-Request-Headers": "range, if-range, user-agent",
        "Access-Control-Request-Method": "GET",
      },
    });

    assert.equal(response.status, 204);
    const allowed = response.headers
      .get("access-control-allow-headers")
      .toLowerCase();
    for (const header of ["range", "if-range", "user-agent"]) {
      assert(
        allowed.split(",").map((value) => value.trim()).includes(header),
        `${header} missing from Access-Control-Allow-Headers`,
      );
    }
  } finally {
    await servers.close();
  }
});
