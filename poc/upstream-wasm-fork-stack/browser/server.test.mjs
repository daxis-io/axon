import assert from "node:assert/strict";
import test from "node:test";

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

test("invalid continuations keep a matching validator so only the framing is wrong", () => {
  const outstanding = { range: "bytes=18-35", "if-range": '"sample"' };
  const plan = (scenario) =>
    planObjectResponse({
      body: SAMPLE,
      etag: '"sample"',
      headers: outstanding,
      scenario,
      attempt: 2,
    });

  const nonPartial = plan("retry-non-partial");
  assert.equal(nonPartial.status, 200);
  assert.equal(nonPartial.headers.etag, '"sample"');

  const changedSize = plan("retry-changed-size");
  assert.equal(changedSize.status, 206);
  assert.equal(changedSize.headers["content-range"], "bytes 18-35/37");
  assert.equal(changedSize.headers["content-length"], "18");

  const enclosing = plan("retry-enclosing");
  assert.equal(enclosing.status, 206);
  assert.equal(enclosing.headers["content-range"], "bytes 0-35/36");
  assert.equal(enclosing.headers["content-length"], "36");

  const shifted = plan("retry-shifted-range");
  assert.equal(shifted.status, 206);
  assert.equal(shifted.headers["content-range"], "bytes 19-35/36");
  assert.equal(shifted.headers["content-length"], "17");

  // Every one of them must still declare a length that agrees with its
  // Content-Range, so the read fails on the property under test rather than on
  // declared-length validation.
  for (const invalid of [changedSize, enclosing, shifted]) {
    assert.equal(Number(invalid.headers["content-length"]), invalid.body.length);
  }
});

test("invalid continuation scenarios truncate their first response to force a retry", () => {
  for (const scenario of [
    "retry-non-partial",
    "retry-changed-size",
    "retry-enclosing",
    "retry-shifted-range",
  ]) {
    const plan = planObjectResponse({
      body: SAMPLE,
      etag: '"sample"',
      headers: {},
      scenario,
      attempt: 1,
    });
    assert.equal(plan.status, 200, scenario);
    assert.equal(plan.truncateAt, 18, scenario);
  }
});

test("encoded range scenario reaches the identity-encoding validator after valid range framing", () => {
  const plan = planObjectResponse({
    body: SAMPLE,
    etag: '"sample"',
    headers: { range: "bytes=1-4" },
    scenario: "encoded",
    attempt: 1,
  });

  assert.equal(plan.status, 206);
  assert.equal(plan.headers["content-encoding"], "identity, identity");
  assert.equal(plan.headers["content-length"], "4");
  assert.equal(plan.headers["content-range"], "bytes 1-4/36");
  assert.deepEqual(plan.body, Buffer.from("1234"));
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
