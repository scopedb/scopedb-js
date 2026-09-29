/*
 * Copyright 2024 ScopeDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { AppendRetryExhaustedError } from "../src/append-stream.js";
import { Client } from "../src/client.js";
import { AppendRowsError, ScopeDBError } from "../src/errors.js";
import { jsonResponse, makeFetchStub } from "./helpers.js";

function unreadableResponse(status: number, cause: Error, retryAfter = "0"): Response {
  return new Response(new ReadableStream({
    start(controller) {
      controller.error(cause);
    },
  }), {
    status,
    headers: {
      "X-Request-ID": "request-with-broken-body",
      "Retry-After": retryAfter,
    },
  });
}

describe("unreadable HTTP error responses", () => {
  for (const status of [401, 403, 503]) {
    it(`preserves direct append metadata and cause for HTTP ${status}`, async () => {
      const cause = new Error("response body interrupted");
      const { fn, calls } = makeFetchStub([unreadableResponse(status, cause, "2")]);
      const table = new Client("http://localhost:8080", { fetch: fn }).table("events");

      await assert.rejects(table.append('{"id":1}'), (error: unknown) => {
        assert.ok(error instanceof AppendRowsError);
        assert.equal(error.appendState, "unknown");
        assert.equal(error.httpStatus, status);
        assert.equal(error.requestId, "request-with-broken-body");
        assert.equal(error.retryAfterMs, 2_000);
        assert.equal(error.retryable, false);
        assert.ok(error.cause instanceof ScopeDBError);
        assert.equal(error.cause.cause, cause);
        return true;
      });
      assert.equal(calls.length, 1);
    });
  }

  for (const [status, retryable] of [
    [401, false], [403, false], [408, true], [429, true], [503, true],
  ] as const) {
    it(`keeps general request classification for HTTP ${status}`, async () => {
      const cause = new Error("response body interrupted");
      const { fn } = makeFetchStub([unreadableResponse(status, cause, "2")]);
      const client = new Client("http://localhost:8080", { fetch: fn });

      await assert.rejects(client.listDatabases(), (error: unknown) => {
        assert.ok(error instanceof ScopeDBError);
        assert.equal(error.httpStatus, status);
        assert.equal(error.requestId, "request-with-broken-body");
        assert.equal(error.retryAfterMs, 2_000);
        assert.equal(error.retryable, retryable);
        assert.equal(error.cause, cause);
        return true;
      });
    });
  }

  for (const status of [401, 403]) {
    it(`does not retry a stream append with HTTP ${status}`, async () => {
      const cause = new Error("response body interrupted");
      const { fn, calls } = makeFetchStub([unreadableResponse(status, cause)]);
      const stream = new Client("http://localhost:8080", { fetch: fn }).table("events")
        .appendStream().maxRetries(1).initialBackoff(0).build();

      await stream.send({ id: 1 });
      await assert.rejects(stream.shutdown(), (error: unknown) => {
        assert.ok(error instanceof AppendRowsError);
        assert.equal(error.appendState, "unknown");
        assert.equal(error.httpStatus, status);
        assert.equal(error.requestId, "request-with-broken-body");
        return true;
      });
      assert.equal(calls.length, 1);
      assert.equal(stream.stats().retries, 0);
      assert.equal(stream.stats().unknownRows, 1);
      assert.equal(stream.stats().lastFailure?.httpStatus, status);
    });
  }

  it("retries HTTP 503 and honors Retry-After when the body is unreadable", async () => {
    const attempts: number[] = [];
    const bodies: unknown[] = [];
    const fetch: typeof globalThis.fetch = async (_input, init) => {
      attempts.push(performance.now());
      bodies.push(init?.body);
      return attempts.length === 1
        ? unreadableResponse(503, new Error("response body interrupted"), "0.02")
        : jsonResponse(200, { append_state: "committed", num_rows_inserted: 1 });
    };
    const stream = new Client("http://localhost:8080", { fetch }).table("events")
      .appendStream().maxRetries(1).initialBackoff(0).build();

    await stream.send({ id: 1 });
    assert.deepEqual(await stream.shutdown(), {
      append_state: "committed",
      num_rows_inserted: 1,
    });
    assert.equal(attempts.length, 2);
    // Allow the same timer rounding tolerance as the rejected-retry test.
    assert.ok(attempts[1]! - attempts[0]! >= 15);
    assert.deepEqual(bodies[0], bodies[1]);
    assert.equal(stream.stats().retries, 1);
    assert.equal(stream.stats().committedRows, 1);
    assert.equal(stream.stats().unknownRows, 0);
  });

  it("retains HTTP metadata after exhausting unreadable HTTP 503 responses", async () => {
    const cause = new Error("response body interrupted");
    const { fn, calls } = makeFetchStub([
      unreadableResponse(503, cause),
      unreadableResponse(503, cause),
    ]);
    const stream = new Client("http://localhost:8080", { fetch: fn }).table("events")
      .appendStream().maxRetries(1).initialBackoff(0).build();

    await stream.send({ id: 1 });
    await assert.rejects(stream.shutdown(), (error: unknown) => {
      assert.ok(error instanceof AppendRetryExhaustedError);
      assert.equal(error.appendState, "unknown");
      assert.equal(error.httpStatus, 503);
      assert.equal(error.requestId, "request-with-broken-body");
      assert.equal(error.retryAfterMs, 0);
      assert.ok(error.cause instanceof AppendRowsError);
      assert.ok(error.cause.cause instanceof ScopeDBError);
      assert.equal(error.cause.cause.cause, cause);
      return true;
    });
    assert.equal(calls.length, 2);
    assert.equal(stream.stats().retries, 1);
    assert.equal(stream.stats().unknownRows, 1);
    assert.equal(stream.stats().lastFailure?.httpStatus, 503);
  });

  it("keeps an unreadable append outcome unknown after a rejected retry", async () => {
    const rejected = jsonResponse(422, {
      message: "invalid row",
      append_state: "rejected",
      row_errors: [{ row_index: 0, column: "id", message: "invalid id" }],
      row_errors_truncated: false,
    });
    rejected.headers.set("X-Request-ID", "rejected-request");
    const { fn, calls } = makeFetchStub([
      unreadableResponse(503, new Error("response body interrupted")),
      rejected,
    ]);
    const stream = new Client("http://localhost:8080", { fetch: fn }).table("events")
      .appendStream().maxRetries(1).initialBackoff(0).build();

    await stream.send({ id: 1 });
    await assert.rejects(stream.shutdown(), (error: unknown) => {
      assert.ok(error instanceof AppendRowsError);
      assert.equal(error.appendState, "unknown");
      assert.equal(error.httpStatus, 422);
      assert.equal(error.requestId, "rejected-request");
      assert.deepEqual(error.rowErrors, [
        { row_index: 0, column: "id", message: "invalid id" },
      ]);
      return true;
    });
    assert.equal(calls.length, 2);
    assert.equal(stream.stats().unknownRows, 1);
    assert.equal(stream.stats().failedRows, 0);
  });
});
