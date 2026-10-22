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
import { it } from "node:test";
import { setImmediate as nextTurn } from "node:timers/promises";
import { Client, AppendRowsError, ScopeDBError, type WriteOptions } from "../src/index.js";
import { jsonResponse, pendingStatus } from "./helpers.js";

const writes = {
  append: (client: Client, options?: WriteOptions) =>
    client.table("events").append('{"id":1}', options),
};

function stalledFetch(response?: Response, honorCancellation = false) {
  let started!: () => void;
  const ready = new Promise<void>((resolve) => { started = resolve; });
  const signals: AbortSignal[] = [];
  const fetch: typeof globalThis.fetch = async (_input, init) => {
    assert.ok(init?.signal);
    signals.push(init.signal);
    started();
    return response ?? new Promise<Response>((_resolve, reject) => {
      if (honorCancellation) {
        init.signal?.addEventListener("abort", () => reject(init.signal?.reason), { once: true });
      }
    });
  };
  return { fetch, ready, signals };
}

function unknownTimeout(error: unknown): boolean {
  assert.ok(error instanceof ScopeDBError);
  assert.equal(error.retryable, false);
  assert.deepEqual(error.writeDetails, { reason: "timeout", outcome: "unknown" });
  assert.equal(error.context().get("write_timeout_ms") !== undefined, true);
  if (error instanceof AppendRowsError) {
    assert.equal(error.appendState, "unknown");
  } else {
    assert.match(error.message, /commit outcome is unknown/);
  }
  return true;
}

for (const [name, write] of Object.entries(writes)) {
  it(`${name} defaults to a 30-second client timeout`, async (t) => {
    t.mock.timers.enable({ apis: ["setTimeout"] });
    const transport = stalledFetch();
    const client = new Client("https://example.com", { fetch: transport.fetch });
    const rejected = assert.rejects(write(client), unknownTimeout);
    await transport.ready;
    t.mock.timers.tick(29_999);
    assert.equal(transport.signals[0]!.aborted, false);
    t.mock.timers.tick(1);
    await rejected;
    assert.equal(transport.signals[0]!.aborted, true);
    assert.equal(transport.signals.length, 1);
  });

  it(`${name} starts its timeout after compression completes`, async (t) => {
    t.mock.timers.enable({ apis: ["setTimeout"] });
    let releaseCompression!: () => void;
    let startedCompression!: () => void;
    const compressionGate = new Promise<void>((resolve) => { releaseCompression = resolve; });
    const preparing = new Promise<void>((resolve) => { startedCompression = resolve; });
    t.mock.method(globalThis, "CompressionStream", function () {
      return new TransformStream<Uint8Array, Uint8Array>({
        async transform(chunk, controller) {
          startedCompression();
          await compressionGate;
          controller.enqueue(chunk);
        },
      });
    });
    const transport = stalledFetch();
    const client = new Client("https://example.com", { fetch: transport.fetch, writeTimeoutMs: 50 });
    const rejected = assert.rejects(write(client), unknownTimeout);
    await preparing;
    t.mock.timers.tick(1_000);
    assert.equal(transport.signals.length, 0);
    releaseCompression();
    await transport.ready;
    t.mock.timers.tick(49);
    assert.equal(transport.signals[0]!.aborted, false);
    t.mock.timers.tick(1);
    await rejected;
  });

  it(`${name} shares one deadline between response headers and body`, async (t) => {
    t.mock.timers.enable({ apis: ["setTimeout"] });
    let releaseHeaders!: (response: Response) => void;
    const headers = new Promise<Response>((resolve) => { releaseHeaders = resolve; });
    const transport = stalledFetch();
    const client = new Client("https://example.com", {
      writeTimeoutMs: 50,
      fetch: async (input, init) => {
        void transport.fetch(input, init);
        return headers;
      },
    });
    const rejected = assert.rejects(write(client), (error: unknown) => {
      unknownTimeout(error);
      assert.ok(error instanceof ScopeDBError);
      assert.equal(error.httpStatus, 200);
      return true;
    });
    await transport.ready;
    t.mock.timers.tick(30);
    releaseHeaders(new Response(new ReadableStream<Uint8Array>({}), { status: 200 }));
    await nextTurn();
    t.mock.timers.tick(20);
    await rejected;
  });

  it(`${name} permits a request timeout longer than the client default`, async (t) => {
    t.mock.timers.enable({ apis: ["setTimeout"] });
    const transport = stalledFetch();
    const client = new Client("https://example.com", { fetch: transport.fetch, writeTimeoutMs: 10 });
    const rejected = assert.rejects(write(client, { timeoutMs: 50 }), unknownTimeout);
    await transport.ready;
    t.mock.timers.tick(10);
    assert.equal(transport.signals[0]!.aborted, false);
    t.mock.timers.tick(40);
    await rejected;
  });

  for (const status of [200, 503]) {
    it(`${name} times out while reading an HTTP ${status} response body`, async (t) => {
      t.mock.timers.enable({ apis: ["setTimeout"] });
      const response = new Response(new ReadableStream<Uint8Array>({}), {
        status,
        headers: { "X-Request-ID": "stalled-body" },
      });
      const transport = stalledFetch(response);
      const client = new Client("https://example.com", { fetch: transport.fetch, writeTimeoutMs: 20 });
      const rejected = assert.rejects(write(client), (error: unknown) => {
        unknownTimeout(error);
        assert.ok(error instanceof ScopeDBError);
        assert.equal(error.httpStatus, status);
        assert.equal(error.requestId, "stalled-body");
        return true;
      });
      await transport.ready;
      await nextTurn();
      t.mock.timers.tick(20);
      await rejected;
    });
  }

  it(`${name} releases its timeout after success`, async (t) => {
    t.mock.timers.enable({ apis: ["setTimeout"] });
    const transport = stalledFetch(jsonResponse(200, {
      status: "committed", num_rows_inserted: 1,
    }));
    const client = new Client("https://example.com", { fetch: transport.fetch });
    await write(client);
    t.mock.timers.tick(30_000);
    assert.equal(transport.signals[0]!.aborted, false);
  });

  it(`${name} preserves caller cancellation before starting`, async () => {
    const transport = stalledFetch();
    const client = new Client("https://example.com", { fetch: transport.fetch });
    const reason = new Error("cancelled by caller");
    await assert.rejects(write(client, { signal: AbortSignal.abort(reason) }), (error) => error === reason);
    assert.equal(transport.signals.length, 0);
  });

  it(`${name} honors caller cancellation while a request is in flight`, async () => {
    const transport = stalledFetch();
    const client = new Client("https://example.com", { fetch: transport.fetch });
    const controller = new AbortController();
    const rejected = assert.rejects(write(client, { signal: controller.signal }), (error: unknown) => {
      assert.ok(error instanceof ScopeDBError);
      assert.equal(error.retryable, false);
      assert.deepEqual(error.writeDetails, { reason: "cancelled", outcome: "unknown" });
      return true;
    });
    await transport.ready;
    controller.abort(new Error("cancelled by caller"));
    await rejected;
    assert.equal(transport.signals[0]!.aborted, true);
  });

  for (const honorCancellation of [false, true]) {
    it(`${name} treats SDK cancellation reasons as unknown with an ${honorCancellation ? "abort-aware" : "abort-ignoring"} transport`, async () => {
      const reasons = [
        new ScopeDBError("ConfigInvalid", "cancelled by caller"),
        new ScopeDBError("Unexpected", "previous request timed out", {
          cause: new DOMException("previous timeout", "TimeoutError"),
          writeDetails: { reason: "timeout", outcome: "unknown" },
        }).withContext("write_timeout_ms", 10).setPersistent(),
        new AppendRowsError({
          message: "previous batch rejected",
          append_state: "rejected",
          row_errors: [],
          row_errors_truncated: false,
        }, "previous batch rejected", {
          httpStatus: 503, requestId: "previous-request", retryAfterMs: 5_000,
        }).withContext("previous_request", true).setTemporary(),
      ];
      for (const reason of reasons) {
        const originalStatus = reason.status();
        const transport = stalledFetch(undefined, honorCancellation);
        const client = new Client("https://example.com", { fetch: transport.fetch });
        const controller = new AbortController();
        const rejected = assert.rejects(write(client, { signal: controller.signal }), (error: unknown) => {
          assert.ok(error instanceof ScopeDBError);
          assert.notEqual(error, reason);
          assert.equal(error.retryable, false);
          assert.deepEqual(error.writeDetails, { reason: "cancelled", outcome: "unknown" });
          assert.ok(error.cause instanceof ScopeDBError);
          assert.equal(error.cause.cause, reason);
          assert.equal(error.httpStatus, undefined);
          assert.equal(error.requestId, undefined);
          assert.equal(error.retryAfterMs, undefined);
          assert.equal(error.context().size, 0);
          if (name === "append") {
            assert.ok(error instanceof AppendRowsError);
            assert.equal(error.appendState, "unknown");
          } else {
            assert.match(error.message, /commit outcome is unknown/);
          }
          return true;
        });
        await transport.ready;
        controller.abort(reason);
        await rejected;
        assert.equal(reason.status(), originalStatus);
        assert.equal(transport.signals[0]!.aborted, true);
      }
    });
  }

  it(`${name} preserves current response metadata when cancelled with an earlier SDK error`, async () => {
    const response = new Response(new ReadableStream<Uint8Array>({}), {
      status: 200,
      headers: { "X-Request-ID": "current-request", "Retry-After": "2" },
    });
    const transport = stalledFetch(response);
    const client = new Client("https://example.com", { fetch: transport.fetch });
    const controller = new AbortController();
    const reason = new ScopeDBError("Unexpected", "previous request failed", {
      httpStatus: 503, requestId: "previous-request", retryAfterMs: 5_000,
    }).withContext("previous_request", true).setTemporary();
    const rejected = assert.rejects(write(client, { signal: controller.signal }), (error: unknown) => {
      assert.ok(error instanceof ScopeDBError);
      assert.equal(error.retryable, false);
      assert.equal(error.httpStatus, 200);
      assert.equal(error.requestId, "current-request");
      assert.equal(error.retryAfterMs, 2_000);
      assert.equal(error.context().size, 0);
      assert.ok(error.cause instanceof ScopeDBError);
      assert.equal(error.cause.cause, reason);
      return true;
    });
    await transport.ready;
    await nextTurn();
    controller.abort(reason);
    await rejected;
    assert.equal(reason.status(), "temporary");
  });

  it(`${name} classifies a caller deadline as timeout without copying earlier request metadata`, async () => {
    const transport = stalledFetch(undefined, true);
    const client = new Client("https://example.com", { fetch: transport.fetch });
    const controller = new AbortController();
    const reason = new DOMException("caller deadline expired", "TimeoutError");
    const rejected = assert.rejects(write(client, { signal: controller.signal }), (error: unknown) => {
      assert.ok(error instanceof ScopeDBError);
      assert.deepEqual(error.writeDetails, { reason: "timeout", outcome: "unknown" });
      assert.equal(error.retryable, false);
      assert.ok(error.cause instanceof ScopeDBError);
      assert.equal(error.cause.cause, reason);
      return true;
    });
    await transport.ready;
    controller.abort(reason);
    await rejected;
  });

  it(`${name} classifies typed transport timeouts without waiting for the SDK timer`, async () => {
    for (const code of ["UND_ERR_CONNECT_TIMEOUT", "UND_ERR_HEADERS_TIMEOUT", "UND_ERR_BODY_TIMEOUT"]) {
      const reason = new TypeError("fetch failed", {
        cause: Object.assign(new Error("transport deadline expired"), { code }),
      });
      const client = new Client("https://example.com", { fetch: async () => { throw reason; } });
      await assert.rejects(write(client), (error: unknown) => {
        assert.ok(error instanceof ScopeDBError);
        assert.deepEqual(error.writeDetails, { reason: "timeout", outcome: "unknown" });
        assert.equal(error.retryable, false);
        assert.ok(error.cause instanceof ScopeDBError);
        assert.equal(error.cause.cause, reason);
        return true;
      });
    }
  });

  it(`${name} preserves the timeout contract when a native response-body deadline expires`, async () => {
    for (const status of [200, 403, 503]) {
      const reason = new TypeError("response terminated", {
        cause: Object.assign(new Error("body deadline expired"), { code: "UND_ERR_BODY_TIMEOUT" }),
      });
      const client = new Client("https://example.com", {
        fetch: async () => new Response(new ReadableStream<Uint8Array>({
          start(controller) { controller.error(reason); },
        }), { status, headers: { "x-request-id": "body-timeout", "retry-after": "2" } }),
      });
      await assert.rejects(write(client), (error: unknown) => {
        assert.ok(error instanceof ScopeDBError);
        assert.deepEqual(error.writeDetails, { reason: "timeout", outcome: "unknown" });
        assert.equal(error.retryable, false);
        assert.equal(error.httpStatus, status);
        assert.equal(error.requestId, "body-timeout");
        assert.equal(error.retryAfterMs, 2_000);
        let cause: unknown = error;
        while (cause instanceof Error && cause !== reason) cause = cause.cause;
        assert.equal(cause, reason);
        return true;
      });
    }
  });
}

for (const mode of ["stop", "continue"] as const) {
  it(`${mode} streams inherit the client timeout and settle without retrying`, async (t) => {
    t.mock.timers.enable({ apis: ["setTimeout"] });
    const transport = stalledFetch();
    const client = new Client("https://example.com", { fetch: transport.fetch, writeTimeoutMs: 50 });
    const stream = client.table("events").appendStream({ failurePolicy: mode }).build();
    await stream.send({ id: 1 });
    const shutdown = stream.shutdown();
    const settled = mode === "continue"
      ? shutdown
      : assert.rejects(shutdown, /commit outcome is unknown/);
    await transport.ready;
    t.mock.timers.tick(50);
    await settled;
    assert.equal(transport.signals.length, 1);
    if (mode === "continue") {
      assert.ok("stats" in stream);
      assert.equal(stream.stats().unknownRows, 1);
      assert.equal(stream.stats().retries, 0);
    }
  });
}

for (const failurePolicy of ["stop", "continue"] as const) {
  it(`${failurePolicy} append streams can override the client timeout`, async (t) => {
    t.mock.timers.enable({ apis: ["setTimeout"] });
    const transport = stalledFetch();
    const client = new Client("https://example.com", { fetch: transport.fetch, writeTimeoutMs: 10 });
    const stream = client.table("events").appendStream({ failurePolicy }).attemptTimeoutMs(50).build();
    await stream.send({ id: 1 });
    const shutdown = stream.shutdown();
    const settled = failurePolicy === "continue"
      ? shutdown
      : assert.rejects(shutdown, /commit outcome is unknown/);
    await transport.ready;
    t.mock.timers.tick(10);
    assert.equal(transport.signals[0]!.aborted, false);
    t.mock.timers.tick(40);
    await settled;
    assert.equal(stream.stats().unknownRows, 1);
    assert.equal(stream.stats().retries, 0);
  });
}

it("write timeout configuration is validated before sending", async () => {
  for (const writeTimeoutMs of [0, -1, NaN, Infinity, 0.5, 2_147_483_648]) {
    assert.throws(() => new Client("https://example.com", { writeTimeoutMs }),
      (error: unknown) => error instanceof ScopeDBError && error.kind === "ConfigInvalid");
  }
  const transport = stalledFetch();
  const client = new Client("https://example.com", { fetch: transport.fetch });
  for (const write of Object.values(writes)) {
    await assert.rejects(write(client, { timeoutMs: 0 }),
      (error: unknown) => error instanceof ScopeDBError && error.kind === "ConfigInvalid");
  }
  assert.equal(transport.signals.length, 0);
});

it("unknown append errors preserve the request ID from the response payload", async () => {
  const response = jsonResponse(503, { message: "unavailable", request_id: "body-request" });
  response.headers.set("X-Request-ID", "header-request");
  const client = new Client("https://example.com", { fetch: async () => response });
  await assert.rejects(writes.append(client), (error: unknown) => {
    assert.ok(error instanceof AppendRowsError);
    assert.equal(error.appendState, "unknown");
    assert.equal(error.requestId, "body-request");
    assert.equal(error.retryable, false);
    return true;
  });
});

it("statement requests keep their caller signal and have no write timeout", async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout"] });
  const controller = new AbortController();
  let receivedSignal: AbortSignal | null | undefined;
  const client = new Client("https://example.com", {
    writeTimeoutMs: 1,
    fetch: async (_input, init) => {
      receivedSignal = init?.signal;
      return jsonResponse(200, pendingStatus());
    },
  });
  await client.submitStatement({ statement: "SELECT 1", format: "json" }, { signal: controller.signal });
  t.mock.timers.tick(30_000);
  assert.equal(receivedSignal, controller.signal);
  assert.equal(controller.signal.aborted, false);
});

it("a server HTTP 408 rejection remains safely retryable", async () => {
  const client = new Client("https://example.com", {
    fetch: async () => jsonResponse(408, { message: "admission timed out", retryable: true, code: "ingest_failure", details: { status: "rejected" } }),
  });
  await assert.rejects(writes.append(client), (error: unknown) => {
    assert.ok(error instanceof AppendRowsError);
    assert.equal(error.appendState, "rejected");
    assert.equal(error.writeDetails, undefined);
    assert.equal(error.retryable, true);
    return true;
  });
});
