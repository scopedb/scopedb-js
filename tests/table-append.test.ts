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
import { setImmediate as nextTurn } from "node:timers/promises";
import {
  AppendRowsError,
  Client,
  encodeAppendRecord,
  ResultSet,
  ScopeDBError,
  Table,
} from "../src/index.js";
import {
  jsonResponse,
  makeFetchStub,
  pendingStatus,
  requestBodyText,
} from "./helpers.js";

describe("Table.identifier — ScopeQL quoting", () => {
  const client = new Client("http://localhost:8080", {
    fetch: makeFetchStub([]).fn,
  });

  it("quotes database, schema, and table identifiers with special character escaping", () => {
    assert.equal(
      new Table(client, "events").identifier(),
      "`scopedb`.`public`.`events`",
    );
    assert.equal(
      new Table(client, "events", { database: "mydb", schema: "logs" }).identifier(),
      "`mydb`.`logs`.`events`",
    );
    assert.equal(
      new Table(client, "my`table\t\n\r\\ctrl\x01").identifier(),
      "`scopedb`.`public`.`my\\`table\\t\\n\\r\\\\ctrl\\x01`",
    );
  });
});

describe("encodeAppendRecord and strict encoding", () => {
  function rejectsAt(record: unknown, path: string, message: RegExp): void {
    assert.throws(
      () => encodeAppendRecord(record),
      (error: unknown) => {
        assert.ok(error instanceof ScopeDBError);
        assert.equal(error.kind, "AppendRowsFailed");
        assert.equal(error.isPermanent(), true);
        assert.equal(error.context().get("record_path"), path);
        assert.match(error.message, message);
        return true;
      },
    );
  }

  it("emits exact int64 number tokens and preserves JSON escaping, Dates, and shared objects", () => {
    assert.equal(
      encodeAppendRecord({
        id: 9223372036854775807n,
        attrs: { min: -9223372036854775808n },
        items: [0n, "9223372036854775807", "1n"],
      }),
      '{"id":9223372036854775807,"attrs":{"min":-9223372036854775808},"items":[0,"9223372036854775807","1n"]}',
    );

    const shared = { value: 1.25 };
    const record = Object.assign(Object.create(null) as Record<string, unknown>, {
      'quote"\\\n': "中文 🦉\n\"\\\ud800",
      date: new Date("2026-10-29T00:00:00.123Z"),
      items: [shared, true, null, shared, Number.MIN_VALUE, Number.MAX_SAFE_INTEGER],
    });
    record["__proto__"] = { enabled: true };
    assert.equal(encodeAppendRecord(record), JSON.stringify(record));
    assert.equal(encodeAppendRecord({}), "{}");
  });

  it("rejects non-finite, unsafe, out-of-range, omitted, custom, or cyclic values at their path", () => {
    for (const value of [NaN, Infinity, -Infinity]) {
      rejectsAt({ attrs: [{ score: value }] }, '$["attrs"][0]["score"]', /finite/);
    }
    for (const value of [Number.MAX_SAFE_INTEGER + 1, -(Number.MAX_SAFE_INTEGER + 1), 1e100]) {
      rejectsAt({ id: value }, '$["id"]', /safe.*bigint/);
    }
    for (const value of [9223372036854775808n, -9223372036854775809n]) {
      rejectsAt({ attrs: { unsigned: value } }, '$["attrs"]["unsigned"]', /signed 64-bit/);
    }
    for (const value of [undefined, () => 1, Symbol("value")]) {
      rejectsAt({ value }, '$["value"]', /unsupported/);
      rejectsAt({ values: [value] }, '$["values"][0]', /unsupported/);
    }
    rejectsAt({ values: new Array(1) }, '$["values"][0]', /sparse/);
    rejectsAt({ date: new Date(NaN) }, '$["date"]', /Date must be valid/);
    rejectsAt({ [Symbol("key")]: 1 }, "$", /keys must be strings/);
    for (const value of [null, [], new Date(), "{}", 1, 1n]) {
      rejectsAt(value, "$", /plain object/);
    }
    for (const value of [new Map(), new Set(), new Number(1), new Uint8Array([1])]) {
      rejectsAt({ value }, '$["value"]', /convert custom objects/);
    }
    let invoked = false;
    rejectsAt(
      { toJSON() { invoked = true; return { id: NaN }; } },
      '$["toJSON"]',
      /unsupported function/,
    );
    rejectsAt({ get id() { invoked = true; return 1; } }, '$["id"]', /accessor/);
    assert.equal(invoked, false);

    const cycle: Record<string, unknown> = {};
    cycle["again"] = cycle;
    rejectsAt({ nested: cycle }, '$["nested"]["again"]', /circular/);
  });

  it("integrates with AppendStream.strictEncoding(true) and Table.append()", async () => {
    const record = ResultSet.fromStatementResultSet({
      metadata: {
        num_rows: 1,
        fields: [
          { name: "id", data_type: "int" },
          { name: "unsigned", data_type: "uint" },
          { name: "time", data_type: "timestamp" },
        ],
      },
      format: "json",
      rows: [["-9223372036854775808", "9223372036854775807", "2026-10-29T00:00:00.123Z"]],
    }).first();
    const { fn, calls } = makeFetchStub([
      jsonResponse(200, { status: "committed", num_rows_inserted: 1 }),
      jsonResponse(200, { status: "committed", num_rows_inserted: 3 }),
    ]);
    const table = new Client("http://localhost:8080", { fetch: fn }).table("events");

    const stream = table.appendStream().strictEncoding(true).build();
    await stream.send(record);
    assert.equal((await stream.shutdown())?.num_rows_inserted, 1);
    assert.equal(
      requestBodyText(calls[0]?.init),
      '{"id":-9223372036854775808,"unsigned":9223372036854775807,"time":"2026-10-29T00:00:00.123Z"}',
    );

    const stream2 = table
      .appendStream()
      .strictEncoding(true)
      .flushIntervalMs(60_000)
      .build();
    await assert.rejects(
      stream2.sendAll([{ id: 1n }, { id: NaN }, { id: 3n }]),
      /finite/,
    );
    assert.equal(stream2.stats().acceptedRows, 1);
    assert.equal(stream2.trySend({ id: Infinity }), false);
    assert.equal(stream2.stats().droppedByReason.invalidRecord, 1);
    await stream2.sendAll([{ id: 2n }, { id: 3n }]);
    assert.equal((await stream2.shutdown())?.num_rows_inserted, 3);
    assert.equal(requestBodyText(calls[1]?.init), '{"id":1}\n{"id":2}\n{"id":3}');
  });
});

describe("Table.append — request and response contracts", () => {
  it("posts gzip-compressed NDJSON to the escaped table ingest endpoint", async () => {
    const { fn, calls } = makeFetchStub([
      jsonResponse(200, { status: "committed", num_rows_inserted: 2 }),
    ]);
    const client = new Client("http://localhost:8080", { fetch: fn });
    const table = client.table("page views?#", {
      database: "analytics/raw",
      schema: "events 2026",
    });
    const ndjson = '{"id":1}\n{"id":2}\n';

    assert.deepEqual(await table.append(ndjson), {
      status: "succeeded",
      num_rows_inserted: 2,
    });
    assert.equal(
      calls[0]!.url,
      "http://localhost:8080/v1/catalog/databases/analytics%2Fraw/schemas/events%202026/tables/page%20views%3F%23/ingest",
    );
    const init = calls[0]!.init as RequestInit;
    assert.equal(init.method, "POST");
    assert.equal(requestBodyText(init), ndjson);
    const headers = init.headers as Headers;
    assert.equal(headers.get("Content-Type"), "application/x-ndjson");
    assert.equal(headers.get("Content-Encoding"), "gzip");
  });

  it("rejects payloads over 8 MiB or 200,000 rows locally before sending", async () => {
    const { fn, calls } = makeFetchStub([]);
    const table = new Client("http://localhost:8080", { fetch: fn }).table("events");

    for (const payload of [
      "x".repeat(8 * 1024 * 1024 + 1),
      "{}\n".repeat(200_001),
    ]) {
      await assert.rejects(
        () => table.append(payload),
        (err: unknown) => err instanceof AppendRowsError && err.outcome === "failed",
      );
    }
    assert.equal(calls.length, 0);
  });

  for (const [name, status, body, succeeded] of [
    ["modern success", 200, { num_rows_inserted: 1 }, true],
    ["legacy success", 200, { status: "committed", num_rows_inserted: 1 }, true],
    ["row count mismatch", 200, { num_rows_inserted: 2 }, false],
    ["wrong HTTP status", 201, { num_rows_inserted: 1 }, false],
    ["invalid legacy status", 200, { status: "unknown", num_rows_inserted: 1 }, false],
    ["null legacy status", 200, { status: null, num_rows_inserted: 1 }, false],
  ] as const) {
    it(`validates ${name}`, async () => {
      const client = new Client("https://example.com", {
        fetch: async () => Response.json(body, { status }),
      });
      const result = client.table("events").append('{"id":1}');
      if (succeeded) {
        assert.deepEqual(await result, {
          status: "succeeded",
          num_rows_inserted: 1,
        });
      } else {
        await assert.rejects(
          result,
          (err) => err instanceof AppendRowsError && err.outcome === "ambiguous",
        );
      }
    });
  }

  it("preserves row-level rejection details and classifies HTTP 408 / 503 / unreadable bodies", async () => {
    const cause = new Error("response body interrupted");
    const unreadable = new Response(
      new ReadableStream({
        start(controller) {
          controller.error(cause);
        },
      }),
      {
        status: 503,
        headers: {
          "X-Request-ID": "broken-body",
          "Retry-After": "2",
        },
      },
    );
    const { fn } = makeFetchStub([
      jsonResponse(422, {
        message: "row validation failed",
        code: "ingest_failure",
        details: {
          status: "rejected",
          row_errors: [
            { row_index: 1, column: "id", message: "expected int, got string" },
          ],
          row_errors_truncated: false,
        },
      }),
      jsonResponse(408, {
        message: "admission timed out",
        retryable: true,
        code: "ingest_failure",
        details: { status: "rejected" },
      }),
      unreadable,
    ]);
    const table = new Client("http://localhost:8080", { fetch: fn }).table("events");

    await assert.rejects(
      () => table.append('{"id":"bad"}'),
      (err: unknown) => {
        assert.ok(err instanceof AppendRowsError);
        assert.equal(err.outcome, "failed");
        assert.deepEqual(err.rowErrors, [
          { row_index: 1, column: "id", message: "expected int, got string" },
        ]);
        assert.ok(err.isPermanent());
        return true;
      },
    );

    await assert.rejects(
      () => table.append('{"id":1}'),
      (err: unknown) => {
        assert.ok(err instanceof AppendRowsError);
        assert.equal(err.outcome, "failed");
        assert.equal(err.writeDetails, undefined);
        assert.equal(err.retryable, false);
        return true;
      },
    );

    await assert.rejects(
      () => table.append('{"id":1}'),
      (err: unknown) => {
        assert.ok(err instanceof AppendRowsError);
        assert.equal(err.outcome, "ambiguous");
        assert.equal(err.httpStatus, 503);
        assert.equal(err.requestId, "broken-body");
        assert.equal(err.retryAfterMs, 2_000);
        assert.equal(err.retryable, false);
        return true;
      },
    );
  });
});

describe("Write timeouts and cancellation", () => {
  function stalledFetch(response?: Response, honorCancellation = false) {
    let started!: () => void;
    const ready = new Promise<void>((resolve) => {
      started = resolve;
    });
    const signals: AbortSignal[] = [];
    const fetch: typeof globalThis.fetch = async (_input, init) => {
      assert.ok(init?.signal);
      signals.push(init.signal);
      started();
      return response ?? new Promise<Response>((_resolve, reject) => {
        if (honorCancellation) {
          init.signal?.addEventListener(
            "abort",
            () => reject(init.signal?.reason),
            { once: true },
          );
        }
      });
    };
    return { fetch, ready, signals };
  }

  function unknownTimeout(error: unknown): boolean {
    assert.ok(error instanceof AppendRowsError);
    assert.equal(error.outcome, "ambiguous");
    assert.equal(error.retryable, false);
    assert.deepEqual(error.writeDetails, {
      reason: "timeout",
      outcome: "ambiguous",
    });
    assert.equal(error.context().get("write_timeout_ms") !== undefined, true);
    return true;
  }

  it("validates writeTimeoutMs and timeoutMs before sending", async () => {
    for (const writeTimeoutMs of [0, -1, NaN, Infinity, 0.5, 2_147_483_648]) {
      assert.throws(
        () => new Client("https://example.com", { writeTimeoutMs }),
        (err: unknown) => err instanceof ScopeDBError && err.kind === "ConfigInvalid",
      );
    }
    const transport = stalledFetch();
    const client = new Client("https://example.com", { fetch: transport.fetch });
    await assert.rejects(
      client.table("events").append('{"id":1}', { timeoutMs: 0 }),
      (err: unknown) => err instanceof ScopeDBError && err.kind === "ConfigInvalid",
    );
    assert.equal(transport.signals.length, 0);
  });

  it("defaults to 30s, starts after compression, shares budget across headers/body, and allows per-call override", async (t) => {
    t.mock.timers.enable({ apis: ["setTimeout"] });
    const transport = stalledFetch();
    const client = new Client("https://example.com", {
      fetch: transport.fetch,
    });
    const rejected = assert.rejects(
      client.table("events").append('{"id":1}'),
      unknownTimeout,
    );
    await transport.ready;
    t.mock.timers.tick(29_999);
    assert.equal(transport.signals[0]!.aborted, false);
    t.mock.timers.tick(1);
    await rejected;
    assert.equal(transport.signals[0]!.aborted, true);

    // Per-call timeoutMs override
    const transport2 = stalledFetch();
    const client2 = new Client("https://example.com", {
      fetch: transport2.fetch,
      writeTimeoutMs: 10,
    });
    const rejected2 = assert.rejects(
      client2.table("events").append('{"id":1}', { timeoutMs: 50 }),
      unknownTimeout,
    );
    await transport2.ready;
    t.mock.timers.tick(10);
    assert.equal(transport2.signals[0]!.aborted, false);
    t.mock.timers.tick(40);
    await rejected2;

    // Stalled response body shares the deadline
    const response = new Response(new ReadableStream<Uint8Array>({}), {
      status: 200,
      headers: { "X-Request-ID": "stalled-body" },
    });
    const transport3 = stalledFetch(response);
    const client3 = new Client("https://example.com", {
      fetch: transport3.fetch,
      writeTimeoutMs: 20,
    });
    const rejected3 = assert.rejects(
      client3.table("events").append('{"id":1}'),
      (error: unknown) => {
        unknownTimeout(error);
        assert.ok(error instanceof AppendRowsError);
        assert.equal(error.httpStatus, 200);
        assert.equal(error.requestId, "stalled-body");
        return true;
      },
    );
    await transport3.ready;
    await nextTurn();
    t.mock.timers.tick(20);
    await rejected3;
  });

  it("distinguishes pre-aborted signals, in-flight caller cancellation, and transport timeout codes", async () => {
    const transport = stalledFetch();
    const client = new Client("https://example.com", { fetch: transport.fetch });
    const preAbortReason = new Error("cancelled before start");
    await assert.rejects(
      client.table("events").append('{"id":1}', {
        signal: AbortSignal.abort(preAbortReason),
      }),
      (err) => err === preAbortReason,
    );
    assert.equal(transport.signals.length, 0);

    const controller = new AbortController();
    const earlierError = new ScopeDBError("Unexpected", "previous failure", {
      httpStatus: 503,
      requestId: "prev-req",
    }).setTemporary();
    const rejected = assert.rejects(
      client.table("events").append('{"id":1}', { signal: controller.signal }),
      (error: unknown) => {
        assert.ok(error instanceof AppendRowsError);
        assert.notEqual(error, earlierError);
        assert.equal(error.outcome, "ambiguous");
        assert.equal(error.retryable, false);
        assert.deepEqual(error.writeDetails, {
          reason: "cancelled",
          outcome: "ambiguous",
        });
        assert.equal(error.requestId, undefined);
        return true;
      },
    );
    await transport.ready;
    controller.abort(earlierError);
    await rejected;

    for (const code of [
      "UND_ERR_CONNECT_TIMEOUT",
      "UND_ERR_HEADERS_TIMEOUT",
      "UND_ERR_BODY_TIMEOUT",
    ]) {
      const reason = new TypeError("fetch failed", {
        cause: Object.assign(new Error("transport deadline expired"), { code }),
      });
      const timeoutClient = new Client("https://example.com", {
        fetch: async () => {
          throw reason;
        },
      });
      await assert.rejects(
        timeoutClient.table("events").append('{"id":1}'),
        (error: unknown) => {
          assert.ok(error instanceof AppendRowsError);
          assert.deepEqual(error.writeDetails, {
            reason: "timeout",
            outcome: "ambiguous",
          });
          assert.equal(error.retryable, false);
          return true;
        },
      );
    }
  });

  it("does not apply writeTimeoutMs to statement requests and allows disabling write deadlines with null", async (t) => {
    t.mock.timers.enable({ apis: ["setTimeout"] });
    const controller = new AbortController();
    let receivedSignal: AbortSignal | null | undefined;
    const stmtClient = new Client("https://example.com", {
      writeTimeoutMs: 1,
      fetch: async (_input, init) => {
        receivedSignal = init?.signal;
        return jsonResponse(200, pendingStatus());
      },
    });
    await stmtClient.statement("SELECT 1").submit({ signal: controller.signal });
    t.mock.timers.tick(30_000);
    assert.equal(receivedSignal, controller.signal);
    assert.equal(controller.signal.aborted, false);

    const transport = stalledFetch();
    const writeClient = new Client("https://example.com", {
      fetch: transport.fetch,
      writeTimeoutMs: null,
    });
    const writeController = new AbortController();
    const writePromise = writeClient.table("events").append('{"id":1}', {
      signal: writeController.signal,
    });
    const rejected = assert.rejects(
      writePromise,
      (err) => err instanceof AppendRowsError && err.writeDetails?.reason === "cancelled",
    );
    await transport.ready;
    t.mock.timers.tick(60_000);
    assert.equal(transport.signals[0]!.aborted, false);
    writeController.abort();
    await rejected;
  });
});
