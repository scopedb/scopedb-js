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
import { Client, encodeAppendRecord, ResultSet, ScopeDBError } from "../src/index.js";
import { jsonResponse, makeFetchStub, requestBodyText } from "./helpers.js";

function rejectsAt(record: unknown, path: string, message: RegExp): void {
  assert.throws(() => encodeAppendRecord(record), (error: unknown) => {
    assert.ok(error instanceof ScopeDBError);
    assert.equal(error.kind, "AppendRowsFailed");
    assert.equal(error.isPermanent(), true);
    assert.equal(error.context().get("record_path"), path);
    assert.match(error.message, message);
    return true;
  });
}

function makeTable(rows: number) {
  const { fn, calls } = makeFetchStub([
    jsonResponse(200, { status: "committed", num_rows_inserted: rows }),
  ]);
  return { table: new Client("http://localhost:8080", { fetch: fn }).table("events"), calls };
}

describe("encodeAppendRecord", () => {
  it("emits exact int64 number tokens without rewriting similar strings", () => {
    assert.equal(
      encodeAppendRecord({ id: 9223372036854775807n, attrs: { min: -9223372036854775808n }, items: [0n, "9223372036854775807", "1n"] }),
      '{"id":9223372036854775807,"attrs":{"min":-9223372036854775808},"items":[0,"9223372036854775807","1n"]}',
    );
  });

  it("preserves JSON escaping, null-prototype objects, shared values, and Dates", () => {
    const shared = { value: 1.25 };
    const record = Object.assign(Object.create(null) as Record<string, unknown>, {
      'quote"\\\n': "中文 🦉\n\"\\\ud800",
      date: new Date("2026-10-29T00:00:00.123Z"),
      items: [shared, true, null, shared, Number.MIN_VALUE, Number.MAX_SAFE_INTEGER],
    });
    record["__proto__"] = { enabled: true };
    assert.equal(encodeAppendRecord(record), JSON.stringify(record));
    assert.equal(encodeAppendRecord({}), "{}");
    assert.deepEqual(JSON.parse(encodeAppendRecord(record)), JSON.parse(JSON.stringify(record)));
  });

  it("rejects non-finite and unsafe integer-valued numbers at their nested paths", () => {
    for (const value of [NaN, Infinity, -Infinity]) {
      rejectsAt({ attrs: [{ score: value }] }, '$["attrs"][0]["score"]', /finite/);
    }
    for (const value of [Number.MAX_SAFE_INTEGER + 1, -(Number.MAX_SAFE_INTEGER + 1), 1e100]) {
      rejectsAt({ id: value }, '$["id"]', /safe.*bigint/);
    }
  });

  it("rejects integers outside the server NDJSON range, including nested uint values", () => {
    for (const value of [9223372036854775808n, -9223372036854775809n]) {
      rejectsAt({ attrs: { unsigned: value } }, '$["attrs"]["unsigned"]', /signed 64-bit/);
    }
  });

  it("rejects omitted and substituted values instead of silently changing them", () => {
    for (const value of [undefined, () => 1, Symbol("value")]) {
      rejectsAt({ value }, '$["value"]', /unsupported/);
      rejectsAt({ values: [value] }, '$["values"][0]', /unsupported/);
    }
    rejectsAt({ values: new Array(1) }, '$["values"][0]', /sparse/);
    rejectsAt({ date: new Date(NaN) }, '$["date"]', /Date must be valid/);
    rejectsAt({ [Symbol("key")]: 1 }, "$", /keys must be strings/);
  });

  it("requires a plain record and explicit conversion of custom objects", () => {
    for (const value of [null, [], new Date(), "{}", 1, 1n]) {
      rejectsAt(value, "$", /plain object/);
    }
    for (const value of [new Map(), new Set(), new Number(1), new Uint8Array([1])]) {
      rejectsAt({ value }, '$["value"]', /convert custom objects/);
    }
    let invoked = false;
    const record = { toJSON() { invoked = true; return { id: NaN }; } };
    rejectsAt(record, '$["toJSON"]', /unsupported function/);
    rejectsAt({ get id() { invoked = true; return 1; } }, '$["id"]', /accessor/);
    assert.equal(invoked, false);
  });

  it("identifies cycles but allows the same object in independent branches", () => {
    const cycle: Record<string, unknown> = {};
    cycle["again"] = cycle;
    rejectsAt({ nested: cycle }, '$["nested"]["again"]', /circular/);
    const shared = { id: 1n };
    assert.equal(encodeAppendRecord({ left: shared, right: shared }), '{"left":{"id":1},"right":{"id":1}}');
  });
});

describe("strict append stream encoding", () => {
  it("writes default typed query integers and Dates through the public stream", async () => {
    const record = ResultSet.fromStatementResultSet({
      metadata: { num_rows: 1, fields: [
        { name: "id", data_type: "int" },
        { name: "unsigned", data_type: "uint" },
        { name: "time", data_type: "timestamp" },
      ] },
      format: "json",
      rows: [["-9223372036854775808", "9223372036854775807", "2026-10-29T00:00:00.123Z"]],
    }).first();
    const { table, calls } = makeTable(1);
    const stream = table.appendStream().strictEncoding(true).build();
    await stream.send(record);
    assert.equal((await stream.shutdown())?.num_rows_inserted, 1);
    assert.equal(calls.length, 1);
    assert.equal(requestBodyText(calls[0]?.init), '{"id":-9223372036854775808,"unsigned":9223372036854775807,"time":"2026-10-29T00:00:00.123Z"}');
  });

  it("also supplies exact NDJSON to direct append", async () => {
    const { table, calls } = makeTable(2);
    const ndjson = [{ id: 9007199254740993n }, { id: 9007199254740995n }]
      .map(encodeAppendRecord).join("\n");
    await table.append(ndjson);
    assert.equal(requestBodyText(calls[0]?.init), '{"id":9007199254740993}\n{"id":9007199254740995}');
  });

  it("preserves the accepted prefix on sendAll failure and can admit corrected input", async () => {
    const { table, calls } = makeTable(3);
    const stream = table.appendStream().strictEncoding(true).flushIntervalMs(60_000).build();
    await assert.rejects(stream.sendAll([{ id: 1n }, { id: NaN }, { id: 3n }]), /finite/);
    assert.equal(stream.stats().acceptedRows, 1);
    assert.equal(stream.stats().committedRows, 0);
    assert.equal(stream.stats().pendingRows, 1);
    assert.equal(stream.stats().droppedRows, 0);
    assert.equal(stream.trySend({ id: Infinity }), false);
    assert.equal(stream.stats().droppedByReason.invalidRecord, 1);
    await stream.sendAll([{ id: 2n }, { id: 3n }]);
    assert.equal((await stream.shutdown())?.num_rows_inserted, 3);
    assert.equal(calls.length, 1);
    assert.equal(requestBodyText(calls[0]?.init), '{"id":1}\n{"id":2}\n{"id":3}');
    assert.equal(stream.stats().pendingBytes, 0);
  });

  it("retains bounded byte admission and counts invalid records in continue mode", async () => {
    const { table, calls } = makeTable(1);
    const stream = table.appendStream({ failurePolicy: "continue" })
      .strictEncoding(true).maxBufferedBytes(16).build();
    assert.equal(stream.trySend({ id: NaN }), false);
    assert.equal(stream.trySend({ id: 9223372036854775807n }), false);
    assert.equal(stream.stats().droppedByReason.invalidRecord, 1);
    assert.equal(stream.stats().droppedByReason.recordTooLarge, 1);
    assert.equal(stream.stats().acceptedRows, 0);
    assert.equal(stream.stats().pendingBytes, 0);
    await stream.send({ id: 1n });
    const report = await stream.shutdown();
    assert.equal(report.committedRows, 1);
    assert.equal(report.droppedRows, 2);
    assert.equal(calls.length, 1);
  });

  it("keeps default JSON.stringify behavior and snapshots the builder setting", async () => {
    const { table, calls } = makeTable(1);
    const builder = table.appendStream();
    const defaultStream = builder.build();
    builder.strictEncoding(true);
    const strictStream = builder.build();
    builder.strictEncoding(false);
    await assert.rejects(strictStream.send({ value: NaN }), /finite/);
    assert.equal(await strictStream.shutdown(), null);
    await assert.rejects(defaultStream.send({ id: 1n }), /serialize/);
    await defaultStream.send({ value: NaN, missing: undefined, id: Number.MAX_SAFE_INTEGER + 1 });
    await defaultStream.shutdown();
    assert.equal(requestBodyText(calls[0]?.init), '{"value":null,"id":9007199254740992}');
    assert.throws(() => builder.strictEncoding("true" as never), /must be a boolean/);
  });
});
