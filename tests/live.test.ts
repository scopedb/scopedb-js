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
import { test } from "node:test";
import {
  AppendRowsError,
  type CatalogSchema,
  Client,
  encodeAppendRecord,
  ScopeDBError,
} from "../src/index.js";

test(
  "live ScopeDB end-to-end lifecycle",
  { skip: !process.env["SCOPEDB_ENDPOINT"] },
  async () => {
    const client = new Client(process.env["SCOPEDB_ENDPOINT"]!, {
      apiKey: process.env["SCOPEDB_API_KEY"],
    });
    const name = `sdk_js_${Date.now()}`;
    const resource = client.catalog().schema("scopedb", name);
    const definition: CatalogSchema = {
      name,
      tables: [
        {
          name: "events",
          columns: [
            { name: "id", data_type: "int" },
            { name: "source", data_type: "string" },
          ],
        },
      ],
    };
    const preview = await resource.apply(definition, { dryRun: true });
    assert.equal(preview.before, null);
    assert.ok(preview.after);
    assert.ok(preview.version);
    await assert.rejects(
      resource.get(),
      (e) => e instanceof ScopeDBError && e.httpStatus === 404,
    );
    try {
      const applied = await resource.apply(definition, {
        expectedVersion: preview.version,
      });
      assert.equal((await resource.get()).version, applied.version);
      await assert.rejects(
        resource.apply(definition, { expectedVersion: preview.version }),
        (e: unknown) => {
          assert.ok(e instanceof ScopeDBError);
          assert.equal(e.httpStatus, 412);
          assert.equal(e.code, "precondition_failed");
          return true;
        },
      );
      const table = client.table("events", { schema: name });
      assert.equal(
        (await table.append('{"id":1,"source":"js"}\n{"id":2,"source":"js"}'))
          .num_rows_inserted,
        2,
      );
      await assert.rejects(
        table.append('{"id":"bad","source":"js"}'),
        (e: unknown) => {
          assert.ok(e instanceof AppendRowsError);
          assert.equal(e.outcome, "failed");
          assert.equal(e.code, "bad_request");
          assert.equal(e.httpStatus, 400);
          return true;
        },
      );
      const handle = await client
        .statement(`FROM ${table.identifier()}`)
        .submit();
      const result = await handle.wait();
      assert.equal(result.toObjects().length, 2);

      const late = client.statementHandle(handle.statementId);
      assert.equal((await late.cancel()).status, "finished");
      assert.equal((await late.wait()).toObjects().length, 2);

      await assert.rejects(
        client.query("SELECT UNKNOWN_FUNCTION()"),
        (e: unknown) => {
          assert.ok(e instanceof ScopeDBError);
          assert.equal(e.code, "statement_failed");
          assert.ok(e.message);
          return true;
        },
      );
      await assert.rejects(
        client
          .statement(`FROM ${table.identifier()}`)
          .withMaxElapsedSeconds(1e-9)
          .execute(),
        (e: unknown) => {
          assert.ok(e instanceof ScopeDBError);
          assert.match(e.code ?? "", /timeout/);
          return true;
        },
      );
      const special = client.table("events / ` encoded", { schema: name });
      await special.catalog().apply({
        name: "events / ` encoded",
        retention_days: 7,
        columns: [
          { name: "id", data_type: "int" },
          {
            name: "message",
            data_type: "string",
            indexes: [{ type: "search", analyzer: "log" }],
          },
          { name: "doubled", data_type: "int", expression: "id * 2" },
        ],
      });
      await special.append('{"id":2,"message":"hello"}');
      const view = client.catalog().view("scopedb", name, "computed_view");
      await view.apply({
        name: "computed_view",
        statement: `FROM ${special.identifier()} SELECT id, doubled`,
      });
      const viewTable = client.table("computed_view", { schema: name });
      assert.deepEqual(
        (await client.query(`FROM ${viewTable.identifier()}`)).first(),
        { id: 2n, doubled: 4n },
      );
      const specialSnapshot = await special.catalog().get();
      await special.catalog().apply(
        {
          ...specialSnapshot.definition,
          comment: "preserve indexes and expressions",
        },
        { expectedVersion: specialSnapshot.version },
      );
      await assert.rejects(
        special.catalog().delete({ dryRun: true }),
        (error) => error instanceof ScopeDBError && error.code === "bad_request",
      );
      await view.delete();
      const deletion = await special.catalog().delete({ dryRun: true });
      assert.equal(deletion.destructive, true);
      assert.equal(deletion.after, null);
      await assert.rejects(
        special.catalog().delete(),
        (error) => error instanceof ScopeDBError && error.code === "conflict",
      );

      const snapshot = await table.catalog().get();
      const replacement = {
        ...snapshot.definition,
        comment: "conditional round trip",
      };
      await table
        .catalog()
        .apply(replacement, { expectedVersion: snapshot.version });
      await assert.rejects(
        table.catalog().delete({
          expectedVersion: snapshot.version,
          allowDestructive: true,
        }),
        (error) =>
          error instanceof ScopeDBError && error.code === "precondition_failed",
      );
      assert.equal(
        (await table.catalog().get()).definition.comment,
        replacement.comment,
      );

      const stream = table
        .appendStream()
        .maxBatchRows(128)
        .maxConcurrentBatches(4)
        .maxBufferedBytes(256 * 1024)
        .build();
      for (let id = 3; id <= 2_050; id++) {
        await stream.send({ id, source: "stream" });
      }
      assert.equal((await stream.flush())?.num_rows_inserted, 2_048);
      await stream.send({ id: 2_051, source: "last" });
      assert.equal((await stream.shutdown())?.num_rows_inserted, 1);
      assert.equal(stream.stats().pendingBytes, 0);
      assert.equal(
        (
          await client.query(
            `FROM ${table.identifier()} AGGREGATE count() AS n`,
          )
        ).first()?.["n"],
        2_051n,
      );

      const bestEffort = table
        .appendStream({ failurePolicy: "continue" })
        .maxBatchRows(1)
        .build();
      await bestEffort.send({ id: 2_052, source: "valid" });
      await bestEffort.send({ id: "bad", source: "invalid" });
      const partial = await bestEffort.shutdown();
      assert.equal(partial.committedRows, 1);
      assert.equal(partial.failedRows, 1);
      assert.equal(partial.ambiguousRows, 0);
      assert.equal(bestEffort.stats().lastFailure?.error.code, "bad_request");

      const boundaries = await client.query(
        "SELECT -9223372036854775808 AS lo, 9223372036854775807 AS hi, 18446744073709551615 AS unsigned",
      );
      assert.deepEqual(boundaries.first(), {
        lo: -(1n << 63n),
        hi: (1n << 63n) - 1n,
        unsigned: (1n << 64n) - 1n,
      });
      await table.append('{"id":9007199254740993,"source":"exact"}');
      assert.equal(
        (
          await client.query(
            `FROM ${table.identifier()} WHERE source = 'exact' SELECT id`,
          )
        ).first()?.["id"],
        9007199254740993n,
      );

      if (process.env["SCOPEDB_API_KEY"]) {
        for (const apiKey of [undefined, "invalid-test-credential"]) {
          await assert.rejects(
            new Client(process.env["SCOPEDB_ENDPOINT"]!, { apiKey }).query(
              "SELECT 1",
            ),
            (error) =>
              error instanceof ScopeDBError && error.httpStatus === 401,
          );
        }
      }

      for (const replay of [false, true]) {
        const label = replay ? "replay" : "no-replay";
        let attempts = 0;
        const faultClient = new Client(process.env["SCOPEDB_ENDPOINT"]!, {
          apiKey: process.env["SCOPEDB_API_KEY"],
          fetch: async (input, init) => {
            const response = await fetch(input, init);
            if (++attempts === 1) {
              assert.equal(response.status, 200);
              assert.equal(
                (await response.json() as { num_rows_inserted: number })
                  .num_rows_inserted,
                1,
              );
              throw new TypeError("test discarded a committed acknowledgement");
            }
            return response;
          },
        });
        const writing = faultClient
          .table("events", { schema: name })
          .appendStream()
          .retryAmbiguous(replay)
          .initialBackoff(0)
          .build();
        await writing.send({ id: 3_000, source: label });
        if (replay) {
          assert.equal((await writing.shutdown())?.num_rows_inserted, 1);
        } else {
          await assert.rejects(
            writing.shutdown(),
            (error) =>
              error instanceof AppendRowsError && error.outcome === "ambiguous",
          );
        }
        const count = (
          await client.query(
            `FROM ${table.identifier()} WHERE source = '${label}' AGGREGATE count() AS n`,
          )
        ).first()?.["n"];
        assert.equal(count, replay ? 2n : 1n);
        assert.equal(attempts, replay ? 2 : 1);
      }

      let timeoutAttempts = 0;
      const timeoutClient = new Client(process.env["SCOPEDB_ENDPOINT"]!, {
        apiKey: process.env["SCOPEDB_API_KEY"],
        writeTimeoutMs: 5_000,
        fetch: async (input, init) => {
          timeoutAttempts++;
          const response = await fetch(input, init);
          assert.equal(response.status, 200);
          assert.equal(
            (await response.json() as { num_rows_inserted: number })
              .num_rows_inserted,
            1,
          );
          return new Response(
            new ReadableStream({
              start(controller) {
                init?.signal?.addEventListener(
                  "abort",
                  () => controller.error(init.signal?.reason),
                  { once: true },
                );
              },
            }),
            { headers: { "Content-Type": "application/json" } },
          );
        },
      });
      const timed = timeoutClient
        .table("events", { schema: name })
        .appendStream()
        .retryAmbiguous(true)
        .build();
      await timed.send({ id: 3_001, source: "timed" });
      await assert.rejects(
        timed.shutdown(),
        (error) =>
          error instanceof AppendRowsError &&
          error.writeDetails?.reason === "timeout",
      );
      assert.equal(timeoutAttempts, 1);
      assert.equal(
        (
          await client.query(
            `FROM ${table.identifier()} WHERE source = 'timed' AGGREGATE count() AS n`,
          )
        ).first()?.["n"],
        1n,
      );

      const catalogFault = new Client(process.env["SCOPEDB_ENDPOINT"]!, {
        apiKey: process.env["SCOPEDB_API_KEY"],
        fetch: async (input, init) => {
          const response = await fetch(input, init);
          assert.equal(response.status, 200);
          await response.arrayBuffer();
          throw new TypeError(
            "test discarded a committed catalog acknowledgement",
          );
        },
      })
        .table("catalog_lost_ack", { schema: name })
        .catalog();
      const catalogTarget = client
        .table("catalog_lost_ack", { schema: name })
        .catalog();
      const unknownMutation = (error: unknown) =>
        error instanceof ScopeDBError && error.catalogOutcome === "unknown";
      await assert.rejects(
        catalogFault.apply({
          name: "catalog_lost_ack",
          columns: [{ name: "id", data_type: "int" }],
        }),
        unknownMutation,
      );
      assert.equal(
        (await catalogTarget.get()).definition.name,
        "catalog_lost_ack",
      );
      await assert.rejects(
        catalogFault.delete({ allowDestructive: true }),
        unknownMutation,
      );
      await assert.rejects(
        catalogTarget.get(),
        (error) => error instanceof ScopeDBError && error.httpStatus === 404,
      );
    } finally {
      await resource.delete({ allowDestructive: true });
    }
  },
);

test(
  "live ScopeDB append JSON encodings and atomic rejection",
  { skip: !process.env["SCOPEDB_ENDPOINT"] },
  async () => {
    const endpoint = process.env["SCOPEDB_ENDPOINT"]!;
    const apiKey = process.env["SCOPEDB_API_KEY"];
    const client = new Client(endpoint, { apiKey });
    const schemaName = `sdk_js_encoding_${Date.now()}`;
    const schema = client.catalog().schema("scopedb", schemaName);
    const table = client.table("encodings", { schema: schemaName });
    try {
      await schema.apply({
        name: schemaName,
        tables: [
          {
            name: "encodings",
            columns: [
              { name: "id", data_type: "int" },
              { name: "unsigned", data_type: "uint" },
              { name: "score", data_type: "float" },
              { name: "ts", data_type: "timestamp" },
              { name: "attrs", data_type: "object" },
              { name: "items", data_type: "array" },
              { name: "value", data_type: "any" },
              { name: "payload", data_type: "binary" },
              { name: "elapsed", data_type: "interval" },
              { name: "enabled", data_type: "boolean" },
              { name: "label", data_type: "string" },
              { name: "empty", data_type: "string" },
              { name: "doubled", data_type: "int", expression: "id * 2" },
            ],
          },
        ],
      });

      const valid = [
        '{"id":1,"unsigned":9223372036854775807,"score":1.5,"ts":"2026-10-23T12:34:56.123456789Z","attrs":{"nested":null,"ok":true},"items":[1,"two",null],"value":{"a":[true]},"payload":"aGVsbG8=","elapsed":"PT1H2M3.000000004S","enabled":true,"label":"hello","empty":null}',
        '{"id":2,"score":1e3,"ts":"2026-10-23T20:34:56+08:00","value":[1,"two"],"elapsed":"-PT0.5S"}',
        '{"id":3,"score":9007199254740992,"value":true}',
        '{"id":4,"value":42}',
        '{"id":5,"value":"2026-10-23T12:34:56Z"}',
        '{"id":6,"value":null}',
        "{}",
      ];
      assert.equal(
        (await table.append(valid.join("\n"))).num_rows_inserted,
        valid.length,
      );
      const result = await client.query(
        `FROM ${table.identifier()} WHERE id = 1 SELECT unsigned, score, ts, attrs["ok"]::boolean AS ok, items[0]::int AS first_item, enabled, label, empty, doubled`,
      );
      const row = result.first()!;
      assert.equal(row["unsigned"], 9223372036854775807n);
      assert.equal(row["score"], 1.5);
      assert.equal(row["ok"], true);
      assert.equal(row["first_item"], 1n);
      assert.equal(row["enabled"], true);
      assert.equal(row["label"], "hello");
      assert.equal(row["empty"], null);
      assert.equal(row["doubled"], 2n);
      assert.equal(result.rawRows()[0]?.[2], "2026-10-23T12:34:56.123456789Z");
      assert.equal(
        (
          await client.query(
            `FROM ${table.identifier()} WHERE id = 2 SELECT enabled`,
          )
        ).first()?.["enabled"],
        null,
      );
      for (const input of [
        '{"id":"42"}',
        '{"id":42.0}',
        '{"id":42e0}',
        '{"unsigned":-1}',
        '{"unsigned":9223372036854775808}',
        '{"score":9007199254740993}',
        '{"score":1e999}',
        '{"score":1e-999}',
        '{"score":NaN}',
        '{"score":"NaN"}',
        '{"score":Infinity}',
        '{"ts":"2026-10-23"}',
        '{"ts":"2026-10-23T12:34:56"}',
        '{"ts":1792758896}',
        '{"attrs":"{\\"ok\\":true}"}',
        '{"items":{}}',
        '{"payload":"aGVsbG8"}',
        '{"payload":"_w=="}',
        '{"elapsed":"P1D"}',
        '{"enabled":1}',
        '{"label":1}',
        '{"extra":null}',
        '{"doubled":null}',
        '{"attrs":{"a":1,"a":2}}',
        '{"value":{"id":9223372036854775808}}',
      ]) {
        await assert.rejects(table.append(`{"id":999}\n${input}`), (error) => {
          assert.ok(error instanceof AppendRowsError, input);
          assert.equal(error.outcome, "failed", input);
          assert.ok([400, 422].includes(error.httpStatus ?? 0), input);
          return true;
        });
      }
      assert.equal(
        (await client.query(`FROM ${table.identifier()} AGGREGATE count() AS n`))
          .first()?.["n"],
        BigInt(valid.length),
      );
      const stream = table.appendStream().strictEncoding(true).build();
      try {
        await assert.rejects(stream.send({ id: 40n, score: NaN }), /finite/);
        await stream.send({
          id: 41n,
          unsigned: 9223372036854775807n,
          ts: new Date("2026-10-29T00:00:00.123Z"),
          attrs: { min: -9223372036854775808n },
          items: [9007199254740993n],
        });
      } finally {
        await stream.shutdown();
      }
      const strictResult = await client.query(
        `FROM ${table.identifier()} WHERE id = 41 SELECT id, unsigned, ts`,
      );
      const typed = strictResult.first()!;
      assert.equal(typed["unsigned"], 9223372036854775807n);
      assert.equal(
        (typed["ts"] as Date).toISOString(),
        "2026-10-29T00:00:00.123Z",
      );
      const nested = (
        await client.query(
          `FROM ${table.identifier()} WHERE id = 41 SELECT attrs["min"]::int AS min, items[0]::int AS item`,
        )
      ).first()!;
      assert.equal(nested["min"], -9223372036854775808n);
      assert.equal(nested["item"], 9007199254740993n);
      await table.append(encodeAppendRecord({ ...typed, id: 42n }));
      assert.equal(
        (
          await client.query(
            `FROM ${table.identifier()} WHERE id = 42 SELECT unsigned`,
          )
        ).first()?.["unsigned"],
        9223372036854775807n,
      );
      assert.equal(
        (
          await client.query(
            `FROM ${table.identifier()} AGGREGATE count() AS n`,
          )
        ).first()?.["n"],
        BigInt(valid.length + 2),
      );
    } finally {
      try {
        await schema.delete({ allowDestructive: true });
      } catch (error) {
        if (!(error instanceof ScopeDBError) || error.httpStatus !== 404) {
          throw error;
        }
      }
    }
  },
);
