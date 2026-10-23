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
import { AppendRowsError, Client, ScopeDBError } from "../src/index.js";
import { test } from "node:test";
test(
  "native 0.6 lifecycle",
  { skip: !process.env["SCOPEDB_ENDPOINT"] },
  async () => {
    const client = new Client(process.env["SCOPEDB_ENDPOINT"]!, {
      apiKey: process.env["SCOPEDB_API_KEY"],
    });
    const name = `sdk06_js_${Date.now()}`;
    const resource = client.catalog().schema("scopedb", name);
    const definition: import("../src/catalog.js").CatalogSchema = {
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
    assert.ok(preview.etag);
    await assert.rejects(
      resource.get(),
      (e) => e instanceof ScopeDBError && e.httpStatus === 404,
    );
    try {
      const applied = await resource.apply(definition, {
        ifMatch: preview.etag,
      });
      assert.equal((await resource.get()).etag, applied.etag);
      await assert.rejects(
        resource.apply(definition, { ifMatch: preview.etag }),
        (e: unknown) => {
          assert.ok(e instanceof ScopeDBError);
          assert.equal(e.httpStatus, 412);
          assert.equal(e.code, "catalog_version_mismatch");
          console.log(`STALE_ETAG code=${e.code}`);
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
          assert.ok(e instanceof ScopeDBError);
          assert.ok(e instanceof AppendRowsError);
          assert.equal(e.outcome, "rejected");
          assert.equal(e.code, "invalid_schema");
          assert.equal(e.rowErrors.length, 1);
          assert.equal((e.details as { status: string }).status, "rejected");
          console.log(
            `REJECTED code=${e.code} row_errors=${e.rowErrors.length}`,
          );
          return true;
        },
      );
      const handle = await client
        .statement(`FROM ${table.identifier()}`)
        .submit();
      const result = await handle.wait();
      assert.equal(result.toObjects().length, 2);
      console.log(
        `QUERY rows=2 compute_seconds=${
          handle.snapshot()?.progress!.compute_seconds
        }`,
      );
      const late = client.statementHandle(handle.statementId);
      assert.equal((await late.cancel()).status, "finished");
      assert.equal((await late.wait()).toObjects().length, 2);
      console.log("CANCEL completed statement retained result");
      await assert.rejects(
        client.query("SELECT UNKNOWN_FUNCTION()"),
        (e: unknown) => {
          assert.ok(e instanceof ScopeDBError);
          assert.equal(e.code, "statement_failed");
          assert.ok(e.message);
          console.log(`FAILED code=${e.code}`);
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
          console.log(`TIMEOUT code=${e.code}`);
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
      await special.catalog().apply({
        ...specialSnapshot.definition,
        comment: "preserve indexes and expressions",
      }, { ifMatch: specialSnapshot.etag });
      await assert.rejects(
        special.catalog().delete({ dryRun: true }),
        (error) =>
          error instanceof ScopeDBError && error.code === "invalid_request",
      );
      await view.delete();
      const deletion = await special.catalog().delete({ dryRun: true });
      assert.equal(deletion.destructive, true);
      assert.equal(deletion.after, null);
      await assert.rejects(
        special.catalog().delete(),
        (error) =>
          error instanceof ScopeDBError &&
          error.code === "catalog_destructive_change",
      );
      console.log(
        "CATALOG encoded identities, views, computed columns, indexes, and destructive guards passed",
      );

      // Bounded ingestion, conditional round trips, and real lost-acknowledgement probes.
      const snapshot = await table.catalog().get();
      const replacement = {
        ...snapshot.definition,
        comment: "conditional round trip",
      };
      await table.catalog().apply(replacement, { ifMatch: snapshot.etag });
      await assert.rejects(
        table.catalog().delete({
          ifMatch: snapshot.etag,
          allowDestructive: true,
        }),
        (error) =>
          error instanceof ScopeDBError &&
          error.code === "catalog_version_mismatch",
      );
      assert.equal(
        (await table.catalog().get()).definition.comment,
        replacement.comment,
      );

      const stream = table.appendStream().maxBatchRows(128)
        .maxConcurrentBatches(4).maxBufferedBytes(256 * 1024).build();
      for (let id = 3; id <= 2_050; id++) {
        await stream.send({ id, source: "stream" });
      }
      assert.equal((await stream.flush())?.num_rows_inserted, 2_048);
      await stream.send({ id: 2_051, source: "last" });
      assert.equal((await stream.shutdown())?.num_rows_inserted, 1);
      assert.equal(stream.stats().pendingBytes, 0);
      assert.equal(
        (await client.query(
          `FROM ${table.identifier()} AGGREGATE count() AS n`,
        )).first()?.["n"],
        2_051n,
      );

      const bestEffort = table.appendStream({ failurePolicy: "continue" })
        .maxBatchRows(1).build();
      await bestEffort.send({ id: 2_052, source: "valid" });
      await bestEffort.send({ id: "bad", source: "invalid" });
      const partial = await bestEffort.shutdown();
      assert.equal(partial.committedRows, 1);
      assert.equal(partial.failedRows, 1);
      assert.equal(partial.unknownRows, 0);
      assert.equal(
        bestEffort.stats().lastFailure?.error.code,
        "invalid_schema",
      );
      console.log(
        "STREAM bounded concurrent batches, flush intervals, and partial delivery passed",
      );

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
        (await client.query(
          `FROM ${table.identifier()} WHERE source = 'exact' SELECT id`,
        )).first()?.["id"],
        9007199254740993n,
      );
      console.log(
        "NUMBERS signed/unsigned query boundaries and exact NDJSON input passed",
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
        console.log("AUTH missing and invalid credentials rejected");
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
                (await response.json() as { status: string }).status,
                "committed",
              );
              throw new TypeError("test discarded a committed acknowledgement");
            }
            return response;
          },
        });
        const writing = faultClient.table("events", { schema: name })
          .appendStream().retryUnknown(replay).initialBackoff(0).build();
        await writing.send({ id: 3_000, source: label });
        if (replay) {
          assert.equal((await writing.shutdown())?.num_rows_inserted, 1);
        } else {await assert.rejects(writing.shutdown(), (error) =>
            error instanceof AppendRowsError && error.outcome === "unknown");}
        const count = (await client.query(
          `FROM ${table.identifier()} WHERE source = '${label}' AGGREGATE count() AS n`,
        )).first()?.["n"];
        assert.equal(count, replay ? 2n : 1n);
        assert.equal(attempts, replay ? 2 : 1);
      }
      console.log(
        "LOST_ACK one stored row by default, two only with explicit replay",
      );
      let timeoutAttempts = 0;
      const timeoutClient = new Client(process.env["SCOPEDB_ENDPOINT"]!, {
        apiKey: process.env["SCOPEDB_API_KEY"],
        writeTimeoutMs: 5_000,
        fetch: async (input, init) => {
          timeoutAttempts++;
          const response = await fetch(input, init);
          assert.equal(response.status, 200);
          assert.equal(
            (await response.json() as { status: string }).status,
            "committed",
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
      const timed = timeoutClient.table("events", { schema: name })
        .appendStream().retryUnknown(true).build();
      await timed.send({ id: 3_001, source: "timed" });
      await assert.rejects(
        timed.shutdown(),
        (error) =>
          error instanceof AppendRowsError &&
          error.writeDetails?.reason === "timeout",
      );
      assert.equal(timeoutAttempts, 1);
      assert.equal(
        (await client.query(
          `FROM ${table.identifier()} WHERE source = 'timed' AGGREGATE count() AS n`,
        )).first()?.["n"],
        1n,
      );
      console.log(
        "TIMEOUT committed row retained, acknowledgement timeout not replayed",
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
      }).table("catalog_lost_ack", { schema: name }).catalog();
      const catalogTarget = client.table("catalog_lost_ack", { schema: name })
        .catalog();
      const unknownMutation = (error: unknown) =>
        error instanceof ScopeDBError &&
        error.code === "catalog_commit_outcome_unknown";
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
      console.log(
        "CATALOG_LOST_ACK create/delete remain unknown and are not replayed",
      );
      console.log("PASS native 0.6 lifecycle");
    } finally {
      await resource.delete({ allowDestructive: true });
      console.log("CLEANUP schema removed");
    }
  },
);
