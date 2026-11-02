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
      await table.append(
        encodeAppendRecord({ id: 9007199254740993n, source: "exact" }),
      );
      assert.equal(
        (
          await client.query(
            `FROM ${table.identifier()} WHERE source = 'exact' SELECT id`,
          )
        ).first()?.["id"],
        9007199254740993n,
      );

      const strictStream = table.appendStream().strictEncoding(true).build();
      try {
        await assert.rejects(
          strictStream.send({ id: 9007199254740994, source: "unsafe" }),
          /safe integer/,
        );
        await strictStream.send({ id: 9007199254740995n, source: "strict" });
      } finally {
        await strictStream.shutdown();
      }
      assert.equal(
        (
          await client.query(
            `FROM ${table.identifier()} WHERE source = 'strict' SELECT id`,
          )
        ).first()?.["id"],
        9007199254740995n,
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
    } finally {
      await resource.delete({ allowDestructive: true });
    }
  },
);
