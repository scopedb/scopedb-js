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

// TODO(legacy): Run against both 0.5.28 and 0.6 with compatibility routes until
// 0.5 support ends. The endpoint must allow creating disposable tables.

import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { test } from "node:test";
import { AppendRowsError, Client, ScopeDBError } from "../../src/index.js";

const endpoint = process.env["SCOPEDB_ENDPOINT"];
if (!endpoint) throw new Error("SCOPEDB_ENDPOINT is required for integration tests");
const client = new Client(endpoint, { apiKey: process.env["SCOPEDB_API_KEY"] });

test("HTTP upgrade compatibility", { timeout: 60_000 }, async (t) => {
  const name = `js_compat_${randomUUID().replaceAll("-", "")}`;
  const table = client.table(name);
  const peer = client.table(`${name}_peer`);
  const options = { signal: AbortSignal.timeout(50_000) };
  await client.statement(`CREATE TABLE ${table.identifier()} (id int, name string)`).execute(options);
  try {
    await client.statement(`CREATE TABLE ${peer.identifier()} (id int)`).execute(options);
    await t.test("catalog reads and pagination", async () => {
      assert.equal((await client.fetchDatabase("scopedb", options)).name, "scopedb");
      assert.equal((await client.fetchSchema("scopedb", "public", options)).name, "public");
      assert.equal((await client.fetchSchema("scopedb", "system", options)).name, "system");
      const databases = await collect(client.iterateDatabases({ ...options, pageSize: 1 }));
      assert.ok(databases.some((item) => item.name === "scopedb"));
      const schemas = await collect(client.iterateSchemas("scopedb", { ...options, pageSize: 1 }));
      assert.ok(schemas.some((item) => item.name === "public"));
      assert.ok(schemas.some((item) => item.name === "system"));
      const tables = await collect(client.iterateTables("scopedb", "public", { ...options, pageSize: 1 }));
      assert.ok(tables.some((item) => item.name === name));
      assert.ok(tables.some((item) => item.name === `${name}_peer`));
      const description = await table.describe(options);
      assert.deepEqual(description.columns.map((column) => column.dataType), ["int", "string"]);
    });

    await t.test("gzip append and append stream", async () => {
      const appended = await table.append('{"id":1,"name":"direct"}\n', options);
      assert.equal(appended.append_state, "committed");
      assert.equal(appended.num_rows_inserted, 1);
      const stream = table.appendStream().maxBatchRows(1).build();
      try {
        await stream.send({ id: 2, name: "stream" }, options);
        await stream.send({ id: 3, name: "stream" }, options);
        await stream.flush(options);
      } finally {
        await stream.shutdown(options);
      }
    });

    await t.test("transform ingest and ingest stream", async () => {
      const transform = `SELECT $0["id"]::int AS id, $0["name"]::string AS name INSERT INTO ${table.identifier()}`;
      const ingested = await client.insert('{"id":4,"name":"transform"}', transform, options);
      assert.equal(ingested.num_rows_inserted, 1);
      const stream = client.ingestStream(transform).build();
      try {
        await stream.send({ id: 5, name: "ingest stream" });
      } finally {
        await stream.shutdown();
      }
    });

    await t.test("append rejection keeps commit and row diagnostics", async () => {
      await assert.rejects(table.append('{"id":"bad","name":"rejected"}', options), (error: unknown) => {
        assert.ok(error instanceof AppendRowsError);
        assert.equal(error.appendState, "rejected");
        assert.ok(error.rowErrors.length > 0);
        return true;
      });
    });

    await t.test("submit, resume, finished cancel and wait retain all rows", async () => {
      const handle = await client.statement(`FROM ${table.identifier()}`).withExecTimeout("10s").submit(options);
      assert.equal((await handle.wait(options)).numRows(), 5);
      const resumed = client.statementHandle(handle.statementId);
      assert.equal((await resumed.cancel(options)).status, "finished");
      const result = await resumed.wait(options);
      assert.deepEqual(result.toObjects().map((row) => row["id"]).sort(), [1n, 2n, 3n, 4n, 5n]);
      assert.equal((await resumed.cancel(options)).status, "finished");
    });

    await t.test("execution timeout remains effective", async () => {
      const handle = await client.statement(`FROM ${table.identifier()}`).withExecTimeout("1ns").submit(options);
      const checkFailure = (error: unknown) => {
        assert.ok(error instanceof ScopeDBError);
        assert.equal(error.kind, "StatementFailed");
        assert.ok(error.message.length > 0);
        assert.match(error.statementDetails?.code ?? "", /timeout/);
        return true;
      };
      await assert.rejects(handle.wait(options), checkFailure);
    });

    await t.test("failed cancel preserves the original execution error", async () => {
      const handle = await client.statement(`FROM ${table.identifier()} SELECT name::int AS invalid`).submit(options);
      let failure: ScopeDBError | undefined;
      await assert.rejects(handle.wait(options), (error: unknown) => {
        assert.ok(error instanceof ScopeDBError);
        assert.equal(error.kind, "StatementFailed");
        assert.ok(error.statementDetails?.code);
        failure = error;
        return true;
      });
      const resumed = client.statementHandle(handle.statementId);
      assert.equal((await resumed.cancel(options)).status, "failed");
      await assert.rejects(resumed.wait(options), (error: unknown) => {
        assert.ok(error instanceof ScopeDBError);
        assert.equal(error.message, failure!.message);
        assert.deepEqual(error.statementDetails, failure!.statementDetails);
        return true;
      });
    });

    await t.test("HTTP errors retain status and message", async () => {
      await assert.rejects(client.statementHandle(randomUUID()).status(options), (error: unknown) => {
        assert.ok(error instanceof ScopeDBError);
        assert.equal(error.httpStatus, 404);
        assert.ok(error.message.length > 0);
        assert.equal(error.retryable, false);
        return true;
      });
    });
  } finally {
    await client.statement(`DROP TABLE IF EXISTS ${peer.identifier()}`).execute();
    await table.drop();
  }
});

async function collect<T>(items: AsyncIterable<T>): Promise<T[]> {
  const result: T[] = [];
  for await (const item of items) result.push(item);
  return result;
}
