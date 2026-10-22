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
import { Client, ScopeDBError, AppendRowsError } from "../src/index.js";
import { test } from "node:test";
test(
  "native 0.6 lifecycle",
  { skip: !process.env["SCOPEDB_ENDPOINT"] },
  async () => {
    const client = new Client(process.env["SCOPEDB_ENDPOINT"]!, {
      apiKey: process.env["SCOPEDB_API_KEY"],
      fetch: async (input, init) => {
        const response = await fetch(input, init);
        const url = new URL(
          input instanceof Request ? input.url : input.toString(),
        );
        console.log(
          `HTTP ${init?.method} ${url.pathname}${url.search} -> ${response.status}`,
        );
        return response;
      },
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
          assert.equal(e.appendState, "rejected");
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
        `QUERY rows=2 compute_seconds=${handle.progress()!.compute_seconds}`,
      );
      const late = client.statementHandle(handle.statementId);
      assert.equal((await late.cancel()).status, "finished");
      assert.equal(late.resultSet()!.toObjects().length, 2);
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
      console.log("PASS native 0.6 lifecycle");
    } finally {
      await resource.delete({ allowDestructive: true });
      console.log("CLEANUP schema removed");
    }
  },
);
