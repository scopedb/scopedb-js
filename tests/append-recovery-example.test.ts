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
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { it, type TestContext } from "node:test";
import { pathToFileURL } from "node:url";
import ts from "typescript";
import { AppendRowsError } from "../src/index.js";
import { jsonResponse, requestBodyText } from "./helpers.js";

async function fixture(t: TestContext, outcome: "committed" | "rejected" | "unknown") {
  const directory = await mkdtemp(join(tmpdir(), "scopedb-recovery-example-"));
  t.after(() => rm(directory, { recursive: true, force: true }));
  const environment = {
    SCOPEDB_ENDPOINT: "https://example.invalid",
    SCOPEDB_API_KEY: "test-key",
    SCOPEDB_TABLE: "events",
    SCOPEDB_DATABASE: "scopedb",
    SCOPEDB_SCHEMA: "public",
  };
  const originalFetch = globalThis.fetch;
  const savedEnvironment = new Map(Object.keys(environment).map((key) => [key, process.env[key]]));
  t.after(() => {
    globalThis.fetch = originalFetch;
    for (const [key, value] of savedEnvironment) {
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
  });
  Object.assign(process.env, environment);
  const source = await readFile(new URL("../../examples/patterns/admission-recovery.ts", import.meta.url), "utf8");
  const compiled = ts.transpileModule(source, {
    compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.ES2022 },
  }).outputText;
  const entry = join(directory, "entry.mjs");
  await writeFile(entry, compiled);
  const moduleDirectory = join(directory, "node_modules/scopedb");
  await mkdir(moduleDirectory, { recursive: true });
  await writeFile(join(moduleDirectory, "package.json"), JSON.stringify({ type: "module", exports: "./index.js" }));
  await writeFile(join(moduleDirectory, "index.js"), `export * from ${JSON.stringify(new URL("../src/index.js", import.meta.url).href)};`);
  const batches: Array<Array<{ id: number }>> = [];
  globalThis.fetch = async (_input, init) => {
    const rows = requestBodyText(init).split("\n").map((line) => JSON.parse(line) as { id: number });
    batches.push(rows);
    if (outcome !== "committed") {
      return jsonResponse(outcome === "rejected" ? 422 : 503, {
        message: "mock delivery failure",
        code: "test_failure",
        retryable: false,
        details: { status: outcome, row_errors: [], row_errors_truncated: false },
      });
    }
    return jsonResponse(200, { status: "committed", num_rows_inserted: rows.length });
  };
  return { run: () => import(pathToFileURL(entry).href), batches };
}

it("runs the recovery example without replaying its confirmed prefix", async (t) => {
  const { run, batches } = await fixture(t, "committed");
  await run();
  assert.deepEqual(batches.map((rows) => rows.map((row) => row.id)), [[1], [2, 3]]);
});

for (const outcome of ["rejected", "unknown"] as const) {
  it(`does not start recovery after ${outcome} delivery`, async (t) => {
    const { run, batches } = await fixture(t, outcome);
    await assert.rejects(run(), (error: unknown) => {
      assert.ok(error instanceof AppendRowsError);
      assert.equal(error.outcome, outcome);
      return true;
    });
    assert.deepEqual(batches.map((rows) => rows.map((row) => row.id)), [[1]]);
  });
}
