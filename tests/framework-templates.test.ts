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
import { it } from "node:test";
import { pathToFileURL } from "node:url";
import ts from "typescript";
import { requestBodyText } from "./helpers.js";

const environment = {
  SCOPEDB_ENDPOINT: "https://example.invalid",
  SCOPEDB_API_KEY: "test-key",
  SCOPEDB_TABLE: "events",
  SCOPEDB_DATABASE: "scopedb",
  SCOPEDB_SCHEMA: "public",
  APP_WRITE_TOKEN: "app-token",
};

for (const [kind, sourcePath] of [
  ["Worker", "frameworks/cloudflare-worker/worker.ts"],
  ["Next.js", "frameworks/nextjs-route-handler/route.ts"],
] as const) {
  it(`${kind} template bounds streaming input and rejects unsafe JSON numbers`, async (t) => {
    const directory = await mkdtemp(join(tmpdir(), "scopedb-template-test-"));
    t.after(() => rm(directory, { recursive: true, force: true }));
    const originalFetch = globalThis.fetch;
    const savedEnvironment = new Map(
      Object.keys(environment).map((key) => [key, process.env[key]]),
    );
    t.after(() => {
      globalThis.fetch = originalFetch;
      for (const [key, value] of savedEnvironment) {
        if (value === undefined) delete process.env[key];
        else process.env[key] = value;
      }
    });
    Object.assign(process.env, environment);

    // Compile the checked-in templates; their framework marker has no behavior
    // in this Node test; the handlers and SDK execute without substitution.
    const source = await readFile(new URL(`../../examples/${sourcePath}`, import.meta.url), "utf8");
    const compiled = ts.transpileModule(source, {
      compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.ES2022 },
    }).outputText;
    const entry = join(directory, "entry.mjs");
    await writeFile(entry, compiled);
    for (const [name, body] of [
      ["scopedb", `export * from ${JSON.stringify(new URL("../src/index.js", import.meta.url).href)};`],
      ["server-only", "export {};"],
    ]) {
      const moduleDirectory = join(directory, "node_modules", name!);
      await mkdir(moduleDirectory, { recursive: true });
      await writeFile(join(moduleDirectory, "package.json"), JSON.stringify({ type: "module", exports: "./index.js" }));
      await writeFile(join(moduleDirectory, "index.js"), body!);
    }
    const payloads: unknown[] = [];
    globalThis.fetch = async (_input, init) => {
      payloads.push(JSON.parse(requestBodyText(init)));
      return Response.json({ status: "committed", num_rows_inserted: 1 });
    };
    const template = await import(pathToFileURL(entry).href) as {
      POST(request: Request): Promise<Response>;
      default: { fetch(request: Request, env: typeof environment): Promise<Response> };
    };
    const post = (request: Request) => kind === "Worker"
      ? template.default.fetch(request, environment)
      : template.POST(request);

    for (const body of [
      '{"name":"event","attributes":{"id":9007199254740993}}',
      '{"name":"event","attributes":{"nested":[1e999]}}',
    ]) {
      assert.equal((await post(request(body))).status, 400);
    }
    assert.equal(payloads.length, 0);

    let chunksRead = 0;
    let cancelled = false;
    const oversized = new ReadableStream<Uint8Array>({
      pull(controller) {
        if (chunksRead === 16) return controller.close();
        chunksRead++;
        controller.enqueue(new Uint8Array(64 * 1024));
      },
      cancel() { cancelled = true; },
    });
    const streamingRequest = request(oversized);
    assert.equal(streamingRequest.headers.get("Content-Length"), null);
    assert.equal((await post(streamingRequest)).status, 413);
    assert.equal(cancelled, true);
    assert.ok(chunksRead < 16, "must stop before buffering the entire request");
    assert.equal(payloads.length, 0);

    const bytes = new TextEncoder().encode(
      '{"name":"café","attributes":{"id":9007199254740991,"nested":[1.5,true]}}',
    );
    const split = bytes.indexOf(0xc3) + 1;
    const valid = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(bytes.slice(0, split));
        controller.enqueue(bytes.slice(split));
        controller.close();
      },
    });
    assert.equal((await post(request(valid))).status, 201);
    assert.equal(payloads.length, 1);
    const payload = payloads[0] as Record<string, unknown>;
    assert.equal(payload["name"], "café");
    assert.deepEqual(payload["attributes"], { id: 9007199254740991, nested: [1.5, true] });
  });
}

function request(body: BodyInit): Request {
  const options: RequestInit & { duplex: "half" } = {
    method: "POST",
    headers: { Authorization: "Bearer app-token" },
    body,
    duplex: "half",
  };
  return new Request("https://example.invalid/events", options);
}
