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
      return Response.json({ num_rows_inserted: 1 });
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

it("Cloudflare Tail Worker template normalizes traces, redacts credentials, and sanitizes logs", async (t) => {
  const directory = await mkdtemp(join(tmpdir(), "scopedb-tail-template-test-"));
  t.after(() => rm(directory, { recursive: true, force: true }));
  const originalFetch = globalThis.fetch;
  t.after(() => {
    globalThis.fetch = originalFetch;
  });

  const source = await readFile(
    new URL("../../examples/frameworks/cloudflare-worker/tail-worker.ts", import.meta.url),
    "utf8",
  );
  const compiled = ts.transpileModule(source, {
    compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.ES2022 },
  }).outputText;
  const entry = join(directory, "tail-worker.mjs");
  await writeFile(entry, compiled);
  const moduleDirectory = join(directory, "node_modules", "scopedb");
  await mkdir(moduleDirectory, { recursive: true });
  await writeFile(
    join(moduleDirectory, "package.json"),
    JSON.stringify({ type: "module", exports: "./index.js" }),
  );
  await writeFile(
    join(moduleDirectory, "index.js"),
    `export * from ${JSON.stringify(new URL("../src/index.js", import.meta.url).href)};`,
  );

  const appendedTexts: string[] = [];
  globalThis.fetch = async (_input, init) => {
    const text = requestBodyText(init);
    appendedTexts.push(text);
    const rowCount = text.trim().split("\n").length;
    return Response.json({ num_rows_inserted: rowCount });
  };

  const template = await import(pathToFileURL(entry).href) as {
    default: {
      tail(events: readonly unknown[], env: typeof environment): Promise<void>;
    };
  };

  await template.default.tail([], environment);
  assert.equal(appendedTexts.length, 0);

  const cyclic: Record<string, unknown> = { ok: true };
  cyclic["self"] = cyclic;
  // eslint-disable-next-line no-sparse-arrays
  const sparse = [1, , 3];

  await template.default.tail([
    {
      scriptName: "checkout-worker",
      outcome: "exception",
      eventTimestamp: 1762041600000,
      wallTime: 42.5,
      cpuTime: 3.25,
      event: {
        request: {
          url: "https://shop.example.com/api/checkout?token=secret-query-token#frag",
          method: "POST",
          headers: {
            Authorization: "Bearer secret-header-token",
            Cookie: "session=secret-cookie",
            "CF-Ray": "8db1234567890abc-SJC",
          },
          cf: {
            colo: "SJC",
            country: "US",
          },
        },
        response: {
          status: 502,
        },
      },
      logs: [
        {
          timestamp: 1762041600100,
          level: "error",
          message: [
            "gateway failed",
            {
              order_id: 9007199254740993n,
              unsafe_int: 9007199254740992,
              not_finite: Number.NaN,
              err: new Error("upstream timeout"),
              cyclic,
              sparse,
            },
          ],
        },
      ],
      exceptions: [
        {
          timestamp: 1762041600200,
          name: "GatewayTimeoutError",
          message: "Credit card gateway timed out",
          stack: "GatewayTimeoutError: Credit card gateway timed out\n    at fetch",
        },
      ],
    },
  ], environment);

  assert.equal(appendedTexts.length, 1);
  const rawPayload = appendedTexts[0]!;
  assert.equal(rawPayload.includes("secret-query-token"), false);
  assert.equal(rawPayload.includes("secret-header-token"), false);
  assert.equal(rawPayload.includes("secret-cookie"), false);
  assert.equal(rawPayload.includes('"order_id":9007199254740993'), true);
  assert.equal(rawPayload.includes('"unsafe_int":"9007199254740992"'), true);

  const rows = rawPayload.trim().split("\n").map((line) => JSON.parse(line) as Record<string, unknown>);
  assert.equal(rows.length, 3);
  assert.deepEqual(rows.map((row) => row["name"]), [
    "worker.invocation",
    "worker.log",
    "worker.exception",
  ]);
  for (const row of rows) {
    assert.equal(row["path"], "/api/checkout");
    assert.equal(row["status"], 502);
  }

  const invocationAttrs = rows[0]!["attributes"] as Record<string, unknown>;
  const logAttrs = rows[1]!["attributes"] as Record<string, unknown>;
  const exceptionAttrs = rows[2]!["attributes"] as Record<string, unknown>;

  assert.equal(invocationAttrs["invocation_id"], rows[0]!["event_id"]);
  assert.equal(logAttrs["invocation_id"], rows[0]!["event_id"]);
  assert.equal(exceptionAttrs["invocation_id"], rows[0]!["event_id"]);
  assert.equal(invocationAttrs["script_name"], "checkout-worker");
  assert.equal(invocationAttrs["outcome"], "exception");
  assert.equal(invocationAttrs["method"], "POST");
  assert.equal(invocationAttrs["ray_id"], "8db1234567890abc-SJC");
  assert.equal(invocationAttrs["colo"], "SJC");
  assert.equal(invocationAttrs["country"], "US");
  assert.equal(invocationAttrs["wall_time_ms"], 42.5);
  assert.equal(invocationAttrs["cpu_time_ms"], 3.25);
  assert.equal(invocationAttrs["log_count"], 1);
  assert.equal(invocationAttrs["exception_count"], 1);

  const logMessage = logAttrs["message"] as unknown[];
  assert.equal(logMessage[0], "gateway failed");
  const logDetails = logMessage[1] as Record<string, unknown>;
  assert.equal(logDetails["not_finite"], "NaN");
  assert.deepEqual(logDetails["cyclic"], { ok: true, self: "[Circular]" });
  assert.deepEqual(logDetails["sparse"], [1, null, 3]);
  assert.equal((logDetails["err"] as Record<string, unknown>)["message"], "upstream timeout");

  assert.equal(exceptionAttrs["exception_name"], "GatewayTimeoutError");
  assert.equal(exceptionAttrs["message"], "Credit card gateway timed out");
});

function request(body: BodyInit): Request {
  const options: RequestInit & { duplex: "half" } = {
    method: "POST",
    headers: { Authorization: "Bearer app-token" },
    body,
    duplex: "half",
  };
  return new Request("https://example.invalid/events", options);
}
