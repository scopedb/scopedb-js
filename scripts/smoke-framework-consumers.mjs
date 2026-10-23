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
import { execFile, spawn } from "node:child_process";
import { once } from "node:events";
import { cp, copyFile, lstat, mkdir, mkdtemp, rm } from "node:fs/promises";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import { gunzipSync } from "node:zlib";

const exec = promisify(execFile);
const repositoryRoot = fileURLToPath(new URL("../", import.meta.url));
const temporaryRoot = await mkdtemp(join(tmpdir(), "scopedb-framework-smoke-"));
const consumerRoot = join(temporaryRoot, "consumer");
const bin = (name) => join(consumerRoot, "node_modules", ".bin", name);
const sdkApiKey = "packed-smoke-sdk-key";
const applicationWriteToken = "packed-smoke-write-token";
const appendedRows = [];
const backend = createServer(handleRequest).listen(0, "127.0.0.1");
await once(backend, "listening");
const bindings = {
  SCOPEDB_ENDPOINT: `http://127.0.0.1:${backend.address().port}`,
  SCOPEDB_API_KEY: sdkApiKey,
  SCOPEDB_DATABASE: "scopedb",
  SCOPEDB_SCHEMA: "public",
  SCOPEDB_TABLE: "events",
  APP_WRITE_TOKEN: applicationWriteToken,
};
const environment = {
  ...process.env,
  ...bindings,
  NEXT_TELEMETRY_DISABLED: "1",
  WRANGLER_SEND_METRICS: "false",
  CI: "1",
};

try {
  const packed = JSON.parse(await run("npm", [
    "pack", "--json", "--ignore-scripts", "--pack-destination", temporaryRoot,
  ], repositoryRoot));
  await cp(join(repositoryRoot, "tests/consumers/frameworks"), consumerRoot, { recursive: true });
  await mkdir(join(consumerRoot, "app/api/events"), { recursive: true });
  await copyFile(
    join(repositoryRoot, "examples/frameworks/nextjs-route-handler/route.ts"),
    join(consumerRoot, "app/api/events/route.ts"),
  );
  await copyFile(
    join(repositoryRoot, "examples/frameworks/cloudflare-worker/worker.ts"),
    join(consumerRoot, "worker/worker.ts"),
  );
  await run("npm", [
    "install", "--no-package-lock", "--no-audit", "--no-fund", "--no-save", "--include=dev",
    join(temporaryRoot, packed[0].filename),
  ]);
  assert.equal((await lstat(join(consumerRoot, "node_modules/scopedb"))).isSymbolicLink(), false);
  await run(bin("tsc"), ["-p", "worker/tsconfig.json"]);
  await run(bin("next"), ["build"]);
  await smoke("Next.js", bin("next"), ["start", "--hostname", "127.0.0.1"], consumerRoot, "/api/events");
  await smoke("Workers", bin("wrangler"), [
    "dev", "--local", "--ip", "127.0.0.1",
    ...Object.entries(bindings).flatMap(([key, value]) => ["--var", `${key}:${value}`]),
  ], join(consumerRoot, "worker"), "/");
  assert.deepEqual(appendedRows.map((row) => row.name), ["Next.js", "Workers"]);
  console.log("packed framework consumer smoke passed");
} finally {
  backend.closeAllConnections();
  await new Promise((resolve) => backend.close(resolve));
  if (process.env["KEEP_FRAMEWORK_SMOKE_TEMP"] === "1") {
    console.log(`kept framework smoke files at ${temporaryRoot}`);
  } else {
    await rm(temporaryRoot, { recursive: true, force: true });
  }
}

async function smoke(name, command, args, cwd, path) {
  const reservation = createServer().listen(0, "127.0.0.1");
  await once(reservation, "listening");
  const port = reservation.address().port;
  await new Promise((resolve) => reservation.close(resolve));
  const child = spawn(command, [...args, "--port", String(port)], {
    cwd, env: environment, stdio: "inherit", detached: process.platform !== "win32",
  });
  let spawnError;
  child.on("error", (error) => {
    spawnError = error;
  });
  const closed = new Promise((resolve) => child.once("close", resolve));
  const url = `http://127.0.0.1:${port}${path}`;

  try {
    let response;
    const deadline = Date.now() + 60_000;
    while (response === undefined) {
      if (spawnError) {
        throw spawnError;
      }
      assert.ok(child.exitCode === null && child.signalCode === null, `${name} exited before readiness`);
      assert.ok(Date.now() < deadline, `${name} did not become ready within 60 seconds`);
      try {
        response = await fetch(url, { signal: AbortSignal.timeout(2_000) });
      } catch {
        await delay(200);
      }
    }
    assert.equal(response.status, 200);
    assert.deepEqual(await response.json(), { ready: 1 });
    response = await fetch(url, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${applicationWriteToken}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ name, attributes: { source: "packed-sdk" } }),
      signal: AbortSignal.timeout(10_000),
    });
    assert.equal(response.status, 201);
    assert.deepEqual(await response.json(), { insertedRows: 1 });
    console.log(`${name} packed consumer passed`);
  } finally {
    // Wrangler starts workerd as a child; stop the process group as well as the CLI.
    const kill = (signal) => {
      try {
        if (process.platform === "win32") {
          child.kill(signal);
        } else if (child.pid !== undefined) {
          process.kill(-child.pid, signal);
        }
      } catch (error) {
        if (error.code !== "ESRCH") {
          throw error;
        }
      }
    };
    const timeout = setTimeout(() => kill("SIGKILL"), 5_000);
    try {
      kill("SIGTERM");
      await closed;
    } finally {
      clearTimeout(timeout);
    }
  }
}

async function run(command, args, cwd = consumerRoot) {
  console.log(`> ${command} ${args.join(" ")}`);
  try {
    return (await exec(command, args, {
      cwd, env: environment, timeout: 5 * 60_000, maxBuffer: 16 * 1024 * 1024,
    })).stdout;
  } catch (error) {
    console.error(error.stdout);
    throw error;
  }
}

async function handleRequest(request, response) {
  try {
    assert.equal(request.method, "POST");
    assert.equal(request.headers.authorization, `Bearer ${sdkApiKey}`);
    assert.equal(request.headers["content-encoding"], "gzip");
    const chunks = [];
    for await (const chunk of request) {
      chunks.push(chunk);
    }
    const body = gunzipSync(Buffer.concat(chunks)).toString("utf8");
    let result;
    if (request.url === "/v1/statements") {
      assert.equal(JSON.parse(body).statement, "SELECT 1 AS ready");
      result = {
        status: "finished",
        statement_id: "framework-smoke-statement",
        created_at: "2026-08-01T00:00:00Z",
        progress: {
          compute_seconds: 0.001,
          total_percentage: 100,
          nanos_from_submitted: 1,
          nanos_from_started: 1,
          total_stages: 1,
          total_partitions: 1,
          total_rows: 1,
          total_compressed_bytes: 1,
          total_uncompressed_bytes: 1,
          scanned_stages: 1,
          scanned_partitions: 1,
          scanned_rows: 1,
          scanned_compressed_bytes: 1,
          scanned_uncompressed_bytes: 1,
          skipped_partitions: 0,
          skipped_rows: 0,
          skipped_compressed_bytes: 0,
          skipped_uncompressed_bytes: 0,
        },
        result_set: {
          metadata: { fields: [{ name: "ready", data_type: "int" }], num_rows: 1 },
          format: "json",
          rows: [["1"]],
        },
      };
    } else {
      assert.equal(request.url, "/v1/catalog/databases/scopedb/schemas/public/tables/events/ingest");
      const rows = body.trim().split("\n").map((line) => JSON.parse(line));
      appendedRows.push(...rows);
      result = { status: "committed", num_rows_inserted: rows.length };
    }
    response.writeHead(200, { "Content-Type": "application/json" });
    response.end(JSON.stringify(result));
  } catch (error) {
    console.error(error);
    response.writeHead(500, { "Content-Type": "application/json" });
    response.end(JSON.stringify({ message: String(error) }));
  }
}
