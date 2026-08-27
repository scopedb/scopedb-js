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
import { spawn } from "node:child_process";
import { cp, copyFile, lstat, mkdir, mkdtemp, readFile, readdir, rm } from "node:fs/promises";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import { basename, dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const repositoryRoot = fileURLToPath(new URL("../", import.meta.url));
const fixtureRoot = join(repositoryRoot, "tests", "consumers", "frameworks");
const temporaryRoot = await mkdtemp(join(tmpdir(), "scopedb-framework-smoke-"));
const artifactsDirectory = join(temporaryRoot, "artifacts");
const consumerRoot = join(temporaryRoot, "consumer");
const keepTemporaryFiles = process.env["KEEP_FRAMEWORK_SMOKE_TEMP"] === "1";

const sdkApiKey = "packed-smoke-sdk-key";
const applicationWriteToken = "packed-smoke-write-token";
const tableName = "sdk framework/smoke?#";
const mock = await startMockScopeDb();

try {
  await mkdir(artifactsDirectory, { recursive: true });
  const packResult = await runCommand("pack SDK", "npm", [
    "pack",
    "--json",
    "--ignore-scripts",
    "--pack-destination",
    artifactsDirectory,
  ], { cwd: repositoryRoot });
  const packMetadata = JSON.parse(packResult.stdout);
  assert.equal(packMetadata.length, 1, "npm pack must describe exactly one tarball");
  assert.equal(typeof packMetadata[0]?.filename, "string");
  const tarball = join(artifactsDirectory, basename(packMetadata[0].filename));
  await lstat(tarball);

  await cp(fixtureRoot, consumerRoot, { recursive: true });
  await copyCheckedInTemplate(
    join(repositoryRoot, "examples", "frameworks", "nextjs-route-handler", "route.ts"),
    join(consumerRoot, "next-app", "app", "api", "events", "route.ts"),
  );
  await copyCheckedInTemplate(
    join(repositoryRoot, "examples", "frameworks", "cloudflare-worker", "worker.ts"),
    join(consumerRoot, "worker", "src", "template.ts"),
  );

  await runCommand("install locked framework consumer", "npm", [
    "ci",
    "--no-audit",
    "--no-fund",
  ], { cwd: consumerRoot, timeoutMs: 5 * 60_000 });
  await runCommand("install packed SDK", "npm", [
    "install",
    "--ignore-scripts",
    "--no-audit",
    "--no-fund",
    "--no-save",
    tarball,
  ], { cwd: consumerRoot, timeoutMs: 2 * 60_000 });

  const installedPackage = JSON.parse(await readFile(
    join(consumerRoot, "node_modules", "scopedb", "package.json"),
    "utf8",
  ));
  assert.equal(installedPackage.name, "scopedb");
  assert.equal(
    (await lstat(join(consumerRoot, "node_modules", "scopedb"))).isSymbolicLink(),
    false,
    "packed SDK installation must not be a workspace or npm link",
  );
  await lstat(join(consumerRoot, "node_modules", "scopedb", "dist", "index.d.ts"));

  await runCommand("type-check Web runtime consumer", join(
    consumerRoot,
    "node_modules",
    ".bin",
    "tsc",
  ), ["-p", "worker/tsconfig.json"], {
    cwd: consumerRoot,
  });

  const runtimeEnvironment = {
    ...process.env,
    SCOPEDB_ENDPOINT: mock.endpoint,
    SCOPEDB_API_KEY: sdkApiKey,
    SCOPEDB_DATABASE: "scopedb",
    SCOPEDB_SCHEMA: "public",
    SCOPEDB_TABLE: tableName,
    APP_WRITE_TOKEN: applicationWriteToken,
    NEXT_TELEMETRY_DISABLED: "1",
    WRANGLER_SEND_METRICS: "false",
    CI: "1",
  };

  await smokeNext(consumerRoot, runtimeEnvironment, mock.requests);
  await smokeWorker(consumerRoot, runtimeEnvironment, mock.requests);
  assert.deepEqual(mock.errors, [], `mock ScopeDB errors: ${mock.errors.join("; ")}`);
  console.log("packed framework consumer smoke passed");
} finally {
  await mock.close();
  if (keepTemporaryFiles) {
    console.log(`kept framework smoke files at ${temporaryRoot}`);
  } else {
    await rm(temporaryRoot, { recursive: true, force: true });
  }
}

async function smokeNext(consumer, environment, backendRequests) {
  const appDirectory = join(consumer, "next-app");
  const nextBinary = join(consumer, "node_modules", ".bin", "next");
  await runCommand("build Next.js consumer", nextBinary, ["build"], {
    cwd: appDirectory,
    env: environment,
    timeoutMs: 3 * 60_000,
  });
  assert.equal(
    await directoryContains(
      join(appDirectory, ".next", "static"),
      Buffer.from(sdkApiKey),
    ),
    false,
    "ScopeDB API key leaked into Next.js static output",
  );

  const port = await availablePort();
  const server = startService("Next.js", nextBinary, [
    "start",
    "--hostname",
    "127.0.0.1",
    "--port",
    String(port),
  ], { cwd: appDirectory, env: environment });
  const baseUrl = `http://127.0.0.1:${port}/api/events`;
  const requestStart = backendRequests.length;

  try {
    const queryResponse = await waitForResponse(baseUrl, server);
    assert.equal(queryResponse.status, 200);
    assert.deepEqual(await queryResponse.json(), { ready: 1 });

    await assertProtectedWriteBoundary(baseUrl, backendRequests);

    const appendResponse = await fetch(baseUrl, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${applicationWriteToken}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ name: "next", attributes: { runtime: "node" } }),
    });
    assert.equal(appendResponse.status, 201);
    assert.deepEqual(await appendResponse.json(), { insertedRows: 1 });
  } finally {
    await stopService(server);
  }

  const requests = backendRequests.slice(requestStart);
  assertBackendContract(requests, {
    statementRequests: 1,
    appendedRows: 1,
    rowNames: ["next"],
  });
  console.log("Next.js packed consumer passed");
}

async function smokeWorker(consumer, environment, backendRequests) {
  const workerDirectory = join(consumer, "worker");
  const config = await readFile(join(workerDirectory, "wrangler.jsonc"), "utf8");
  assert.equal(config.includes("nodejs_compat"), false, "Worker smoke must not enable nodejs_compat");

  const wranglerBinary = join(consumer, "node_modules", ".bin", "wrangler");
  const port = await availablePort();
  const server = startService("Wrangler/workerd", wranglerBinary, [
    "dev",
    "--local",
    "--ip",
    "127.0.0.1",
    "--port",
    String(port),
    "--var",
    `SCOPEDB_ENDPOINT:${environment.SCOPEDB_ENDPOINT}`,
    "--var",
    `SCOPEDB_API_KEY:${sdkApiKey}`,
    "--var",
    `SCOPEDB_TABLE:${tableName}`,
    "--var",
    `APP_WRITE_TOKEN:${applicationWriteToken}`,
  ], { cwd: workerDirectory, env: environment });
  const baseUrl = `http://127.0.0.1:${port}`;
  const requestStart = backendRequests.length;

  try {
    const queryResponse = await waitForResponse(baseUrl, server);
    assert.equal(queryResponse.status, 200);
    assert.deepEqual(await queryResponse.json(), { ready: 1 });

    await assertProtectedWriteBoundary(baseUrl, backendRequests);

    const appendResponse = await fetch(baseUrl, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${applicationWriteToken}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ name: "worker", attributes: { runtime: "workerd" } }),
    });
    assert.equal(appendResponse.status, 201);
    assert.deepEqual(await appendResponse.json(), { insertedRows: 1 });

    const requestsBeforeStream = backendRequests.length;
    const unauthorizedStream = await fetch(`${baseUrl}/append-stream`, {
      method: "POST",
    });
    assert.equal(unauthorizedStream.status, 401);
    assert.equal(
      backendRequests.length,
      requestsBeforeStream,
      "unauthorized stream write reached ScopeDB",
    );

    const streamResponse = await fetch(`${baseUrl}/append-stream`, {
      method: "POST",
      headers: { Authorization: `Bearer ${applicationWriteToken}` },
    });
    assert.equal(streamResponse.status, 201);
    assert.deepEqual(await streamResponse.json(), { insertedRows: 3 });
  } finally {
    await stopService(server);
  }

  const requests = backendRequests.slice(requestStart);
  assertBackendContract(requests, {
    statementRequests: 1,
    appendedRows: 4,
    rowNames: ["worker", "first", "second", "third"],
  });
  const appendRequests = requests.filter((request) => request.pathname.endsWith("/rows"));
  assert.equal(appendRequests.length, 3, "direct append plus two stream batches expected");
  console.log("Wrangler/workerd packed consumer passed");
}

async function assertProtectedWriteBoundary(url, backendRequests) {
  const requestsBefore = backendRequests.length;
  const unauthorized = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ name: "unauthorized" }),
  });
  assert.equal(unauthorized.status, 401);
  assert.equal(backendRequests.length, requestsBefore, "unauthorized write reached ScopeDB");

  const oversized = await fetch(url, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${applicationWriteToken}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ name: "x".repeat(65 * 1024) }),
  });
  assert.equal(oversized.status, 413);
  assert.equal(backendRequests.length, requestsBefore, "oversized write reached ScopeDB");
}

function assertBackendContract(requests, expected) {
  const statements = requests.filter((request) => request.pathname === "/v1/statements");
  const appends = requests.filter((request) => request.pathname.endsWith("/rows"));
  assert.equal(statements.length, expected.statementRequests);
  assert.equal(
    appends.reduce((total, request) => total + ndjsonRows(request.body), 0),
    expected.appendedRows,
  );
  assert.ok(requests.length > 0);
  assert.ok(requests.every((request) => request.authorization === `Bearer ${sdkApiKey}`));
  assert.ok(requests.every((request) => request.accept === "application/json"));
  assert.ok(appends.every((request) => request.contentType === "application/x-ndjson"));
  assert.ok(appends.every((request) => request.pathname ===
    "/v1/databases/scopedb/schemas/public/tables/sdk%20framework%2Fsmoke%3F%23/rows"));
  assert.ok(statements.every((request) => request.contentType === "application/json"));
  const rows = appends.flatMap((request) => request.body
    .split("\n")
    .filter((line) => line.length > 0)
    .map((line) => JSON.parse(line)));
  assert.ok(rows.every((row) => typeof row === "object" && row !== null));
  assert.ok(rows.every((row) => typeof row.event_id === "string"));
  assert.deepEqual(
    rows.map((row) => row.name).sort(),
    [...expected.rowNames].sort(),
  );
  assert.ok(statements.every((request) =>
    JSON.parse(request.body).statement === "SELECT 1 AS ready"));
}

async function startMockScopeDb() {
  const requests = [];
  const errors = [];
  const server = createServer(async (request, response) => {
    try {
      const body = await readRequestBody(request);
      const pathname = new URL(request.url ?? "/", "http://scope.test").pathname;
      requests.push({
        method: request.method,
        pathname,
        authorization: request.headers.authorization,
        accept: request.headers.accept,
        contentType: request.headers["content-type"],
        body,
      });

      if (request.method === "POST" && pathname === "/v1/statements") {
        respondJson(response, finishedStatement());
        return;
      }
      if (request.method === "POST" && pathname.endsWith("/rows")) {
        respondJson(response, {
          append_state: "committed",
          num_rows_inserted: ndjsonRows(body),
        });
        return;
      }
      respondJson(response, { message: "not found" }, 404);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      errors.push(message);
      respondJson(response, { message }, 500);
    }
  });

  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  const address = server.address();
  assert.notEqual(address, null);
  assert.equal(typeof address, "object");

  return {
    endpoint: `http://127.0.0.1:${address.port}`,
    requests,
    errors,
    close: () => new Promise((resolve, reject) => {
      server.close((error) => error === undefined ? resolve() : reject(error));
    }),
  };
}

function finishedStatement() {
  return {
    status: "finished",
    statement_id: "framework-smoke-statement",
    created_at: "2026-08-01T00:00:00Z",
    progress: {
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
      metadata: {
        fields: [{ name: "ready", data_type: "int" }],
        num_rows: 1,
      },
      format: "json",
      rows: [["1"]],
    },
  };
}

function respondJson(response, value, status = 200) {
  response.writeHead(status, { "Content-Type": "application/json" });
  response.end(JSON.stringify(value));
}

async function readRequestBody(request) {
  const chunks = [];
  for await (const chunk of request) {
    chunks.push(chunk);
  }
  return Buffer.concat(chunks).toString("utf8");
}

function ndjsonRows(body) {
  return body.split("\n").filter((line) => line.length > 0).length;
}

async function copyCheckedInTemplate(source, destination) {
  await mkdir(dirname(destination), { recursive: true });
  await copyFile(source, destination);
}

async function directoryContains(directory, needle) {
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    const path = join(directory, entry.name);
    if (entry.isDirectory()) {
      if (await directoryContains(path, needle)) {
        return true;
      }
    } else if ((await readFile(path)).includes(needle)) {
      return true;
    }
  }
  return false;
}

async function availablePort() {
  const server = createServer();
  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  const address = server.address();
  assert.notEqual(address, null);
  assert.equal(typeof address, "object");
  await new Promise((resolve, reject) => {
    server.close((error) => error === undefined ? resolve() : reject(error));
  });
  return address.port;
}

async function waitForResponse(url, service) {
  const deadline = Date.now() + 60_000;
  let lastError;
  while (Date.now() < deadline) {
    if (childHasExited(service.child)) {
      throw new Error(`${service.name} exited before becoming ready\n${service.output()}`);
    }
    try {
      const response = await fetch(url, { signal: AbortSignal.timeout(2_000) });
      if (response.ok) {
        return response;
      }
      lastError = new Error(`HTTP ${response.status}: ${await response.text()}`);
    } catch (error) {
      lastError = error;
    }
    await new Promise((resolve) => setTimeout(resolve, 200));
  }
  throw new Error(
    `${service.name} did not become ready: ${String(lastError)}\n${service.output()}`,
  );
}

async function runCommand(label, command, args, options) {
  const service = startService(label, command, args, options);
  const timeoutMs = options.timeoutMs ?? 60_000;
  let didTimeOut = false;
  let termination;
  const timeout = setTimeout(() => {
    didTimeOut = true;
    termination = stopService(service);
    void termination.catch(() => {});
  }, timeoutMs);
  const outcome = await new Promise((resolve) => {
    service.child.once("error", (error) => resolve({ error }));
    service.child.once("close", (code, signal) => resolve({ code, signal }));
  });
  clearTimeout(timeout);
  if (termination !== undefined) {
    await termination;
  }
  if (didTimeOut) {
    throw new Error(`${label} timed out after ${timeoutMs}ms\n${service.output()}`);
  }
  if ("error" in outcome) {
    throw outcome.error;
  }
  if (outcome.code !== 0) {
    throw new Error(
      `${label} failed with code ${outcome.code} signal ${outcome.signal}\n${service.output()}`,
    );
  }
  return { stdout: service.stdout(), stderr: service.stderr() };
}

function startService(name, command, args, options) {
  const child = spawn(command, args, {
    cwd: options.cwd,
    env: options.env ?? process.env,
    detached: process.platform !== "win32",
    stdio: ["ignore", "pipe", "pipe"],
  });
  let stdout = "";
  let stderr = "";
  child.stdout.on("data", (chunk) => {
    stdout = `${stdout}${chunk}`.slice(-128 * 1024);
  });
  child.stderr.on("data", (chunk) => {
    stderr = `${stderr}${chunk}`.slice(-128 * 1024);
  });
  return {
    name,
    child,
    stdout: () => stdout,
    stderr: () => stderr,
    output: () => `${stdout}\n${stderr}`,
  };
}

async function stopService(service) {
  if (childHasExited(service.child)) {
    return;
  }
  const closed = new Promise((resolve) => service.child.once("close", resolve));
  signalProcess(service.child, "SIGTERM");
  let graceTimer;
  const gracePeriod = new Promise((resolve) => {
    graceTimer = setTimeout(resolve, 5_000, "timeout");
  });
  const result = await Promise.race([closed, gracePeriod]);
  clearTimeout(graceTimer);
  if (result === "timeout") {
    signalProcess(service.child, "SIGKILL");
    await closed;
  }
}

function childHasExited(child) {
  return child.exitCode !== null || child.signalCode !== null;
}

function signalProcess(child, signal) {
  try {
    if (process.platform === "win32") {
      child.kill(signal);
    } else if (child.pid !== undefined) {
      process.kill(-child.pid, signal);
    }
  } catch (error) {
    if (error?.code !== "ESRCH") {
      throw error;
    }
  }
}
