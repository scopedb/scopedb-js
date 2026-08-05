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

import { randomUUID } from "node:crypto";
import { Client } from "scopedb";

// Layer: integration template (type-checked, not directly runnable).
// Requires a Node 20 Fetch-style runtime with a waitUntil() lifecycle hook.

const tableName = process.env["SCOPEDB_TABLE"];
if (tableName === undefined || tableName.length === 0) {
  throw new Error(
    "Set SCOPEDB_TABLE to a disposable table before deploying this template",
  );
}

const client = new Client(
  process.env["SCOPEDB_ENDPOINT"] ?? "http://127.0.0.1:6543",
  { token: process.env["SCOPEDB_TOKEN"] },
);

// This is a Node 20 Fetch-style integration template. Adapt the exported
// Request/Response and lifecycle types for platforms such as AWS Lambda.
// Module scope lets warm invocations reuse the stream. Do not call shutdown()
// after each invocation because shutdown permanently closes it.
const telemetry = client
  .table(tableName)
  .withDatabase(process.env["SCOPEDB_DATABASE"] ?? "scopedb")
  .withSchema(process.env["SCOPEDB_SCHEMA"] ?? "public")
  .appendStream({ onFailure: "continue" })
  .attemptTimeout(500)
  .maxRetries(1)
  .circuitBreaker(false)
  .build();

export interface FetchExecutionContext {
  /** Keeps the invocation alive for background work after returning a response. */
  waitUntil(task: Promise<unknown>): void;
}

/** Handles one invocation in Fetch runtimes that provide waitUntil(). */
export function handler(
  request: Request,
  context: FetchExecutionContext,
): Response {
  const response = new Response("ok", { status: 200 });
  const accepted = telemetry.trySend({
    event_id: randomUUID(),
    occurred_at: new Date().toISOString(),
    path: new URL(request.url).pathname,
    status: response.status,
  });

  if (accepted) {
    // This settlement-first recipe flushes after every invocation. It favors
    // lifecycle safety over large batches. For higher throughput in a process
    // with graceful shutdown, use ../patterns/telemetry.ts.
    context.waitUntil(settleTelemetry());
  } else {
    console.error("serverless telemetry dropped locally", telemetry.stats());
  }

  return response;
}

async function settleTelemetry(): Promise<void> {
  try {
    // Keep the real barrier promise alive. attemptTimeout() bounds each HTTP
    // attempt, while the platform's waitUntil budget governs the shared barrier.
    const report = await telemetry.flush();
    // A module-level report can cover other concurrent invocations too.
    if (report.outcome !== "ok") {
      console.error("serverless telemetry loss", report);
    }
  } catch (error) {
    console.error("serverless telemetry flush failed", error);
  }
}
