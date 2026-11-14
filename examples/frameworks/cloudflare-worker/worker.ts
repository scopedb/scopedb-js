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

import {
  AppendRowsError,
  Client,
  encodeAppendRecord,
  ScopeDBError,
  type Table,
} from "scopedb";

// ScopeDB uses standard Web APIs (`fetch`, `CompressionStream`, `TextEncoder`,
// `crypto.randomUUID()`) and does not require the Workers `nodejs_compat` flag.
// Store `SCOPEDB_ENDPOINT`, `SCOPEDB_API_KEY`, and `APP_WRITE_TOKEN` as Worker
// secrets, and replace the example bearer check with your application's auth.
//
// To collect Worker invocations, `console.*` logs, and uncaught exceptions
// across a Cloudflare account without modifying application handlers, deploy
// https://github.com/scopedb/cloudflare-tail-worker (guide:
// https://www.scopedb.io/docs/guides/analyze-cloudflare-worker-traffic).
interface Env {
  SCOPEDB_ENDPOINT: string;
  SCOPEDB_API_KEY: string;
  SCOPEDB_TABLE: string;
  APP_WRITE_TOKEN: string;
  SCOPEDB_DATABASE?: string;
  SCOPEDB_SCHEMA?: string;
}

export interface WorkerExecutionContext {
  waitUntil(promise: Promise<unknown>): void;
}

const MAX_EVENT_BYTES = 64 * 1024;

export default {
  async fetch(
    request: Request,
    env: Env,
    ctx: WorkerExecutionContext,
  ): Promise<Response> {
    // Generate this binding type from your Wrangler config in a real project.
    // Runtime checks keep missing secrets fail-closed despite TypeScript types.
    const endpoint = requiredBinding(env.SCOPEDB_ENDPOINT, "SCOPEDB_ENDPOINT");
    const apiKey = requiredBinding(env.SCOPEDB_API_KEY, "SCOPEDB_API_KEY");
    const tableName = requiredBinding(env.SCOPEDB_TABLE, "SCOPEDB_TABLE");
    const writeToken = requiredBinding(env.APP_WRITE_TOKEN, "APP_WRITE_TOKEN");
    const client = new Client(endpoint, { apiKey });

    if (request.method === "GET") {
      const result = await client.query("SELECT 1 AS ready", {
        signal: AbortSignal.timeout(10_000),
      });
      // Use `integerMode: "string"` when returning arbitrary 64-bit integer columns as JSON.
      return Response.json(result.first({ integerMode: "number" }));
    }

    if (request.method !== "POST") {
      return new Response("method not allowed", { status: 405 });
    }

    if (!hasWriteAccess(request, writeToken)) {
      return Response.json({ error: "unauthorized" }, {
        status: 401,
        headers: { "WWW-Authenticate": "Bearer" },
      });
    }

    // Validate and encode synchronously before returning 202 so malformed JSON,
    // oversized bodies, and unsafe numbers fail closed on the request path.
    const prepared = await readEventRecord(request);
    if (prepared instanceof Response) {
      return prepared;
    }

    const table = client.table(tableName, {
      database: env.SCOPEDB_DATABASE ?? "scopedb",
      schema: env.SCOPEDB_SCHEMA ?? "public",
    });
    ctx.waitUntil(appendEventInBackground(table, prepared.ndjson));
    return Response.json(
      { accepted: true, eventId: prepared.eventId },
      { status: 202 },
    );
  },
};

async function appendEventInBackground(
  table: Table,
  ndjson: string,
): Promise<void> {
  try {
    await table.append(ndjson, {
      signal: AbortSignal.timeout(25_000),
    });
  } catch (error) {
    if (error instanceof ScopeDBError) {
      console.error("background event append failed", {
        kind: error.kind,
        code: error.code,
        httpStatus: error.httpStatus,
        requestId: error.requestId,
        outcome: error instanceof AppendRowsError ? error.outcome : undefined,
      });
      return;
    }
    console.error("background event append failed", error);
  }
}

interface EventInput {
  name: string;
  attributes?: Record<string, unknown>;
}

interface PreparedEventRecord {
  eventId: string;
  ndjson: string;
}

async function readEventRecord(
  request: Request,
): Promise<PreparedEventRecord | Response> {
  const declaredBytes = Number(request.headers.get("Content-Length"));
  if (Number.isFinite(declaredBytes) && declaredBytes > MAX_EVENT_BYTES) {
    return Response.json({ error: "request body is too large" }, { status: 413 });
  }

  const body = await readBody(request);
  if (body instanceof Response) return body;

  let value: unknown;
  try {
    value = JSON.parse(body);
  } catch {
    return Response.json({ error: "request body must be valid JSON" }, { status: 400 });
  }
  if (!isEventInput(value)) {
    return Response.json(
      { error: "expected { name: string, attributes?: object }" },
      { status: 400 },
    );
  }
  const eventId = crypto.randomUUID();
  try {
    return {
      eventId,
      ndjson: encodeAppendRecord({
        event_id: eventId,
        occurred_at: new Date().toISOString(),
        name: value.name,
        attributes: value.attributes ?? {},
      }),
    };
  } catch {
    return Response.json(
      { error: "attributes contain a number that cannot be represented safely" },
      { status: 400 },
    );
  }
}

async function readBody(request: Request): Promise<string | Response> {
  if (request.body === null) return "";
  const reader = request.body.getReader();
  const decoder = new TextDecoder();
  let body = "";
  let bytes = 0;
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      bytes += value.byteLength;
      if (bytes > MAX_EVENT_BYTES) {
        await reader.cancel().catch(() => {});
        return Response.json({ error: "request body is too large" }, { status: 413 });
      }
      body += decoder.decode(value, { stream: true });
    }
    return body + decoder.decode();
  } finally {
    reader.releaseLock();
  }
}

function isEventInput(value: unknown): value is EventInput {
  if (!isJsonObject(value) || typeof value["name"] !== "string") {
    return false;
  }
  const name = value["name"];
  if (name.length === 0 || name.length > 256) {
    return false;
  }
  const attributes = value["attributes"];
  return attributes === undefined || isJsonObject(attributes);
}

function isJsonObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function hasWriteAccess(request: Request, token: string): boolean {
  return request.headers.get("Authorization") === `Bearer ${token}`;
}

function requiredBinding(value: unknown, name: string): string {
  if (typeof value !== "string" || value.length === 0) {
    throw new Error(`Missing Worker binding: ${name}`);
  }
  return value;
}
