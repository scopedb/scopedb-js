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

import { Client } from "scopedb";

// ScopeDB uses standard Fetch APIs and does not require the Workers
// `nodejs_compat` flag. Store both keys below as Worker secrets. Replace the
// example bearer check with your application's normal authentication.
interface Env {
  SCOPEDB_ENDPOINT: string;
  SCOPEDB_API_KEY: string;
  SCOPEDB_TABLE: string;
  APP_WRITE_TOKEN: string;
  SCOPEDB_DATABASE?: string;
  SCOPEDB_SCHEMA?: string;
}

const MAX_EVENT_BYTES = 64 * 1024;

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    // Generate this binding type from your Wrangler config in a real project.
    // Runtime checks keep missing secrets fail-closed despite TypeScript types.
    const endpoint = requiredBinding(env.SCOPEDB_ENDPOINT, "SCOPEDB_ENDPOINT");
    const apiKey = requiredBinding(env.SCOPEDB_API_KEY, "SCOPEDB_API_KEY");
    const tableName = requiredBinding(env.SCOPEDB_TABLE, "SCOPEDB_TABLE");
    const writeToken = requiredBinding(env.APP_WRITE_TOKEN, "APP_WRITE_TOKEN");
    const client = new Client(endpoint, {
      apiKey,
    });

    if (request.method === "GET") {
      const result = await client.query("SELECT 1 AS ready", {
        signal: AbortSignal.timeout(10_000),
      });
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

    const event = await readEvent(request);
    if (event instanceof Response) {
      return event;
    }

    const table = client.table(tableName, {
      database: env.SCOPEDB_DATABASE ?? "scopedb",
      schema: env.SCOPEDB_SCHEMA ?? "public",
    });
    const result = await table.append(JSON.stringify({
      event_id: crypto.randomUUID(),
      occurred_at: new Date().toISOString(),
      name: event.name,
      attributes: event.attributes ?? {},
    }), {
      signal: AbortSignal.timeout(30_000),
    });
    return Response.json(
      { insertedRows: result.num_rows_inserted },
      { status: 201 },
    );
  },
};

interface EventInput {
  name: string;
  attributes?: Record<string, unknown>;
}

async function readEvent(request: Request): Promise<EventInput | Response> {
  const declaredBytes = Number(request.headers.get("Content-Length"));
  if (Number.isFinite(declaredBytes) && declaredBytes > MAX_EVENT_BYTES) {
    return Response.json({ error: "request body is too large" }, { status: 413 });
  }

  const body = await request.text();
  if (new TextEncoder().encode(body).byteLength > MAX_EVENT_BYTES) {
    return Response.json({ error: "request body is too large" }, { status: 413 });
  }

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
  return value;
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
