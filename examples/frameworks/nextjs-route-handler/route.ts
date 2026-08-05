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

import "server-only";
import { Client } from "scopedb";

// Copy this file to app/api/events/route.ts in a Next.js application.
// ScopeDB API keys belong in Route Handlers, Server Actions, or other trusted
// server code. Never import the configured client from a Client Component.
// Replace the example bearer check below with your application's normal auth.
export const runtime = "nodejs";

const MAX_EVENT_BYTES = 64 * 1024;

const client = new Client(requiredEnv("SCOPEDB_ENDPOINT"), {
  apiKey: requiredEnv("SCOPEDB_API_KEY"),
});
const events = client.table(requiredEnv("SCOPEDB_TABLE"), {
  database: process.env["SCOPEDB_DATABASE"] ?? "scopedb",
  schema: process.env["SCOPEDB_SCHEMA"] ?? "public",
});

export async function GET(): Promise<Response> {
  const result = await client.query("SELECT 1 AS ready", {
    signal: AbortSignal.timeout(10_000),
  });
  return Response.json(result.first({ integerMode: "number" }));
}

export async function POST(request: Request): Promise<Response> {
  if (!hasWriteAccess(request, requiredEnv("APP_WRITE_TOKEN"))) {
    return Response.json({ error: "unauthorized" }, {
      status: 401,
      headers: { "WWW-Authenticate": "Bearer" },
    });
  }

  const event = await readEvent(request);
  if (event instanceof Response) {
    return event;
  }

  const result = await events.append(JSON.stringify({
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
}

function requiredEnv(name: string): string {
  const value = process.env[name];
  if (value === undefined || value.length === 0) {
    throw new Error(`Missing ${name}`);
  }
  return value;
}

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
