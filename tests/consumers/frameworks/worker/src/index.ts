import { Client } from "scopedb";
import template from "./template";

interface Env {
  SCOPEDB_ENDPOINT: string;
  SCOPEDB_API_KEY: string;
  SCOPEDB_TABLE: string;
  APP_WRITE_TOKEN: string;
  SCOPEDB_DATABASE?: string;
  SCOPEDB_SCHEMA?: string;
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const globals = globalThis as typeof globalThis & {
      Buffer?: unknown;
      process?: unknown;
    };
    if (globals.Buffer !== undefined || globals.process !== undefined) {
      throw new Error("Worker smoke unexpectedly has Node globals");
    }

    const url = new URL(request.url);
    if (url.pathname !== "/append-stream") {
      return template.fetch(request, env);
    }

    if (request.method !== "POST") {
      return new Response("method not allowed", { status: 405 });
    }
    if (request.headers.get("Authorization") !== `Bearer ${env.APP_WRITE_TOKEN}`) {
      return Response.json({ error: "unauthorized" }, {
        status: 401,
        headers: { "WWW-Authenticate": "Bearer" },
      });
    }

    const client = new Client(env.SCOPEDB_ENDPOINT, {
      apiKey: env.SCOPEDB_API_KEY,
    });
    const stream = client.table(env.SCOPEDB_TABLE, {
      database: env.SCOPEDB_DATABASE ?? "scopedb",
      schema: env.SCOPEDB_SCHEMA ?? "public",
    })
      .appendStream()
      .maxBatchRows(2)
      .maxConcurrentBatches(1)
      .flushIntervalMs(60_000)
      .build();

    await stream.sendAll([
      { event_id: "stream-1", name: "first" },
      { event_id: "stream-2", name: "second" },
      { event_id: "stream-3", name: "third" },
    ]);
    const result = await stream.shutdown();
    return Response.json({ insertedRows: result?.num_rows_inserted ?? 0 }, {
      status: 201,
    });
  },
};
