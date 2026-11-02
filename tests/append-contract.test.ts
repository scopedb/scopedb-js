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
import { it } from "node:test";
import { AppendRowsError, Client } from "../src/index.js";

const cases = [
  { name: "modern rate limit", status: 429, body: { error: { code: "too_many_requests", message: "busy" } }, outcome: "failed", retry: true },
  { name: "explicit refusal", status: 429, body: { error: { code: "too_many_requests", message: "busy", retryable: false } }, outcome: "failed", retry: false },
  { name: "HTTP timeout", status: 408, body: { error: { code: "request_timeout", message: "late", retryable: true } }, outcome: "failed", retry: false },
  { name: "server error", status: 503, body: { error: { code: "service_unavailable", message: "busy", details: { status: "rejected" } } }, outcome: "ambiguous", retry: false },
  { name: "future code", status: 409, body: { error: { code: "future_conflict", message: "conflict" } }, outcome: "failed", retry: false },
  { name: "mismatched code", status: 429, body: { error: { code: "bad_request", message: "bad" } }, outcome: "ambiguous", retry: false },
  { name: "statement code", status: 400, body: { error: { code: "invalid_request", message: "bad" } }, outcome: "ambiguous", retry: false },
  { name: "missing code", status: 429, body: { error: { message: "busy" } }, outcome: "ambiguous", retry: false },
  { name: "null message", status: 429, body: { error: { code: "too_many_requests", message: null } }, outcome: "ambiguous", retry: false },
  { name: "invalid retry hint", status: 429, body: { error: { code: "too_many_requests", message: "busy", retryable: "true" } }, outcome: "ambiguous", retry: false },
  { name: "legacy 5xx rejection", status: 503, body: { code: "ingest_failure", message: "busy", details: { status: "rejected" } }, outcome: "ambiguous", retry: false },
  { name: "legacy missing outcome", status: 429, body: { code: "ingest_failure", message: "busy" }, outcome: "ambiguous", retry: false },
] as const;

for (const entry of cases) {
  it(`classifies ${entry.name} and applies safe stream replay policy`, async () => {
    let calls = 0;
    const client = new Client("https://example.com", { fetch: async () => {
      calls++;
      if (calls === 2) return Response.json({ num_rows_inserted: 1 });
      return Response.json({ ...entry.body, request_id: "request-body" }, {
        status: entry.status, headers: { "X-Request-ID": "request-header", "Retry-After": "0" },
      });
    } });
    const stream = client.table("events").appendStream({ failurePolicy: "continue" })
      .maxRetries(1).initialBackoff(0).build();
    await stream.send({ id: 1 });
    const report = await stream.shutdown();
    assert.equal(calls, entry.retry ? 2 : 1);
    assert.equal(report.outcome, entry.retry ? "succeeded" : entry.outcome);
    if (!entry.retry) {
      const failure = stream.stats().lastFailure!.error;
      assert.equal(failure.httpStatus, entry.status);
      // Malformed envelopes may only preserve the header request ID.
      if (entry.name !== "null message") assert.equal(failure.requestId, "request-body");
    }
  });
}

for (const [name, status, body, succeeded] of [
  ["modern success", 200, { num_rows_inserted: 1 }, true],
  ["legacy success", 200, { status: "committed", num_rows_inserted: 1 }, true],
  ["row count mismatch", 200, { num_rows_inserted: 2 }, false],
  ["wrong HTTP status", 201, { num_rows_inserted: 1 }, false],
  ["invalid legacy status", 200, { status: "unknown", num_rows_inserted: 1 }, false],
  ["null legacy status", 200, { status: null, num_rows_inserted: 1 }, false],
] as const) {
  it(`validates ${name}`, async () => {
    const client = new Client("https://example.com", { fetch: async () => Response.json(body, { status }) });
    const result = client.table("events").append('{"id":1}');
    if (succeeded) assert.deepEqual(await result, { status: "succeeded", num_rows_inserted: 1 });
    else await assert.rejects(result, error => error instanceof AppendRowsError && error.outcome === "ambiguous");
  });
}

for (const retryable of [false, true]) {
  it(`honors a server retry refusal for ambiguous replay (${retryable})`, async () => {
    let calls = 0;
    const client = new Client("https://example.com", { fetch: async () => {
      if (++calls === 2) return Response.json({ num_rows_inserted: 1 });
      return Response.json({ error: { code: "service_unavailable", message: "busy", retryable } }, { status: 503 });
    } });
    const stream = client.table("events").appendStream({ failurePolicy: "continue" })
      .retryAmbiguous(true).maxRetries(1).initialBackoff(0).build();
    await stream.send({ id: 1 });
    const report = await stream.shutdown();
    assert.equal(calls, retryable ? 2 : 1);
    assert.equal(report.outcome, retryable ? "succeeded" : "ambiguous");
  });
}
