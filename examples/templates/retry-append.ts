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

import { setTimeout as delay } from "node:timers/promises";
import { AppendRowsError, type Table } from "scopedb";

// Pass a signal with an overall deadline, such as AbortSignal.timeout(30_000).
// Preserve the same NDJSON bytes between attempts. Reconcile unknown outcomes
// outside this loop; a later rejection cannot resolve an earlier unknown write.
export async function appendWithRetry(
  table: Table,
  ndjson: string,
  signal: AbortSignal,
) {
  for (let retries = 0;; retries++) {
    signal.throwIfAborted();
    try {
      return await table.append(ndjson, { signal });
    } catch (error) {
      if (
        !(error instanceof AppendRowsError) || error.outcome !== "rejected" ||
        !error.isTemporary() || error.serverRetryable === false ||
        error.writeDetails !== undefined || retries >= 2
      ) {
        throw error;
      }
      const backoffMs = 200 * 2 ** retries * (0.5 + Math.random() / 2);
      const waitMs = Math.max(backoffMs, error.retryAfterMs ?? 0);
      // Give long server delays back to the caller; never shorten Retry-After.
      if (waitMs > 30_000) throw error;
      await delay(waitMs, undefined, { signal });
    }
  }
}
