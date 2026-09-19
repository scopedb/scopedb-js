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

/** The source durably owns these rows until checkpoint() succeeds. */
export interface AuditInterval {
  endCursor: string;
  rows: readonly Record<string, unknown>[];
}

/** One worker owns a source partition; failed checkpoints leave it replayable. */
export interface AuditOutbox {
  readUnconfirmed(): Promise<AuditInterval | null>;
  checkpoint(endCursor: string): Promise<void>;
}

/** Replaying after a crash or unknown outcome may duplicate committed events. */
export async function deliverAuditInterval(
  client: Client,
  tableName: string,
  outbox: AuditOutbox,
): Promise<number> {
  const interval = await outbox.readUnconfirmed();
  if (interval === null) return 0;
  const stream = client.table(tableName).appendStream().build();
  try {
    for (const row of interval.rows) await stream.send(row);
    await stream.shutdown();
    await outbox.checkpoint(interval.endCursor);
    return interval.rows.length;
  } catch (error) {
    // Settle outstanding requests before another worker replays the interval.
    await stream.shutdown().catch(() => {});
    throw error;
  }
}
