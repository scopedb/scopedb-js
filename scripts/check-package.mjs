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

const sdk = await import("scopedb");
const expected = [
  [sdk, "Client"],
  [sdk, "AppendStream"],
  [sdk, "AppendRowsError"],
  [sdk.Client.prototype, "iterateDatabases"],
  [sdk.ResultSet.prototype, "rawRows"],
  [sdk.ResultSet.prototype, "toValues"],
  [sdk.ResultSet.prototype, "toObjects"],
  [sdk.StatementHandle.prototype, "refresh"],
  [sdk.StatementHandle.prototype, "wait"],
  [sdk.Table.prototype, "describe"],
  [sdk.AppendStreamBuilder.prototype, "targetBatchBytes"],
  [sdk.AppendStreamBuilder.prototype, "maxBatchRows"],
  [sdk.AppendStreamBuilder.prototype, "flushIntervalMs"],
  [sdk.AppendStreamBuilder.prototype, "maxConcurrentBatches"],
  [sdk.AppendStreamBuilder.prototype, "maxBufferedBytes"],
  [sdk.AppendStreamBuilder.prototype, "attemptTimeoutMs"],
];

for (const [owner, name] of expected) {
  if (typeof owner?.[name] !== "function") {
    throw new Error(`public package entry is missing ${name}`);
  }
}
