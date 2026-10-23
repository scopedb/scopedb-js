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

import type { Client, WriteOptions } from "./client.js";
import {
  type AppendFailurePolicy,
  AppendStreamBuilder,
  type AppendStreamOptions,
} from "./append-stream.js";
import type { AppendRowsResult } from "./protocol.js";
import type { CatalogResource, CatalogTable } from "./catalog.js";

export interface TableOptions {
  database?: string;
  schema?: string;
}

export class Table {
  private readonly databaseName: string;
  private readonly schemaName: string;

  constructor(
    private readonly client: Client,
    private readonly tableName: string,
    options: TableOptions = {},
  ) {
    this.databaseName = options.database ?? "scopedb";
    this.schemaName = options.schema ?? "public";
  }

  identifier(): string {
    return [this.databaseName, this.schemaName, this.tableName].map((name) =>
      quoteIdent(name, "`")
    ).join(".");
  }

  /** Appends at most 8 MiB of uncompressed newline-delimited JSON. */
  async append(
    ndjson: string,
    options: WriteOptions = {},
  ): Promise<AppendRowsResult> {
    return this.client.appendRows(
      this.databaseName,
      this.schemaName,
      this.tableName,
      ndjson,
      options,
    );
  }

  /** Builds an asynchronous, concurrent NDJSON append stream for this table. */
  appendStream(): AppendStreamBuilder<"stop">;
  appendStream(options: undefined): AppendStreamBuilder<"stop">;
  /** Builds an append stream with an explicit batch-failure policy. */
  appendStream<Policy extends AppendFailurePolicy>(
    options: AppendStreamOptions<Policy>,
  ): AppendStreamBuilder<Policy>;
  appendStream<Policy extends AppendFailurePolicy>(
    options: AppendStreamOptions<Policy> | undefined,
  ): AppendStreamBuilder<Policy | "stop">;
  appendStream(
    options?: AppendStreamOptions<AppendFailurePolicy>,
  ): AppendStreamBuilder<AppendFailurePolicy> {
    const failurePolicy = options === undefined
      ? "stop"
      : options?.failurePolicy;
    return AppendStreamBuilder.create(
      this.client,
      this.databaseName,
      this.schemaName,
      this.tableName,
      failurePolicy as AppendFailurePolicy,
    );
  }

  /** Selects the complete definition and ETag used for conditional mutations. */
  catalog(): CatalogResource<CatalogTable> {
    return this.client.catalog().table(
      this.databaseName,
      this.schemaName,
      this.tableName,
    );
  }
}

function quoteIdent(input: string, quote: string): string {
  return quoteScopeQL(input, quote);
}

function quoteScopeQL(input: string, quote: string): string {
  let out = quote;
  for (const ch of input) {
    switch (ch) {
      case "\t":
        out += "\\t";
        break;
      case "\n":
        out += "\\n";
        break;
      case "\r":
        out += "\\r";
        break;
      case "\\":
        out += "\\\\";
        break;
      default:
        if (ch === quote) {
          out += `\\${ch}`;
        } else if (ch < " ") {
          out += `\\x${ch.charCodeAt(0).toString(16).padStart(2, "0")}`;
        } else {
          out += ch;
        }
    }
  }
  out += quote;
  return out;
}
