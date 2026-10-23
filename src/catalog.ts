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

import type { RequestOptions } from "./client.js";
import type { DataType } from "./protocol.js";
import { isDataType } from "./protocol.js";

export interface CatalogOptions extends RequestOptions {
  /** Total deadline for a catalog mutation, including its response body. */
  timeoutMs?: number | null;
  dryRun?: boolean;
  allowDestructive?: boolean;
  ifMatch?: string;
}

export interface CatalogDocument {
  [extension: string]: unknown;
  databases?: CatalogDatabase[];
}
export interface CatalogDatabase {
  [extension: string]: unknown;
  name: string;
  comment?: string | null;
  schemas?: CatalogSchema[];
}
export interface CatalogSchema {
  [extension: string]: unknown;
  name: string;
  comment?: string | null;
  tables?: CatalogTable[];
  views?: CatalogView[];
}
export interface CatalogColumn {
  [extension: string]: unknown;
  name: string;
  data_type: DataType;
  expression?: string | null;
  comment?: string | null;
  indexes?: Array<{ type: "search"; analyzer?: "log" | null }>;
}
export interface CatalogTable {
  [extension: string]: unknown;
  name: string;
  comment?: string | null;
  columns?: CatalogColumn[];
  partition_by?: string[];
  cluster_by?: string[];
  distinct?: { on?: string[]; by?: string[] };
  retention_days?: number | null;
  storage?: Record<string, unknown> | null;
}
export interface CatalogView {
  [extension: string]: unknown;
  name: string;
  statement: string;
  comment?: string | null;
}
export interface CatalogSnapshot<T> {
  definition: T;
  etag: string;
}
export interface CatalogResult<T> {
  before: T | null;
  after: T | null;
  destructive: boolean;
  etag: string;
}

type Transport = (
  method: string,
  path: string[],
  definition: unknown,
  options: CatalogOptions,
) => Promise<{ body: Record<string, unknown>; etag: string }>;

/** A complete declarative resource. Writes are single attempts and never automatically retried. */
export class CatalogResource<T> {
  constructor(
    private readonly transport: Transport,
    private readonly path: string[],
  ) {}

  async get(options: RequestOptions = {}): Promise<CatalogSnapshot<T>> {
    const result = await this.transport("GET", this.path, undefined, options);
    return { definition: result.body as T, etag: result.etag };
  }

  /** Replaces the entire resource. Omitted fields and children are cleared; use dryRun to preview. */
  async apply(
    definition: T,
    options: CatalogOptions = {},
  ): Promise<CatalogResult<T>> {
    const result = await this.transport("PUT", this.path, definition, options);
    return { ...result.body, etag: result.etag } as unknown as CatalogResult<T>;
  }

  /** Removes a resource; potentially destructive changes require allowDestructive. */
  async delete(options: CatalogOptions = {}): Promise<CatalogResult<T>> {
    const result = await this.transport(
      "DELETE",
      this.path,
      undefined,
      options,
    );
    return { ...result.body, etag: result.etag } as unknown as CatalogResult<T>;
  }
}

/** Selects resources by unescaped names; reads return complete definitions without pagination. */
export class Catalog {
  private readonly root: CatalogResource<CatalogDocument>;
  constructor(private readonly transport: Transport) {
    this.root = new CatalogResource(transport, []);
  }
  get(options: RequestOptions = {}): Promise<CatalogSnapshot<CatalogDocument>> {
    return this.root.get(options);
  }
  apply(
    definition: CatalogDocument,
    options: CatalogOptions = {},
  ): Promise<CatalogResult<CatalogDocument>> {
    return this.root.apply(definition, options);
  }
  database(database: string): CatalogResource<CatalogDatabase> {
    return new CatalogResource(this.transport, ["databases", database]);
  }
  schema(database: string, schema: string): CatalogResource<CatalogSchema> {
    return new CatalogResource(this.transport, [
      "databases",
      database,
      "schemas",
      schema,
    ]);
  }
  table(
    database: string,
    schema: string,
    table: string,
  ): CatalogResource<CatalogTable> {
    return new CatalogResource(this.transport, [
      "databases",
      database,
      "schemas",
      schema,
      "tables",
      table,
    ]);
  }
  view(
    database: string,
    schema: string,
    view: string,
  ): CatalogResource<CatalogView> {
    return new CatalogResource(this.transport, [
      "databases",
      database,
      "schemas",
      schema,
      "views",
      view,
    ]);
  }
}

// Validate modeled fields without rebuilding objects: extensions survive a
// read/modify/write cycle at every level, including future index/storage fields.
export function validateCatalogResponse(
  value: unknown,
  path: string[],
  method: string,
): void {
  const kind = path.length === 0 ? "catalog" : path[path.length - 2]!;
  const name = path.at(-1);
  if (method === "GET") {
    definition(value, kind, name);
    return;
  }
  const result = record(value, "catalog mutation");
  if (typeof result["destructive"] !== "boolean") {
    throw new Error("invalid catalog destructive flag");
  }
  for (const field of ["before", "after"]) {
    if (result[field] !== null) definition(result[field], kind, name);
  }
  if (method === "DELETE" && result["after"] !== null) {
    throw new Error("catalog deletion returned an existing resource");
  }
}

function record(value: unknown, label: string): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error(`invalid ${label}`);
  }
  return value as Record<string, unknown>;
}

function definition(value: unknown, kind: string, name?: string): void {
  const object = record(value, kind);
  if (
    kind !== "catalog" &&
    (typeof object["name"] !== "string" ||
      (name !== undefined && object["name"] !== name))
  ) {
    throw new Error(`invalid ${kind} name`);
  }
  const optional = (field: string, check: (value: unknown) => boolean) => {
    if (object[field] !== undefined && !check(object[field])) {
      throw new Error(`invalid ${kind}.${field}`);
    }
  };
  const children = (field: string) =>
    optional(field, (value) => {
      if (!Array.isArray(value)) return false;
      for (const child of value) definition(child, field);
      return true;
    });
  const string = (value: unknown) => typeof value === "string";
  const strings = (value: unknown) =>
    Array.isArray(value) && value.every(string);
  optional("comment", (value) => value === null || string(value));
  switch (kind) {
    case "catalog":
      if (!Array.isArray(object["databases"])) {
        throw new Error("invalid catalog.databases");
      }
      children("databases");
      break;
    case "databases":
      children("schemas");
      break;
    case "schemas":
      children("tables");
      children("views");
      break;
    case "tables":
      children("columns");
      optional("partition_by", strings);
      optional("cluster_by", strings);
      optional(
        "retention_days",
        (value) =>
          value === null ||
          (Number.isSafeInteger(value) && (value as number) >= 0),
      );
      optional("storage", (value) => {
        if (value !== null) record(value, "storage");
        return true;
      });
      optional("distinct", (value) => {
        const distinct = record(value, "distinct");
        return ["on", "by"].every((key) =>
          distinct[key] === undefined || strings(distinct[key])
        );
      });
      break;
    case "columns":
      if (!isDataType(object["data_type"])) {
        throw new Error("invalid column data_type");
      }
      optional("expression", (value) => value === null || string(value));
      optional(
        "indexes",
        (value) =>
          Array.isArray(value) && value.every((index) => {
            const fields = record(index, "index");
            return fields["type"] === "search" &&
              (fields["analyzer"] == null || fields["analyzer"] === "log");
          }),
      );
      break;
    case "views":
      if (!string(object["statement"])) {
        throw new Error("invalid view statement");
      }
      break;
    default:
      throw new Error(`unknown catalog resource ${kind}`);
  }
}
