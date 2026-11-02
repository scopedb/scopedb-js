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
import template from "./worker.js";

export default {
  /**
   * @param {Request} request
   * @param {Parameters<typeof template.fetch>[1]} env
   */
  async fetch(request, env) {
    if (new URL(request.url).pathname !== "/stream") {
      return template.fetch(request, env);
    }

    const client = new Client(env.SCOPEDB_ENDPOINT, { apiKey: env.SCOPEDB_API_KEY });
    const stream = client.table(env.SCOPEDB_TABLE, {
      database: env.SCOPEDB_DATABASE,
      schema: env.SCOPEDB_SCHEMA,
    }).appendStream().strictEncoding(true).build();

    await stream.sendAll([
      { id: 9007199254740993n, name: "stream-first" },
      { id: 9223372036854775807n, name: "stream-last" },
    ]);
    const result = await stream.shutdown();
    return Response.json({ insertedRows: result?.num_rows_inserted });
  },
};
