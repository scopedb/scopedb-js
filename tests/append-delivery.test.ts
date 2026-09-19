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

import assert from 'node:assert/strict';
import {it} from 'node:test';
import {setTimeout as delay} from 'node:timers/promises';
import {Client} from '../src/client.js';
import {AppendRowsError} from '../src/errors.js';
import {AppendRetryExhaustedError} from '../src/append-stream.js';
const ok=()=>Response.json({append_state:'committed',num_rows_inserted:1});
const failure=(state='unknown',status=503,headers={})=>Response.json({message:'failure',append_state:state,row_errors:[],row_errors_truncated:false},{status,headers});
const table=(fetch:typeof globalThis.fetch)=>new Client('http://localhost:8080',{fetch}).table('events');

for(const kind of ['lost ACK','invalid ACK','unknown 503','unstructured 503','timeout']) {
  it(`retries ${kind} with the original payload`,async()=>{
    const bodies:unknown[]=[];
    const stream=table(async(_url,init)=>{
      bodies.push(init!.body);
      if(bodies.length>1)return ok();
      if(kind==='lost ACK')throw new TypeError('socket closed');
      if(kind==='invalid ACK')return Response.json({});
      if(kind==='unknown 503')return failure();
      if(kind==='unstructured 503')return new Response('unavailable',{status:503});
      return await new Promise<Response>((_resolve,reject)=>{
        const signal=init!.signal!;
        if(signal.aborted)reject(signal.reason);
        else signal.addEventListener('abort',()=>reject(signal.reason),{once:true});
      });
    }).appendStream().attemptTimeoutMs(100).initialBackoff(0).build();
    await stream.send({id:1});const result=await stream.shutdown();
    assert.equal(result?.num_rows_inserted,1);assert.equal(bodies.length,2);
    assert.deepEqual(bodies[0],bodies[1]);assert.equal(stream.stats().retries,1);
    assert.equal(stream.stats().unknownRows,0);assert.equal(stream.stats().pendingBytes,0);
  });
}
it('keeps unknown sticky after a permanent rejection and preserves diagnostics',async()=>{
  let calls=0;
  const stream=table(async()=>++calls===1?failure():failure('rejected',422,{'X-Request-Id':'last-request'}))
    .appendStream().initialBackoff(0).build();
  await stream.send({id:1});
  await assert.rejects(stream.shutdown(),(e:unknown)=>{
    assert.ok(e instanceof AppendRowsError);assert.equal(e.appendState,'unknown');
    assert.equal(e.httpStatus,422);assert.equal(e.requestId,'last-request');return true;
  });
  assert.equal(calls,2);assert.equal(stream.stats().unknownRows,1);
  assert.equal(stream.stats().lastFailure?.requestId,'last-request');
});
it('does not retry permanent unknown HTTP failures',async()=>{
  for(const status of [400,401,403,422]){
    let calls=0;const stream=table(async()=>{calls++;return failure('unknown',status);}).appendStream().build();
    await stream.send({id:1});await assert.rejects(stream.shutdown(),AppendRowsError);assert.equal(calls,1);
  }
});
it('honors Retry-After beyond maxBackoff and exposes retries during recovery',async()=>{
  let first=0,second=0;
  const stream=table(async()=>{if(!first){first=performance.now();return failure('rejected',429,{'Retry-After':'0.08'});}second=performance.now();return ok();})
    .appendStream().maxBackoff(1).build();
  await stream.send({id:1});await stream.shutdown();assert.ok(second-first>=80);
  assert.equal(stream.stats().retries,1);
});
it('bounds Retry-After by elapsed budget without retrying early',async()=>{
  let calls=0;const stream=table(async()=>{calls++;return failure('rejected',429,{'Retry-After':'10'});})
    .appendStream().maxElapsedTimeMs(80).build();
  await stream.send({id:1});await assert.rejects(stream.shutdown(),AppendRetryExhaustedError);
  assert.equal(calls,1);assert.equal(stream.stats().pendingBytes,0);
});
it('bounds a stalled request by the total elapsed budget',async()=>{
  let aborted=false;
  const stream=table(async(_url,init)=>await new Promise<Response>((_resolve,reject)=>{
    const signal=init!.signal!;
    const abort=()=>{aborted=true;reject(signal.reason);};
    if(signal.aborted)abort();else signal.addEventListener('abort',abort,{once:true});
  })).appendStream().attemptTimeoutMs(1000).maxElapsedTimeMs(80).build();
  await stream.send({id:1});await assert.rejects(stream.shutdown(),AppendRetryExhaustedError);
  assert.ok(aborted);assert.equal(stream.stats().unknownRows,1);
});
it('disables retries with maxRetries(0)',async()=>{
  let calls=0;const stream=table(async()=>{calls++;return failure();}).appendStream().maxRetries(0).build();
  await stream.send({id:1});await assert.rejects(stream.shutdown(),AppendRetryExhaustedError);assert.equal(calls,1);
});
it('honors a flush wait cancellation after failure while settling in-flight requests',async()=>{
  let calls=0,release!:(response:Response)=>void;
  const stream=table(async()=>++calls===1?await new Promise<Response>(r=>release=r):failure('rejected',422))
    .appendStream().maxBatchRows(1).build();
  await stream.send({id:1});while(!release)await delay(1);
  await stream.send({id:2});while(stream.stats().state!=='failed')await delay(1);
  const controller=new AbortController();const flushing=stream.flush({signal:controller.signal});controller.abort();
  await assert.rejects(flushing,e=>e===controller.signal.reason);
  assert.equal(stream.stats().inFlightBatches,1);
  release(ok());await assert.rejects(stream.shutdown(),AppendRowsError);
  assert.equal(stream.stats().committedRows,1);assert.equal(stream.stats().pendingBytes,0);
});
it('replays a caller-owned source interval after a partial commit',async()=>{
  let repaired=false;const copies=[0,0];let calls=0;
  const t=table(async()=>{const id=(calls++%2);if(id===1&&!repaired)return failure('rejected',422);copies[id]!++;return ok();});
  const source=[{id:1},{id:2}];
  for(const repair of [false,true]){
    repaired=repair;const stream=t.appendStream().maxConcurrentBatches(1).maxBatchRows(1).build();
    for(const row of source)await stream.send(row);
    if(!repair)await assert.rejects(stream.shutdown());else await stream.shutdown();
  }
  assert.deepEqual(copies,[2,1]);
});
