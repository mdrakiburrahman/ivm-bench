const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const {approved, check} = require('./gci-review-gate.cjs');
const sha = 'a'.repeat(40);
const pr = {state:'open', draft:false, user:{login:'author'}, head:{sha,repo:{full_name:'o/r'}}, base:{repo:{full_name:'o/r'}}};
const review = {id:1,state:'APPROVED',commit_id:sha,user:{login:'reviewer'}};
const trusted = new Set(['reviewer']);
test('only exact independently approved same-repository head passes', () => {
  assert.equal(approved(pr,[review],sha,trusted),true);
  for (const changed of [{...pr,draft:true},{...pr,state:'closed'},
    {...pr,head:{...pr.head,sha:'b'.repeat(40)}}, {...pr,head:{sha,repo:{full_name:'fork/r'}}}]) {
    assert.equal(approved(changed,[review],sha,trusted),false);
  }
  assert.equal(approved(pr,[{...review,commit_id:'b'.repeat(40)}],sha,trusted),false);
  assert.equal(approved(pr,[review],sha,new Set()),false);
  assert.equal(approved(pr,[{...review,user:{login:'author'}}],sha,new Set(['author'])),false);
});
test('change requests block until superseded; comments do not erase decisions', () => {
  const changes = {...review,id:2,state:'CHANGES_REQUESTED'};
  assert.equal(approved(pr,[review,changes],sha,trusted),false);
  assert.equal(approved(pr,[review,changes,{...review,id:3,state:'COMMENTED'}],sha,trusted),false);
  assert.equal(approved(pr,[review,changes,{...review,id:4}],sha,trusted),true);
  assert.equal(approved(pr,[review,{...review,id:2,state:'DISMISSED'}],sha,trusted),false);
  assert.equal(approved(pr,[review,{...changes,user:{login:'other'}}],sha,trusted),false);
});
test('manual dispatch cannot bypass SHA manifest', async () => {
  const context={eventName:'workflow_dispatch',payload:{inputs:{reviewed_pr:'3',reviewed_sha:sha}},sha:'b'.repeat(40)};
  assert.equal((await check({github:{},context})).allowed,false);
  context.payload.inputs.reviewed_pr='not a number';
  assert.equal((await check({github:{},context})).allowed,false);
});
test('API gate paginates reviews and checks reviewer and manual actor permissions', async () => {
  let permission='write';
  const checked=[];
  const listReviews=()=>{};
  const github={rest:{pulls:{get:async()=>({data:pr}),listReviews},
    repos:{getCollaboratorPermissionLevel:async({username})=>{
      checked.push(username); return {data:{permission}};
    }}},paginate:async(method,args)=>{
      assert.equal(method,listReviews); assert.equal(args.per_page,100); return [review];
    }};
  const context={eventName:'workflow_dispatch',payload:{inputs:{reviewed_pr:'3',reviewed_sha:sha}},
    sha,actor:'dispatcher',repo:{owner:'o',repo:'r'}};
  assert.deepEqual(await check({github,context}),{allowed:true,sha,reason:`Reviewed PR #3 at ${sha}`});
  assert.deepEqual(checked,['dispatcher','reviewer']);
  permission='read';
  assert.equal((await check({github,context})).allowed,false);
  context.eventName='pull_request_review';
  context.payload={pull_request:{number:3,head:{sha}}};
  assert.equal((await check({github,context})).allowed,false);
});
test('API failures cannot return admission', async () => {
  const github={rest:{pulls:{get:async()=>{throw new Error('unavailable');}}}};
  const context={eventName:'pull_request',payload:{pull_request:{number:3,head:{sha}}},repo:{owner:'o',repo:'r'}};
  await assert.rejects(check({github,context}),/unavailable/);
});
test('workflow never loads policy from candidate checkout', () => {
  const workflow = fs.readFileSync(path.join(__dirname,'../workflows/gci.yaml'),'utf8');
  assert.equal((workflow.match(/require\('\.\/.gci-trusted\/\.github\/scripts\/gci-review-gate.cjs'\)/g)||[]).length,2);
  assert.equal((workflow.match(/head.repo.full_name == github.repository/g)||[]).length,2);
  const benchmark = workflow.slice(workflow.indexOf('\n  benchmark:'));
  assert.ok(benchmark.indexOf('ref: ${{ needs.review-gate.outputs.policy_sha }}') < benchmark.indexOf('gate.check'));
  assert.ok(benchmark.indexOf('gate.check') < benchmark.indexOf('ref: ${{ needs.review-gate.outputs.sha }}'));
});
