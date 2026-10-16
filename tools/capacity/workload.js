import http from 'k6/http';
import { sleep } from 'k6';
import { SharedArray } from 'k6/data';
import execution from 'k6/execution';
import { Counter, Rate, Trend } from 'k6/metrics';

const actors = new SharedArray('synthetic participants', () => {
  const raw = JSON.parse(open('/capacity/actors.json'));
  const groups = Object.values(raw.reduce((groups, actor) => {
    (groups[actor.contest_id] ||= []).push(actor);
    return groups;
  }, {}));
  return groups[0].flatMap((_, index) => groups.map(group => group[index]));
});
const users = Number(__ENV.USERS || 1000);
const duration = __ENV.DURATION || '30s';
const rate = Number(__ENV.RATE || 0);
const pace = Number(__ENV.PACE || 0);
const profile = __ENV.PROFILE || 'mixed';
const base = __ENV.BASE_URL || 'http://127.0.0.1:18081';
const paddingBytes = Number(__ENV.SOURCE_PADDING_BYTES || 0);
if (!Number.isInteger(paddingBytes) || paddingBytes < 0 || paddingBytes > 500000) {
  throw new Error('SOURCE_PADDING_BYTES must be an integer from 0 to 500000');
}
const padding = __ENV.SOURCE_PADDING_FILE ? open(__ENV.SOURCE_PADDING_FILE) : ' '.repeat(paddingBytes);
if (padding.length !== paddingBytes) throw new Error('Source padding file size does not match SOURCE_PADDING_BYTES');
const sourcePadding = paddingBytes ? '\n/*' + padding + '*/\n' : '';
const etags = {};
let reportedUnexpected = false;
export const options = {
  discardResponseBodies: true,
  noConnectionReuse: false,
  summaryTrendStats: ['avg', 'med', 'p(95)', 'p(99)', 'max'],
  scenarios: rate ? { load: { executor: 'constant-arrival-rate', rate, timeUnit:'1s',
    duration, preAllocatedVUs:users, maxVUs:users, gracefulStop:'10s' } }
    : { load: { executor:'constant-vus', vus:users, duration, gracefulStop:'10s' } },
};
const counts = {};
const good = {};
const server = {};
for (const kind of ['scoreboard','submissions','problems','submit','status']) {
  for (const status of ['200','304','429','503','error']) counts[kind+status] = new Counter('count_'+kind+'_'+status);
  good[kind] = new Trend('success_'+kind, true);
  server[kind] = new Trend('server_'+kind, true);
}
const rejected = new Rate('rejected');
const unexpected = new Rate('unexpected_error');

export default function () {
  const turn = rate ? execution.scenario.iterationInTest : __ITER;
  const actor = actors[(rate ? turn : __VU-1)%Math.min(users,actors.length)];
  if (__ITER === 0 && !rate) sleep((__VU%100)/100*Number(__ENV.RAMP || 5));
  let kind = profile;
  if (profile === 'mixed' || profile === 'interactive') {
    const reads = profile === 'interactive'
      ? ['scoreboard','submissions','problems','submissions','scoreboard']
      : ['scoreboard','submissions','scoreboard','problems','status','submissions'];
    kind = __ENV.WRITES === 'true' && (rate ? turn%20 === 19 : __ITER%20 === __VU%20) ? 'submit'
      : reads[turn%reads.length];
  }
  const cid = actor.contest_id;
  const paths = {
    scoreboard:`/api/contests/${cid}/divisions/${actor.division_id}/scoreboard`,
    submissions:`/api/contests/${cid}/submissions?limit=20`,
    problems:`/api/contests/${cid}/problems`,
    status:`/api/contests/${cid}/submissions/${actor.submission_id}/status:wait?wait_seconds=2&poll_interval_seconds=0.5`,
    submit:`/api/contests/${cid}/problems/${actor.problem_id}/submissions`,
  };
  const headers = { Authorization:'Bearer '+actor.token, 'Content-Type':'application/json' };
  if (__ENV.CONDITIONAL !== 'false' && etags[cid] && kind === 'scoreboard') headers['If-None-Match'] = etags[cid];
  const params = { headers, timeout:'8s', tags:{name:kind} };
  if (kind === 'status') params.responseType = 'text';
  const response = kind === 'submit'
    ? http.post(base+paths[kind], JSON.stringify({language:'cpp17',
      source_code:'// capacity '+(__ENV.OUTPUT || 'unknown').split('/').pop()+' '+__VU+' '+turn+'\n#include <cstdio>\nint main(){puts("3");}'+sourcePadding}), params)
    : http.get(base+paths[kind], params);
  const status = String(response.status);
  (counts[kind+status] || counts[kind+'error']).add(1);
  const accepted = response.status === 200 || response.status === 304;
  if (accepted) good[kind].add(response.timings.duration+response.timings.blocked);
  rejected.add(response.status === 429 || response.status === 503);
  unexpected.add(!accepted && response.status !== 429 && response.status !== 503);
  if (!accepted && response.status !== 429 && response.status !== 503 && !reportedUnexpected) {
    reportedUnexpected = true;
    console.error(JSON.stringify({kind, status:response.status, error_code:response.error_code,
      body:typeof response.body === 'string' ? response.body.slice(0,200) : null}));
  }
  const timing = response.headers['Server-Timing'];
  if (timing && timing.startsWith('app;dur=')) server[kind].add(Number(timing.split('=')[1]));
  if (kind === 'scoreboard' && response.headers.Etag) etags[cid] = response.headers.Etag;
  if (kind === 'scoreboard' && response.headers.ETag) etags[cid] = response.headers.ETag;
  if (!rate && pace) sleep(pace*(0.85+((__VU+__ITER)%30)/100));
}

export function handleSummary(data) {
  return { [__ENV.OUTPUT || '/capacity/k6-summary.json']: JSON.stringify(data,null,2) };
}
