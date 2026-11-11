// k6 load test for Relay: a constant arrival rate of events on POST /v1/events, each delivered as a signed
// webhook to the sink. Run it with loadtest/run.sh, which pins Relay to 2 CPUs and collects the results.
import http from 'k6/http';
import { check } from 'k6';
import exec from 'k6/execution';
import { Counter } from 'k6/metrics';

const BASE = __ENV.RELAY_URL || 'http://api:8080';
const SINK = __ENV.SINK_URL || 'http://sink:8090';
const RATE = parseInt(__ENV.RATE || '600', 10);
const DURATION = __ENV.DURATION || '2m';
const accepted = new Counter('events_accepted');

export const options = {
  scenarios: {
    ingest: {
      executor: 'constant-arrival-rate',
      rate: RATE,
      timeUnit: '1s',
      duration: DURATION,
      preAllocatedVUs: 60,
      maxVUs: 400,
    },
  },
  thresholds: {
    'http_req_duration{name:ingest}': ['p(99)<50'],
    'http_req_failed{name:ingest}': ['rate<0.001'],
    'checks{check:accepted}': ['rate>0.999'],
    dropped_iterations: ['count<10'],
  },
  summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
};

const json = (key) => ({ headers: { Authorization: `Bearer ${key}`, 'Content-Type': 'application/json' } });

export function setup() {
  const admin = json(__ENV.ADMIN_KEY);
  const slug = `load-${Date.now()}`;
  const limits = { ingest_rate: 5000, ingest_burst: 5000, delivery_rate: 5000, max_in_flight: 256 };
  let r = http.post(`${BASE}/v1/tenants`, JSON.stringify({ slug, name: 'Load test', limits }), admin);
  if (r.status !== 201) throw new Error(`tenant: ${r.status} ${r.body}`);
  r = http.post(`${BASE}/v1/tenants/${slug}/keys`, JSON.stringify({ name: 'k6' }), admin);
  const key = r.json('key');
  r = http.post(`${BASE}/v1/tenants/${slug}/endpoints`, JSON.stringify({
    channel: 'webhook', url: `${SINK}/hook`, max_in_flight: 64, event_types: ['job.*'],
  }), json(key));
  if (r.status !== 201) throw new Error(`endpoint: ${r.status} ${r.body}`);
  http.put(`${SINK}/config`, JSON.stringify({ secret: r.json('secret') }));
  http.post(`${SINK}/reset`);
  return { key };
}

// About 700 bytes, shaped like a FieldOps job event.
const payload = {
  job: { id: 'J-104233', title: 'Quarterly AC maintenance', status: 'completed', priority: 'normal' },
  customer: { id: 'cus_01jd8x', name: 'Northwind Dental Clinic', site: '12 Oak Street, Springfield' },
  technician: { id: 'usr_01jd8y', name: 'Tom Rivera' },
  completed_at: '2026-11-11T09:30:00Z',
  checklist: { done: 12, total: 12 },
  parts: [{ sku: 'FLT-2020', name: 'Air filter 20x20', qty: 2 }, { sku: 'CAP-45', name: 'Run capacitor 45 µF', qty: 1 }],
  notes: 'Replaced both filters and the run capacitor; system cooling within spec. Customer signed on site.',
};

export default function (data) {
  const body = JSON.stringify({ type: 'job.completed', payload });
  const params = json(data.key);
  params.headers['Idempotency-Key'] = `k6-${exec.vu.idInTest}-${exec.vu.iterationInInstance}`;
  params.tags = { name: 'ingest' };
  const r = http.post(`${BASE}/v1/events`, body, params);
  if (check(r, { accepted: (x) => x.status === 202 })) accepted.add(1);
}
