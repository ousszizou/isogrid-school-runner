ISOGrid school load runner -- synthetic data only

Build the ZIP root with Dockerfile. Deploy as a PRIVATE background worker,
one micro replica (0.5 vCPU / 512 MB, confirmed 0 DZD) on the same private
network as bench-db. Supply DATABASE_URL using the existing sealed secret;
it must target bench_target. No credentials belong in this archive.

Defaults:
  RUN_ID=school-20261006-v1
  CLIENT_STAGES=25,50,100
  DURATION_SECONDS=300
  ENABLE_API_SMOKE=1

Before the SQL stages the same worker contacts the existing private backend
at http://odjaidri-s-organization-backend:8080, checks /healthz and /api/info,
POSTs /api/runs with scale=1000, concurrency=1 then 4, durationSeconds=5,
and polls GET /api/runs/{id}. These original benchmark API results are stored
separately from school SQL measurements. No API run is retried automatically.

The runner checks fixture counts, ANALYZEs key tables, preflights every SQL
script once, then tests the stages. The closed-loop, unthrottled mix is:
80% guardian dashboard, 10% payment transaction, 5% class attendance batch,
5% current-year financial report. Every script includes BEGIN and COMMIT.
Payment tests add fictional 0.01-unit payments to partially paid invoices;
attendance tests upsert a fictional current-day class roll call. Original
historical attendance and seed payments remain present.

Results are persisted in school_demo.load_runs and emitted with the
SCHOOL_STAGE_DONE prefix. Full pgbench transaction logs produce exact
P50/P95/P99/max by workload and overall. No sampling is enabled.
Includes private-network client/server latency and commit acknowledgement.
Does not measure the school browser app, HTTP auth or Algeria WAN latency.
100 DB clients can exceed the instance's max_connections=100 after reserved
slots and existing applications; any failure is recorded, not called success.
A micro runner itself can limit throughput; its cgroup CPU counters are saved.

Existing phase markers prevent automatic reruns. A running marker after a
crash stops all further load and requires operator review. After completion
the worker idles; stop it after collecting results. Do not delete the DB.

Reference for pgbench command and transaction log format:
https://www.postgresql.org/docs/16/pgbench.html
