"""Private, finite SQL load runner. Credentials are supplied only at deployment.

Each pgbench script performs a committed transaction. Measured client latency
includes private-network round trips and COMMIT, but no browser/HTTP school app.
Default phases are 25/50/100 persistent DB clients for 300 seconds each. The
100-client phase may fail at the server's current 100-connection ceiling.
Persisted phase markers prevent automatic container restarts from rerunning
an existing phase. A running marker after a crash requires operator review.
"""
import glob
import json
import math
import os
from pathlib import Path
import re
import subprocess
import time
from datetime import datetime, timezone
from urllib.parse import urlparse, parse_qs, unquote
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parent
RUN_ID = os.environ.get("RUN_ID", "school-20261006-v1")
DURATION = int(os.environ.get("DURATION_SECONDS", "300"))
STAGES = [int(n) for n in os.environ.get("CLIENT_STAGES", "25,50,100").split(",")]
WORKLOADS = [("guardian", 80), ("payment", 10), ("attendance", 5), ("finance", 5)]
API_BASE = os.environ.get("API_BASE_URL", "http://odjaidri-s-organization-backend:8080").rstrip("/")
SECRETS = []


def utcnow():
    return datetime.now(timezone.utc).isoformat()


def redact(value):
    result = str(value)
    for secret in sorted(set(SECRETS), key=len, reverse=True):
        if secret:
            result = result.replace(secret, "[redacted]")
    return result


def emit(label, payload):
    print(label + " " + redact(json.dumps(payload, ensure_ascii=True)), flush=True)


def configure_database():
    uri = os.environ.get("DATABASE_URL")
    if not uri and os.environ.get("DATABASE_URL_FILE"):
        uri = Path(os.environ["DATABASE_URL_FILE"]).read_text().strip()
    if not uri:
        raise RuntimeError("Missing sealed DATABASE_URL")
    SECRETS.append(uri)
    parsed = urlparse(uri)
    if parsed.scheme not in {"postgres", "postgresql"}:
        raise RuntimeError("Invalid database URL scheme")
    password = unquote(parsed.password or "")
    SECRETS.append(password)
    dbname = unquote(parsed.path.lstrip("/"))
    if dbname != "bench_target":
        raise RuntimeError("Refusing to run outside bench_target")
    os.environ.update({"PGHOST": parsed.hostname or "", "PGPORT": str(parsed.port or 5432),
                       "PGDATABASE": dbname, "PGUSER": unquote(parsed.username or ""),
                       "PGPASSWORD": password, "PGCONNECT_TIMEOUT": "15",
                       "PGAPPNAME": "school-load-runner"})
    options = parse_qs(parsed.query)
    for key, env in [("sslmode", "PGSSLMODE"), ("sslrootcert", "PGSSLROOTCERT")]:
        if options.get(key):
            os.environ[env] = options[key][0]


def psql(sql, variables=None, timeout=60):
    args = ["psql", "--no-psqlrc", "--no-align", "--tuples-only", "--quiet", "-v", "ON_ERROR_STOP=1"]
    for key, value in (variables or {}).items():
        args += ["-v", key + "=" + str(value)]
    result = subprocess.run(args, input=sql, text=True, capture_output=True, timeout=timeout)
    if result.returncode:
        raise RuntimeError(redact(result.stderr[-1500:]))
    return result.stdout.strip()


def persist(phase, payload):
    psql("""INSERT INTO school_demo.load_runs(run_id,phase,recorded_at,result)
    VALUES(:'run_id',:'phase',clock_timestamp(),:'payload'::jsonb)
    ON CONFLICT(run_id,phase) DO UPDATE SET recorded_at=excluded.recorded_at,result=excluded.result;""",
         {"run_id": RUN_ID, "phase": phase, "payload": redact(json.dumps(payload))})


def existing(phase):
    text = psql("SELECT result::text FROM school_demo.load_runs WHERE run_id=:'run_id' AND phase=:'phase';",
                {"run_id": RUN_ID, "phase": phase})
    return json.loads(text) if text else None


def percentile(sorted_values, percent):
    if not sorted_values:
        return None
    pos = (len(sorted_values) - 1) * percent / 100
    lo, hi = math.floor(pos), math.ceil(pos)
    value = sorted_values[lo] + (sorted_values[hi] - sorted_values[lo]) * (pos - lo)
    return round(value / 1000, 3)


def describe(values, failures=0):
    values = sorted(values)
    return {"transactions": len(values), "logged_failures": failures,
            "p50_ms": percentile(values, 50), "p95_ms": percentile(values, 95),
            "p99_ms": percentile(values, 99), "max_ms": round(values[-1] / 1000, 3) if values else None}


def parse_logs(paths):
    timings = [[] for _ in WORKLOADS]
    failures = [0 for _ in WORKLOADS]
    malformed = 0
    for path in paths:
        with open(path) as stream:
            for line in stream:
                fields = line.split()
                if len(fields) < 6:
                    malformed += 1
                    continue
                try:
                    script = int(fields[3])
                    if not 0 <= script < len(WORKLOADS):
                        raise ValueError("invalid script number")
                    if fields[2] in {"failed", "serialization", "deadlock", "skipped"}:
                        failures[script] += 1
                    else:
                        latency = float(fields[2])
                        if latency < 0 or not math.isfinite(latency):
                            raise ValueError("invalid latency")
                        timings[script].append(latency)
                except (ValueError, OverflowError):
                    malformed += 1
    all_values = [x for group in timings for x in group]
    return {"overall": describe(all_values, sum(failures)),
            "workloads": {WORKLOADS[i][0]: describe(v, failures[i]) for i, v in enumerate(timings)},
            "malformed_log_lines": malformed}


def cgroup_cpu():
    try:
        return dict((k, int(v)) for k, v in (line.split() for line in Path("/sys/fs/cgroup/cpu.stat").read_text().splitlines()))
    except (OSError, ValueError):
        return {}


def db_snapshot():
    return json.loads(psql("""SELECT json_build_object(
      'max_connections',current_setting('max_connections')::int,
      'synchronous_commit',current_setting('synchronous_commit'),
      'database_size_bytes',pg_database_size(current_database()),
      'numbackends',numbackends,'xact_commit',xact_commit,'xact_rollback',xact_rollback,
      'deadlocks',deadlocks,'blks_read',blks_read,'blks_hit',blks_hit)
      FROM pg_stat_database WHERE datname=current_database();"""))


def prepare():
    counts = json.loads(psql("""SELECT json_build_object(
      'students',(SELECT count(*) FROM school_demo.students),
      'staff',(SELECT count(*) FROM school_demo.staff),
      'guardians',(SELECT count(*) FROM school_demo.guardians),
      'historical_attendance',(SELECT count(*) FROM school_demo.attendance WHERE attended_on<date '2026-10-06'),
      'invoices',(SELECT count(*) FROM school_demo.invoices),
      'seed_payments',(SELECT count(*) FROM school_demo.payments WHERE idempotency_key LIKE 'synthetic-seed-%'));"""))
    if counts != {"students": 1000, "staff": 30, "guardians": 1500,
                  "historical_attendance": 540000, "invoices": 27000, "seed_payments": 27000}:
        raise RuntimeError("School seed counts do not match fixture; refusing load")
    psql("""CREATE TABLE IF NOT EXISTS school_demo.load_runs(
       run_id text NOT NULL,phase text NOT NULL,recorded_at timestamptz NOT NULL,result jsonb NOT NULL,
       PRIMARY KEY(run_id,phase));""")
    emit("SCHOOL_FIXTURE_READY", counts)
    if not existing("preflight"):
        for name in ["guardian_students", "enrollments", "attendance", "invoices", "payments"]:
            psql("ANALYZE school_demo." + name + ";", timeout=120)
        for name, _ in WORKLOADS:
            check = subprocess.run(["pgbench", "-n", "-c", "1", "-j", "1", "-t", "1", "-f", str(ROOT / (name + ".sql"))],
                                   text=True, capture_output=True, timeout=60)
            if check.returncode:
                raise RuntimeError("Preflight " + name + " failed: " + redact(check.stderr[-1000:]))
        result = {"status": "complete", "counts": counts, "completed_at": utcnow()}
        persist("preflight", result)
        emit("SCHOOL_PREFLIGHT_DONE", result)


def safe_api_result(value):
    if isinstance(value, dict):
        return {k: ("[redacted]" if any(s in k.lower() for s in
                ["password", "secret", "token", "dsn", "databaseurl", "database_url", "connectionstring"])
                else safe_api_result(v)) for k, v in value.items()}
    if isinstance(value, list):
        return [safe_api_result(v) for v in value]
    return redact(value) if isinstance(value, str) else value


def http_json(path, payload=None):
    # The only destination is the existing private benchmark backend.
    if API_BASE != "http://odjaidri-s-organization-backend:8080":
        raise RuntimeError("Refusing unexpected API destination")
    request = Request(API_BASE + path, data=json.dumps(payload).encode() if payload is not None else None,
                      headers={"Content-Type": "application/json"})
    with urlopen(request, timeout=15) as response:
        return json.load(response)


def api_smoke(concurrency):
    phase = "api-" + str(concurrency)
    old = existing(phase)
    if old:
        emit("SCHOOL_API_EXISTS_NO_RERUN", {"phase": phase, "previous_status": old.get("status")})
        if old.get("status") in {"running", "unresolved"}:
            raise RuntimeError("Unresolved prior API phase needs operator review")
        return
    started = utcnow()
    body = {"scale": 1000, "concurrency": concurrency, "durationSeconds": 5}
    run_id = None
    posted = False
    try:
        health = http_json("/healthz")
        info = http_json("/api/info")
        if health.get("status") != "healthy":
            raise RuntimeError("Existing benchmark backend is unhealthy")
        if info.get("running"):
            raise RuntimeError("An existing API run is active; refusing overlapping school load")
        if concurrency > info.get("maxConcurrency", 0):
            raise RuntimeError("API requested concurrency exceeds reported limit")
        marker = {"status": "running", "started_at": started, "request": body}
        persist(phase, marker)
        posted = True
        created = http_json("/api/runs", body)
        run_id = created.get("id")
        if not isinstance(run_id, str) or not re.fullmatch(r"[0-9a-fA-F-]{36}", run_id):
            raise RuntimeError("API did not return a valid run id")
        marker["api_run_id"] = run_id
        persist(phase, marker)
        emit("SCHOOL_API_STARTED", {"phase": phase, "api_run_id": run_id, "request": body})
        deadline = time.monotonic() + 600
        while time.monotonic() < deadline:
            report = http_json("/api/runs/" + run_id)
            if report.get("status") in {"completed", "failed"}:
                result = {"status": "complete" if report["status"] == "completed" else "failed",
                          "started_at": started, "finished_at": utcnow(), "request": body,
                          "api_run_id": run_id, "api_report": safe_api_result(report),
                          "scope": "Actual private benchmark HTTP API, original temporary synthetic workload, not school_demo or Algeria WAN."}
                persist(phase, result)
                emit("SCHOOL_API_DONE", result)
                # Status may be recorded just before the cleanup/release defer.
                for _ in range(30):
                    if not http_json("/api/info").get("running"):
                        return
                    time.sleep(1)
                raise RuntimeError("API still active after recorded completion")
            time.sleep(2)
        raise RuntimeError("API run did not reach a terminal state within 600 seconds")
    except Exception as error:
        result = {"status": "failed", "started_at": started, "finished_at": utcnow(),
                  "request": body, "api_run_id": run_id, "error": redact(error)}
        # Do not send a second POST or contaminate school stages with an active API run.
        if posted or "active" in str(error):
            try:
                active = http_json("/api/info").get("running")
            except Exception:
                active = True  # No proof of an idle API: stop further load.
            if active:
                result["status"] = "unresolved"
                persist(phase, result)
                emit("SCHOOL_API_FAILED", result)
                raise RuntimeError("API outcome is unresolved; refusing further school load")
        persist(phase, result)
        emit("SCHOOL_API_FAILED", result)


def stage(clients):
    phase = "sql-" + str(clients)
    old = existing(phase)
    if old is not None:
        emit("SCHOOL_STAGE_EXISTS_NO_RERUN", {"phase": phase, "previous_status": old.get("status")})
        if old.get("status") == "running":
            raise RuntimeError("Existing running phase requires operator review; refusing further load")
        return
    started_at = utcnow()
    snapshot_before = db_snapshot()
    persist(phase, {"status": "running", "clients": clients, "duration_seconds_requested": DURATION, "started_at": started_at})
    emit("SCHOOL_STAGE_START", {"phase": phase, "clients": clients, "duration_seconds_requested": DURATION,
                                "started_at": started_at, "mix_percent": dict(WORKLOADS)})
    logdir = Path("/tmp/school-load-logs") / RUN_ID / phase
    logdir.mkdir(parents=True, exist_ok=True)
    args = ["pgbench", "-n", "-c", str(clients), "-j", "2", "-T", str(DURATION), "-M", "prepared",
            "-P", "30", "-l", "--failures-detailed", "--max-tries=1", "--log-prefix=" + str(logdir / "tx")]
    for name, weight in WORKLOADS:
        args += ["-f", str(ROOT / (name + ".sql")) + "@" + str(weight)]
    cpu_before = cgroup_cpu()
    started_clock = time.monotonic()
    stdout_file = logdir / "stdout.txt"
    stderr_file = logdir / "stderr.txt"
    timed_out = False
    with stdout_file.open("w") as out, stderr_file.open("w") as err:
        process = subprocess.Popen(args, stdout=out, stderr=err)
        try:
            returncode = process.wait(timeout=DURATION + 90)
        except subprocess.TimeoutExpired:
            timed_out = True
            process.terminate()
            try:
                returncode = process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()
                returncode = process.wait(timeout=15)
    elapsed = time.monotonic() - started_clock
    cpu_after = cgroup_cpu()
    stdout = redact(stdout_file.read_text(errors="replace"))
    stderr = redact(stderr_file.read_text(errors="replace"))
    stats = parse_logs(glob.glob(str(logdir / "tx.*")))
    tps_match = re.search(r"^tps = ([0-9.]+)", stdout, re.M)
    result = {"status": "complete" if returncode == 0 and not timed_out else "failed",
              "clients": clients, "duration_seconds_requested": DURATION, "elapsed_seconds": round(elapsed, 3),
              "started_at": started_at, "finished_at": utcnow(), "exit_code": returncode,
              "timed_out": timed_out, "tps": float(tps_match.group(1)) if tps_match else None,
              "mix_percent": dict(WORKLOADS), "log_sampling": "all transactions", **stats,
              "runner_cgroup_cpu_delta": {k: cpu_after.get(k, 0) - v for k, v in cpu_before.items()},
              "database_before": snapshot_before, "database_after": db_snapshot(),
              "pgbench_stdout": stdout[-12000:], "pgbench_stderr": stderr[-6000:],
              "scope": "Committed SQL transactions over private network; persistent DB connections; unthrottled closed-loop clients; no school HTTP app or Algeria WAN latency; runner is micro 0.5 vCPU/512MB."}
    persist(phase, result)
    emit("SCHOOL_STAGE_DONE", result)


def main():
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,79}", RUN_ID):
        raise RuntimeError("Invalid run identifier")
    if DURATION < 1 or DURATION > 600 or any(c < 1 or c > 100 for c in STAGES):
        raise RuntimeError("Invalid bounded stage settings")
    configure_database()
    prepare()
    if os.environ.get("ENABLE_API_SMOKE", "1") == "1":
        for concurrency in [1, 4]:
            api_smoke(concurrency)
    for clients in STAGES:
        stage(clients)
        time.sleep(5)
    persist("finished", {"status": "complete", "finished_at": utcnow(), "note": "Inspect every stage status, including connection-capacity failures."})
    emit("SCHOOL_RUN_FINISHED", {"run_id": RUN_ID, "finished_at": utcnow()})


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        emit("SCHOOL_RUN_FATAL", {"error": redact(error), "run_id": RUN_ID})
    # Keep a worker alive until the operator stops it. Do not restart the suite.
    while True:
        time.sleep(60)
