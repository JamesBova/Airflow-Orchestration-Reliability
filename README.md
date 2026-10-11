# Airflow Orchestration & Reliability

A hands-on Apache Airflow project focused on **production-style orchestration, reliability, control flow, concurrency, and configuration**.

The project uses small DAGs to isolate individual Airflow concepts, then combines them in a final capstone workflow.

---

## What This Project Covers

| Area | Concepts |
|---|---|
| Workflow development | TaskFlow API, Operators, Hooks, Connections, Variables, Sensors, Jinja templating, runtime context |
| Communication & control | XCom, TaskInstance (`ti`), branching, trigger rules, conditional processing, dynamic task mapping |
| Reliability | Retries, callbacks, failure handling, logging, idempotency, reruns, catchup, backfills |
| Scaling & configuration | Pools, `max_active_tasks`, `max_active_runs`, CeleryExecutor, provider packages, secrets |

---

## Airflow Environment

The project runs Airflow with Docker Compose using a multi-service architecture:

```text
DAG files
   ↓
DAG Processor
   ↓
Scheduler
   ↓
CeleryExecutor
   ↓
Redis
   ↓
Worker
   ↓
Task execution
```

Supporting services:

- **PostgreSQL** — Airflow metadata database
- **Redis** — Celery message broker
- **API Server** — Airflow UI and REST API
- **Triggerer** — supports deferred/asynchronous task behavior

---

## Project Structure

```text
Airflow-Orchestration-Reliability/
├── dags/
│   ├── 01_connection_hook.py
│   ├── 01_b_connections_hooks.py
│   ├── 02_variables.py
│   ├── 03_jinja_templating.py
│   ├── 03_b_jinja_templating.py
│   ├── 04_sensor.py
│   ├── 04_b_sensor.py
│   ├── 05_branching.py
│   ├── 05_b_branching.py
│   ├── 06_trigger_rules.py
│   ├── 06_b_trigger_rules.py
│   ├── 07_dynamic_task_mapping.py
│   ├── 07_b_dynamic_task_mapping.py
│   ├── 08_retries_failure.py
│   ├── 08_b_retries.py
│   ├── 09_callbacks.py
│   ├── 10_failure_logging.py
│   ├── 11_idempotency.py
│   ├── 12_catchup.py
│   ├── 13_backfill.py
│   ├── 14_pools_concurrency.py
│   ├── 15_concurrency.py
│   ├── 16_secrets.py
│   ├── 16_b_secrets.py
│   ├── 17_xcom_task_instance.py
│   └── 18_orchestration_reliability_capstone.py
├── data/
├── logs/
├── plugins/
├── scripts/
├── .env
├── .env.example
├── .gitignore
├── docker-compose.yaml
└── README.md
```

---

## DAG Index

| File | Focus |
|---|---|
| `01_connection_hook.py` | Airflow Connections and `PostgresHook` |
| `01_b_connections_hooks.py` | Deeper look at connection sources and Connection objects |
| `02_variables.py` | Airflow Variables |
| `03_jinja_templating.py` | Basic Jinja templating |
| `03_b_jinja_templating.py` | Runtime context, params, Variables, Connections, data intervals |
| `04_sensor.py` | Basic `PythonSensor` |
| `04_b_sensor.py` | `@task.sensor`, `PokeReturnValue`, reschedule behavior |
| `05_branching.py` | Single-path branching |
| `05_b_branching.py` | Returning one or multiple branch task IDs |
| `06_trigger_rules.py` | Trigger rules after branching |
| `06_b_trigger_rules.py` | Comparing common trigger rules |
| `07_dynamic_task_mapping.py` | Basic `expand()` |
| `07_b_dynamic_task_mapping.py` | `expand_kwargs()`, `partial()`, fan-out/fan-in |
| `08_retries_failure.py` | Basic retries and retry delay |
| `08_b_retries.py` | `default_args`, overrides, `ti.try_number` |
| `09_callbacks.py` | Retry and failure callbacks |
| `10_failure_logging.py` | Python logging and exception handling |
| `11_idempotency.py` | Safe reruns and idempotent output |
| `12_catchup.py` | Automatic historical scheduling with catchup |
| `13_backfill.py` | Explicit historical processing |
| `14_pools_concurrency.py` | Shared resource limits with pools |
| `15_concurrency.py` | DAG-level concurrency controls |
| `16_secrets.py` | Environment-based Airflow Connection |
| `16_b_secrets.py` | Environment-based Variables and secret resolution |
| `17_xcom_task_instance.py` | Implicit/explicit XCom and TaskInstance |
| `18_orchestration_reliability_capstone.py` | Integrated reliability capstone |

---

# Concept Walkthrough

## 01 — Connections & Hooks

### `01_connection_hook.py`

Introduces Airflow **Connections** and `PostgresHook`.

```text
Connection = configuration
Hook       = Python code that uses the configuration
```

A Hook can use a Connection stored in:

- Airflow Admin / Connections
- environment variables
- a configured secrets backend

### `01_b_connections_hooks.py`

Goes deeper into:

- connection IDs
- resolved Connection objects
- connection fields such as host, schema, login, and port
- different connection sources

---

## 02 — Variables

### `02_variables.py`

Airflow Variables provide small runtime configuration values without hardcoding them into DAG source.

Example use cases:

- environment name
- processing mode
- feature flag
- file prefix
- configurable threshold

---

## 03 — Jinja Templating

### `03_jinja_templating.py`

Introduces Jinja expressions inside templated operator fields.

Examples:

```text
{{ dag.dag_id }}
{{ task.task_id }}
{{ run_id }}
{{ ds }}
```

### `03_b_jinja_templating.py`

Expands into:

- `data_interval_start`
- `data_interval_end`
- DAG `params`
- Airflow Variables
- Airflow Connections
- Python runtime context through `get_current_context()`

Key distinction:

```text
Templated operator field → Jinja
Python TaskFlow task      → get_current_context()
```

---

## 04 — Sensors

### `04_sensor.py`

Uses a `PythonSensor` to wait for a file before downstream processing continues.

### `04_b_sensor.py`

Adds:

- `@task.sensor`
- `PokeReturnValue`
- `is_done`
- optional `xcom_value`
- `poke_interval`
- `timeout`
- `mode="reschedule"`

Example sensor flow:

```text
Check condition
   ↓
False → wait and check again
   ↓
True
   ↓
Sensor succeeds
   ↓
Downstream task runs
```

`PokeReturnValue` can also pass a small value downstream through XCom.

---

## 05 — Branching

### `05_branching.py`

Uses `@task.branch` to choose a downstream task at runtime.

### `05_b_branching.py`

Shows that a branch can return:

```python
"process_transactions"
```

or multiple task IDs:

```python
[
    "process_transactions",
    "process_accounts",
]
```

Non-selected downstream tasks are marked **skipped**.

---

## 06 — Trigger Rules

### `06_trigger_rules.py`

Shows how trigger rules control whether downstream tasks run after branching.

### `06_b_trigger_rules.py`

Compares common rules:

| Rule | Meaning |
|---|---|
| `all_success` | Every direct upstream task succeeded |
| `none_failed` | No upstream task failed |
| `none_failed_min_one_success` | No failures and at least one success |
| `all_done` | All upstream tasks reached a terminal state |

Mental model:

```text
Dependency   → Which tasks are upstream?
Trigger rule → Which upstream states are acceptable?
```

---

## 07 — Dynamic Task Mapping

### `07_dynamic_task_mapping.py`

Uses `expand()` to create one mapped task instance per input item.

```text
["transactions.csv", "accounts.csv", "customers.csv"]
                     ↓
             process_file.expand(...)
                     ↓
              3 task instances
```

### `07_b_dynamic_task_mapping.py`

Adds:

- dictionaries
- `expand_kwargs()`
- fixed values with `partial()`
- mapped outputs
- fan-out / fan-in

Quick reference:

```text
expand()        → map individual arguments
expand_kwargs() → map dictionaries of arguments
partial()       → fixed arguments shared by mapped tasks
```

---

## 08 — Retries

### `08_retries_failure.py`

Demonstrates:

```python
retries=2
retry_delay=timedelta(seconds=10)
```

### `08_b_retries.py`

Adds:

- DAG-level retry defaults with `default_args`
- task-level overrides
- `ti.try_number`
- fail-once-then-succeed behavior
- disabling retries for one task

---

## 09 — Callbacks

### `09_callbacks.py`

Demonstrates:

- `on_retry_callback`
- `on_failure_callback`

Typical production uses include:

- alerts
- audit records
- notifications
- operational metadata
- external service calls

---

## 10 — Failure Handling & Logging

### `10_failure_logging.py`

Uses Python's built-in `logging` module.

Demonstrates:

- `logger.info()`
- `logger.exception()`
- traceback capture
- re-raising exceptions so Airflow still marks the task failed
- `finally` behavior

---

## 11 — Idempotency

### `11_idempotency.py`

Demonstrates the principle:

> Running the same logical task again should produce the same final state rather than duplicate or corrupt data.

Possible strategies depend on the workload:

- replace instead of append
- delete and reload
- `MERGE` / upsert
- replace a partition
- check business keys
- deduplicate before insert

---

## 12 — Catchup

### `12_catchup.py`

Uses:

```python
catchup=True
```

with a historical `start_date`.

Airflow automatically creates missed scheduled intervals.

---

## 13 — Backfill

### `13_backfill.py`

Backfill is historical processing that is deliberately requested.

```text
Catchup  → Airflow automatically creates missed scheduled runs
Backfill → You explicitly request historical runs
Rerun    → Repeat work from an existing run
```

---

## 14 — Pools

### `14_pools_concurrency.py`

Pools limit how many tasks can use a shared resource at once.

Good use cases:

- vendor API
- legacy SQL Server
- constrained warehouse
- SFTP endpoint
- rate-limited external service

```text
Pool = shared task concurrency limit
```

Pools can affect tasks from multiple DAGs.

---

## 15 — DAG-Level Concurrency

### `15_concurrency.py`

Demonstrates:

```python
max_active_tasks=2
```

Related setting:

```python
max_active_runs=1
```

Comparison:

```text
Pool             → shared resource limit across DAGs
max_active_tasks → task concurrency limit for one DAG
max_active_runs  → active run limit for one DAG
```

---

## 16 — Secrets & Configuration

### `16_secrets.py`

Demonstrates an Airflow Connection supplied through an environment variable.

Naming convention:

```text
AIRFLOW_CONN_<CONNECTION_ID>
```

Example:

```text
AIRFLOW_CONN_DEMO_POSTGRES_ENV=postgresql://airflow:airflow@postgres:5432/airflow
```

Connection URI format:

```text
protocol://username:password@host:port/database
```

### `16_b_secrets.py`

Adds environment-based Variables:

```text
AIRFLOW_VAR_<VARIABLE_NAME>
```

Example:

```text
AIRFLOW_VAR_SECRET_SOURCE_TEST=environment
```

The DAG retrieves the value through normal Airflow APIs and does not need to know where the configuration originated.

---

## 17 — XCom & TaskInstance

### `17_xcom_task_instance.py`

Shows both implicit and explicit XCom.

### Implicit XCom with TaskFlow

```python
value = producer()
consumer(value)
```

Conceptually:

```text
producer
   ↓
return value
   ↓
XCom
   ↓
consumer
```

### Explicit XCom with `ti`

```python
ti.xcom_push(...)
ti.xcom_pull(...)
```

`ti` is the **TaskInstance** — one specific execution of one task within one DAG run.

XCom is appropriate for small values such as:

- file paths
- row counts
- batch IDs
- dates
- status flags
- small dictionaries
- small lists

Large datasets should be stored externally, with XCom passing only a reference.

---

## 18 — Orchestration & Reliability Capstone

### `18_orchestration_reliability_capstone.py`

Combines the major DAG-level concepts from the project:

- Sensor
- Jinja templating
- Variables
- Connection + Hook
- Branching
- conditional processing
- XCom
- dynamic task mapping
- trigger rules
- retries
- callbacks
- logging
- idempotent output
- pools
- DAG-level concurrency

Some topics are intentionally demonstrated outside the DAG itself:

- **Backfill** — execution operation against a DAG
- **Catchup** — scheduling behavior
- **CeleryExecutor** — runtime configuration
- **Provider packages** — environment dependencies

---

# Environment Variables and Airflow

The Docker Compose services load `.env` with:

```yaml
env_file:
  - .env
```

The flow is:

```text
.env
  ↓
Docker Compose
  ↓
container environment variables
  ↓
Airflow recognizes AIRFLOW_* naming conventions
```

Examples:

```text
AIRFLOW_CONN_DEMO_POSTGRES_ENV
AIRFLOW_VAR_SECRET_SOURCE_TEST
```

This keeps configuration outside DAG source code.

---

# Running the Project

## Initialize Airflow

For a fresh environment:

```cmd
docker compose run --rm airflow-apiserver airflow db migrate
```

## Start

```cmd
docker compose up -d
```

## Check services

```cmd
docker compose ps
```

## Stop

```cmd
docker compose down
```

Avoid:

```cmd
docker compose down -v
```

unless you intentionally want to delete persistent Docker volumes and Airflow metadata.

---

## Airflow UI

The Airflow container listens internally on port `8080`.

A host mapping may use another port:

```yaml
ports:
  - "8081:8080"
```

That means:

```text
Browser                  → localhost:8081
Airflow inside Docker    → airflow-apiserver:8080
```

---

# Security Notes

- Do not commit `.env`.
- Do not hardcode passwords in DAG files.
- Do not print credentials into task logs.
- Environment variables are useful for development, but production environments often use a dedicated secrets manager.
- XCom should not be used as bulk data storage.

---

# Key Takeaways

Airflow is primarily an **orchestration platform**. It coordinates when work runs, in what order, under what conditions, and with what reliability controls.

A production-style Airflow workflow needs more than scheduling. It also needs:

- controlled concurrency
- safe retries
- clear failure behavior
- idempotent processing
- observable logs
- secure configuration
- conditional execution
- historical rerun capability
- resource protection
- task-level and DAG-level controls

The project demonstrates both the convenient **TaskFlow API** and the broader Airflow ecosystem of Operators, Hooks, Sensors, Connections, Pools, XCom, provider packages, and runtime services.
