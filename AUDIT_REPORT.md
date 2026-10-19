# Retail Data Platform — audit and change log

**Date:** October 19, 2026 (America/New_York)

**Repository:** `/Users/kavitshah/Documents/Python/retail-data-platform`

**Starting commit:** `80dff45` — Adding README.md for the project

**Status:** Changes applied to the local repository, tested, and reviewed; no commit or push.

## Quick summary

The original seven unit tests passed, and the recently integrated pipeline DAG
already imported successfully. The audit found and fixed reproducible problems
around fresh Docker output, machine-specific paths, overlapping pipeline runs,
Snowflake connection cleanup, retry tracking, a missing PostgreSQL table, and
Kafka failure reporting. Existing transformation formulas and Snowflake loading
SQL were preserved.

**After changes:** all **24 offline tests pass**. Three DAGs import with the
expected 11 / 3 / 1 tasks. Focused Python lint, GitHub Actions lint, dbt parsing,
Terraform validation, and Compose validation pass. A local synthetic Spark run
produced 1,000 unique orders and 100 unique customers, with matching payment
totals and no missing customer links. PostgreSQL ETL passed twice against a
new, isolated temporary database.

**Still unverified:** Docker image builds/runs (engine stopped), live Snowflake
and dbt execution, S3 permissions/stage contents, and a hosted GitHub Actions
run. These are limits of the audit, not claims that those components are broken.

## What was preserved

- The five CSV files that were already modified when the audit started were
  preserved byte for byte: customers (100 rows), products (50), orders (1,000),
  order items (3,024), and payments (1,000). Synthetic smoke data was generated
  separately in the chat workspace.
- Existing local virtual environments, Airflow metadata, AWS/Snowflake
  configuration, Terraform state, and credentials were not edited.
- No DAG tasks were run against AWS or Snowflake. No AWS API calls, Snowflake
  connections, Terraform plans/applies, Kafka publishes, commits, staging,
  pushes, or credential reads were performed by the audit.
- Temporary PostgreSQL instances used private Unix sockets with TCP disabled,
  operated only on new audit databases, and were stopped afterward.
- The Snowflake SQL still loads both temporary tables before beginning the
  destination replacement transaction. Failure tests verify rollback behavior.

## File-by-file changes

| File | What changed and why | Evidence / result |
|---|---|---|
| `dags/retail_pipeline_dag.py` | Resolve the project from the DAG location, allow explicit worker executable/root/bucket overrides, and limit this DAG to one active run. The original absolute path failed when the repository moved; the default permitted 16 overlapping runs over shared datasets. | Relocation and concurrency checks fail against the original code and pass after the fix. Existing task graph remains 11 tasks. |
| `python/load_snowflake.py` | Close the connection if cursor creation fails or cursor cleanup raises. Allow environment overrides for account, user, role, warehouse, and key-file path, keeping the existing defaults and fixed database/schema. | Both cleanup regression tests fail before and pass after. Mocked COPY validation, commit, and rollback tests pass. Loading SQL and destination semantics unchanged. |
| `python/generate_customers.py` | Create the output directory before writing and explicitly use UTF-8. A fresh Docker container has no `/app/data` unless a volume supplies it. | Original fails with `FileNotFoundError` in an empty directory; updated script generates 100 valid customer rows. |
| `dags/retry_test_dag.py` | Use Airflow's task attempt number instead of a shared `/tmp` marker. A marker can remain from another run or disappear when the retry moves to another worker. | First-attempt failure and second-attempt success tested using mocked Airflow context. No marker file is created. |
| `python/kafka_order_producer.py` | Record delivery failures, bound final flush to 30 seconds, and exit with an error when messages fail or remain queued. Previously the script printed completion and exited successfully in both failure cases. | Two mocked delivery failures reproduce false success before the fix. Both now raise errors; successful mocked delivery still prints completion. No real broker used. |
| `sql/01_create_tables.sql` | Declare the PostgreSQL `customer_summary(customer_id, full_name, state)` table already required by `customer_etl.py`, with a customer foreign key. | Original schema fails with “relation customer_summary does not exist.” Updated schema and the actual ETL script pass in isolated PostgreSQL, including a repeat upsert. |
| `.github/workflows/ci.yml` | Add focused Ruff checks, DAG imports and mocked task checks, offline dbt parsing, Compose validation, and a fresh-container generator smoke test. Require Terraform initialization to respect its lockfile. | Final workflow passes actionlint and YAML checks. Hosted execution and Docker builds are pending; no GitHub run was triggered. |
| `.gitignore` | Ignore local environment files, key files, private dbt profiles, logs, and audit/test/lint caches. Explicitly allow the fake dbt fixture. | Source review plus ignore-rule checks. This does not remove already tracked files or past history. |
| `README.md` | Add tested environment setup, configuration variables, Snowflake prerequisites, safe local checks, and explicit cloud effects. Replace the unsupported current end-to-end-success claim with the audit's actual validation scope. | Reviewed against source and completed checks. Warns that the existing publish task deletes stale Parquet objects and that cloud execution can cost money. |
| `retail_dbt/README.md` | Replace starter text with this project's model/source layout, profile setup, offline parse command, and live validation limits. | Parsed manifest contains four models, nine declared tests, and two sources. |
| `pytest.ini` (new) | Restrict default test discovery to `tests/`. | Plain `pytest` now avoids collecting standalone `python/test_spark.py` and `python/test_delta.py`, which start Spark and can write files/download JARs during collection. |
| `requirements-airflow.txt` (new) | Record the observed working Airflow and loader connector versions in a separate environment. | Local Airflow 3.3.2 / connector 5.0.0 were used for imports and regression tests. Fresh Linux installation remains a hosted-CI check. |
| `requirements-dbt.txt` (new) | Record dbt core 1.12.5 / Snowflake adapter 1.12.1 separately from Airflow. | Offline parsing passed with these installed versions. |
| `requirements-dev.txt` (new) | Record the test, focused lint, generator, and loader-test dependencies. | pytest 9.1.1, Ruff 0.17.1, Faker 40.39.0, connector 5.0.0 used in the audit. Audit-only test dependencies were installed under the chat workspace, not into the existing project environments. |
| `scripts/check_dags.py` (new) | Provide a reusable import-only check for all expected DAG IDs and task counts. | All three DAGs pass without executing their tasks. |
| `tests/test_dags.py` (new) | Cover dependency wiring, serialized runs, repository location, missing local data, upload failure before S3 deletion, and retry attempt handling. | Five tests pass; task subprocess calls are mocked. |
| `tests/test_load_snowflake.py` (new) | Cover bad/empty COPY results, commit ordering, rollback on insert failure, and cleanup failures. | Seven tests pass; all connections/cursors are mocked. |
| `tests/test_generators.py` (new) | Run the generator in an empty temporary directory and check identifiers/required values. | One test passes. |
| `tests/test_kafka_producer.py` (new) | Simulate delivery errors, unflushed messages, and successful delivery. | Three tests pass without Kafka. |
| `tests/test_postgres_schema.py` (new) | Add a small SQLite-based schema contract test for the ETL destination. | One test passes. This lightweight CI check is supplemented by the audit's actual PostgreSQL test; SQLite is not a full PostgreSQL substitute. |
| `tests/fixtures/dbt/profiles.yml` (new) | Add intentionally unusable offline credentials for parsing without reading private profiles. | Offline parse succeeds; the fixture is not suitable for live authentication. |
| `retail_dbt/profiles.example.yml` (new) | Add a key-pair profile template using environment variables, with no embedded credentials. | Field names checked against installed dbt Snowflake adapter 1.12.1. Existing private profiles were neither read nor overwritten. |
| `AUDIT_REPORT.md` (new) | This human-readable findings and change log. | Includes scope, per-file reasoning, before/after results, and remaining limits. |
| `AUDIT_COMMAND_LOG.md` (new) | Exact commands and summarized outcomes for the audit. | Includes expected baseline failures, corrected audit-harness errors, and final checks. |

## Before and after

| Check | Before | After |
|---|---|---|
| Original unit suite | 7 passed | Same 7 still pass |
| Expanded regression suite | Six failures in initial 20-test back-test, plus a missing-schema failure and two Kafka failure cases | 24 passed, none skipped in the full audit environment |
| Fresh customer generator | Fails: output directory absent | 100 customer rows written |
| DAG imports | All 3 already import | All 3 import; graph preserved |
| Pipeline location / run concurrency | Hard-coded machine path; 16 allowed active runs | Repository-relative default; explicit overrides; 1 active run |
| Snowflake transaction and COPY validation | Mocked safety checks already pass | Still pass; cleanup leak fixed |
| PostgreSQL fresh schema + customer ETL | Missing destination table | Actual ETL succeeds twice; exactly 1 resulting test row |
| Kafka unsuccessful delivery | Reports success despite error/queued messages | Raises error; success path preserved |
| Python correctness lint | Pass | Pass |
| Workflow lint | Original passes but omits key coverage | Expanded workflow passes |
| dbt parse | Pass | Pass: 4 models, 9 declared tests, 2 sources |
| Terraform formatting / validation | Pass (validation needed local provider execution outside sandbox) | Configuration unchanged and valid |
| Compose configuration | Pass | Pass |
| Docker image runtime | Engine unavailable | Still blocked locally; smoke check added to CI |

## Checks executed and practical limits

The companion `AUDIT_COMMAND_LOG.md` records the exact commands. Main results:

1. **Python:** `compileall`, focused Ruff rules `E9,F63,F7,F82`, original tests,
   baseline regression reproductions, and final 24-test suite passed as
   described above. This is a correctness lint pass, not a full style rewrite.
   `pip check` passed in all three existing environments.
2. **Airflow:** Imported all DAGs with Airflow 3.3.2, using a separate temporary
   `AIRFLOW_HOME`; checked expected IDs, task counts, and key dependencies.
   Invoked only mocked task callables for failure tests. Did not start a
   scheduler, migrate the user's metadata database, or trigger a live DAG.
3. **dbt/Snowflake:** Ran `dbt parse --no-partial-parse` with a dummy profile and
   anonymous usage reporting disabled; inspected its manifest. Parse does not
   execute warehouse SQL or run the nine declared data tests. Snowflake loading
   was validated through mocks only; no passphrase/private key was read.
4. **Terraform:** Formatting passes. Initialized offline using the existing
   local AWS provider cache, `-backend=false -input=false -lockfile=readonly`,
   and an audit-only `TF_DATA_DIR`. Validation passed outside the sandbox after
   its provider handshake was blocked inside. No provider/configuration/lock
   changes were needed. Local-cache installation was reported as unauthenticated
   by Terraform; no provider binary was downloaded or replaced.
5. **Spark:** Generated fresh CSVs in audit scratch space, ran the unchanged
   batch pipeline, then read its Parquet to check unique IDs, row counts,
   payment reconciliation (tolerance 0.000001 for existing floating point
   arithmetic), and customer links. No original processed output was touched.
6. **PostgreSQL:** Created isolated temporary clusters with TCP disabled. Loaded
   original and updated schemas into new databases, reproduced the missing
   table, ran the actual ETL twice with synthetic input, and stopped the server.
7. **Docker/CI:** Compose and actionlint 1.7.12 pass. The actionlint download was
   checked against the official release SHA-256 list. External shellcheck and
   pyflakes integration were disabled; Python correctness was checked with
   Ruff separately. Docker daemon is not running, so images were not built.
8. **Credentials/diffs:** Reviewed every tracked source/configuration file,
   dependency pins/lockfile, data headers/counts, and generated diffs. A
   value-suppressed heuristic scan of tracked text found no obvious embedded
   private key, AWS access key, GitHub token, or literal password. This is not
   an exhaustive historical secret audit. Ignored credential directories and
   private profiles were not opened.

## Remaining blockers and follow-up decisions

- **Live cloud validation:** Confirm account access, target table column order
  and types, role grants, stage URL/integration, and Parquet format before a
  separately authorized live run. Snowflake DDL/grants/stage creation are not
  present in this repository. Loader target database/schema and stage references
  remain fixed; changing the bucket alone does not change the Snowflake stage.
- **S3 publishing remains non-atomic:** The existing two-phase synchronization
  uploads both datasets before deleting obsolete Parquet files. No live delete
  was executed here. A partial upload can leave mixed old/new files, and other
  writers are not coordinated by this DAG's active-run limit. Versioned
  publishing would need a separate design/change rather than a guessed rewrite.
- **Docker and hosted CI:** Start Docker and run builds/smoke tests, then review
  the next authorized GitHub Actions run. Linux dependency resolution and
  remote runner behavior have not been demonstrated by local linting.
- **Streaming/Delta experiments:** Sources were inspected and syntax/lint
  checked. Continuous jobs and Delta/Kafka connector JAR downloads were not
  run; no broker, streaming checkpoint, or existing Delta output was touched.
  Databricks runtime/notebook execution is outside the checked-in source here.
- **Existing logs/history:** `logs/dbt.log` is already tracked. New ignore rules
  do not untrack it or remove history. No staging/history rewriting was done;
  decide separately whether it belongs in version control.
- **Existing CSV diff warnings:** A whole-repository `git diff --check` reports
  carriage-return line endings in the user's pre-existing generated CSV changes.
  They were preserved. Check the audit's own source changes separately.
- **Money arithmetic:** Spark and the generator still use the existing float
  behavior. The synthetic batch reconciles within the stated tolerance; moving
  all monetary fields to decimal needs its own reviewed compatibility change.

## Notes on intermediate failures

Audit harness issues were corrected without treating them as project defects:
Airflow 3.3.2 no longer accepts the older `include_examples` DagBag argument;
one Terraform retry initially omitted the audit provider-cache path; and an
isolated PostgreSQL harness initially supplied an empty service name. The
initial Ruff download was blocked by sandbox DNS and succeeded in the isolated
workspace with network access. Workflow lint also caught an invalid context in
an early draft of the expanded CI and it was corrected before delivery.

## Delivery verification

The reviewed patch passed `git apply --check --whitespace=error` before
application. Changes were applied only after checking all original tracked
file hashes for concurrent edits. The final 24-test suite, focused Ruff,
actionlint, and the audit's own diff whitespace check passed in the actual
repository. The five pre-existing CSV changes still match their starting
hashes; the Git index is unchanged and HEAD remains `80dff45`.

## Reference checks

Snowflake's [transaction documentation](https://docs.snowflake.com/en/sql-reference/transactions)
was consulted when reviewing why temporary-table DDL stays before the explicit
replacement transaction. [COPY INTO documentation](https://docs.snowflake.com/en/sql-reference/sql/copy-into-table)
was consulted for load-result/column behavior. No SQL changes were made based
on assumptions about unseen warehouse objects. dbt profile fields were checked
against the installed adapter because the current online documentation also
covers newer major versions.
