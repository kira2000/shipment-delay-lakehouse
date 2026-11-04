# Scheduling the pipeline as a Databricks Job

Free Edition only has serverless compute, so the job uses serverless notebook tasks (no custom clusters, no external orchestrator).

| Order | Task name | Notebook | Depends on |
| --- | --- | --- | --- |
| 1 | `data_quality_gate` | `10_data_quality_tests` | none |
| 2 | `weekly_brief` | `09_weekly_brief` | `data_quality_gate` |

- Schedule: weekly, Monday 08:00. Notifications: email on failure.
- If any `error` severity test fails, task 1 fails and task 2 does not run, so no brief is published from bad data.
- Every run appends to `dq_results` (run timestamp, test, severity, offending rows, status), so you can query failure history.

Not used: dbt and Airflow (neither runs on Free Edition serverless without external infrastructure).
