# Changelog

## 2.0.0 - 2026-10-12

### Changed (behaviour)
- Statistics `sum` discontinuities at the join between inputs are now corrected **by default**, by shifting
  the **older** data so it continues into the newest data. The newest data (and the mapping database's
  short-term statistics) are never touched, which avoids the "snap-back" that appears once Home Assistant
  starts recording on a file whose newer sums had been shifted. Opt out with `--no-fix-sum-offset`.
- Unit differences that are only an SI prefix (Wh/kWh, W/kW, ...) are converted **by default**. Opt out with
  `--no-convert-units`.
- Detection of sum discontinuities now catches jumps as well as falls, using the sensor's own state change.
- `--fill-states-from-statistics` stays opt-in; its report and documentation now say it is cosmetic.
- New exit code 4 = "completed, but with warnings that need attention".

### Added
- `--check-db DB`: scan an existing database for sum steps and short-term/long-term inconsistencies and print
  repair SQL.
- `--report FILE`: JSON report of inputs, coverage, warnings, corrections and checks.
- Post-merge checks: sum continuity at the joins; long-term vs short-term sum consistency.
- Warnings when the mapping database is not the newest input, and for WAL-mode inputs that have no `-wal`.
- `--fill-states-from-statistics` (estimated hourly states from statistics, marked `ha_merge_db_synthetic`).
- Automated test suite (`test_ha_merge_db.py`), continuous integration, linting configuration.

### Internal
- `run_merge` split into documented stages; docstrings throughout; removed string-substitution of SQL aliases.

## 1.0.0 - 2026-10-08
- First version: merge databases by name-based id remapping, preflight checks, corrupt-database repair hints,
  overlap/gap warnings, optional unit conversion and sum offset.
