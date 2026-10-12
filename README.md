# ha_merge_db.py

Merge several Home Assistant recorder SQLite databases into one **new** database, and check
databases for statistics problems.

Use it when you have databases from the *same* Home Assistant system that cover different time
ranges (for example after a corruption and rebuild), and you want a single database with the
whole history.

- Standalone: one Python file, standard library only. Written for Python 3.8 or newer and run on Linux
  and macOS (the CI matrix covers 3.8-3.13 on Linux).
- Non-destructive: inputs are only ever opened read-only; the result goes into a new file.
- Careful: every input is checked first; overlaps, gaps, unit differences and statistics `sum`
  discontinuities are detected and, by default, corrected; the result is checked again afterwards.
- Tested: `test_ha_merge_db.py` (32 tests on small generated databases). See [Tests](#tests).

> **Back up first.** This tool never modifies the databases you give it, and writes a new file, but
> you will eventually put that file in front of Home Assistant. Stop Home Assistant, keep a copy of
> your live database, and test the merged file before relying on it. There is no warranty (see
> [LICENSE](LICENSE)).
>
> **Tested with** Home Assistant recorder schema version 53 (Home Assistant 2025/2026 era), on databases
> of about 1 GiB. Other schema versions are refused. Please report what you try it on.

## Why not just copy the rows across?

The numeric IDs that link tables together (`states_meta.metadata_id`, `statistics_meta.id`,
`event_types.event_type_id`, `attributes_id`, ...) are **not the same** from one database to the
next. The same sensor can be `metadata_id` 12 in one file and 340 in another. Copying rows
verbatim would attach history to the wrong entities.

So you nominate one database as the **mapping database**. The output starts as a copy of it, and
rows from the other inputs are re-mapped by name (`entity_id`, `statistic_id`, `event_type`) onto
its IDs. **Use the newest database as the mapping database**: Home Assistant carries on from the
newest data, and the script leaves that data untouched (see [Sum continuity](#sum-continuity)).

## Quick start

```bash
# 1. See what would happen. Nothing is written.
./ha_merge_db.py --dry-run --mapping-db newer.db -o combined.db older.db

# 2. Do it.
./ha_merge_db.py --mapping-db newer.db -o combined.db older.db

# 3. Optional: also draw the gap in the History panel (see "Home Assistant behaviour" below)
./ha_merge_db.py --fill-states-from-statistics --mapping-db newer.db -o combined.db older.db

# 4. Check any database (for example a copy of the one Home Assistant is using)
./ha_merge_db.py --check-db home-assistant_v2.db > fix.sql
```

The mapping database is always included in the merge, so listing it among the inputs is
optional. You can pass any number of inputs.

Before using the result with Home Assistant: **stop Home Assistant, keep a backup of your live
database, and test the merged file first.** Raise `purge_keep_days` too if you want to keep the
old states (see below).

## Options

| Option | Default | Description |
|---|---|---|
| `--mapping-db DB` | required | Database whose entity/statistic/event-type IDs are authoritative. Normally the newest. |
| `-o`, `--output DB` | required | New database to create. Refuses to overwrite an existing file unless `--force` is given, and never overwrites an input. |
| `--tables LIST` | `states,events,statistics,runs` | Comma-separated groups to merge, or `all`. Groups: `states`, `events`, `statistics`, `statistics_short_term`, `runs`. Short-term statistics are excluded by default because only the long-term data is normally wanted. |
| `--unique-entities include\|ignore` | `include` | What to do with entities or statistics that exist only in a non-mapping input. `include` adds them to the output with new IDs; `ignore` drops their rows. |
| `--on-overlap skip\|error` | `skip` | If input time ranges overlap: `skip` keeps the mapping DB's rows and drops duplicate incoming rows (with a warning); `error` aborts. |
| `--no-convert-units` | converts | By default, statistics whose unit differs from the mapping DB only by an SI prefix (Wh vs kWh, W vs kW, ...) are scaled to the mapping unit. This turns that off; the values are then imported as they are (wrong by the unit ratio) with a warning. |
| `--no-fix-sum-offset` | fixes | By default, where the running `sum` of a long-term statistic restarts or jumps at the join between inputs, the **older** data is shifted so the series continues smoothly into the newest data. This turns that off (warning). |
| `--fill-states-from-statistics` | off | Estimate: write one synthetic state per hour per sensor in gaps in the merged states, from the hourly statistics. See below. Needs `states` and `statistics` in `--tables`. |
| `--gap-threshold HOURS` | `24` | Report gaps between inputs longer than this, and define a "gap" for `--fill-states-from-statistics`. |
| `--check-db DB` | | Scan one existing database for statistics `sum` problems and print repair SQL on stdout. See below. |
| `--report FILE` | | Also write a JSON report: inputs, coverage, warnings, corrections applied, row counts, checks. |
| `--quick-check` | off | Use `PRAGMA quick_check` instead of the slower, fuller `integrity_check`, for inputs and output. |
| `--dry-run` | off | Run every check and the full analysis, write nothing. |
| `--force` | off | Overwrite the output file if it already exists. |
| `--vacuum` | off | `VACUUM` the result. Slower and needs extra disk space. |

The old `--convert-units` and `--fix-sum-offset` options still work; they are now the default and
are accepted for compatibility.

## What it checks before writing anything

Every input is examined first, and **all** problems are reported together:

| Problem | What you see |
|---|---|
| Corrupt or unreadable database | The error, plus a ready-to-run repair command (see below). |
| SQL text script (`.dump` / `.recover` output) | The `sqlite3 new.db < file.sql` command to load it into a real database first. |
| Compressed file (`.bz2`, `.gz`, `.zip`, `.zst`) | The command to decompress it. |
| Empty file, missing file, not a Home Assistant database | A clear message. |
| Different schema version or columns from the mapping DB | Refused. Start Home Assistant on the older file to migrate it first. |
| Not enough free disk space | Refused, with the estimate. |

Notes (not errors):

- A non-empty `-wal` file beside an input is read too.
- A database in WAL mode with **no** `-wal` file is flagged. If it was copied from a running Home
  Assistant, its newest writes may be missing. Stop HA first, copy the `-wal` and `-shm` files
  with it, or use the sqlite3 shell's `.backup` command.

## Corrupt databases

If an input is corrupt the script stops before writing anything and prints something like:

```
X damaged.db
  CORRUPT / unreadable: database disk image is malformed
  To attempt a repair run:
    sqlite3 damaged.db ".recover" | sqlite3 damaged.db.recovered.db
```

Run that command (it needs a `sqlite3` command-line tool version 3.29 or newer, and free disk
space about the size of the file), then re-run the merge using the `.recovered.db` file.
Recovery may create a `lost_and_found` table of unattached fragments; this script ignores it.

## Sum continuity

Long-term statistics for energy-style sensors store a running `sum`. Home Assistant works out
energy from the *differences* between sums. If the `sum` restarts (for example it starts again from
zero in a rebuilt database), the Energy dashboard shows a huge spike or negative step at the join.

By default the script:

1. Compares the last sum of each older input with the first sum of the next, allowing for the
   sensor's own change in state. A fall **or** a jump that the state does not explain is a
   discontinuity.
2. Shifts the **older** data (long-term and, if merged, short-term) by the difference, so it
   continues smoothly into the newest data. The newest data is **never changed**, so Home Assistant
   can carry on from it (live recording, short-term statistics) with no "snap-back".

Consequences:

- Older sums can become **negative** (and large). That is legal and does not matter: dashboards
  only use differences.
- Corrections are made per statistic. With three or more inputs the shift is chained back from the
  newest input.
- Why not shift the newer data instead? Home Assistant builds each new hourly sum from the latest
  *short-term* row. Shifting only the newer long-term rows leaves the short-term rows on the old
  baseline, and the sum "snaps back" at the first hour Home Assistant records after loading the
  file. That produces exactly the spike this feature is meant to prevent.

## Checks on the result

After merging, the script re-checks the output:

- Row counts reconcile for every table (read = inserted + duplicates skipped + unmapped).
- No new orphaned references.
- No `sum` step at the joins between inputs.
- For every statistic, the long-term `sum` for an hour equals the sum of the last short-term row in
  that hour (if they differ, Home Assistant would snap the sum back).
- The database passes `integrity_check`.

## `--check-db`: scan a database

```bash
./ha_merge_db.py --check-db home-assistant_v2.db > fix.sql
```

Scans one database (it is only read; copy it first or stop Home Assistant) and reports

- steps in the running sum that the sensor's state does not explain, in both statistics tables;
- hours where the long-term sum and the short-term sum disagree.

It prints a summary on stderr and **repair SQL on stdout**. Review the SQL, stop Home Assistant,
back up the database, run it **once** (`sqlite3 home-assistant_v2.db < fix.sql`), and run
`--check-db` again to confirm it is clean. Exit code 4 means problems were found.

This is general-purpose: it finds the "snap-back" that appears when a database's long-term sums and
short-term sums are on different baselines (which an earlier version of this tool could cause). It also reports genuine oddities in your history, for example counters
whose sum jumps while their state does not; read the list before running the SQL, and delete lines
you do not want.

## Warnings it reports

- **Time overlaps.** Where inputs cover overlapping time ranges for the same table. Duplicate
  rows are skipped (the mapping DB's rows win), or the run aborts with `--on-overlap error`.
- **Gaps.** Periods between inputs with no data. Information only, but it shows the history is not
  continuous.
- **Mapping DB is not the newest input.** Usually a mistake; see above.
- **Unit differences** that cannot be converted automatically, or any difference when
  `--no-convert-units` is used.
- **Uncorrected sum discontinuities** when `--no-fix-sum-offset` is used, and any step or
  short-term/long-term mismatch left in the result.

Corrections that were applied (unit conversions, sum shifts) are listed in the output and in the
`--report` file as notes, not warnings.

## Exit codes

| Code | Meaning |
|---|---|
| 0 | Success, nothing needing attention. |
| 1 | Invalid arguments or paths (for example output already exists). Option-parsing errors from the command line itself exit with 2. |
| 2 | Problem with an input, an overlap with `--on-overlap error`, or not enough disk space. Nothing was written. |
| 3 | The merge failed part-way (for example corruption found while reading). The partial output was removed. |
| 4 | Completed, but with warnings that need attention (listed at the end and in `--report`). Also: `--check-db` found problems. |
| 130 | Interrupted. A `.partial` file may remain and can be deleted. |

## What gets merged

| Group | Tables | Notes |
|---|---|---|
| `states` | `states`, `state_attributes`, `states_meta` | Remapped by `entity_id`. Attributes are de-duplicated by hash and content. |
| `events` | `events`, `event_data`, `event_types` | Remapped by event type name. Event data is de-duplicated by hash and content. |
| `statistics` | `statistics`, `statistics_meta` | Remapped by `statistic_id`. |
| `statistics_short_term` | `statistics_short_term`, `statistics_meta` | Not merged by default. The output still contains the **mapping DB's** short-term rows, because it starts as a copy of that database. |
| `runs` | `recorder_runs`, `statistics_runs` | De-duplicated by start time. |

Not merged: `schema_changes` and `migration_changes` (taken from the mapping DB),
`sqlite_stat1`, and `lost_and_found`.

Duplicates are detected by:

- states: `(entity, last_updated_ts)`
- events: `(event type, time_fired_ts, context)`
- statistics: `(statistic, start_ts)`

## Home Assistant behaviour worth knowing

These surprised us while testing on a real system, so they are written down here.

**Where the History panel gets its data.** The History panel draws from *states*. For the period
before the oldest state in the database it falls back to the long-term *statistics*. If the states
have a gap in the middle, the panel does not use the statistics there; it carries the last known
value forward as a **flat line** until the next state. Graphs that read statistics (the Statistics
graph card, the Energy dashboard) show the gap correctly.

**Why there is a gap at all.** The recorder deletes states older than `purge_keep_days` (default
10) every night at 04:12. Database snapshots taken months apart therefore each contain only about
the last ten days of states, so no merge can recover the states in between. The long-term
statistics are never purged.

**`--fill-states-from-statistics`.** Writes one estimated state per hour per sensor into such a gap,
so the History panel draws a line. The values are the hourly mean (or the meter reading for total
sensors), placed mid-hour. They are **estimates, not real readings**, and each carries the
attribute `ha_merge_db_synthetic: true`. It only covers sensors that have statistics. It is purely
cosmetic: once the nightly purge removes the old states, the History panel draws the same period
from the statistics anyway.

**Purge and the merged file.** If you want to keep the imported (and synthetic) old states, raise
`purge_keep_days` (or disable `auto_purge`) in the recorder configuration *before* starting Home
Assistant on the merged file. Otherwise the 04:12 purge deletes them. The statistics are not
affected either way.

**Units.** The same sensor can be recorded in Wh in one database and kWh in another (for example
after a firmware or integration change). Imported unchanged, one period would be wrong by 1000x.
The script converts SI-prefix differences by default; other unit changes (for example °C vs °F) are
reported but not converted.

**Short-term sums.** Home Assistant continues the running `sum` from the latest short-term row. See
[Sum continuity](#sum-continuity).

### Verifying in Home Assistant

After loading the merged file, useful checks (all read-only):

- Open the History panel for a sensor over the whole period. Expect a continuous line (or, after the
  nightly purge, the statistics-based graph).
- Open the Energy dashboard for the join month. Expect no spike or negative bar at the join.
- Re-run `--check-db` on a copy of the database after Home Assistant has been running for a few
  hours. This catches the "snap-back" that appears only once HA starts recording.
- With a long-lived access token you can also query `recorder/statistics_during_period` over the
  websocket API and compare the sums around the join.

## How it works

1. Open and check every input read-only (immutable where possible, so not even `-shm` files
   are touched).
2. Analyse time coverage, overlaps, gaps, entity differences, unit differences and `sum`
   continuity. Stop here for `--dry-run`.
3. Copy the mapping DB to `<output>.partial`.
4. For each other input: map IDs by name, add (or ignore) entities unique to it, then insert
   rows, skipping duplicates and re-pointing the links between tables.
5. Shift the older sums where they do not continue (unless `--no-fix-sum-offset`).
6. Optionally synthesise states for gaps.
7. Verify (see above), then rename `<output>.partial` to the output name. On any failure the
   partial file is deleted.

## Development

```bash
python3 -m unittest -v test_ha_merge_db     # the tests (no real data needed)
python3 -m pip install flake8 && python3 -m flake8 ha_merge_db.py test_ha_merge_db.py
```

The code is one file on purpose, so it can be copied onto a Home Assistant host or NAS and run. It is
organised top to bottom as: helpers, input checking (`Source`, `open_source`), analysis, the merge
steps (`merge_states`, `merge_events`, `merge_stats`, ...), sum health checks, then the command line
(`Plan`, `run_merge` and its stage functions, `main`). Pull requests are welcome; please add a test for
any behaviour change, and keep the README and CHANGELOG up to date.

## Tests

The tests build small Home Assistant-style databases in a temporary directory (no real data
needed) and cover: ID remapping, unique entities, overlaps, units, sum restarts and jumps (older
data shifted, newest data and short-term rows untouched), three inputs, corrupt/empty/dump/bzip2
inputs, WAL notes, the synthetic states, `--check-db` (including that its SQL really repairs a
snap-back and that counter resets are not flagged), dry runs, `--report` and exit codes.

## Limitations

- All inputs must be the same schema version (tested with version 53).
- `states.old_state_id` links that pointed into a *different* input are cleared (set to NULL).
  Home Assistant tolerates this.
- Unit conversion handles SI-prefix differences only (for example Wh/kWh/MWh, W/kW). Other unit
  differences are reported but imported unconverted.
- Older sums can end up negative after a correction; see [Sum continuity](#sum-continuity).
- `--check-db` uses heuristics; it can report genuine quirks of your own history. Review its SQL.
- Sum correction handles one restart per join per statistic. More complex histories (several
  restarts inside one input) are reported by `--check-db` but not repaired automatically.
- The output needs roughly the combined size of the inputs in free disk space. The script
  estimates and checks this first.
- Not tested against every Home Assistant version: stop HA, back up, and try the merged file
  before relying on it.

## Licence

MIT. See [LICENSE](LICENSE). Changes are listed in [CHANGELOG.md](CHANGELOG.md).

## Credits

Related prior art (not used as a code source): [olicooper/HAmerger](https://github.com/olicooper/HAmerger) and
[JoryHogeveen/home-assistant-merge-db](https://github.com/JoryHogeveen/home-assistant-merge-db).
