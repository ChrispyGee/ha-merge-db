# AGENTS.md

Guidance for AI coding agents (and humans) working on this repository.

## What this is

`ha_merge_db.py` merges several Home Assistant recorder SQLite databases (same system, different time ranges)
into one **new** database, and can scan a database for statistics `sum` problems (`--check-db`). It is a
single-file, standard-library-only Python script for Linux and macOS. See `README.md` for user documentation
and `CHANGELOG.md` for history.

| File | Purpose |
|---|---|
| `ha_merge_db.py` | The whole tool. One file on purpose, so it can be copied onto a Home Assistant host or NAS. |
| `test_ha_merge_db.py` | Test suite. Builds tiny schema-53 databases in a temp directory; needs no real data. |
| `README.md`, `CHANGELOG.md` | User documentation and release notes. Keep both in step with behaviour changes. |
| `.flake8`, `.github/workflows/test.yml` | Lint settings and CI (Python 3.8-3.13). |

## Commands

```bash
python3 -m unittest -v test_ha_merge_db                                   # tests (about 1 second)
python3 -W error::ResourceWarning -m unittest test_ha_merge_db            # also catches unclosed files/connections
python3 -m pip install flake8 && python3 -m flake8 ha_merge_db.py test_ha_merge_db.py
./ha_merge_db.py --help
```

Run the tests and flake8 before every commit; both must pass.

## Rules that must not be broken

1. **Inputs are never modified.** They are opened read-only (`sqlite_uri()`: `immutable=1`, or `mode=ro` when a
   non-empty `-wal` exists). Output is written to `<output>.partial` and renamed with `os.replace` only after
   verification. Never write to an input path, and never leave a half-written output behind.
2. **Never shift the newest data's sums.** Sum discontinuities are corrected by shifting the *older* inputs
   (`analyse_sums`, `apply_sum_shifts`). Shifting newer data leaves short-term rows on the old baseline and Home
   Assistant's sums "snap back" at its first recording (this happened; see CHANGELOG 2.0.0 and the README).
3. **Standard library only, Python 3.8 compatible.** No third-party imports, no walrus operator, no `match`, no
   `X | Y` type syntax, no `argparse.BooleanOptionalAction`. CI runs 3.8-3.13.
4. **stdout is machine output, stderr is for people.** `log()` and `step()` write to stderr. `--check-db` prints
   repair SQL on stdout so it can be redirected to a file; do not print anything else there.
5. **SQL safety.** Identifiers (tables/columns) go through `q()`. Values are bound parameters. The only values
   interpolated into SQL text are integers the script computed itself, or strings quoted by `_sql_str()` in SQL
   that is printed for a human to review.
6. **Exit codes are part of the interface:** 0 ok, 1 bad arguments, 2 problem with an input (nothing written),
   3 merge failed (partial output removed), 4 completed with warnings, 130 interrupted. Use the `EXIT_*`
   constants and document any new code in the README.
7. **Corrections are the default, opt-outs are `--no-...`.** Unit conversion and sum correction are on by default;
   anything that needs attention is a `rep.warn()` (exit 4), anything informational is a `rep.note()`.

## Conventions

- Comments and docstrings explain **why**, not what. Every function has a docstring.
- Keep the file organised top to bottom: helpers, input checking, analysis, merge steps, sum health checks, command
  line (`Plan`, `run_merge` and its stage functions, `main`). Add new stages as small functions rather than growing
  `run_merge`.
- `flake8` with `max-line-length = 130` (long lines are mostly SQL strings).
- New behaviour needs a test in `test_ha_merge_db.py`, an entry in `CHANGELOG.md`, and an update to the README
  (options table, exit codes, or the relevant section). Bump `VERSION` for user-visible changes.
- Tests must close every file and connection (`-W error::ResourceWarning` should stay clean).

## Domain notes (non-obvious Home Assistant behaviour)

- Ids are **not stable between databases** (`metadata_id`, `statistics_meta.id`, `event_type_id`,
  `attributes_id`, ...). Everything is re-mapped by name onto the mapping database. New rows keep their original id
  plus an offset so links between them can be translated arithmetically.
- The recorder **purges states** older than `purge_keep_days` (default 10) nightly; long-term statistics are never
  purged. So states for old gaps usually cannot be recovered; `--fill-states-from-statistics` only writes cosmetic
  estimates (marked `ha_merge_db_synthetic`).
- The History panel draws from states and only falls back to statistics for the period *before* the oldest state.
- Home Assistant builds each new hourly `sum` from the **latest short-term row**, so long-term and short-term sums
  must share a baseline (`check_short_term_consistency`).
- The same sensor can change units between databases (Wh vs kWh). SI-prefix differences are converted;
  anything else is reported, not converted.
- Home Assistant databases may be in WAL mode with no `-wal` file when copied from a running instance; recent writes
  can be missing. The script flags this.
- Only schema version 53 has been tested. Other versions are refused.

## Safety when testing with real data

- Real Home Assistant databases contain a household's full history. **Never commit them** (`.gitignore` excludes
  `*.db`, `*.sql`, `*.partial`, `*.bz2`) and never put real entity names, paths, hostnames or addresses in tests,
  docs or commit messages. The tests use generated data only.
- Work on copies. Do not run any repair SQL on a live database; give it to the user to review and run with Home
  Assistant stopped and a backup taken.
