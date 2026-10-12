#!/usr/bin/env python3
"""
ha_merge_db.py - merge several Home Assistant recorder SQLite databases into one.

Copyright (c) 2026 Chris Good.  SPDX-License-Identifier: MIT  (see the LICENSE file)

The databases are assumed to come from the *same* Home Assistant system but cover
different time ranges.  The numeric ids that link tables together (states_meta.metadata_id,
statistics_meta.id, event_types.event_type_id, attributes_id, ...) are NOT stable between
databases, so rows are re-mapped by name (entity_id / statistic_id / event_type) onto the
database you nominate with --mapping-db.

Safety properties
  * Inputs are only ever opened read-only (immutable where possible); they are never modified.
  * Output is written to "<output>.partial" and atomically renamed only when everything,
    including an integrity check, succeeded.  An existing output is never overwritten
    without --force.
  * Every input is checked up-front (file type, integrity_check, schema version, columns).
    Problems are all reported together, with a ready-to-run repair command for corrupt files.
  * Time overlaps, gaps, unit differences and statistics 'sum' discontinuities are detected.  By default
    unit differences (Wh vs kWh ...) are converted and sum discontinuities are corrected by shifting the
    OLDER data, so the newest data - which Home Assistant carries on from - is never altered.
  * The merged result is checked for sum steps and for short-term/long-term sum consistency.
  * --check-db scans any existing database (e.g. the one Home Assistant is running) for the same problems
    and prints repair SQL.  --report writes a JSON report.  Exit code 4 means "completed with warnings".

Requires only Python 3.8+ (standard library).  Works on Linux and macOS.
Run with --help for the options.
"""
import argparse
import datetime
import json
import os
import shlex
import shutil
import sqlite3
import sys
import time
import urllib.parse

VERSION = "2.0"
SQLITE_MAGIC = b"SQLite format 3\x00"

# Table groups selectable with --tables.  A group is merged as a unit because its tables reference each other.
ALL_GROUPS = ["states", "events", "statistics", "statistics_short_term", "runs"]
DEFAULT_GROUPS = ["states", "events", "statistics", "runs"]  # short-term stats excluded by default
GROUP_TABLES = {
    "states": ["states", "state_attributes", "states_meta"],
    "events": ["events", "event_data", "event_types"],
    "statistics": ["statistics", "statistics_meta"],
    "statistics_short_term": ["statistics_short_term", "statistics_meta"],
    "runs": ["recorder_runs", "statistics_runs"],
}
# (table, time column) used for range/overlap/gap analysis
GROUP_TIME = {
    "states": ("states", "last_updated_ts"),
    "events": ("events", "time_fired_ts"),
    "statistics": ("statistics", "start_ts"),
    "statistics_short_term": ("statistics_short_term", "start_ts"),
}

T0 = time.time()
_OPEN_CONNECTIONS = []      # every source connection, closed by main() when it finishes

# multiply values in the FIRST unit by the factor to get the SECOND unit
_PREFIX = {"": 1.0, "k": 1e3, "M": 1e6, "G": 1e9}
UNIT_BASES = ("Wh", "W", "VA", "var", "Hz", "A", "V")


def unit_factor(src_unit, dst_unit):
    """Conversion factor src->dst for simple SI-prefix differences (Wh<->kWh, W<->kW ...), else None."""
    if src_unit == dst_unit:
        return 1.0
    if not src_unit or not dst_unit:
        return None
    for base in UNIT_BASES:
        if src_unit.endswith(base) and dst_unit.endswith(base):
            ps, pd = src_unit[:-len(base)], dst_unit[:-len(base)]
            if ps in _PREFIX and pd in _PREFIX:
                return _PREFIX[ps] / _PREFIX[pd]
    return None


# --------------------------------------------------------------------------- helpers
def log(msg=""):
    """Print a message to stderr (stdout is reserved for machine-readable output such as --check-db SQL)."""
    print(msg, file=sys.stderr, flush=True)


def step(msg):
    """Print a progress line prefixed with the elapsed time."""
    log("[%6.1fs] %s" % (time.time() - T0, msg))


def q(name):
    """Quote an SQL identifier (table/column name).  Values are always passed as bound parameters instead."""
    return '"' + name.replace('"', '""') + '"'


def fmt_ts(ts):
    """Format a Unix timestamp as UTC 'YYYY-MM-DD HH:MM:SS' ('-' for None)."""
    if ts is None:
        return "-"
    return datetime.datetime.fromtimestamp(ts, datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def fmt_size(n):
    """Format a byte count for humans (KiB/MiB/GiB)."""
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if n < 1024 or unit == "TiB":
            return "%.1f %s" % (n, unit) if unit != "B" else "%d B" % n
        n /= 1024.0


def sqlite_uri(path, immutable):
    """SQLite URI for opening `path` read-only.  immutable=1 promises SQLite the file cannot change, so
    it neither locks the file nor touches -wal/-shm files (but also ignores any WAL content)."""
    return "file:" + urllib.parse.quote(os.path.abspath(path), safe="/") + (
        "?immutable=1" if immutable else "?mode=ro")


def scalar(conn, sql, params=()):
    """Run a query and return the first column of the first row (None if there are no rows)."""
    row = conn.execute(sql, params).fetchone()
    return row[0] if row else None


def table_cols(conn, schema, table):
    """Column names of <schema>.<table>, in table order."""
    return [r[1] for r in conn.execute("PRAGMA %s.table_info(%s)" % (schema, q(table)))]


def has_table(conn, schema, table):
    """True if <schema> contains a table called `table`."""
    return scalar(conn, "SELECT COUNT(*) FROM %s.sqlite_master WHERE type='table' AND name=?" % schema,
                  (table,)) > 0


def repair_hint(path):
    """The shell command a user can run to try to recover a corrupt database with sqlite3's .recover."""
    p = shlex.quote(path)
    rec = shlex.quote(path + ".recovered.db")
    return ("sqlite3 %s \".recover\" | sqlite3 %s\n"
            "        (needs a sqlite3 CLI >= 3.29 and free disk space about the size of the file;\n"
            "         then re-run this script using the .recovered.db file.  Any 'lost_and_found'\n"
            "         table it creates is ignored by this script.)" % (p, rec))


# --------------------------------------------------------------------------- sources
class Source(object):
    """One input database: its path, an open read-only connection, and what we learned about it.

    `problems` are fatal (nothing is written); `warnings` are shown but do not stop the run."""
    def __init__(self, path, is_mapping=False):
        self.path = path
        self.name = os.path.basename(path)
        self.is_mapping = is_mapping
        self.problems = []      # fatal
        self.warnings = []
        self.conn = None
        self.immutable = True
        self.schema_version = None
        self.ranges = {}        # group -> (min, max, count)
        self.wal_mode = False
        self.size = os.path.getsize(path) if os.path.exists(path) else 0

    def problem(self, msg, repair=False):
        """Record a fatal problem; repair=True means the user can try the sqlite3 .recover command."""
        self.problems.append((msg, repair))


def classify_file(src):
    """Cheap look at the file header before sqlite touches it."""
    path = src.path
    if not os.path.exists(path):
        src.problem("file does not exist")
        return False
    if not os.path.isfile(path):
        src.problem("not a regular file")
        return False
    if src.size == 0:
        src.problem("file is empty (0 bytes) - nothing to merge")
        return False
    with open(path, "rb") as fh:
        head = fh.read(64)
    if head.startswith(SQLITE_MAGIC):
        return True
    if head.startswith(b"BZh"):
        src.problem("file is bzip2-compressed, not a database.  Decompress first:  bunzip2 -k %s"
                    % shlex.quote(path))
    elif head.startswith(b"\x1f\x8b"):
        src.problem("file is gzip-compressed, not a database.  Decompress first:  gunzip -k %s"
                    % shlex.quote(path))
    elif head.startswith(b"PK\x03\x04"):
        src.problem("file is a zip archive, not a database.  Unzip it first.")
    elif head.startswith(b"\x28\xb5\x2f\xfd"):
        src.problem("file is zstd-compressed, not a database.  Decompress it first (unzstd -k).")
    else:
        text = head.lstrip()
        if text[:1] in (b".", b"-", b"B", b"P", b"C", b"I", b"/") and all(
                (c in (9, 10, 13) or 32 <= c < 127) for c in head):
            src.problem("file is a SQL text script (e.g. output of '.dump' or '.recover'), not a "
                        "database.  Load it into a database first:\n"
                        "        sqlite3 %s < %s" % (shlex.quote(path + ".db"), shlex.quote(path)))
        else:
            src.problem("file header is not a valid SQLite header - the file is damaged or not a "
                        "SQLite database", repair=True)
    return False


def open_source(src, quick):
    """Open `src` read-only and check it; on any problem record it in src.problems instead of raising.

    Order: file-type sniffing (so a bzip2 file or SQL dump gets a helpful message rather than 'not a
    database'), WAL notes, SQLite's integrity check, then a check that it looks like a Home Assistant
    recorder database.  Uses immutable mode unless a non-empty -wal file exists (then the WAL must be read)."""
    if not classify_file(src):
        return
    wal = src.path + "-wal"
    wal_nonempty = os.path.exists(wal) and os.path.getsize(wal) > 0
    with open(src.path, "rb") as fh:
        hdr = fh.read(32)
    src.wal_mode = len(hdr) >= 20 and (hdr[18] == 2 or hdr[19] == 2)
    if src.wal_mode and not wal_nonempty:
        src.warnings.append("this database is in WAL mode but has no (non-empty) -wal file beside it.  If it was "
                            "copied from a running Home Assistant its most recent writes may be missing; copy "
                            "the -wal and -shm files together with it, stop HA first, or use "
                            "the sqlite3 shell's .backup command to take a consistent copy")
    if wal_nonempty:
        src.immutable = False
        src.warnings.append("a non-empty write-ahead log (%s, %s) exists beside this database; it will "
                            "be read too (opened read-only, not immutable).  Make sure Home Assistant is "
                            "stopped / the WAL is consistent." % (os.path.basename(wal),
                                                                  fmt_size(os.path.getsize(wal))))
    try:
        conn = sqlite3.connect(sqlite_uri(src.path, src.immutable), uri=True)
        _OPEN_CONNECTIONS.append(conn)
        pragma = "quick_check" if quick else "integrity_check"
        step("checking %s (%s) ..." % (src.name, pragma))
        res = [r[0] for r in conn.execute("PRAGMA %s" % pragma)]
        if res != ["ok"]:
            src.problem("CORRUPT - %s reported: %s%s" % (pragma, "; ".join(res[:3]),
                                                         " (+%d more)" % (len(res) - 3) if len(res) > 3 else ""),
                        repair=True)
            conn.close()
            return
        if not has_table(conn, "main", "schema_changes"):
            src.problem("no schema_changes table - this does not look like a Home Assistant recorder "
                        "database")
            conn.close()
            return
        src.schema_version = scalar(conn, "SELECT MAX(schema_version) FROM schema_changes")
        src.conn = conn
        _OPEN_CONNECTIONS.append(conn)
    except sqlite3.DatabaseError as exc:
        src.problem("CORRUPT / unreadable: %s" % exc, repair=True)


# --------------------------------------------------------------------------- analysis
def check_schemas(sources, groups):
    """Require every input to have the same schema version and the same columns as the mapping DB
    for the tables being merged; records a problem on any input that does not."""
    ref = sources[0]
    for s in sources:
        if s.conn is None:
            continue
        if s.schema_version != ref.schema_version:
            s.problem("schema version %s differs from the mapping database's %s - merging across "
                      "schema versions is not supported (start Home Assistant on the old file to "
                      "migrate it first)" % (s.schema_version, ref.schema_version))
            continue
        needed = set()
        for g in groups:
            needed.update(GROUP_TABLES[g])
        for t in sorted(needed):
            if not has_table(s.conn, "main", t):
                s.problem("required table '%s' is missing (needed for --tables %s)"
                          % (t, ",".join(groups)), repair=False)
            elif s is not ref and ref.conn is not None and has_table(ref.conn, "main", t):
                if set(table_cols(s.conn, "main", t)) != set(table_cols(ref.conn, "main", t)):
                    s.problem("table '%s' has different columns than the mapping database" % t)


def gather_ranges(sources, groups):
    """Record (first, last, row count) of the time column of each selected table for every source."""
    for s in sources:
        for g in groups:
            if g not in GROUP_TIME:
                continue
            tbl, col = GROUP_TIME[g]
            row = s.conn.execute("SELECT MIN(%s), MAX(%s), COUNT(*) FROM %s" % (col, col, q(tbl))).fetchone()
            s.ranges[g] = row


def analyse_time(sources, groups, gap_hours):
    """Returns (overlaps, gaps) lists of printable strings; overlaps non-empty means a warning."""
    overlaps, gaps = [], []
    for g in groups:
        if g not in GROUP_TIME:
            continue
        items = [(s.ranges[g][0], s.ranges[g][1], s) for s in sources if s.ranges[g][2]]
        for i in range(len(items)):
            for j in range(i + 1, len(items)):
                lo = max(items[i][0], items[j][0])
                hi = min(items[i][1], items[j][1])
                if lo <= hi:
                    overlaps.append("%-22s %s .. %s  overlap between %s and %s"
                                    % (g, fmt_ts(lo), fmt_ts(hi), items[i][2].name, items[j][2].name))
        items.sort(key=lambda t: t[0])
        reach = None
        for lo, hi, s in items:
            if reach is not None and lo - reach[0] > gap_hours * 3600:
                gaps.append("%-22s no data from %s to %s (%.1f days) between %s and %s"
                            % (g, fmt_ts(reach[0]), fmt_ts(lo), (lo - reach[0]) / 86400.0, reach[1], s.name))
            if reach is None or hi > reach[0]:
                reach = (hi, s.name)
    return overlaps, gaps


def meta_keys(conn, table, col):
    """The set of names (entity_id / statistic_id) in a metadata table."""
    return set(r[0] for r in conn.execute("SELECT %s FROM %s" % (q(col), q(table))))


def analyse_entities(sources, groups):
    """For each metadata table, how many names each input shares with the mapping DB, has only itself,
    or are only in the mapping DB.  Returns tuples (table, input name, common, only_in_input, only_in_mapping)."""
    out = []
    ref = sources[0]
    specs = []
    if "states" in groups:
        specs.append(("states_meta", "entity_id"))
    if "statistics" in groups or "statistics_short_term" in groups:
        specs.append(("statistics_meta", "statistic_id"))
    for table, col in specs:
        base = meta_keys(ref.conn, table, col)
        for s in sources[1:]:
            keys = meta_keys(s.conn, table, col)
            out.append((table, s.name, len(keys & base), len(keys - base), len(base - keys)))
    return out


def analyse_meta_mismatch(sources):
    """statistics_meta definitions that disagree with the mapping database (units especially)."""
    cols = "statistic_id, source, unit_of_measurement, has_sum, has_mean"
    base = dict((r[0], r[1:]) for r in sources[0].conn.execute("SELECT %s FROM statistics_meta" % cols))
    out = []
    for s in sources[1:]:
        for r in s.conn.execute("SELECT %s FROM statistics_meta" % cols):
            b = base.get(r[0])
            if b is not None and tuple(r[1:]) != tuple(b):
                out.append(dict(sid=r[0], src=s.name, mine=r[1:], base=b,
                                factor=unit_factor(r[2], b[1]) if r[2] != b[1] else 1.0,
                                unit_differs=(r[2] != b[1])))
    return out


def sum_edges(conn):
    """First and last row (time, state, sum) of every has_sum statistic in one database, plus its unit.
    These are all that is needed to judge whether two databases join smoothly."""
    res = {}
    cur = conn.execute("SELECT m.statistic_id, s.metadata_id, MIN(s.start_ts), MAX(s.start_ts), m.unit_of_measurement "
                       "FROM statistics s "
                       "JOIN statistics_meta m ON m.id = s.metadata_id WHERE m.has_sum = 1 "
                       "GROUP BY s.metadata_id").fetchall()
    for sid, mid, lo, hi, unit in cur:
        first = conn.execute("SELECT state, sum FROM statistics WHERE metadata_id=? AND start_ts=?",
                             (mid, lo)).fetchone()
        last = conn.execute("SELECT state, sum FROM statistics WHERE metadata_id=? AND start_ts=?",
                            (mid, hi)).fetchone()
        res[sid] = dict(f_ts=lo, f_state=first[0], f_sum=first[1], l_ts=hi, l_state=last[0], l_sum=last[1],
                        unit=unit)
    return res


def analyse_sums(sources, convert_units):
    """Detect statistics 'sum' discontinuities at the joins between inputs.

    The running 'sum' of a long-term statistic must continue smoothly from one input to the next.  When
    it does not (a counter that restarted from zero, or jumped), the OLDER data is shifted so that it
    lines up with the newest data.  The newest data is never touched: Home Assistant carries on from it
    (live recording, short-term statistics), so leaving it alone avoids a 'snap-back' after startup.

    Returns (findings, shifts) where shifts[statistic_id] = [(from_ts or None, to_ts or None, shift)]
    (rows with from_ts <= start_ts < to_ts are shifted; None = unbounded)."""
    per = {}
    map_edges = None
    for s in sources:
        edges = sum_edges(s.conn)
        if map_edges is None:
            map_edges = edges
        for sid, e in edges.items():
            e = dict(e)
            e["src"] = s.name
            if convert_units and sid in map_edges and e["unit"] != map_edges[sid]["unit"]:
                f = unit_factor(e["unit"], map_edges[sid]["unit"])
                if f is not None:
                    for k in ("f_state", "f_sum", "l_state", "l_sum"):
                        if e[k] is not None:
                            e[k] *= f
            per.setdefault(sid, []).append(e)
    findings, shifts = [], {}
    for sid, segs in per.items():
        n = len(segs)
        if n < 2:
            continue
        segs.sort(key=lambda e: e["f_ts"])
        seg_shift = [0.0] * n                      # the newest segment keeps shift 0
        for i in range(n - 2, -1, -1):
            prev, cur = segs[i], segs[i + 1]
            seg_shift[i] = seg_shift[i + 1]        # continuous join: same shift as the newer neighbour
            if cur["f_ts"] > prev["l_ts"] and prev["l_sum"] is not None and cur["f_sum"] is not None:
                # Expected change in sum across the join = change in the sensor's state (or the new state
                # if it was reset).  Anything else - a fall OR a jump - is a discontinuity.
                delta = 0.0
                if cur["f_state"] is not None and prev["l_state"] is not None:
                    delta = cur["f_state"] - prev["l_state"]
                    if delta < 0:
                        delta = cur["f_state"]
                expected = prev["l_sum"] + delta
                tol = max(0.01 * abs(prev["l_sum"]), 0.05)
                if abs(cur["f_sum"] - expected) > tol:
                    seg_shift[i] = seg_shift[i + 1] + cur["f_sum"] - expected
                    findings.append("%s: sum %s from %.4f (%s, %s) to %.4f (%s, %s) -> older data shifted by %+.4f"
                                    % (sid, "jumps" if cur["f_sum"] > expected else "falls", prev["l_sum"],
                                       fmt_ts(prev["l_ts"]), prev["src"], cur["f_sum"], fmt_ts(cur["f_ts"]),
                                       cur["src"], seg_shift[i] - seg_shift[i + 1]))
        ranges = []
        for i in range(n):
            if seg_shift[i] != 0.0:
                ranges.append((None if i == 0 else segs[i]["f_ts"],
                               segs[i + 1]["f_ts"] if i + 1 < n else None, seg_shift[i]))
        if ranges:
            shifts[sid] = ranges
    return findings, shifts


# --------------------------------------------------------------------------- merge steps
def colsql(db, table, overrides, alias="s"):
    """Build the column list and SELECT list for copying every column of main.<table> from src.<table>.

    overrides maps a column name to the SQL expression to use instead of `alias.<column>`; a value of
    None leaves the column out of the INSERT so SQLite assigns a fresh id.  Using the real column list of
    the output table (not a hard-coded one) keeps this working if Home Assistant adds columns."""
    dest, sel = [], []
    for c in table_cols(db, "main", table):
        if c in overrides:
            if overrides[c] is None:
                continue
            dest.append(q(c))
            sel.append(overrides[c])
        else:
            dest.append(q(c))
            sel.append(alias + "." + q(c))
    return ", ".join(dest), ", ".join(sel)


def build_meta_map(db, table, idcol, keycol, include_unique, mapname, report):
    """Map src ids -> main ids by name.  Adds entities that only exist in src when include_unique."""
    db.execute("DROP TABLE IF EXISTS temp.%s" % mapname)
    db.execute("CREATE TEMP TABLE %s(old INTEGER PRIMARY KEY, new INTEGER, factor REAL NOT NULL DEFAULT 1.0)"
               % mapname)
    unique_n = scalar(db, "SELECT COUNT(*) FROM src.%s s WHERE s.%s NOT IN (SELECT %s FROM main.%s)"
                      % (q(table), q(keycol), q(keycol), q(table)))
    added = 0
    if include_unique and unique_n:
        dest, sel = colsql(db, table, {idcol: None})
        cur = db.execute("INSERT INTO main.%s (%s) SELECT %s FROM src.%s s WHERE s.%s NOT IN "
                         "(SELECT %s FROM main.%s) ORDER BY s.%s"
                         % (q(table), dest, sel, q(table), q(keycol), q(keycol), q(table), q(idcol)))
        added = cur.rowcount
    db.execute("INSERT INTO temp.%s(old, new) SELECT s.%s, m.%s FROM src.%s s JOIN main.%s m ON m.%s = s.%s"
               % (mapname, q(idcol), q(idcol), q(table), q(table), q(keycol), q(keycol)))
    report["%s: matched by name" % table] = scalar(db, "SELECT COUNT(*) FROM temp.%s" % mapname) - added
    report["%s: only in this input -> %s" % (table, "added" if include_unique else "IGNORED")] = unique_n
    return unique_n


def apply_unit_factors(db, mismatches, src_name):
    """Record the conversion factor for each statistic of input `src_name` whose unit differs from the
    mapping DB only by an SI prefix, in the temp.st_map table used when merging the statistics rows."""
    n = 0
    for m in mismatches:
        if m["src"] == src_name and m["unit_differs"] and m["factor"] is not None:
            db.execute("UPDATE temp.st_map SET factor = ? WHERE old = (SELECT id FROM src.statistics_meta "
                       "WHERE statistic_id = ?)", (m["factor"], m["sid"]))
            n += 1
    return n


def merge_event_types(db, report):
    """Add event types the output does not have yet and build temp.et_map (input id -> output id) by name."""
    db.execute("DROP TABLE IF EXISTS temp.et_map")
    db.execute("CREATE TEMP TABLE et_map(old INTEGER PRIMARY KEY, new INTEGER)")
    db.execute("INSERT INTO main.event_types(event_type) SELECT s.event_type FROM src.event_types s "
               "WHERE s.event_type NOT IN (SELECT event_type FROM main.event_types) ORDER BY s.event_type_id")
    db.execute("INSERT INTO temp.et_map SELECT s.event_type_id, m.event_type_id FROM src.event_types s "
               "JOIN main.event_types m ON m.event_type = s.event_type")


def merge_states(db, report):
    """Merge src.states (and the state_attributes they use) into main.

    Ids are not stable between databases, so: states_meta ids are mapped by entity_id (temp.sm_map, built
    earlier); a state is a duplicate if the output already has one for the same entity at the same
    last_updated_ts (the output's row wins); attributes are shared between states, so they are de-duplicated
    by (hash, content) against what is already in the output.  New rows keep their original ids plus an
    offset (the output's current maximum) so they cannot collide and the links between them
    (old_state_id, attributes_id) can be translated with simple arithmetic."""
    total = scalar(db, "SELECT COUNT(*) FROM src.states")
    off = scalar(db, "SELECT COALESCE(MAX(state_id),0) FROM main.states")
    off_a = scalar(db, "SELECT COALESCE(MAX(attributes_id),0) FROM main.state_attributes")
    db.execute("DROP TABLE IF EXISTS temp.keep_states")
    db.execute("CREATE TEMP TABLE keep_states(state_id INTEGER PRIMARY KEY, new_mid INTEGER NOT NULL)")
    step("  states: selecting rows to import ...")
    db.execute("INSERT INTO temp.keep_states SELECT s.state_id, m.new FROM src.states s "
               "JOIN temp.sm_map m ON m.old = s.metadata_id "
               "WHERE NOT EXISTS (SELECT 1 FROM main.states o WHERE o.metadata_id = m.new "
               "AND o.last_updated_ts = s.last_updated_ts)")
    keep = scalar(db, "SELECT COUNT(*) FROM temp.keep_states")
    mappable = scalar(db, "SELECT COUNT(*) FROM src.states s JOIN temp.sm_map m ON m.old = s.metadata_id")

    step("  states: de-duplicating state_attributes ...")
    db.execute("DROP TABLE IF EXISTS temp.attr_map")
    db.execute("CREATE TEMP TABLE attr_map(old_id INTEGER PRIMARY KEY, existing_id INTEGER)")
    db.execute("INSERT INTO temp.attr_map SELECT a.attributes_id, "
               "(SELECT MIN(o.attributes_id) FROM main.state_attributes o "
               " WHERE o.hash = a.hash AND o.shared_attrs = a.shared_attrs) "
               "FROM src.state_attributes a WHERE a.attributes_id IN "
               "(SELECT s.attributes_id FROM src.states s JOIN temp.keep_states k ON k.state_id = s.state_id)")
    dest, sel = colsql(db, "state_attributes", {"attributes_id": "a.attributes_id + %d" % off_a}, alias="a")
    cur = db.execute("INSERT INTO main.state_attributes (%s) SELECT %s FROM src.state_attributes a "
                     "JOIN temp.attr_map m ON m.old_id = a.attributes_id WHERE m.existing_id IS NULL "
                     "ORDER BY a.attributes_id" % (dest, sel))
    attrs_new = cur.rowcount
    attrs_reused = scalar(db, "SELECT COUNT(*) FROM temp.attr_map WHERE existing_id IS NOT NULL")

    step("  states: inserting %d rows ..." % keep)
    ov = {
        "state_id": "s.state_id + %d" % off,
        "old_state_id": "CASE WHEN s.old_state_id IN (SELECT state_id FROM temp.keep_states) "
                        "THEN s.old_state_id + %d END" % off,
        "attributes_id": "(SELECT COALESCE(am.existing_id, am.old_id + %d) FROM temp.attr_map am "
                         "WHERE am.old_id = s.attributes_id)" % off_a,
        "metadata_id": "k.new_mid",
    }
    dest, sel = colsql(db, "states", ov)
    cur = db.execute("INSERT INTO main.states (%s) SELECT %s FROM src.states s "
                     "JOIN temp.keep_states k ON k.state_id = s.state_id ORDER BY s.state_id" % (dest, sel))
    report["states"] = dict(read=total, inserted=cur.rowcount, duplicates=mappable - keep,
                            unmapped=total - mappable)
    report["state_attributes"] = dict(read=attrs_new + attrs_reused, inserted=attrs_new, duplicates=attrs_reused,
                                      unmapped=0)


def merge_events(db, report):
    """Merge src.events (and the event_data they use) into main, with the same approach as merge_states.

    The duplicate key is (event type, time_fired_ts, context): several different events can fire in the same
    instant, but they have different contexts."""
    total = scalar(db, "SELECT COUNT(*) FROM src.events")
    off = scalar(db, "SELECT COALESCE(MAX(event_id),0) FROM main.events")
    off_d = scalar(db, "SELECT COALESCE(MAX(data_id),0) FROM main.event_data")
    db.execute("DROP TABLE IF EXISTS temp.keep_events")
    db.execute("CREATE TEMP TABLE keep_events(event_id INTEGER PRIMARY KEY, new_tid INTEGER NOT NULL)")
    step("  events: selecting rows to import ...")
    db.execute("INSERT INTO temp.keep_events SELECT e.event_id, m.new FROM src.events e "
               "JOIN temp.et_map m ON m.old = e.event_type_id "
               "WHERE NOT EXISTS (SELECT 1 FROM main.events o WHERE o.event_type_id = m.new "
               "AND o.time_fired_ts = e.time_fired_ts AND o.context_id_bin IS e.context_id_bin)")
    keep = scalar(db, "SELECT COUNT(*) FROM temp.keep_events")
    mappable = scalar(db, "SELECT COUNT(*) FROM src.events e JOIN temp.et_map m ON m.old = e.event_type_id")
    db.execute("DROP TABLE IF EXISTS temp.data_map")
    db.execute("CREATE TEMP TABLE data_map(old_id INTEGER PRIMARY KEY, existing_id INTEGER)")
    db.execute("INSERT INTO temp.data_map SELECT d.data_id, "
               "(SELECT MIN(o.data_id) FROM main.event_data o WHERE o.hash = d.hash AND o.shared_data = d.shared_data) "
               "FROM src.event_data d WHERE d.data_id IN "
               "(SELECT e.data_id FROM src.events e JOIN temp.keep_events k ON k.event_id = e.event_id)")
    dest, sel = colsql(db, "event_data", {"data_id": "d.data_id + %d" % off_d}, alias="d")
    cur = db.execute("INSERT INTO main.event_data (%s) SELECT %s FROM src.event_data d "
                     "JOIN temp.data_map m ON m.old_id = d.data_id WHERE m.existing_id IS NULL "
                     "ORDER BY d.data_id" % (dest, sel))
    d_new = cur.rowcount
    d_reused = scalar(db, "SELECT COUNT(*) FROM temp.data_map WHERE existing_id IS NOT NULL")
    step("  events: inserting %d rows ..." % keep)
    ov = {
        "event_id": "s.event_id + %d" % off,
        "data_id": "(SELECT COALESCE(dm.existing_id, dm.old_id + %d) FROM temp.data_map dm "
                   "WHERE dm.old_id = s.data_id)" % off_d,
        "event_type_id": "k.new_tid",
    }
    dest, sel = colsql(db, "events", ov)
    cur = db.execute("INSERT INTO main.events (%s) SELECT %s FROM src.events s "
                     "JOIN temp.keep_events k ON k.event_id = s.event_id ORDER BY s.event_id" % (dest, sel))
    report["events"] = dict(read=total, inserted=cur.rowcount, duplicates=mappable - keep,
                            unmapped=total - mappable)
    report["event_data"] = dict(read=d_new + d_reused, inserted=d_new, duplicates=d_reused, unmapped=0)


def merge_stats(db, table, report):
    """Merge src.<table> (statistics or statistics_short_term) into main, remapping metadata_id by
    statistic_id and scaling values by the unit conversion factor (1.0 when the units match).
    A row is a duplicate if the output already has the same (statistic, start_ts)."""
    total = scalar(db, "SELECT COUNT(*) FROM src.%s" % q(table))
    mappable = scalar(db, "SELECT COUNT(*) FROM src.%s s JOIN temp.st_map m ON m.old = s.metadata_id" % q(table))
    ov = {"id": None, "metadata_id": "m.new"}
    for c in ("mean", "min", "max", "state", "sum"):
        ov[c] = "s.%s * m.factor" % q(c)
    dest, sel = colsql(db, table, ov)
    step("  %s: inserting ..." % table)
    cur = db.execute("INSERT INTO main.%s (%s) SELECT %s FROM src.%s s JOIN temp.st_map m ON m.old = s.metadata_id "
                     "WHERE NOT EXISTS (SELECT 1 FROM main.%s o WHERE o.metadata_id = m.new AND o.start_ts = s.start_ts) "
                     "ORDER BY s.start_ts" % (q(table), dest, sel, q(table), q(table)))
    report[table] = dict(read=total, inserted=cur.rowcount, duplicates=mappable - cur.rowcount,
                         unmapped=total - mappable)


def merge_runs(db, report):
    """Merge recorder_runs and statistics_runs.  They are only bookkeeping; duplicates are matched by start time."""
    for table, key in (("recorder_runs", "start"), ("statistics_runs", "start")):
        total = scalar(db, "SELECT COUNT(*) FROM src.%s" % q(table))
        dest, sel = colsql(db, table, {"run_id": None})
        cur = db.execute("INSERT INTO main.%s (%s) SELECT %s FROM src.%s s WHERE NOT EXISTS "
                         "(SELECT 1 FROM main.%s o WHERE o.%s = s.%s) ORDER BY s.run_id"
                         % (q(table), dest, sel, q(table), q(table), q(key), q(key)))
        report[table] = dict(read=total, inserted=cur.rowcount, duplicates=total - cur.rowcount, unmapped=0)


# Indexes the merge queries depend on (one per lookup the NOT EXISTS / duplicate checks do).  Home Assistant
# databases normally have equivalents; databases rebuilt with .recover sometimes do not, and without them the
# merge would scan whole tables once per row.  Any that are missing are created temporarily (and dropped).
HELPER_INDEXES = [
    ("states", "_hamerge_states", "metadata_id, last_updated_ts"),
    ("state_attributes", "_hamerge_attrs", "hash"),
    ("events", "_hamerge_events", "event_type_id, time_fired_ts"),
    ("event_data", "_hamerge_edata", "hash"),
    ("statistics", "_hamerge_stats", "metadata_id, start_ts"),
    ("statistics_short_term", "_hamerge_stats_st", "metadata_id, start_ts"),
    ("recorder_runs", "_hamerge_rr", "start"),
    ("statistics_runs", "_hamerge_sr", "start"),
]


def orphan_counts(db):
    """Count rows that point at a row that does not exist (dangling foreign keys), per relationship."""
    c = {}
    c["states.metadata_id -> states_meta"] = scalar(
        db, "SELECT COUNT(*) FROM states s WHERE s.metadata_id IS NOT NULL AND NOT EXISTS "
            "(SELECT 1 FROM states_meta m WHERE m.metadata_id = s.metadata_id)")
    c["states.attributes_id -> state_attributes"] = scalar(
        db, "SELECT COUNT(*) FROM states s WHERE s.attributes_id IS NOT NULL AND NOT EXISTS "
            "(SELECT 1 FROM state_attributes a WHERE a.attributes_id = s.attributes_id)")
    c["states.old_state_id -> states"] = scalar(
        db, "SELECT COUNT(*) FROM states s WHERE s.old_state_id IS NOT NULL AND NOT EXISTS "
            "(SELECT 1 FROM states o WHERE o.state_id = s.old_state_id)")
    c["events.event_type_id -> event_types"] = scalar(
        db, "SELECT COUNT(*) FROM events e WHERE e.event_type_id IS NOT NULL AND NOT EXISTS "
            "(SELECT 1 FROM event_types t WHERE t.event_type_id = e.event_type_id)")
    c["events.data_id -> event_data"] = scalar(
        db, "SELECT COUNT(*) FROM events e WHERE e.data_id IS NOT NULL AND NOT EXISTS "
            "(SELECT 1 FROM event_data d WHERE d.data_id = e.data_id)")
    c["statistics.metadata_id -> statistics_meta"] = scalar(
        db, "SELECT COUNT(*) FROM statistics s WHERE NOT EXISTS (SELECT 1 FROM statistics_meta m WHERE m.id = s.metadata_id)")
    return c


# --------------------------------------------------------------------------- synthetic states
SYNTH_FLAG = "ha_merge_db_synthetic"
SYNTH_OFFSET = 1800  # seconds: place each synthetic state in the middle of the hour it represents


def fnv1a_32(data):
    """32-bit FNV-1a hash, the hash Home Assistant uses for state_attributes.hash / event_data.hash."""
    h = 0x811C9DC5
    for b in data:
        h = ((h ^ b) * 0x01000193) & 0xFFFFFFFF
    return h


def fill_states_from_statistics(db, gap_hours, result):
    """Create one estimated state per hour, from the long-term statistics, for every sensor that has
    statistics inside a gap in the merged states table.  Each synthetic state carries the sensor's normal
    attributes plus a SYNTH_FLAG marker attribute."""
    buckets = [r[0] for r in db.execute(
        "SELECT DISTINCT CAST(last_updated_ts / 3600 AS INTEGER) FROM main.states "
        "WHERE last_updated_ts IS NOT NULL ORDER BY 1")]
    windows = []
    for a, b in zip(buckets, buckets[1:]):
        if b - a - 1 > gap_hours:
            windows.append(((a + 1) * 3600, b * 3600))
    result["windows"] = windows
    result["rows"] = 0
    result["attrs"] = 0
    result["entities"] = set()
    result["skipped_no_entity"] = 0
    if not windows:
        return
    entities = db.execute(
        "SELECT m.id, sm.metadata_id, m.statistic_id, m.unit_of_measurement FROM main.statistics_meta m "
        "JOIN main.states_meta sm ON sm.entity_id = m.statistic_id WHERE m.source = 'recorder'").fetchall()
    result["skipped_no_entity"] = scalar(
        db, "SELECT COUNT(*) FROM main.statistics_meta m WHERE m.source = 'recorder' AND m.statistic_id NOT IN "
            "(SELECT entity_id FROM main.states_meta)")
    result["skipped_names"] = [r[0] for r in db.execute(
        "SELECT m.statistic_id FROM main.statistics_meta m WHERE m.source = 'recorder' AND m.statistic_id NOT IN "
        "(SELECT entity_id FROM main.states_meta) ORDER BY 1 LIMIT 500")]
    state_cols = set(table_cols(db, "main", "states"))
    for (lo, hi) in windows:
        step("  filling %s .. %s from statistics" % (fmt_ts(lo), fmt_ts(hi)))
        for stat_id, smid, entity_id, unit in entities:
            n = scalar(db, "SELECT COUNT(*) FROM main.statistics WHERE metadata_id=? AND start_ts + ? >= ? "
                           "AND start_ts + ? < ?", (stat_id, SYNTH_OFFSET, lo, SYNTH_OFFSET, hi))
            if not n:
                continue
            row = db.execute("SELECT a.shared_attrs FROM main.states s JOIN main.state_attributes a "
                             "ON a.attributes_id = s.attributes_id WHERE s.metadata_id = ? AND s.last_updated_ts >= ? "
                             "ORDER BY s.last_updated_ts LIMIT 1", (smid, hi)).fetchone()
            if not row:
                row = db.execute("SELECT a.shared_attrs FROM main.states s JOIN main.state_attributes a "
                                 "ON a.attributes_id = s.attributes_id WHERE s.metadata_id = ? AND s.last_updated_ts < ? "
                                 "ORDER BY s.last_updated_ts DESC LIMIT 1", (smid, lo)).fetchone()
            try:
                attrs = json.loads(row[0]) if row and row[0] else {}
                if not isinstance(attrs, dict):
                    attrs = {}
            except ValueError:
                attrs = {}
            if "unit_of_measurement" not in attrs and unit:
                attrs["unit_of_measurement"] = unit
            attrs[SYNTH_FLAG] = True
            blob = json.dumps(attrs, separators=(",", ":"), ensure_ascii=False)
            cur = db.execute("INSERT INTO main.state_attributes (hash, shared_attrs) VALUES (?, ?)",
                             (fnv1a_32(blob.encode("utf-8")), blob))
            attr_id = cur.lastrowid
            result["attrs"] += 1
            cols = ["state", "last_changed_ts", "last_reported_ts", "last_updated_ts", "attributes_id",
                    "metadata_id", "origin_idx"]
            cols = [c for c in cols if c in state_cols]
            exprs = {
                "state": "CAST(round(CASE WHEN m.has_sum = 1 THEN COALESCE(t.state, t.mean) "
                         "ELSE COALESCE(t.mean, t.state) END, 4) AS TEXT)",
                "last_changed_ts": "t.start_ts + %d" % SYNTH_OFFSET,
                "last_reported_ts": "t.start_ts + %d" % SYNTH_OFFSET,
                "last_updated_ts": "t.start_ts + %d" % SYNTH_OFFSET,
                "attributes_id": str(attr_id),
                "metadata_id": str(smid),
                "origin_idx": "0",
            }
            cur = db.execute(
                "INSERT INTO main.states (%s) SELECT %s FROM main.statistics t JOIN main.statistics_meta m "
                "ON m.id = t.metadata_id WHERE t.metadata_id = ? AND t.start_ts + ? >= ? AND t.start_ts + ? < ? "
                "AND COALESCE(t.mean, t.state) IS NOT NULL ORDER BY t.start_ts"
                % (", ".join(q(c) for c in cols), ", ".join(exprs[c] for c in cols)),
                (stat_id, SYNTH_OFFSET, lo, SYNTH_OFFSET, hi))
            result["rows"] += cur.rowcount
            if cur.rowcount:
                result["entities"].add(entity_id)


# --------------------------------------------------------------------------- sum health checks
def _sql_str(s):
    """Quote a string as an SQL literal (for generated SQL that a human will review and run)."""
    return "'" + s.replace("'", "''") + "'"


def scan_sum_steps(conn, table, factor=20.0):
    """Find single-step discontinuities in the running 'sum' of has_sum statistics.

    A step is flagged when the change in sum is far outside the statistic's normal range (more than
    `factor` x its 99th-percentile hourly change) AND does not match the change in the sensor's state.
    Normal consumption, counter resets (sum keeps growing while state drops) and long gaps are not flagged.
    Returns a list of dicts, each with the shift that would remove the step from that row onward."""
    import itertools
    out = []
    if not has_table(conn, "main", table):
        return out
    cur = conn.execute(
        "SELECT m.statistic_id, s.metadata_id, s.start_ts, s.sum, s.state FROM main.%s s "
        "JOIN main.statistics_meta m ON m.id = s.metadata_id "
        "WHERE m.has_sum = 1 AND s.sum IS NOT NULL AND s.state IS NOT NULL "
        "ORDER BY s.metadata_id, s.start_ts" % q(table))
    for mid, grp in itertools.groupby(cur, key=lambda r: r[1]):
        rows = list(grp)
        if len(rows) < 3:
            continue
        ds = sorted(abs(rows[i][3] - rows[i - 1][3]) for i in range(1, len(rows)))
        thr = max(factor * ds[int(0.99 * (len(ds) - 1))], 1.0)
        for i in range(1, len(rows)):
            dsum = rows[i][3] - rows[i - 1][3]
            dstate = rows[i][4] - rows[i - 1][4]
            expected = dstate if dstate >= 0 else rows[i][4]      # a falling state means a counter reset
            if abs(dsum) > thr and abs(dsum - expected) > 0.5 * abs(dsum):
                out.append(dict(statistic_id=rows[i][0], table=table, ts=rows[i][2], dsum=dsum,
                                expected=expected, shift=expected - dsum))
    return out


def check_short_term_consistency(conn):
    """Long-term 'sum' for an hour must equal the sum of the last short-term row in that hour.  Where it
    does not, Home Assistant will snap the sum back to the short-term baseline once it starts recording.
    Compared at the earliest hour where both tables have data.  Returns a list of mismatch dicts."""
    if not (has_table(conn, "main", "statistics") and has_table(conn, "main", "statistics_short_term")):
        return []
    out = []
    for sid, mid, ts, lsum, ssum in conn.execute(
            "SELECT m.statistic_id, l.metadata_id, MIN(l.start_ts), l.sum, st.sum FROM main.statistics l "
            "JOIN main.statistics_short_term st ON st.metadata_id = l.metadata_id AND st.start_ts = l.start_ts + 3300 "
            "JOIN main.statistics_meta m ON m.id = l.metadata_id "
            "WHERE m.has_sum = 1 AND l.sum IS NOT NULL AND st.sum IS NOT NULL GROUP BY l.metadata_id").fetchall():
        if abs(lsum - ssum) > max(0.01, 1e-7 * abs(lsum)):
            out.append(dict(statistic_id=sid, ts=ts, long_term=lsum, short_term=ssum, diff=lsum - ssum))
    return out


def fix_sql(steps, mismatches):
    """SQL that removes the discontinuities found by scan_sum_steps / check_short_term_consistency.
    Rows from a step onward are shifted in BOTH tables (they share the new baseline); short-term rows before
    the first step (or all of them when there is no step) are shifted by the long/short mismatch."""
    L = ["-- Generated by ha_merge_db.py --check-db.  REVIEW BEFORE RUNNING.",
         "-- Stop Home Assistant first, back up the database, run once, then re-run --check-db to confirm.",
         ".bail on", "BEGIN;"]
    first_step = {}
    for s in steps:
        sel = "(SELECT id FROM statistics_meta WHERE statistic_id=%s)" % _sql_str(s["statistic_id"])
        tables = ["statistics", "statistics_short_term"] if s["table"] == "statistics" else [s["table"]]
        for t in tables:
            L.append("UPDATE %s SET sum = sum + (%.6f) WHERE start_ts >= %d AND metadata_id = %s;"
                     % (t, s["shift"], s["ts"], sel))
        if s["table"] == "statistics" and (s["statistic_id"] not in first_step or s["ts"] < first_step[s["statistic_id"]]):
            first_step[s["statistic_id"]] = s["ts"]
    for m in mismatches:
        sel = "(SELECT id FROM statistics_meta WHERE statistic_id=%s)" % _sql_str(m["statistic_id"])
        cond = ""
        if m["statistic_id"] in first_step:
            cond = " AND start_ts < %d" % first_step[m["statistic_id"]]
        L.append("UPDATE statistics_short_term SET sum = sum + (%.6f) WHERE metadata_id = %s%s;"
                 % (m["diff"], sel, cond))
    L.append("COMMIT;")
    return "\n".join(L) + "\n"


# --------------------------------------------------------------------------- main
EXIT_OK, EXIT_BAD_ARGS, EXIT_INPUT_PROBLEM, EXIT_MERGE_FAILED, EXIT_WARNINGS = 0, 1, 2, 3, 4


class Report(object):
    """Collects everything that needs attention (warnings) and structured data for --report."""

    def __init__(self):
        """Start with no warnings, notes or data."""
        self.warnings = []
        self.notes = []
        self.data = {}

    def warn(self, msg):
        """Record and print something that needs the user's attention (makes the exit code 4)."""
        self.warnings.append(msg)
        log("  WARNING: " + msg)

    def note(self, msg):
        """Record and print something informational (a correction that was made, a caveat)."""
        self.notes.append(msg)
        log("  note: " + msg)


def parse_args(argv=None):
    """Define and parse the command line.  `argv` defaults to sys.argv[1:] (a list is accepted for tests)."""
    p = argparse.ArgumentParser(
        description="Merge several Home Assistant recorder SQLite databases (same system, different time "
                    "ranges) into a NEW database, remapping ids using the database named by --mapping-db.  "
                    "Also: --check-db scans an existing database for statistics 'sum' discontinuities.",
        epilog="Examples:\n"
               "  %(prog)s --dry-run --mapping-db newer.db -o combined.db older.db\n"
               "  %(prog)s --mapping-db newer.db -o combined.db older.db\n"
               "  %(prog)s --check-db home-assistant_v2.db > fix.sql\n\n"
               "Exit codes: 0 ok, 1 bad arguments, 2 problem with an input (nothing written), 3 merge failed, "
               "4 completed but with warnings that need attention (see the output / --report).",
        formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("inputs", nargs="*", metavar="INPUT_DB", help="databases to merge (the mapping DB is "
                   "always included, so listing it is optional)")
    p.add_argument("--mapping-db", metavar="DB",
                   help="database whose entity/statistic/event-type id mappings are authoritative; the output "
                        "starts as a copy of it and the other inputs are merged in.  Normally the NEWEST "
                        "database, because Home Assistant carries on from the newest data")
    p.add_argument("-o", "--output", metavar="DB", help="new database to create")
    p.add_argument("--tables", default=",".join(DEFAULT_GROUPS), metavar="LIST",
                   help="comma-separated groups to merge, or 'all'.  Groups: %s.  Default: %s "
                        "(short-term statistics are excluded by default)"
                        % (", ".join(ALL_GROUPS), ",".join(DEFAULT_GROUPS)))
    p.add_argument("--unique-entities", choices=["include", "ignore"], default="include",
                   help="entities/statistics that exist only in a non-mapping input: 'include' adds them to the "
                        "output (default); 'ignore' drops their rows")
    p.add_argument("--on-overlap", choices=["skip", "error"], default="skip",
                   help="when inputs cover overlapping time ranges: 'skip' keeps the mapping DB's rows and drops "
                        "duplicate incoming rows (default, with a warning); 'error' aborts")
    p.add_argument("--no-fix-sum-offset", dest="fix_sum_offset", action="store_false",
                   help="do NOT correct statistics 'sum' discontinuities at the joins between inputs.  By default "
                        "the OLDER data is shifted so the running sum continues smoothly into the newest data "
                        "(which is left untouched); without the correction, energy totals spike at the join")
    p.add_argument("--fix-sum-offset", dest="fix_sum_offset", action="store_true", help=argparse.SUPPRESS)
    p.add_argument("--no-convert-units", dest="convert_units", action="store_false",
                   help="do NOT convert statistics whose unit differs from the mapping database only by an SI "
                        "prefix (Wh vs kWh, W vs kW ...).  By default incoming values are scaled to the mapping "
                        "unit; without this they are imported unconverted, i.e. wrong by 1000x")
    p.add_argument("--convert-units", dest="convert_units", action="store_true", help=argparse.SUPPRESS)
    p.set_defaults(fix_sum_offset=True, convert_units=True)
    p.add_argument("--fill-states-from-statistics", action="store_true",
                   help="ESTIMATE (cosmetic): for gaps in the merged states (where recorded states were purged), "
                        "write one synthetic state per hour per sensor from the long-term statistics so the "
                        "History panel draws a line instead of a flat carry-forward.  Values are hourly "
                        "averages, not real readings; each has a 'ha_merge_db_synthetic' attribute.  After "
                        "Home Assistant's nightly purge the History panel shows the same data from the "
                        "statistics anyway.  Requires --tables to include states and statistics")
    p.add_argument("--gap-threshold", type=float, default=24.0, metavar="HOURS",
                   help="report gaps between inputs longer than this (default 24)")
    p.add_argument("--quick-check", action="store_true",
                   help="use PRAGMA quick_check instead of the slower, fuller integrity_check")
    p.add_argument("--dry-run", action="store_true", help="run all checks and analysis but write nothing")
    p.add_argument("--force", action="store_true", help="overwrite the output file if it already exists")
    p.add_argument("--vacuum", action="store_true", help="VACUUM the result (slower, needs extra disk space)")
    p.add_argument("--report", metavar="FILE",
                   help="also write a machine-readable JSON report (inputs, coverage, warnings, corrections "
                        "applied, row counts, checks) to FILE")
    p.add_argument("--check-db", metavar="DB",
                   help="instead of merging, scan an existing database (for example the one Home Assistant is "
                        "using; copy it first or stop HA) for statistics 'sum' steps and short-term/long-term "
                        "inconsistencies, and print SQL (on stdout) that repairs them.  Exit 4 if problems found")
    p.add_argument("--version", action="version", version="%(prog)s " + VERSION)
    args = p.parse_args(argv)
    if not args.check_db and not (args.mapping_db and args.output):
        p.error("--mapping-db and -o/--output are required (unless using --check-db)")
    return args


def write_report(args, rep, code):
    """Write the --report JSON file."""
    doc = dict(tool="ha_merge_db", version=VERSION, generated=datetime.datetime.now(datetime.timezone.utc).isoformat(),
               exit_code=code, arguments={k: v for k, v in vars(args).items()}, warnings=rep.warnings,
               notes=rep.notes, details=rep.data)
    with open(args.report, "w") as fh:
        json.dump(doc, fh, indent=2, default=str)
    log("  report written to %s" % args.report)


def print_sum_findings(steps, mism, limit=25):
    """Print up to `limit` sum steps and short-term/long-term mismatches in a readable form."""
    for s in steps[:limit]:
        log("    %s  %s: sum steps by %.4f at %s where the state changes by %.4f (shift %+.4f)"
            % (s["table"], s["statistic_id"], s["dsum"], fmt_ts(s["ts"]), s["expected"], s["shift"]))
    if len(steps) > limit:
        log("    ... and %d more steps" % (len(steps) - limit))
    for m in mism[:limit]:
        log("    short-term vs long-term: %s long-term sum %.4f but short-term %.4f at %s (diff %+.4f)"
            % (m["statistic_id"], m["long_term"], m["short_term"], fmt_ts(m["ts"]), m["diff"]))
    if len(mism) > limit:
        log("    ... and %d more mismatches" % (len(mism) - limit))


def run_check_db(args, rep):
    """Implement --check-db: scan one database, print a summary (stderr) and repair SQL (stdout).
    Returns EXIT_OK if clean, EXIT_WARNINGS if problems were found."""
    src = Source(os.path.realpath(args.check_db), True)
    open_source(src, args.quick_check)
    if src.problems:
        for msg, repair in src.problems:
            log("\n  X %s\n    %s" % (src.name, msg))
            if repair:
                log("    To attempt a repair run:\n      " + repair_hint(src.path))
        return EXIT_INPUT_PROBLEM
    for w in src.warnings:
        log("  ! %s: %s" % (src.name, w))
    step("scanning %s for statistics sum discontinuities ..." % src.name)
    steps = scan_sum_steps(src.conn, "statistics") + scan_sum_steps(src.conn, "statistics_short_term")
    mism = check_short_term_consistency(src.conn)
    rep.data["steps"] = steps
    rep.data["short_term_mismatches"] = mism
    if not steps and not mism:
        log("\nNo sum discontinuities or short-term/long-term inconsistencies found.")
        return EXIT_OK
    log("\n%d sum step(s) and %d short-term/long-term mismatch(es) found:" % (len(steps), len(mism)))
    print_sum_findings(steps, mism)
    rep.warnings.append("%d sum steps, %d short-term/long-term mismatches" % (len(steps), len(mism)))
    sys.stdout.write(fix_sql(steps, mism))
    log("\nRepair SQL written to stdout (redirect it to a file).  Review it, stop Home Assistant, back up the "
        "database, run it ONCE with sqlite3, then re-run --check-db to confirm.")
    return EXIT_WARNINGS


def latest_time(s):
    """Timestamp of the newest row in any of the (selected) tables of a source, or None."""
    vals = [r[1] for g, r in s.ranges.items() if r[1] is not None]
    return max(vals) if vals else None


class Plan(object):
    """The validated request: which table groups to merge, which files, and where the output goes.

    Invalid requests exit immediately with an 'error:' message (exit code 1) before any database is
    opened.  `paths[0]` is always the mapping database."""

    def __init__(self, args):
        if args.tables.strip().lower() == "all":
            groups = list(ALL_GROUPS)
        else:
            groups = [g.strip() for g in args.tables.split(",") if g.strip()]
            bad = [g for g in groups if g not in ALL_GROUPS]
            if bad or not groups:
                sys.exit("error: unknown --tables group(s): %s (valid: %s)"
                         % (", ".join(bad) or "(none)", ", ".join(ALL_GROUPS)))
        self.groups = [g for g in ALL_GROUPS if g in groups]
        if args.fill_states_from_statistics and not ("states" in self.groups and "statistics" in self.groups):
            sys.exit("error: --fill-states-from-statistics needs --tables to include both states and statistics")

        # Resolve symlinks so the same file named twice is only merged once.
        self.paths = [os.path.realpath(args.mapping_db)]
        for p in args.inputs:
            rp = os.path.realpath(p)
            if rp not in self.paths:
                self.paths.append(rp)
        if len(self.paths) < 2:
            sys.exit("error: need at least one input database besides --mapping-db")

        self.out = os.path.abspath(args.output)
        self.partial = self.out + ".partial"      # written first, renamed only when verified
        # Never write over an input (or its -wal/-shm/-journal side files).
        if os.path.realpath(self.out) in self.paths:
            sys.exit("error: the output path is the same as an input database; refusing.")
        for suffix in ("-wal", "-shm", "-journal"):
            if os.path.realpath(self.out + suffix) in [p + suffix for p in self.paths]:
                sys.exit("error: output collides with an input file; refusing.")
        if os.path.exists(self.out) and not args.force and not args.dry_run:
            sys.exit("error: output %s already exists (use --force to overwrite)" % self.out)
        if not os.path.isdir(os.path.dirname(self.out)):
            sys.exit("error: output directory does not exist: %s" % os.path.dirname(self.out))


class Analysis(object):
    """Everything learned about the inputs before writing anything."""

    def __init__(self):
        self.overlaps = []
        self.mismatches = []        # statistics_meta definitions that differ from the mapping DB
        self.sum_findings = []      # human-readable descriptions of sum discontinuities
        self.sum_shifts = {}        # statistic_id -> [(from_ts, to_ts, shift)]


class MergeFailed(Exception):
    """The merge could not be completed; the partial output is discarded and the inputs are untouched."""


def open_inputs(plan, args, rep):
    """Open and check every input.  Returns the list of Sources, or None if any has a problem.

    All problems are reported together (not just the first) so the user can fix them in one go."""
    sources = [Source(p, is_mapping=(i == 0)) for i, p in enumerate(plan.paths)]
    for s in sources:
        open_source(s, args.quick_check)
    if all(s.conn for s in sources):
        check_schemas(sources, plan.groups)

    bad = [s for s in sources if s.problems]
    if bad:
        log("\n=== PROBLEMS FOUND WITH INPUT DATABASES - nothing was written ===")
        for s in sources:
            for msg, repair in s.problems:
                log("\n  X %s%s\n    %s" % (s.name, "  [MAPPING DB]" if s.is_mapping else "", msg))
                if repair:
                    log("    To attempt a repair run:\n      " + repair_hint(s.path))
        log("")
        rep.data["input_problems"] = {s.name: [m for m, _ in s.problems] for s in bad}
        return None
    for s in sources:
        for w in s.warnings:
            log("  ! %s: %s" % (s.name, w))
            rep.notes.append("%s: %s" % (s.name, w))
        step("%s%s: schema v%s, %s" % (s.name, " [mapping]" if s.is_mapping else "", s.schema_version,
                                       fmt_size(s.size)))
    return sources


def analyse_inputs(plan, args, rep, sources):
    """Print and record what the merge will have to deal with: coverage, overlaps, gaps, entity
    differences, unit differences and sum discontinuities.  Returns an Analysis, or an exit code (int)
    if the run must stop (overlap with --on-overlap error)."""
    groups = plan.groups
    an = Analysis()
    gather_ranges(sources, groups)
    rep.data["inputs"] = [dict(name=s.name, path=s.path, size=s.size, schema=s.schema_version, mapping=s.is_mapping,
                               coverage={g: dict(first=fmt_ts(r[0]), last=fmt_ts(r[1]), rows=r[2])
                                         for g, r in s.ranges.items()}) for s in sources]
    log("\n--- time coverage")
    for g in groups:
        if g not in GROUP_TIME:
            continue
        for s in sources:
            lo, hi, n = s.ranges[g]
            log("  %-22s %-60s %s .. %s  (%d rows)" % (g, s.name[:60], fmt_ts(lo), fmt_ts(hi), n))

    # Overlapping ranges are not fatal: duplicates are skipped, with the mapping DB's rows winning.
    an.overlaps, gaps = analyse_time(sources, groups, args.gap_threshold)
    rep.data["gaps"] = gaps
    for gp in gaps:
        log("  NOTE  gap: " + gp)
    if an.overlaps:
        log("\n  time ranges overlap - duplicate rows will be %s:"
            % ("rejected (--on-overlap error)" if args.on_overlap == "error" else
               "skipped (mapping database's rows win)"))
        for o in an.overlaps:
            log("    " + o)
        rep.data["overlaps"] = an.overlaps
        if args.on_overlap == "error":
            rep.warnings.append("input time ranges overlap (--on-overlap error)")
            return EXIT_INPUT_PROBLEM
        rep.warn("input time ranges overlap; duplicate rows will be skipped (the mapping database's rows win)")

    # Home Assistant carries on from the newest data, so the mapping DB should be the newest input.
    newest = max(sources[1:], key=lambda s: latest_time(s) or 0)
    lt_map, lt_new = latest_time(sources[0]) or 0, latest_time(newest) or 0
    if lt_new > lt_map + 3600:
        rep.warn("the mapping DB (%s) ends at %s but %s has newer data (up to %s).  Home Assistant carries on from "
                 "the newest data, so the newest database is normally the right mapping DB."
                 % (sources[0].name, fmt_ts(lt_map), newest.name, fmt_ts(lt_new)))

    log("\n--- entity mapping vs mapping database")
    ent = analyse_entities(sources, groups)
    rep.data["entities"] = [dict(table=t, input=n, common=c, only_in_input=oi, only_in_mapping=om)
                            for t, n, c, oi, om in ent]
    for table, name, common, only_in, only_map in ent:
        log("  %-16s %-60s common %d, only in this input %d (%s), only in mapping DB %d"
            % (table, name[:60], common, only_in, "will be " + ("added" if args.unique_entities == "include"
                                                                else "IGNORED"), only_map))

    if "statistics" in groups or "statistics_short_term" in groups:
        report_unit_differences(args, rep, an, sources)
    if "statistics" in groups:
        report_sum_discontinuities(args, rep, an, sources)
    return an


def report_unit_differences(args, rep, an, sources):
    """Find statistics recorded in a different unit than in the mapping DB and say what will happen."""
    an.mismatches = analyse_meta_mismatch(sources)
    units = [m for m in an.mismatches if m["unit_differs"]]
    other = [m for m in an.mismatches if not m["unit_differs"]]
    rep.data["unit_differences"] = [dict(statistic=m["sid"], input=m["src"], unit=m["mine"][1],
                                         mapping_unit=m["base"][1], factor=m["factor"]) for m in units]
    if units:
        log("\n  %d statistic(s) use a DIFFERENT UNIT than in the mapping database:" % len(units))
        for m in units[:15]:
            f = m["factor"]
            log("    %s: %s uses '%s', mapping DB uses '%s' (%s)"
                % (m["sid"], m["src"][:30], m["mine"][1], m["base"][1],
                   ("x%g conversion" % f) if f is not None else "NOT auto-convertible"))
        if len(units) > 15:
            log("    ... and %d more" % (len(units) - 15))
        convertible = [m for m in units if m["factor"] is not None]
        not_convertible = [m for m in units if m["factor"] is None]
        if args.convert_units:
            rep.note("%d statistic(s) will be converted to the mapping unit" % len(convertible))
            if not_convertible:
                rep.warn("%d statistic(s) differ in unit and cannot be converted automatically; they will be "
                         "imported unconverted: %s" % (len(not_convertible),
                                                       ", ".join(m["sid"] for m in not_convertible[:5])))
        else:
            rep.warn("%d statistic(s) with a different unit will be imported UNCONVERTED (--no-convert-units); "
                     "their values will be wrong by the unit ratio" % len(units))
    if other:
        rep.note("%d statistic(s) differ from the mapping DB in source/has_sum/has_mean (mapping definition "
                 "wins)" % len(other))


def report_sum_discontinuities(args, rep, an, sources):
    """Find running-sum discontinuities at the joins between inputs and say what will happen."""
    an.sum_findings, an.sum_shifts = analyse_sums(sources, args.convert_units)
    rep.data["sum_discontinuities"] = an.sum_findings
    if not an.sum_findings:
        log("\n  statistics 'sum' continuity across the joins: OK")
        return
    log("\n  statistics 'sum' discontinuity at the join between inputs (%d statistics); energy totals "
        "would spike or step there:" % len(an.sum_findings))
    for f in an.sum_findings[:20]:
        log("    " + f)
    if len(an.sum_findings) > 20:
        log("    ... and %d more" % (len(an.sum_findings) - 20))
    if args.fix_sum_offset:
        rep.note("the OLDER data will be shifted so the sums continue into the newest data (which is "
                 "left untouched)")
    else:
        rep.warn("%d statistics sum discontinuities will NOT be corrected (--no-fix-sum-offset); energy "
                 "dashboards will show a spike at the join" % len(an.sum_findings))


def check_disk_space(plan, sources):
    """The output is a copy of the mapping DB plus the other inputs, so budget for all of them."""
    need = int(sum(s.size for s in sources) * 1.1) + 256 * 1024 * 1024
    free = shutil.disk_usage(os.path.dirname(plan.out)).free
    log("\n  estimated disk needed for output: ~%s, free: %s" % (fmt_size(need), fmt_size(free)))
    if free < need:
        log("  X not enough free disk space in %s" % os.path.dirname(plan.out))
        return False
    return True


def copy_mapping_db(sources, db):
    """Copy the mapping database into the (new, empty) output using SQLite's online backup API."""
    def progress(status, remaining, total):
        if total and (total - remaining) % (total // 10 + 1) < 4096:
            log("          %3d%%" % (100 * (total - remaining) // total))
    sources[0].conn.backup(db, pages=4096, progress=progress)


def merge_one_source(db, s, plan, args, an):
    """Merge a single non-mapping input into the output inside one transaction; returns its row-count report.

    The input is attached read-only as schema 'src'.  Order matters: the name->id maps must exist before
    the tables that use them are merged."""
    groups = plan.groups
    want_stat = "statistics" in groups or "statistics_short_term" in groups
    include = args.unique_entities == "include"
    s.conn.close()
    s.conn = None
    db.execute("ATTACH DATABASE ? AS src", (sqlite_uri(s.path, s.immutable),))
    r = {}
    db.execute("BEGIN")
    if "states" in groups:
        build_meta_map(db, "states_meta", "metadata_id", "entity_id", include, "sm_map", r)
    if want_stat:
        build_meta_map(db, "statistics_meta", "id", "statistic_id", include, "st_map", r)
        if args.convert_units:
            r["statistics_meta: units converted"] = apply_unit_factors(db, an.mismatches, s.name)
    if "events" in groups:
        merge_event_types(db, r)
    if "states" in groups:
        merge_states(db, r)
    if "events" in groups:
        merge_events(db, r)
    for g in ("statistics", "statistics_short_term"):
        if g in groups:
            merge_stats(db, g, r)
    if "runs" in groups:
        merge_runs(db, r)
    db.execute("COMMIT")
    db.execute("DETACH DATABASE src")
    return r


def apply_sum_shifts(db, rep, shifts):
    """Shift the running sum of the OLDER data so it continues smoothly into the newest data.

    Both long-term and short-term rows in each range are shifted (short-term rows only exist for older
    inputs if they were merged; the mapping DB's own short-term rows are in the newest range, which is
    never shifted)."""
    step("shifting the older data of %d statistics so their sums continue into the newest data" % len(shifts))
    db.execute("BEGIN")
    for sid, ranges in shifts.items():
        row = db.execute("SELECT id FROM statistics_meta WHERE statistic_id=?", (sid,)).fetchone()
        if not row:
            continue
        for lo, hi, off in ranges:
            for tbl in ("statistics", "statistics_short_term"):
                if not has_table(db, "main", tbl):
                    continue
                sql = "UPDATE %s SET sum = sum + ? WHERE metadata_id = ?" % tbl
                prm = [off, row[0]]
                if lo is not None:
                    sql += " AND start_ts >= ?"
                    prm.append(lo)
                if hi is not None:
                    sql += " AND start_ts < ?"
                    prm.append(hi)
                db.execute(sql, prm)
    db.execute("COMMIT")
    rep.data["sum_shifts"] = {sid: [dict(from_ts=fmt_ts(lo) if lo is not None else None,
                                         to_ts=fmt_ts(hi) if hi is not None else None, shift=off)
                                    for lo, hi, off in rr] for sid, rr in shifts.items()}


def verify_output(db, plan, args, rep, sources, reports, synth, base_counts, baseline_orphans):
    """Check the finished output; raises MergeFailed for hard errors, records warnings for soft ones."""
    groups = plan.groups
    step("verifying result ...")
    problems = []
    # 1. Row counts: what we started with + what we inserted must equal what is there now.
    for t, before in base_counts.items():
        if t in ("states_meta", "statistics_meta", "event_types"):
            continue                                   # these grow by name lookup, not by plain insert
        after = scalar(db, "SELECT COUNT(*) FROM main.%s" % q(t))
        added = sum(r[t]["inserted"] for _, r in reports if t in r)
        if t == "states":
            added += synth.get("rows", 0)
        elif t == "state_attributes":
            added += synth.get("attrs", 0)
        if after != before + added:
            problems.append("row count mismatch in %s: %d + %d inserted != %d" % (t, before, added, after))
    # 2. Every row read from an input is accounted for.
    for _, r in reports:
        for t, v in r.items():
            if isinstance(v, dict) and v["read"] != v["inserted"] + v["duplicates"] + v["unmapped"]:
                problems.append("accounting mismatch for %s" % t)
    # 3. No NEW dangling references (a few may already exist in Home Assistant's own data).
    orph = orphan_counts(db) if has_table(db, "main", "states") else {}
    for k, v in orph.items():
        base = (baseline_orphans or {}).get(k, 0)
        if v > base and not k.startswith("states.old_state_id"):
            problems.append("%d new orphaned references: %s" % (v - base, k))
    # 4. Sum health.  A merge can only have broken the joins, so only steps near an input's first/last
    #    statistic are reported; steps elsewhere are quirks of the original data, not of the merge.
    if "statistics" in groups:
        step("checking the output for sum discontinuities ...")
        joins = [t for s_ in sources for t in (s_.ranges.get("statistics", (None, None, 0))[:2]) if t is not None]
        steps = [f for f in scan_sum_steps(db, "statistics") + scan_sum_steps(db, "statistics_short_term")
                 if any(abs(f["ts"] - j) <= 3 * 3600 for j in joins)]
        mism = check_short_term_consistency(db)
        rep.data["post_merge_sum_checks"] = dict(steps=steps, short_term_mismatches=mism)
        if steps or mism:
            log("")
            print_sum_findings(steps, mism)
            rep.warn("%d sum step(s) and %d short-term/long-term mismatch(es) remain in the output (see above); "
                     "run --check-db on the result for repair SQL" % (len(steps), len(mism)))
        else:
            log("  sum continuity checks on the output: OK")
    # 5. SQLite's own integrity check.
    step("running %s on the output ..." % ("quick_check" if args.quick_check else "integrity_check"))
    res = [r[0] for r in db.execute("PRAGMA %s" % ("quick_check" if args.quick_check else "integrity_check"))]
    if res != ["ok"]:
        problems.append("output integrity check failed: %s" % "; ".join(res[:3]))
    if problems:
        raise MergeFailed("verification failed:\n  " + "\n  ".join(problems))


def build_output(plan, args, rep, sources, an):
    """Create the merged database at plan.partial and rename it to plan.out once verified.

    Returns (per-input row-count reports, synthetic-state summary).  Raises MergeFailed / sqlite3 errors;
    the caller removes the partial file."""
    for suffix in ("", "-wal", "-shm", "-journal"):     # leftovers from an earlier, interrupted run
        if os.path.exists(plan.partial + suffix):
            os.remove(plan.partial + suffix)
    step("copying mapping database -> %s" % os.path.basename(plan.partial))
    db = sqlite3.connect("file:" + urllib.parse.quote(plan.partial, safe="/"), uri=True, isolation_level=None)
    try:
        copy_mapping_db(sources, db)
        # The output is a throw-away until verified, so trade durability for speed while building it.
        db.execute("PRAGMA journal_mode=DELETE")       # leave WAL mode if the source was in it
        db.execute("PRAGMA synchronous=OFF")
        db.execute("PRAGMA cache_size=-262144")
        db.execute("PRAGMA foreign_keys=OFF")
        db.execute("PRAGMA journal_mode=OFF")

        # The merge relies on indexes on the lookup columns; recovered databases sometimes lack them.
        created = []
        existing_idx = set(r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='index'"))
        for tbl, name, cols in HELPER_INDEXES:
            if has_table(db, "main", tbl) and name not in existing_idx:
                db.execute("CREATE INDEX IF NOT EXISTS %s ON %s(%s)" % (name, q(tbl), cols))
                created.append(name)

        base_counts = {}
        for t in set(sum((GROUP_TABLES[g] for g in plan.groups), [])):
            base_counts[t] = scalar(db, "SELECT COUNT(*) FROM main.%s" % q(t))
        baseline_orphans = orphan_counts(db) if "states" in plan.groups and "events" in plan.groups else None

        reports = []
        for s in sources[1:]:
            step("merging %s" % s.name)
            reports.append((s, merge_one_source(db, s, plan, args, an)))

        if args.fix_sum_offset and an.sum_shifts:
            apply_sum_shifts(db, rep, an.sum_shifts)

        synth = {}
        if args.fill_states_from_statistics:
            step("synthesising states from statistics for gaps > %g h" % args.gap_threshold)
            db.execute("BEGIN")
            fill_states_from_statistics(db, args.gap_threshold, synth)
            db.execute("COMMIT")

        verify_output(db, plan, args, rep, sources, reports, synth, base_counts, baseline_orphans)
        for name in created:
            db.execute("DROP INDEX IF EXISTS %s" % name)
        db.execute("PRAGMA journal_mode=DELETE")
        if args.vacuum:
            step("vacuuming ...")
            db.execute("VACUUM")
    finally:
        db.close()
    os.replace(plan.partial, plan.out)                 # atomic: the output either exists complete or not at all
    return reports, synth


def discard_partial(plan):
    """Remove the partial output (and SQLite side files) after a failed or interrupted merge."""
    for suffix in ("", "-wal", "-shm", "-journal"):
        if os.path.exists(plan.partial + suffix):
            os.remove(plan.partial + suffix)


def summarise_merge(plan, args, rep, an, sources, reports, synth):
    """Print the final report of a successful merge; returns the exit code."""
    groups = plan.groups
    log("\n=== MERGE COMPLETE: %s (%s) ===" % (plan.out, fmt_size(os.path.getsize(plan.out))))
    rep.data["merged"] = {}
    for s, r in reports:
        log("\n  from %s" % s.name)
        rep.data["merged"][s.name] = {k: v for k, v in r.items()}
        for k, v in r.items():
            if not isinstance(v, dict):
                log("    %-55s %d" % (k, v))
        for t, v in r.items():
            if isinstance(v, dict):
                log("    %-24s read %-9d inserted %-9d duplicates skipped %-8d unmapped/ignored %d"
                    % (t, v["read"], v["inserted"], v["duplicates"], v["unmapped"]))
    if an.sum_shifts and args.fix_sum_offset:
        log("\n  Corrected %d statistics sum discontinuit%s by shifting the older data; the newest data is "
            "unchanged." % (len(an.sum_shifts), "y" if len(an.sum_shifts) == 1 else "ies"))
    converted = sum(1 for m in an.mismatches if m["unit_differs"] and m["factor"] is not None)
    if args.convert_units and converted:
        log("  Converted the unit of %d statistic(s) to the mapping database's unit." % converted)
    if args.fill_states_from_statistics:
        log("\n  SYNTHETIC STATES (estimates): %d states for %d sensors, in %d gap(s):"
            % (synth.get("rows", 0), len(synth.get("entities", ())), len(synth.get("windows", ()))))
        for lo, hi in synth.get("windows", ()):
            log("    %s .. %s" % (fmt_ts(lo), fmt_ts(hi)))
        log("    These are HOURLY AVERAGES from long-term statistics (state value = mean, or the meter reading for "
            "total sensors), timestamped mid-hour, not real readings.  Each has the attribute '%s': true."
            % SYNTH_FLAG)
        log("    Skipped %d statistics with no matching entity in states_meta." % synth.get("skipped_no_entity", 0))
        log("    They are cosmetic: Home Assistant's nightly purge (states older than purge_keep_days, default "
            "10, at 04:12) deletes them, after which the History panel draws the same period from the "
            "long-term statistics.  Raise purge_keep_days (or disable auto_purge) BEFORE starting HA on this "
            "file if you want to keep the imported/synthetic states.")
        rep.data["synthetic_states"] = dict(rows=synth.get("rows", 0), sensors=len(synth.get("entities", ())),
                                            windows=[(fmt_ts(a), fmt_ts(b)) for a, b in synth.get("windows", ())],
                                            skipped_without_entity=synth.get("skipped_no_entity", 0),
                                            skipped_names=synth.get("skipped_names", []))
    if "states" in groups:
        log("  note: states.old_state_id links across input boundaries are cleared (NULL), which Home "
            "Assistant tolerates.")
    log("  Inputs were not modified.  Test the new database before using it with Home Assistant "
        "(stop HA, keep a backup of the original).")
    if rep.warnings:
        log("\n  %d warning(s) need attention:" % len(rep.warnings))
        for w in rep.warnings:
            log("    - " + w)
        return EXIT_WARNINGS
    return EXIT_OK


def run_merge(args, rep):
    """Merge the inputs named on the command line into a new database; returns the exit code.

    Stages: validate the request -> open and check inputs -> analyse (coverage, overlaps, units, sums)
    -> [stop here for --dry-run] -> build and verify the output -> report."""
    plan = Plan(args)
    log("ha_merge_db %s   tables: %s   unique entities: %s   on overlap: %s   fix sums: %s   convert units: %s"
        % (VERSION, ",".join(plan.groups), args.unique_entities, args.on_overlap,
           "yes" if args.fix_sum_offset else "NO", "yes" if args.convert_units else "NO"))

    sources = open_inputs(plan, args, rep)
    if sources is None:
        return EXIT_INPUT_PROBLEM
    an = analyse_inputs(plan, args, rep, sources)
    if isinstance(an, int):                             # analysis decided the run must stop
        return an
    if not check_disk_space(plan, sources):
        return EXIT_INPUT_PROBLEM
    if args.dry_run:
        log("\nDry run complete: all inputs are readable and consistent; nothing was written.")
        return EXIT_WARNINGS if rep.warnings else EXIT_OK

    try:
        reports, synth = build_output(plan, args, rep, sources, an)
    except (sqlite3.DatabaseError, MergeFailed) as exc:
        discard_partial(plan)
        log("\nX MERGE FAILED: %s" % exc)
        rep.data["failure"] = str(exc)
        corrupt_words = ("malformed", "corrupt", "not a database")
        if isinstance(exc, sqlite3.DatabaseError) and any(w in str(exc) for w in corrupt_words):
            log("  A database reported corruption part-way through the merge.  The input that was being "
                "read is likely damaged even though its up-front check passed.  Re-run with the full "
                "integrity_check (omit --quick-check), and repair the suspect file:")
            for s in sources:
                log("    " + repair_hint(s.path).split("\n")[0])
        log("  The partial output was removed; your input databases were not modified.")
        return EXIT_MERGE_FAILED
    return summarise_merge(plan, args, rep, an, sources, reports, synth)


def main(argv=None):
    """Entry point.  Returns the process exit code; closes every source connection on the way out."""
    args = parse_args(argv)
    rep = Report()
    try:
        code = run_check_db(args, rep) if args.check_db else run_merge(args, rep)
    finally:
        while _OPEN_CONNECTIONS:
            try:
                _OPEN_CONNECTIONS.pop().close()
            except sqlite3.Error:
                pass
    if args.report:
        write_report(args, rep, code)
    return code


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        log("\ninterrupted - any '.partial' output file can be deleted")
        sys.exit(130)
