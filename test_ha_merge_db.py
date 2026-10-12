#!/usr/bin/env python3
"""
Automated tests for ha_merge_db.py.  They build tiny Home Assistant-style databases (schema 53) in a
temporary directory, so no real data is needed.

    python3 -m unittest -v test_ha_merge_db          (from the directory containing both files)
"""
import bz2
import contextlib
import importlib.util
import io
import json
import os
import shutil
import sqlite3
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
spec = importlib.util.spec_from_file_location("ha_merge_db", os.path.join(HERE, "ha_merge_db.py"))
hmd = importlib.util.module_from_spec(spec)
spec.loader.exec_module(hmd)

SCHEMA = """
CREATE TABLE event_data (data_id INTEGER NOT NULL, hash BIGINT, shared_data TEXT, PRIMARY KEY (data_id));
CREATE TABLE event_types (event_type_id INTEGER NOT NULL, event_type VARCHAR(64), PRIMARY KEY (event_type_id));
CREATE TABLE state_attributes (attributes_id INTEGER NOT NULL, hash BIGINT, shared_attrs TEXT, PRIMARY KEY (attributes_id));
CREATE TABLE states_meta (metadata_id INTEGER NOT NULL, entity_id VARCHAR(255), PRIMARY KEY (metadata_id));
CREATE TABLE statistics_meta (id INTEGER NOT NULL, statistic_id VARCHAR(255), source VARCHAR(32),
  unit_of_measurement VARCHAR(255), unit_class VARCHAR(255), has_mean BOOLEAN, has_sum BOOLEAN, name VARCHAR(255),
  mean_type SMALLINT NOT NULL, PRIMARY KEY (id));
CREATE TABLE recorder_runs (run_id INTEGER NOT NULL, start DATETIME NOT NULL, "end" DATETIME,
  closed_incorrect BOOLEAN NOT NULL, created DATETIME NOT NULL, PRIMARY KEY (run_id));
CREATE TABLE migration_changes (migration_id VARCHAR(255) NOT NULL, version SMALLINT NOT NULL, PRIMARY KEY (migration_id));
CREATE TABLE schema_changes (change_id INTEGER NOT NULL, schema_version INTEGER, changed DATETIME NOT NULL,
  PRIMARY KEY (change_id));
CREATE TABLE statistics_runs (run_id INTEGER NOT NULL, start DATETIME NOT NULL, PRIMARY KEY (run_id));
CREATE TABLE events (event_id INTEGER NOT NULL, event_type CHAR(0), event_data CHAR(0), origin CHAR(0),
  origin_idx SMALLINT, time_fired CHAR(0), time_fired_ts FLOAT, context_id CHAR(0), context_user_id CHAR(0),
  context_parent_id CHAR(0), data_id INTEGER, context_id_bin BLOB, context_user_id_bin BLOB,
  context_parent_id_bin BLOB, event_type_id INTEGER, PRIMARY KEY (event_id));
CREATE TABLE states (state_id INTEGER NOT NULL, entity_id CHAR(0), state VARCHAR(255), attributes CHAR(0),
  event_id SMALLINT, last_changed CHAR(0), last_changed_ts FLOAT, last_reported_ts FLOAT, last_updated CHAR(0),
  last_updated_ts FLOAT, old_state_id INTEGER, attributes_id INTEGER, context_id CHAR(0), context_user_id CHAR(0),
  context_parent_id CHAR(0), origin_idx SMALLINT, context_id_bin BLOB, context_user_id_bin BLOB,
  context_parent_id_bin BLOB, metadata_id INTEGER, PRIMARY KEY (state_id));
CREATE TABLE statistics (id INTEGER NOT NULL, created CHAR(0), created_ts FLOAT, metadata_id INTEGER, start CHAR(0),
  start_ts FLOAT, mean FLOAT, mean_weight FLOAT, min FLOAT, max FLOAT, last_reset CHAR(0), last_reset_ts FLOAT,
  state FLOAT, sum FLOAT, PRIMARY KEY (id));
CREATE TABLE statistics_short_term (id INTEGER NOT NULL, created CHAR(0), created_ts FLOAT, metadata_id INTEGER,
  start CHAR(0), start_ts FLOAT, mean FLOAT, mean_weight FLOAT, min FLOAT, max FLOAT, last_reset CHAR(0),
  last_reset_ts FLOAT, state FLOAT, sum FLOAT, PRIMARY KEY (id));
CREATE UNIQUE INDEX ix_event_types_event_type ON event_types (event_type);
CREATE UNIQUE INDEX ix_states_meta_entity_id ON states_meta (entity_id);
CREATE UNIQUE INDEX ix_statistics_meta_statistic_id ON statistics_meta (statistic_id);
CREATE UNIQUE INDEX ix_statistics_statistic_id_start_ts ON statistics (metadata_id, start_ts);
CREATE UNIQUE INDEX ix_statistics_short_term_statistic_id_start_ts ON statistics_short_term (metadata_id, start_ts);
CREATE INDEX ix_states_metadata_id_last_updated_ts ON states (metadata_id, last_updated_ts);
CREATE INDEX ix_state_attributes_hash ON state_attributes (hash);
"""

H0 = 1700000000 - (1700000000 % 3600)       # an hour boundary


def hour(k):
    return H0 + k * 3600


def make_db(path, entities=(), stat_meta=(), states=(), stats=(), short=(), event_types=(), events=(),
            wal=False):
    """entities: entity ids (ids assigned in list order, so different lists give different ids)
    stat_meta: (statistic_id, unit, has_sum, has_mean)   states: (entity, ts, state[, attrs_json])
    stats / short: (statistic_id, start_ts, mean, state, sum)   events: (event_type, ts)"""
    con = sqlite3.connect(path)
    if wal:
        con.execute("PRAGMA journal_mode=WAL")
    con.executescript(SCHEMA)
    con.execute("INSERT INTO schema_changes VALUES (1, 53, '2026-01-01')")
    for i, e in enumerate(entities, 1):
        con.execute("INSERT INTO states_meta VALUES (?, ?)", (i, e))
    sid = {}
    for i, (name, unit, has_sum, has_mean) in enumerate(stat_meta, 1):
        con.execute("INSERT INTO statistics_meta VALUES (?,?,?,?,?,?,?,?,?)",
                    (i, name, "recorder", unit, None, has_mean, has_sum, None, 0))
        sid[name] = i
    for i, t in enumerate(event_types, 1):
        con.execute("INSERT INTO event_types VALUES (?, ?)", (i, t))
    eid = {e: i for i, e in enumerate(entities, 1)}
    for n, st in enumerate(states, 1):
        entity, ts, state = st[0], st[1], st[2]
        attrs = st[3] if len(st) > 3 else '{"unit_of_measurement":"W"}'
        con.execute("INSERT INTO state_attributes (hash, shared_attrs) VALUES (?, ?)", (hash(attrs) & 0xFFFF, attrs))
        con.execute("INSERT INTO states (state, last_changed_ts, last_updated_ts, attributes_id, metadata_id) "
                    "VALUES (?, ?, ?, ?, ?)", (state, ts, ts, con.execute("SELECT MAX(attributes_id) FROM state_attributes")
                                               .fetchone()[0], eid[entity]))
    for table, rows in (("statistics", stats), ("statistics_short_term", short)):
        for name, ts, mean, state, sm in rows:
            con.execute("INSERT INTO %s (metadata_id, start_ts, created_ts, mean, state, sum) VALUES (?,?,?,?,?,?)"
                        % table, (sid[name], ts, ts, mean, state, sm))
    for n, (etype, ts) in enumerate(events, 1):
        con.execute("INSERT INTO events (event_type_id, time_fired_ts, context_id_bin) VALUES (?, ?, ?)",
                    (event_types.index(etype) + 1, ts, bytes([n % 256]) * 16))
    con.commit()
    if wal:
        con.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    con.close()


def run(argv):
    """Run the tool; returns (exit_code, stderr_text, stdout_text)."""
    err, out = io.StringIO(), io.StringIO()
    with contextlib.redirect_stderr(err), contextlib.redirect_stdout(out):
        try:
            code = hmd.main(argv)
        except SystemExit as e:
            code = e.code if isinstance(e.code, int) else 1
            if isinstance(e.code, str):
                err.write(e.code)
    return code, err.getvalue(), out.getvalue()


def write_file(path, data, mode="w"):
    with open(path, mode) as fh:
        fh.write(data)


def read_file(path):
    with open(path, "rb") as fh:
        return fh.read()


def q(path, sql, *params):
    con = sqlite3.connect(path)
    try:
        return con.execute(sql, params).fetchall()
    finally:
        con.close()


def sums(path, stat):
    return q(path, "SELECT s.start_ts, s.sum FROM statistics s JOIN statistics_meta m ON m.id=s.metadata_id "
                   "WHERE m.statistic_id=? ORDER BY s.start_ts", stat)


class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="hmd-test-")
        self.addCleanup(shutil.rmtree, self.tmp, True)

    def p(self, name):
        return os.path.join(self.tmp, name)

    def merge(self, mapping, others, out="out.db", extra=()):
        if not isinstance(others, (list, tuple)):
            others = [others]
        return run(["--mapping-db", self.p(mapping), "-o", self.p(out)] + list(extra) + [self.p(o) for o in others])


def sum_scenario(self, old_sum, new_sum, third=None):
    """Old DB hours 0-9, new DB hours 10-19; state continues 100+k, sums given by callables."""
    stat = ("sensor.e", "kWh", True, False)
    old = [("sensor.e", hour(k), None, 100.0 + k, old_sum(k)) for k in range(0, 10)]
    new = [("sensor.e", hour(k), None, 100.0 + k, new_sum(k)) for k in range(10, 20)]
    make_db(self.p("old.db"), stat_meta=[stat], stats=old)
    # short-term for the last 2 hours of the new DB, consistent with its long-term sums
    short = []
    for k in (18, 19):
        for m in range(12):
            frac = (m + 1) / 12.0
            short.append(("sensor.e", hour(k) + m * 300, None, 100.0 + k, new_sum(k) - 1 + frac))
    make_db(self.p("new.db"), stat_meta=[stat], stats=new, short=short)


class MergeBasics(Base):
    def test_ids_are_remapped_by_name(self):
        make_db(self.p("old.db"), entities=["sensor.a", "sensor.b"],
                states=[("sensor.a", hour(1), "1"), ("sensor.b", hour(1), "2")])
        make_db(self.p("new.db"), entities=["sensor.b", "sensor.a"],            # opposite ids
                states=[("sensor.a", hour(100), "10"), ("sensor.b", hour(100), "20")])
        code, err, _ = self.merge("new.db", "old.db")
        self.assertEqual(code, 4 if "WARNING" in err else 0, err)
        rows = q(self.p("out.db"), "SELECT m.entity_id, s.last_updated_ts, s.state FROM states s "
                                   "JOIN states_meta m ON m.metadata_id=s.metadata_id ORDER BY 1, 2")
        self.assertEqual(rows, [("sensor.a", hour(1), "1"), ("sensor.a", hour(100), "10"),
                                ("sensor.b", hour(1), "2"), ("sensor.b", hour(100), "20")])

    def test_unique_entities_include_and_ignore(self):
        make_db(self.p("old.db"), entities=["sensor.a", "sensor.old_only"],
                states=[("sensor.a", hour(1), "1"), ("sensor.old_only", hour(1), "9")])
        make_db(self.p("new.db"), entities=["sensor.a"], states=[("sensor.a", hour(100), "10")])
        self.assertIn(self.merge("new.db", "old.db")[0], (0, 4))
        self.assertEqual(q(self.p("out.db"), "SELECT COUNT(*) FROM states")[0][0], 3)
        self.assertIn(self.merge("new.db", "old.db", "out2.db", ["--unique-entities", "ignore"])[0], (0, 4))
        self.assertEqual(q(self.p("out2.db"), "SELECT COUNT(*) FROM states")[0][0], 2)
        self.assertEqual(q(self.p("out2.db"), "SELECT COUNT(*) FROM states_meta")[0][0], 1)

    def test_overlap_warns_and_skips_duplicates(self):
        make_db(self.p("old.db"), entities=["sensor.a"], states=[("sensor.a", hour(1), "1"), ("sensor.a", hour(2), "2")])
        make_db(self.p("new.db"), entities=["sensor.a"], states=[("sensor.a", hour(2), "2b"), ("sensor.a", hour(3), "3")])
        code, err, _ = self.merge("new.db", "old.db")
        self.assertEqual(code, 4)
        self.assertIn("overlap", err)
        rows = q(self.p("out.db"), "SELECT last_updated_ts, state FROM states ORDER BY 1")
        self.assertEqual(rows, [(hour(1), "1"), (hour(2), "2b"), (hour(3), "3")])     # mapping DB's row wins

    def test_overlap_error_mode_aborts(self):
        make_db(self.p("old.db"), entities=["sensor.a"], states=[("sensor.a", hour(1), "1"), ("sensor.a", hour(2), "2")])
        make_db(self.p("new.db"), entities=["sensor.a"], states=[("sensor.a", hour(2), "2b")])
        code, _, _ = self.merge("new.db", "old.db", extra=["--on-overlap", "error"])
        self.assertEqual(code, 2)
        self.assertFalse(os.path.exists(self.p("out.db")))

    def test_events_are_merged_and_event_types_remapped(self):
        make_db(self.p("old.db"), event_types=["a_event", "b_event"], events=[("a_event", hour(1)), ("b_event", hour(2))])
        make_db(self.p("new.db"), event_types=["b_event", "a_event"], events=[("a_event", hour(50))])
        self.assertIn(self.merge("new.db", "old.db")[0], (0, 4))
        rows = q(self.p("out.db"), "SELECT t.event_type, e.time_fired_ts FROM events e "
                                   "JOIN event_types t ON t.event_type_id=e.event_type_id ORDER BY 2")
        self.assertEqual(rows, [("a_event", hour(1)), ("b_event", hour(2)), ("a_event", hour(50))])

    def test_dry_run_writes_nothing_and_existing_output_is_refused(self):
        make_db(self.p("old.db"), entities=["sensor.a"], states=[("sensor.a", hour(1), "1")])
        make_db(self.p("new.db"), entities=["sensor.a"], states=[("sensor.a", hour(100), "10")])
        self.assertEqual(self.merge("new.db", "old.db", extra=["--dry-run"])[0], 0)
        self.assertFalse(os.path.exists(self.p("out.db")))
        self.assertIn(self.merge("new.db", "old.db")[0], (0, 4))
        code, err, _ = self.merge("new.db", "old.db")
        self.assertEqual(code, 1)
        self.assertIn("already exists", err)
        self.assertIn(self.merge("new.db", "old.db", extra=["--force"])[0], (0, 4))

    def test_inputs_are_not_modified(self):
        make_db(self.p("old.db"), entities=["sensor.a"], states=[("sensor.a", hour(1), "1")])
        make_db(self.p("new.db"), entities=["sensor.a"], states=[("sensor.a", hour(100), "10")])
        before = {n: read_file(self.p(n)) for n in ("old.db", "new.db")}
        self.merge("new.db", "old.db")
        for n, b in before.items():
            self.assertEqual(read_file(self.p(n)), b)

    def test_short_term_statistics_not_merged_by_default(self):
        stat = ("sensor.e", "W", False, True)
        make_db(self.p("old.db"), stat_meta=[stat], stats=[("sensor.e", hour(1), 5.0, None, None)],
                short=[("sensor.e", hour(1), 5.0, None, None)])
        make_db(self.p("new.db"), stat_meta=[stat], stats=[("sensor.e", hour(50), 6.0, None, None)],
                short=[("sensor.e", hour(50), 6.0, None, None)])
        self.merge("new.db", "old.db")
        self.assertEqual(q(self.p("out.db"), "SELECT COUNT(*) FROM statistics")[0][0], 2)
        self.assertEqual(q(self.p("out.db"), "SELECT COUNT(*) FROM statistics_short_term")[0][0], 1)
        self.merge("new.db", "old.db", "out2.db", ["--tables", "all"])
        self.assertEqual(q(self.p("out2.db"), "SELECT COUNT(*) FROM statistics_short_term")[0][0], 2)

    def test_mapping_db_not_newest_warns(self):
        make_db(self.p("old.db"), entities=["sensor.a"], states=[("sensor.a", hour(1), "1")])
        make_db(self.p("new.db"), entities=["sensor.a"], states=[("sensor.a", hour(100), "10")])
        code, err, _ = self.merge("old.db", "new.db")
        self.assertEqual(code, 4)
        self.assertIn("newest", err)

    def test_report_file(self):
        make_db(self.p("old.db"), entities=["sensor.a"], states=[("sensor.a", hour(1), "1")])
        make_db(self.p("new.db"), entities=["sensor.a"], states=[("sensor.a", hour(100), "10")])
        self.merge("new.db", "old.db", extra=["--report", self.p("r.json")])
        doc = json.loads(read_file(self.p("r.json")).decode())
        self.assertEqual(doc["tool"], "ha_merge_db")
        self.assertIn("inputs", doc["details"])
        self.assertIn("merged", doc["details"])
        self.assertEqual(doc["exit_code"], 0)


class SumContinuity(Base):
    def assert_continuous(self, path):
        rows = sums(path, "sensor.e")
        for (t0, s0), (t1, s1) in zip(rows, rows[1:]):
            self.assertAlmostEqual(s1 - s0, 1.0, places=6, msg="step between %s and %s" % (t0, t1))

    def _check(self, old_sum, new_sum):
        sum_scenario(self, old_sum, new_sum)
        code, err, _ = self.merge("new.db", "old.db")
        self.assertEqual(code, 0, err)
        self.assert_continuous(self.p("out.db"))
        # the NEWEST data is untouched, including the short-term rows
        self.assertEqual([r[1] for r in sums(self.p("out.db"), "sensor.e")][-10:],
                         [new_sum(k) for k in range(10, 20)])
        self.assertEqual(q(self.p("out.db"), "SELECT COUNT(*) FROM statistics_short_term")[0][0], 24)
        self.assertEqual(q(self.p("out.db"), "SELECT sum FROM statistics_short_term ORDER BY start_ts LIMIT 1"),
                         q(self.p("new.db"), "SELECT sum FROM statistics_short_term ORDER BY start_ts LIMIT 1"))

    def test_restart_from_zero_is_corrected(self):           # newer sum restarts low (the common case)
        self._check(lambda k: 100.0 + k, lambda k: 0.5 + (k - 10))

    def test_jump_upwards_is_corrected(self):                # older sum hugely negative, newer small
        self._check(lambda k: -3150000.0 + k, lambda k: 137.0 + (k - 10))

    def test_continuous_data_is_left_alone(self):
        sum_scenario(self, lambda k: 100.0 + k, lambda k: 100.0 + k)
        code, err, _ = self.merge("new.db", "old.db")
        self.assertEqual(code, 0, err)
        self.assertEqual([r[1] for r in sums(self.p("out.db"), "sensor.e")], [100.0 + k for k in range(20)])

    def test_no_fix_flag_leaves_discontinuity_and_warns(self):
        sum_scenario(self, lambda k: 100.0 + k, lambda k: 0.5 + (k - 10))
        code, err, _ = self.merge("new.db", "old.db", extra=["--no-fix-sum-offset"])
        self.assertEqual(code, 4)
        self.assertIn("NOT be corrected", err)
        self.assertEqual([r[1] for r in sums(self.p("out.db"), "sensor.e")][:10], [100.0 + k for k in range(10)])

    def test_three_inputs(self):
        stat = ("sensor.e", "kWh", True, False)

        def mk(lo, hi, f):
            return [("sensor.e", hour(k), None, 100.0 + k, f(k)) for k in range(lo, hi)]
        make_db(self.p("a.db"), stat_meta=[stat], stats=mk(0, 10, lambda k: 100.0 + k))
        make_db(self.p("b.db"), stat_meta=[stat], stats=mk(10, 20, lambda k: 5.0 + (k - 10)))
        make_db(self.p("c.db"), stat_meta=[stat], stats=mk(20, 30, lambda k: 0.25 + (k - 20)))
        code, err, _ = run(["--mapping-db", self.p("c.db"), "-o", self.p("out.db"), self.p("a.db"), self.p("b.db")])
        self.assertEqual(code, 0, err)
        self.assert_continuous(self.p("out.db"))
        self.assertEqual([r[1] for r in sums(self.p("out.db"), "sensor.e")][-10:], [0.25 + k for k in range(10)])

    def assert_continuous_all(self, path):
        self.assert_continuous(path)

    def test_post_merge_check_flags_nothing_on_clean_merge(self):
        sum_scenario(self, lambda k: 100.0 + k, lambda k: 0.5 + (k - 10))
        code, err, _ = self.merge("new.db", "old.db")
        self.assertIn("sum continuity checks on the output: OK", err)


class Units(Base):
    def _scenario(self):
        make_db(self.p("old.db"), stat_meta=[("sensor.cap", "Wh", False, True)],
                stats=[("sensor.cap", hour(1), 5000.0, None, None)])
        make_db(self.p("new.db"), stat_meta=[("sensor.cap", "kWh", False, True)],
                stats=[("sensor.cap", hour(50), 5.0, None, None)])

    def test_wh_converted_to_kwh_by_default(self):
        self._scenario()
        code, err, _ = self.merge("new.db", "old.db")
        self.assertEqual(code, 0, err)
        self.assertEqual([r[0] for r in q(self.p("out.db"), "SELECT mean FROM statistics ORDER BY start_ts")], [5.0, 5.0])

    def test_no_convert_units_imports_raw_and_warns(self):
        self._scenario()
        code, err, _ = self.merge("new.db", "old.db", extra=["--no-convert-units"])
        self.assertEqual(code, 4)
        self.assertEqual([r[0] for r in q(self.p("out.db"), "SELECT mean FROM statistics ORDER BY start_ts")],
                         [5000.0, 5.0])

    def test_unit_factor(self):
        self.assertEqual(hmd.unit_factor("Wh", "kWh"), 0.001)
        self.assertEqual(hmd.unit_factor("kW", "W"), 1000.0)
        self.assertIsNone(hmd.unit_factor("°C", "°F"))


class BadInputs(Base):
    def _good(self):
        make_db(self.p("good.db"), entities=["sensor.a"], states=[("sensor.a", hour(100), "10")])

    def test_empty_file(self):
        self._good()
        open(self.p("empty.db"), "w").close()
        code, err, _ = self.merge("good.db", "empty.db")
        self.assertEqual(code, 2)
        self.assertIn("empty", err)

    def test_sql_dump(self):
        self._good()
        write_file(self.p("dump.db"), "BEGIN;\nCREATE TABLE t(a);\nCOMMIT;\n")
        code, err, _ = self.merge("good.db", "dump.db")
        self.assertEqual(code, 2)
        self.assertIn("SQL text script", err)
        self.assertIn("sqlite3", err)

    def test_bzip2_file(self):
        self._good()
        write_file(self.p("x.db.bz2"), bz2.compress(b"hello" * 100), "wb")
        code, err, _ = self.merge("good.db", "x.db.bz2")
        self.assertEqual(code, 2)
        self.assertIn("bunzip2", err)

    def test_garbage_header_gives_repair_command(self):
        self._good()
        shutil.copy(self.p("good.db"), self.p("bad.db"))
        with open(self.p("bad.db"), "r+b") as fh:
            fh.write(b"GARBAGEGARBAGEGARB")
        code, err, _ = self.merge("good.db", "bad.db")
        self.assertEqual(code, 2)
        self.assertIn(".recover", err)
        self.assertFalse(os.path.exists(self.p("out.db")))

    def test_corrupt_pages_detected_with_repair_command(self):
        make_db(self.p("old.db"), entities=["sensor.a"],
                states=[("sensor.a", hour(1) + i, str(i)) for i in range(4000)])
        self._good()
        with open(self.p("old.db"), "r+b") as fh:
            fh.seek(4096 * 3)
            fh.write(b"\xff" * 4096 * 4)
        code, err, _ = self.merge("good.db", "old.db")
        self.assertEqual(code, 2)
        self.assertIn("CORRUPT", err)
        self.assertIn(".recover", err)

    def test_missing_file(self):
        self._good()
        code, err, _ = self.merge("good.db", "nope.db")
        self.assertEqual(code, 2)
        self.assertIn("does not exist", err)

    def test_wal_mode_database_note(self):
        make_db(self.p("old.db"), entities=["sensor.a"], states=[("sensor.a", hour(1), "1")], wal=True)
        self._good()
        src = hmd.Source(self.p("old.db"))
        hmd.open_source(src, True)
        self.assertTrue(src.wal_mode)
        self.assertTrue(any("WAL mode" in w for w in src.warnings))


class FillStates(Base):
    def test_fill_creates_marked_hourly_states_in_the_gap(self):
        stat = ("sensor.t", "W", False, True)
        attrs = '{"friendly_name":"T","unit_of_measurement":"W"}'
        stats = [("sensor.t", hour(k), 100.0 + k, None, None) for k in range(0, 200)]
        make_db(self.p("old.db"), entities=["sensor.t"], stat_meta=[stat], stats=stats[:50],
                states=[("sensor.t", hour(k), str(k), attrs) for k in range(0, 50)])
        make_db(self.p("new.db"), entities=["sensor.t"], stat_meta=[stat], stats=stats[50:],
                states=[("sensor.t", hour(k), str(k), attrs) for k in range(150, 200)])
        code, err, _ = self.merge("new.db", "old.db", extra=["--fill-states-from-statistics"])
        self.assertEqual(code, 0, err)
        synth = q(self.p("out.db"), "SELECT s.last_updated_ts, s.state, a.shared_attrs FROM states s "
                                    "JOIN state_attributes a ON a.attributes_id=s.attributes_id "
                                    "WHERE a.shared_attrs LIKE '%ha_merge_db_synthetic%' ORDER BY 1")
        self.assertGreater(len(synth), 90)
        ts, state, blob = synth[0]
        self.assertEqual(ts % 3600, 1800)                         # mid-hour
        self.assertEqual(json.loads(blob)["friendly_name"], "T")  # normal attributes kept
        self.assertTrue(json.loads(blob)["ha_merge_db_synthetic"])
        self.assertAlmostEqual(float(state), 100.0 + (ts - 1800 - H0) / 3600, places=3)
        # real states are untouched
        self.assertEqual(q(self.p("out.db"), "SELECT COUNT(*) FROM states s JOIN state_attributes a "
                                             "ON a.attributes_id=s.attributes_id "
                                             "WHERE a.shared_attrs NOT LIKE '%ha_merge_db_synthetic%'")[0][0], 100)

    def test_requires_states_and_statistics_tables(self):
        make_db(self.p("old.db"))
        make_db(self.p("new.db"))
        code, err, _ = self.merge("new.db", "old.db", extra=["--fill-states-from-statistics", "--tables", "events"])
        self.assertEqual(code, 1)


class CheckDb(Base):
    def _snapback_db(self, path):
        """Long-term hours 0-9 shifted (sum == state), hours 10-19 snapped back by 1000; short-term all unshifted."""
        stat = ("sensor.e", "kWh", True, False)
        stats = [("sensor.e", hour(k), None, 100.0 + k, (100.0 + k) if k < 10 else (100.0 + k - 1000)) for k in range(20)]
        short = []
        for k in range(8, 20):
            for m in range(12):
                short.append(("sensor.e", hour(k) + m * 300, None, 100.0 + k, 100.0 + k - 1000 - 1 + (m + 1) / 12.0))
        make_db(path, stat_meta=[stat], stats=stats, short=short)

    def test_detects_step_and_mismatch_and_sql_repairs_it(self):
        self._snapback_db(self.p("live.db"))
        code, err, sql = run(["--check-db", self.p("live.db")])
        self.assertEqual(code, 4, err)
        self.assertIn("UPDATE statistics ", sql)
        self.assertIn("UPDATE statistics_short_term", sql)
        con = sqlite3.connect(self.p("live.db"))
        con.executescript("\n".join(ln for ln in sql.splitlines() if not ln.startswith(".")))
        con.close()
        code, err, _ = run(["--check-db", self.p("live.db")])
        self.assertEqual(code, 0, err)
        self.assertEqual(q(self.p("live.db"), "SELECT sum FROM statistics ORDER BY start_ts DESC LIMIT 1")[0][0], 119.0)

    def test_clean_database_exits_zero(self):
        stat = ("sensor.e", "kWh", True, False)
        rows = [("sensor.e", hour(k), None, 100.0 + k, 100.0 + k) for k in range(20)]
        make_db(self.p("clean.db"), stat_meta=[stat], stats=rows)
        code, err, sql = run(["--check-db", self.p("clean.db")])
        self.assertEqual(code, 0, err)
        self.assertEqual(sql, "")

    def test_counter_resets_are_not_flagged(self):
        stat = ("sensor.today", "kWh", True, False)
        rows, total = [], 0.0
        for d in range(5):                       # 'energy today' resets to 0 every day
            for h in range(24):
                state = h * 0.5
                total += 0.5 if h else 0.0
                rows.append(("sensor.today", hour(d * 24 + h), None, state, total))
        make_db(self.p("c.db"), stat_meta=[stat], stats=rows)
        code, err, _ = run(["--check-db", self.p("c.db")])
        self.assertEqual(code, 0, err)


class Docs(unittest.TestCase):
    def test_help_runs(self):
        out = io.StringIO()
        with contextlib.redirect_stdout(out), self.assertRaises(SystemExit) as cm:
            hmd.parse_args(["--help"])
        self.assertEqual(cm.exception.code, 0)
        self.assertIn("--check-db", out.getvalue())


if __name__ == "__main__":
    unittest.main(verbosity=2)
