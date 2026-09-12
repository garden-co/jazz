#!/usr/bin/env python3
"""Replay actual synthetic Jazz bundles into indexed SQL stores, then forward them."""

import argparse
from collections import defaultdict
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import time
import uuid

from run import scalar, quoted, reference_query


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def load_capture(path, fixture, columns):
    expected = {
        (w["table"], w["id"]): {k: scalar(v) for k, v in w["cells"].items()}
        for w in fixture["writes"]
    }
    packets = []
    for line in path.open():
        frame = json.loads(line)
        for bundle in frame["bundles"]:
            assert (
                bundle["fate"] == "Accepted"
                and bundle["scope"] == "CompleteTransaction"
            )
            assert (
                bundle["tx"]["kind"] == "Mergeable"
                and bundle["tx"]["n_total_writes"] == 1
            )
            assert len(bundle["versions"]) == 1
            version = bundle["versions"][0]
            assert not version["parents"] and version["branch"] == {"values": []}
            cells = {k: scalar(v) for k, v in version["cells"].items()}
            assert cells == expected[(version["table"], version["row"])]
            tx = canonical(bundle["tx"]["tx_id"])
            meta = canonical(
                {k: bundle[k] for k in ["fate", "scope", "global_time", "durability"]}
            )
            # Complete postcard/JVRR envelope remains byte-identical in storage.
            packets.append(
                (
                    version["table"],
                    version["row"],
                    tx,
                    bytes.fromhex(bundle["tx_hex"]),
                    meta,
                    bytes.fromhex(version["wire_hex"]),
                    *[cells[c] for c in columns[version["table"]]],
                )
            )
    return packets


class Store:
    def __init__(self, args, fixture, columns, name):
        self.pg = bool(args.postgres)
        self.columns = columns
        self.fixture = fixture
        self.args = args
        self.temp = tempfile.TemporaryDirectory(prefix="jazz-sql-sync-")
        self.name = "sql_sync_" + uuid.uuid4().hex
        if self.pg:
            import psycopg

            self.admin = psycopg.connect(args.postgres, autocommit=True)
            self.admin.execute("CREATE DATABASE " + quoted(self.name))
            self.db = psycopg.connect(args.postgres, dbname=self.name, autocommit=True)
            self.db.execute("SET client_encoding TO UTF8")
            self.db.execute(
                "SET synchronous_commit TO "
                + ("on" if args.durability == "durable" else "off")
            )
            self.db.execute("SET jit TO off")
            self.server_pid = self.db.execute("SELECT pg_backend_pid()").fetchone()[0]
        else:
            self.path = Path(self.temp.name) / "node.sqlite"
            self.db = sqlite3.connect(self.path, isolation_level=None)
            self.db.execute("PRAGMA journal_mode=WAL")
            self.db.execute(
                "PRAGMA synchronous="
                + ("FULL" if args.durability == "durable" else "NORMAL")
            )
            self.db.execute("PRAGMA wal_autocheckpoint=0")
        blob = "BYTEA" if self.pg else "BLOB"
        self.db.execute(
            f"CREATE TABLE transactions(tx TEXT PRIMARY KEY, accepted INTEGER NOT NULL, payload {blob}, meta TEXT)"
        )
        self.db.execute(
            f"CREATE TEMP TABLE tx_stage(tx TEXT, payload {blob}, meta TEXT)"
        )
        self.db.execute(
            "CREATE TABLE parents(child TEXT, parent TEXT, PRIMARY KEY(child,parent))"
        )
        self.db.execute("CREATE INDEX parent_lookup ON parents(parent)")
        samples = {}
        for w in fixture["writes"]:
            samples.setdefault(
                w["table"], {k: scalar(v) for k, v in w["cells"].items()}
            )
        for t, cols in columns.items():
            decl = "".join(
                f", {quoted(c)} "
                + (
                    "DOUBLE PRECISION"
                    if isinstance(samples[t][c], float)
                    else "BIGINT"
                    if isinstance(samples[t][c], int)
                    else "TEXT"
                )
                for c in cols
            )
            self.db.execute(
                f"CREATE TABLE {quoted('h_' + t)}(id TEXT, version TEXT PRIMARY KEY, tx TEXT NOT NULL, wire {blob}{decl})"
            )
            self.db.execute(
                f"CREATE TEMP TABLE {quoted('stage_' + t)}(id TEXT, version TEXT, tx TEXT, wire {blob}{decl})"
            )
            self.db.execute(
                f"CREATE INDEX {quoted('id_' + t)} ON {quoted('h_' + t)}(id)"
            )
            for c in cols:
                if c in {
                    "parent_id",
                    "resource",
                    "team",
                    "member_id",
                    "target_id",
                    "user_id",
                    "group_id",
                }:
                    self.db.execute(
                        f"CREATE INDEX {quoted('idx_' + t + '_' + c)} ON {quoted('h_' + t)}({quoted(c)})"
                    )
            self.db.execute(f"""CREATE VIEW {quoted("v_" + t)} AS SELECT h.* FROM {quoted("h_" + t)} h
                JOIN transactions t ON t.tx=h.tx AND t.accepted=1 WHERE NOT EXISTS
                (SELECT 1 FROM parents p JOIN transactions successor ON successor.tx=p.child AND successor.accepted=1 WHERE p.parent=h.version)""")

    def backend_counters(self):
        pid = self.server_pid if self.pg else os.getpid()
        stat = Path(f"/proc/{pid}/stat").read_text().split()
        io = {
            k: int(v)
            for k, v in (
                line.split(":")
                for line in Path(f"/proc/{pid}/io").read_text().splitlines()
            )
        }
        out = {
            "backend_cpu_ms": (int(stat[13]) + int(stat[14]))
            * 1000
            / os.sysconf("SC_CLK_TCK"),
            "backend_write_bytes": io["write_bytes"],
            "backend_read_bytes": io["read_bytes"],
        }
        if self.pg:
            lsn = self.db.execute("SELECT pg_current_wal_insert_lsn()").fetchone()[0]
            high, low = lsn.split("/")
            out["wal_bytes"] = int(high, 16) * 2**32 + int(low, 16)
        return out

    def reopen(self):
        self.db.close()
        if self.pg:
            import psycopg

            self.db = psycopg.connect(
                self.args.postgres, dbname=self.name, autocommit=True
            )
            self.db.execute("SET client_encoding TO UTF8")
        else:
            self.db = sqlite3.connect(self.path, isolation_level=None)

    def load_stage(self, name, rows, width):
        if self.pg:
            with self.db.cursor() as cur:
                with cur.copy("COPY " + quoted(name) + " FROM STDIN") as copy:
                    for row in rows:
                        copy.write_row(row)
        else:
            self.db.executemany(
                "INSERT INTO "
                + quoted(name)
                + " VALUES ("
                + ",".join(["?"] * width)
                + ")",
                rows,
            )

    def ingest(self, packets):
        counters = self.backend_counters()
        start = time.perf_counter_ns()
        cpu = time.process_time_ns()
        unique = {}
        transactions = {}
        grouped = defaultdict(list)
        for packet in packets:
            table, row, tx, payload, meta, wire, *cells = packet
            key = (table, tx)
            if key in unique and unique[key] != packet:
                raise ValueError("conflicting incoming immutable version")
            unique[key] = packet
            if tx in transactions and transactions[tx] != (tx, payload, meta):
                raise ValueError("conflicting incoming transaction")
            transactions[tx] = (tx, payload, meta)
        for table, row, tx, payload, meta, wire, *cells in unique.values():
            grouped[table].append((row, tx, tx, wire, *cells))
        self.db.execute("BEGIN")
        try:
            self.db.execute("DELETE FROM tx_stage")
            self.load_stage("tx_stage", transactions.values(), 3)
            if self.db.execute(
                "SELECT 1 FROM tx_stage s JOIN transactions t ON t.tx=s.tx WHERE t.payload<>s.payload OR t.meta<>s.meta LIMIT 1"
            ).fetchone():
                raise ValueError("conflicting persisted transaction")
            self.db.execute(
                "INSERT INTO transactions SELECT tx,1,payload,meta FROM tx_stage WHERE true ON CONFLICT(tx) DO NOTHING"
            )
            for table, rows in grouped.items():
                stage = quoted("stage_" + table)
                history = quoted("h_" + table)
                self.db.execute("DELETE FROM " + stage)
                self.load_stage("stage_" + table, rows, 4 + len(self.columns[table]))
                if self.db.execute(
                    f"SELECT 1 FROM {stage} s JOIN {history} h ON h.version=s.version WHERE h.wire<>s.wire OR h.id<>s.id LIMIT 1"
                ).fetchone():
                    raise ValueError("conflicting persisted version")
                self.db.execute(
                    f"INSERT INTO {history} SELECT * FROM {stage} WHERE true ON CONFLICT(version) DO NOTHING"
                )
            self.db.execute("COMMIT")
        except Exception:
            self.db.execute("ROLLBACK")
            raise
        commit_ms = (time.perf_counter_ns() - start) / 1e6
        analyze_start = time.perf_counter_ns()
        if self.args.analyze:
            self.db.execute("ANALYZE")
        analyze_ms = (time.perf_counter_ns() - analyze_start) / 1e6
        elapsed = (time.perf_counter_ns() - start) / 1e6
        cpu_ms = (time.process_time_ns() - cpu) / 1e6
        delta = {k: v - counters[k] for k, v in self.backend_counters().items()}
        return {
            "wall_ms": elapsed,
            "commit_ms": commit_ms,
            "analyze_ms": analyze_ms,
            "client_cpu_ms": cpu_ms,
            **delta,
            "incoming": len(packets),
            "unique": len(unique),
            "duplicates": len(packets) - len(unique),
            "commits": 1,
        }

    def serve(self, trusted):
        start = time.perf_counter_ns()
        output = []
        for table, cols in self.columns.items():
            selected = (
                ""
                if trusted
                else " JOIN ("
                + reference_query(self.fixture, self.columns, table, True, False)
                + ") selected ON selected.id=h.id"
            )
            fields = "".join(",h." + quoted(c) for c in cols)
            sql = (
                f"SELECT h.id,h.tx,t.payload,t.meta,h.wire{fields} FROM {quoted('v_' + table)} h JOIN transactions t ON t.tx=h.tx"
                + selected
            )
            for row in self.db.execute(sql).fetchall():
                output.append((table, *row))
        return output, (time.perf_counter_ns() - start) / 1e6

    def local_read(self):
        start = time.perf_counter_ns()
        results = {}
        for table in self.columns:
            results[table] = self.db.execute(
                reference_query(self.fixture, self.columns, table, True, False)
            ).fetchall()
        return results, (time.perf_counter_ns() - start) / 1e6

    def size(self):
        if self.pg:
            return self.db.execute(
                "SELECT pg_database_size(current_database())"
            ).fetchone()[0]
        return sum(
            p.stat().st_size for p in Path(self.temp.name).iterdir() if p.is_file()
        )

    def close(self):
        self.db.close()
        if self.pg:
            self.admin.execute("DROP DATABASE " + quoted(self.name))
            self.admin.close()
        self.temp.cleanup()


def packet_digest(packets):
    digest = hashlib.sha256()
    for p in sorted(set(packets), key=lambda p: (p[0], p[1], p[2])):
        for value in p:
            data = value if isinstance(value, bytes) else canonical(value).encode()
            digest.update(len(data).to_bytes(8, "big"))
            digest.update(data)
    return digest.hexdigest()


def trace_order(packets, captured):
    by_key = {(p[0], p[1], p[2]): p for p in packets}
    return [by_key[(p[0], p[1], p[2])] for p in captured]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("fixture", type=Path)
    ap.add_argument("capture", type=Path)
    ap.add_argument("--analyze", action=argparse.BooleanOptionalAction, default=True)
    ap.add_argument("--postgres")
    ap.add_argument("--durability", choices=["relaxed", "durable"], default="relaxed")
    ap.add_argument("--rounds", type=int, default=3)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()
    fixture = json.loads(args.fixture.read_text())
    columns = {t: [] for t in fixture["expected"]}
    for w in fixture["writes"]:
        columns[w["table"]] = list(w["cells"])
    captured = {
        hop: load_capture(args.capture / (hop + ".jsonl"), fixture, columns)
        for hop in ["core-edge", "edge-client"]
    }
    digests = {hop: packet_digest(packets) for hop, packets in captured.items()}
    expected = {t: {} for t in columns}
    visible = {t: set(ids) for t, ids in fixture["expected"].items()}
    for w in fixture["writes"]:
        if w["id"] in visible[w["table"]]:
            expected[w["table"]][w["id"]] = (
                w["id"],
                *[scalar(w["cells"][c]) for c in columns[w["table"]]],
            )
    receipts = []
    for iteration in range(args.rounds):
        stores = []
        try:

            def create(name):
                store = Store(args, fixture, columns, name)
                stores.append(store)
                return store

            # Direct replay measures actual wire delivery, including duplicates.
            replay = create("replay")
            replay_stats = replay.ingest(captured["edge-client"])
            results, read_ms = replay.local_read()
            assert all(
                {r[0]: r for r in rows} == expected[t] for t, rows in results.items()
            )
            duplicate_stats = replay.ingest(captured["edge-client"])
            served, _ = replay.serve(True)
            assert packet_digest(served) == digests["edge-client"]
            # Conflicting bytes must reject without changing the durable result.
            bad = list(captured["edge-client"][0])
            bad[5] = bad[5] + b"bad"
            try:
                replay.ingest([tuple(bad)])
            except ValueError:
                pass
            else:
                raise AssertionError("conflicting version accepted")
            served, _ = replay.serve(True)
            assert packet_digest(served) == digests["edge-client"]
            replay_size = replay.size()
            replay.reopen()
            served, _ = replay.serve(True)
            assert packet_digest(served) == digests["edge-client"]
            replay.close()
            stores.remove(replay)
            # Seed Core outside the measured downstream load, as in Jazz.
            core = create("core")
            core.ingest(captured["core-edge"])
            core.db.execute("ANALYZE")
            direct = create("direct")
            direct_start = time.perf_counter_ns()
            direct_packets, direct_query_ms = core.serve(False)
            direct_packets = trace_order(direct_packets, captured["edge-client"])
            direct_stats = direct.ingest(direct_packets)
            direct_results, direct_read_ms = direct.local_read()
            direct_total_ms = (time.perf_counter_ns() - direct_start) / 1e6
            assert packet_digest(direct_packets) == digests["edge-client"]
            assert all(
                {r[0]: r for r in rows} == expected[t]
                for t, rows in direct_results.items()
            )
            direct.close()
            stores.remove(direct)
            edge = create("edge")
            client = create("client")
            # Timed serial topology has separate persistent stores and includes
            # query, driver transfer, ingestion, index work and final local reads.
            total_start = time.perf_counter_ns()
            to_edge, core_query_ms = core.serve(True)
            to_edge = trace_order(to_edge, captured["core-edge"])
            edge_stats = edge.ingest(to_edge)
            to_client, edge_query_ms = edge.serve(False)
            to_client = trace_order(to_client, captured["edge-client"])
            client_stats = client.ingest(to_client)
            results, client_read_ms = client.local_read()
            total_ms = (time.perf_counter_ns() - total_start) / 1e6
            assert packet_digest(to_edge) == digests["core-edge"]
            assert packet_digest(to_client) == digests["edge-client"]
            assert all(
                {r[0]: r for r in rows} == expected[t] for t, rows in results.items()
            )
            receipt = {
                "round": iteration,
                "engine": "postgres" if args.postgres else "sqlite",
                "durability": args.durability,
                "analyze": args.analyze,
                "replay": replay_stats,
                "replay_local_read_ms": read_ms,
                "duplicate_replay": duplicate_stats,
                "replay_size_bytes": replay_size,
                "direct_total_ms": direct_total_ms,
                "direct_query_ms": direct_query_ms,
                "direct_ingest": direct_stats,
                "direct_read_ms": direct_read_ms,
                "topology_total_ms": total_ms,
                "core_query_fetch_ms": core_query_ms,
                "edge_ingest": edge_stats,
                "edge_query_fetch_ms": edge_query_ms,
                "client_ingest": client_stats,
                "client_local_read_ms": client_read_ms,
                "edge_size_bytes": edge.size(),
                "client_size_bytes": client.size(),
            }
            client.reopen()
            served, _ = client.serve(True)
            assert packet_digest(served) == digests["edge-client"]
            receipts.append(receipt)
            print(json.dumps(receipt), flush=True)
        finally:
            for store in reversed(stores):
                store.close()
    args.out.write_text(
        json.dumps(
            {
                "receipts": receipts,
                "capture_digests": digests,
                "capture_counts": {k: len(v) for k, v in captured.items()},
                "checks": "full cells, full wire+transaction bytes, duplicate replay, conflicting version rollback, reopen",
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
