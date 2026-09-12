#!/usr/bin/env python3
"""Synthetic permissioned IVM reference; no Jazz runtime code is replaced."""

import argparse
import json
import time
from pathlib import Path
import requests
from run import scalar

BASE = "http://127.0.0.1:18080"
FIELDS = [
    "table_name",
    "row_id",
    "version_id",
    "payload",
    "group_id",
    "user_id",
    "member_id",
    "target_id",
    "resource",
    "team",
    "parent_id",
    "administrator",
]


def sql_program(data):
    kinds = ",\n".join(
        "('%s','%s','%s')" % (r["table"], r["access"], r["child"] or "")
        for r in data["resources"]
    )
    resource_names = ",".join(
        "'%s'" % t for r in data["resources"] for t in [r["table"], r["child"]] if t
    )
    return f"""
CREATE TABLE versions (
 table_name VARCHAR NOT NULL, row_id VARCHAR NOT NULL, version_id BIGINT NOT NULL,
 payload VARCHAR NOT NULL, group_id VARCHAR, user_id VARCHAR, member_id VARCHAR,
 target_id VARCHAR, resource VARCHAR, team VARCHAR, parent_id VARCHAR,
 administrator BIGINT, PRIMARY KEY(table_name, version_id)
);
CREATE TABLE transactions (tx BIGINT NOT NULL PRIMARY KEY, accepted BOOLEAN NOT NULL);
CREATE TABLE parents (child_tx BIGINT NOT NULL, parent_tx BIGINT NOT NULL,
 PRIMARY KEY(child_tx, parent_tx));
CREATE TABLE query_requests (request_id BIGINT NOT NULL PRIMARY KEY,
 account_id VARCHAR NOT NULL, trusted BOOLEAN NOT NULL);
CREATE LOCAL VIEW live_rows AS
 SELECT v.* FROM versions v JOIN transactions t ON t.tx=v.version_id AND t.accepted
 WHERE NOT EXISTS (SELECT 1 FROM parents p JOIN transactions successor
 ON successor.tx=p.child_tx AND successor.accepted WHERE p.parent_tx=v.version_id);
CREATE LOCAL VIEW kinds AS SELECT * FROM (VALUES {kinds})
 AS k(table_name, access_table, child_table);
DECLARE RECURSIVE VIEW reach(account_id VARCHAR NOT NULL, group_id VARCHAR, depth BIGINT NOT NULL);
CREATE VIEW reach AS
 SELECT q.account_id, a.group_id, CAST(0 AS BIGINT) AS depth
 FROM query_requests q JOIN live_rows a ON a.user_id=q.account_id
 WHERE NOT q.trusted AND a.table_name='group_access_edges' AND a.group_id IS NOT NULL
 UNION
 SELECT r.account_id,e.target_id,r.depth+1 FROM reach r
 JOIN live_rows e ON e.member_id=r.group_id AND e.table_name='group_entry'
 JOIN live_rows g ON g.row_id=e.target_id AND g.table_name='group'
 WHERE e.administrator=0 AND r.depth<8 AND e.target_id IS NOT NULL;
CREATE LOCAL VIEW reached AS SELECT DISTINCT account_id,group_id FROM reach;
CREATE LOCAL VIEW allowed AS
 SELECT DISTINCT g.account_id,k.table_name,a.resource AS row_id
 FROM reached g JOIN live_rows a ON a.team=g.group_id
 JOIN kinds k ON k.access_table=a.table_name
 JOIN live_rows p ON p.table_name=k.table_name AND p.row_id=a.resource
 WHERE a.administrator=0;
CREATE MATERIALIZED VIEW scope_rows AS
 SELECT o.* FROM live_rows o JOIN query_requests q ON q.trusted
 UNION ALL
 SELECT o.* FROM live_rows o JOIN query_requests q ON NOT q.trusted
 WHERE o.table_name NOT IN ({resource_names})
 UNION ALL
 SELECT o.* FROM live_rows o JOIN allowed a
 ON a.table_name=o.table_name AND a.row_id=o.row_id
 UNION ALL
 SELECT o.* FROM live_rows o JOIN kinds k ON o.table_name=k.child_table
 JOIN allowed a ON a.table_name=k.table_name AND a.row_id=o.parent_id;
"""


def fixture_rows(data):
    out = []
    for version, w in enumerate(data["writes"], 1):
        cells = {k: scalar(v) for k, v in w["cells"].items()}
        row = {k: None for k in FIELDS}
        row.update(
            table_name=w["table"],
            row_id=w["id"],
            version_id=version,
            payload=json.dumps(cells, sort_keys=True, separators=(",", ":")),
        )
        for k in FIELDS[4:]:
            row[k] = cells.get(k)
        out.append(row)
    return out


class API:
    def __init__(self, base=BASE):
        self.base = base
        self.session = requests.Session()

    def call(self, method, path, **kwargs):
        r = self.session.request(method, self.base + path, timeout=180, **kwargs)
        if not r.ok:
            raise RuntimeError(f"{method} {path}: {r.status_code} {r.text[:3000]}")
        return r

    def create(self, name, sql):
        return self.call(
            "POST",
            "/v0/pipelines",
            json={
                "name": name,
                "program_code": sql,
                "program_config": {"profile": "optimized"},
                "runtime_config": {"workers": 1, "cpu_profiler": False},
            },
        ).json()

    def status(self, name):
        return self.call("GET", f"/v0/pipelines/{name}").json()

    def start(self, name):
        deadline = time.monotonic() + 900
        while True:
            status = self.status(name)
            if status["program_status"] == "Success":
                break
            if status["program_status"].endswith("Error"):
                raise RuntimeError(status["program_error"])
            if time.monotonic() > deadline:
                raise TimeoutError("compile " + name)
            time.sleep(0.2)
        self.call("POST", f"/v0/pipelines/{name}/start", params={"initial": "running"})
        while True:
            status = self.status(name)
            if status["deployment_status"] == "Running":
                return
            if status.get("deployment_error"):
                raise RuntimeError(status["deployment_error"])
            if time.monotonic() > deadline:
                raise TimeoutError("start " + name)
            time.sleep(0.05)

    def stats(self, name):
        return self.call("GET", f"/v0/pipelines/{name}/stats").json()["global_metrics"]

    def push(self, name, table, rows, delete=False):
        if not rows:
            return
        updates = [{"delete" if delete else "insert": r} for r in rows]
        return self.call(
            "POST",
            f"/v0/pipelines/{name}/ingress/{table.upper()}",
            params={
                "format": "json",
                "array": "true",
                "update_format": "insert_delete",
            },
            json=updates,
        ).json()

    def transaction(self, name, updates):
        self.call("POST", f"/v0/pipelines/{name}/start_transaction")
        for table, rows, delete in updates:
            self.push(name, table, rows, delete)
        self.call("POST", f"/v0/pipelines/{name}/commit_transaction")
        deadline = time.monotonic() + 180
        while self.stats(name)["transaction_status"] != "NoTransaction":
            if time.monotonic() > deadline:
                raise TimeoutError("commit " + name)
            time.sleep(0.001)

    def fetch(self, name):
        # SELECT only reads a materialized IVM result. It does not execute the
        # permission predicate through Feldera's nonincremental ad-hoc engine.
        r = self.call(
            "GET",
            f"/v0/pipelines/{name}/query",
            params={"sql": "SELECT * FROM scope_rows", "format": "json"},
        )
        return [
            {k.lower(): v for k, v in json.loads(line).items()}
            for line in r.text.splitlines()
            if line
        ]

    def ingest(self, name, rows):
        self.transaction(
            name,
            [
                ("versions", rows, False),
                (
                    "transactions",
                    [{"tx": r["version_id"], "accepted": True} for r in rows],
                    False,
                ),
            ],
        )

    def stop(self, name):
        self.call("POST", f"/v0/pipelines/{name}/stop", params={"force": "true"})


def verify(rows, expected, by_key):
    actual = {(r["table_name"], r["row_id"]): r for r in rows}
    assert len(actual) == len(rows), "duplicate outputs"
    assert (
        actual.keys() == expected
    ), f"wrong IDs: missing={len(expected-actual.keys())} extra={len(actual.keys()-expected)}"
    for key, row in actual.items():
        assert {k: row.get(k) for k in FIELDS} == by_key[key], f"wrong payload: {key}"


def validate_incremental(api, name, data, rows, by_key, permitted):
    request = {"request_id": 1, "account_id": data["account"], "trusted": False}
    api.transaction(name, [("query_requests", [request], False)])
    verify(api.fetch(name), permitted, by_key)
    # Sensitivity control: deliberately bypass permissions in our synthetic
    # reference. The complete-ID oracle must reject that output.
    api.transaction(name, [("query_requests", [request | {"trusted": True}], False)])
    try:
        verify(api.fetch(name), permitted, by_key)
    except AssertionError:
        pass
    else:
        raise AssertionError("permission oracle did not detect bypass")
    api.transaction(name, [("query_requests", [request], False)])
    grants = [r for r in rows if r["table_name"] == "group_access_edges"]
    api.transaction(name, [("versions", grants, True)])
    resource_tables = {
        t for r in data["resources"] for t in [r["table"], r["child"]] if t
    }
    denied = {
        key
        for key in permitted
        if key[0] not in resource_tables and key[0] != "group_access_edges"
    }
    verify(api.fetch(name), denied, by_key)
    api.transaction(name, [("versions", grants, False)])
    verify(api.fetch(name), permitted, by_key)
    # A later accepted version supersedes an existing permitted row; an
    # unaccepted successor must not hide its predecessor.
    old = next(
        r
        for r in rows
        if r["table_name"] == "res_l_child_3"
        and (r["table_name"], r["row_id"]) in permitted
    )
    new = old | {"version_id": len(rows) + 1}
    cells = json.loads(new["payload"])
    field = next(k for k, v in cells.items() if isinstance(v, str) and k != "parent_id")
    cells[field] += "-successor"
    new["payload"] = json.dumps(cells, sort_keys=True, separators=(",", ":"))
    tx = {"tx": new["version_id"], "accepted": False}
    parent = {"child_tx": new["version_id"], "parent_tx": old["version_id"]}
    api.transaction(
        name,
        [
            ("versions", [new], False),
            ("transactions", [tx], False),
            ("parents", [parent], False),
        ],
    )
    verify(api.fetch(name), permitted, by_key)
    api.transaction(name, [("transactions", [tx | {"accepted": True}], False)])
    changed = by_key | {(new["table_name"], new["row_id"]): new}
    verify(api.fetch(name), permitted, changed)
    api.transaction(name, [("transactions", [tx], False)])
    verify(api.fetch(name), permitted, by_key)
    api.transaction(
        name,
        [
            ("parents", [parent], True),
            ("versions", [new], True),
            ("transactions", [tx], True),
        ],
    )
    verify(api.fetch(name), permitted, by_key)
    api.transaction(name, [("query_requests", [{"request_id": 1}], True)])
    return {
        "permission_bypass_detected": True,
        "revoke_all_grants_rows": len(denied),
        "grant_restore": True,
        "unaccepted_successor": True,
        "accepted_successor": True,
        "reject_successor_restore": True,
    }


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("fixture", type=Path)
    p.add_argument("--out", type=Path, required=True)
    p.add_argument("--name", default="jazz_permission_proxy")
    p.add_argument("--run", action="store_true")
    p.add_argument("--rounds", type=int, default=3)
    args = p.parse_args()
    args.out.mkdir(exist_ok=True, parents=True)
    data = json.loads(args.fixture.read_text())
    sql = sql_program(data)
    (args.out / "program.sql").write_text(sql)
    api = API()
    if not args.run:
        response = api.create(args.name, sql)
        (args.out / "created.json").write_text(json.dumps(response, indent=2))
        print(args.name, flush=True)
        raise SystemExit(0)
    api.start(args.name)
    rows = fixture_rows(data)
    by_key = {(r["table_name"], r["row_id"]): r for r in rows}
    permitted = {(t, i) for t, ids in data["expected"].items() for i in ids}
    request = {"request_id": 1, "account_id": data["account"], "trusted": False}
    api.transaction(args.name, [("query_requests", [{"request_id": 1}], True)])
    seed_start = time.perf_counter()
    api.ingest(args.name, rows)
    seed_ms = (time.perf_counter() - seed_start) * 1000
    receipt = {
        "version": api.call("GET", "/v0/config").json(),
        "seed_ms": seed_ms,
        "single": [],
        "topology": [],
    }

    def save():
        (args.out / "receipt.json").write_text(json.dumps(receipt, indent=2))

    print("seed_ms", seed_ms, flush=True)
    for i in range(args.rounds):
        before = api.stats(args.name)
        start = time.perf_counter()
        api.transaction(args.name, [("query_requests", [request], False)])
        computed = time.perf_counter()
        result = api.fetch(args.name)
        done = time.perf_counter()
        after = api.stats(args.name)
        verify(result, permitted, by_key)
        sample = {
            "activation_ms": (computed - start) * 1000,
            "fetch_ms": (done - computed) * 1000,
            "total_ms": (done - start) * 1000,
            "before": before,
            "after": after,
            "rows": len(result),
        }
        receipt["single"].append(sample)
        save()
        print("single", i, sample["total_ms"], flush=True)
        api.transaction(args.name, [("query_requests", [{"request_id": 1}], True)])
    for i in range(args.rounds):
        suffix = str(time.time_ns())
        edge = "jazz_edge_" + suffix
        client = "jazz_client_" + suffix
        for name in [edge, client]:
            api.create(name, sql)
            api.start(name)
            api.transaction(name, [("query_requests", [request], False)])
        start = time.perf_counter()
        api.transaction(
            args.name, [("query_requests", [request | {"trusted": True}], False)]
        )
        core_rows = api.fetch(args.name)
        core_done = time.perf_counter()
        api.ingest(edge, core_rows)
        edge_ingested = time.perf_counter()
        edge_rows = api.fetch(edge)
        edge_done = time.perf_counter()
        api.ingest(client, edge_rows)
        client_ingested = time.perf_counter()
        client_rows = api.fetch(client)
        done = time.perf_counter()
        verify(core_rows, by_key.keys(), by_key)
        verify(edge_rows, permitted, by_key)
        verify(client_rows, permitted, by_key)
        sample = {
            "core_read_ms": (core_done - start) * 1000,
            "edge_ingest_ms": (edge_ingested - core_done) * 1000,
            "edge_fetch_ms": (edge_done - edge_ingested) * 1000,
            "client_ingest_ms": (client_ingested - edge_done) * 1000,
            "client_fetch_ms": (done - client_ingested) * 1000,
            "total_ms": (done - start) * 1000,
            "core_rows": len(core_rows),
            "edge_rows": len(edge_rows),
            "client_rows": len(client_rows),
            "edge_stats": api.stats(edge),
            "client_stats": api.stats(client),
        }
        receipt["topology"].append(sample)
        save()
        print("topology", i, sample["total_ms"], flush=True)
        for name in [edge, client]:
            api.stop(name)
        api.transaction(args.name, [("query_requests", [{"request_id": 1}], True)])
    receipt["validation"] = validate_incremental(
        api, args.name, data, rows, by_key, permitted
    )
    save()
