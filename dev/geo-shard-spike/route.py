"""Option C prototype: derive per-session routing (reach sets) from read policies.

For every table, classify its read policy into how an unpinned query on it
can be routed:
  direct    - a conjunct compares one of T's own columns with a session value:
              a directory on (T, col) finds the partitions of the rows.
  colocated - an EXISTS over a co-located table U, anchored on a session value:
              a directory on (U, col) finds the partitions.
  inherit   - inherits from the shard parent, whose policy routes.
  two-hop   - inherits from rows that reference it on another partition:
              route the referencing rows first, then follow their ids.
  wide      - anchored only on a literal (e.g. public = true): a directory on
              the literal finds the partitions, but it can be every partition.
  scatter   - nothing narrows it (True, NOT, unanchored predicates).
  none      - never readable (False).
A conjunction routes by its best conjunct; a disjunction by its worst arm.
The shard plan is inferred from the policies: a table's shard parent is the
FK its read policy correlates through (Inherits via, or EXISTS correlation).
"""
import json, sys, collections

RANK = {"none": 0, "direct": 1, "colocated": 1, "inherit": 1, "two-hop": 2, "wide": 3, "scatter": 4}

def is_session(v):
    return v and v.get("type") == "SessionRef" and v["path"][0] != "__jazz_outer_row"

def outer_col(v):
    return v["path"][1] if v and v.get("type") == "SessionRef" and v["path"][0] == "__jazz_outer_row" else None

class App:
    def __init__(self, data):
        self.perms = data["perms"] or {}
        self.schema = data["schema"] or {}
        self.fk = {t: {c["name"]: c.get("references") for c in s.get("columns", []) if c.get("references")}
                   for t, s in self.schema.items()}
        self.parent = {}
        for t in self.perms:
            p = self.shard_parent(t)
            if p:
                self.parent[t] = p
        self.memo = {}
        self.directories = set()

    def select(self, t):
        return (self.perms.get(t) or {}).get("select", {}).get("using", {"type": "False"})

    def shard_parent(self, t):
        """(fk column, parent table) the read policy correlates through."""
        found = []
        def walk(e):
            ty = e.get("type")
            if ty == "Inherits" and e["via_column"] in self.fk.get(t, {}):
                found.append((e["via_column"], self.fk[t][e["via_column"]]))
            elif ty == "Exists":
                for c in conjuncts(e["condition"]):
                    if c.get("type") == "Cmp" and c["op"] == "Eq":
                        oc = outer_col(c["value"])
                        if oc and oc in self.fk.get(t, {}):
                            found.append((oc, self.fk[t][oc]))
                        elif oc == "id" and c["column"] == "id" and e["table"] != t:
                            pass
            for k in ("exprs",):
                for x in e.get(k, []): walk(x)
            if ty == "Not": walk(e["expr"]) if "expr" in e else None
        walk(self.select(t))
        other = [f for f in found if f[1] != t]
        if other:
            return other[0]
        # A self-referencing tree shards with its top ancestor.
        return None

    def root(self, t, seen=()):
        if t in seen or t not in self.parent:
            return t
        return self.root(self.parent[t][1], seen + (t,))

    def route(self, t, depth=0):
        if (t, depth) in self.memo:
            return self.memo[(t, depth)]
        self.memo[(t, depth)] = ("scatter", "cycle")
        r = self.expr(t, self.select(t), depth)
        self.memo[(t, depth)] = r
        return r

    def expr(self, t, e, depth):
        ty = e.get("type")
        if ty == "False":
            return ("none", "")
        if ty == "True":
            return ("scatter", "public (True)")
        if ty in ("Cmp", "InList") and ty == "Cmp" and e["op"] == "Eq" and is_session(e["value"]):
            self.directories.add((t, e["column"]))
            return ("direct", f"{t}.{e['column']} = session")
        if ty == "In":
            self.directories.add((t, e["column"]))
            return ("direct", f"{t}.{e['column']} in session array")
        if ty == "Cmp" and e["op"] == "Eq" and e["value"].get("type") == "Literal":
            return ("wide", f"{t}.{e['column']} = literal")
        if ty == "InList" and all(v.get("type") == "Literal" for v in e["values"]):
            return ("wide", f"{t}.{e['column']} in literals")
        if ty == "And":
            rs = [self.expr(t, x, depth) for x in e["exprs"]]
            return min(rs, key=lambda r: RANK[r[0]])
        if ty == "Or":
            rs = [self.expr(t, x, depth) for x in e["exprs"]]
            worst = max(rs, key=lambda r: RANK[r[0]])
            if all(RANK[r[0]] <= 1 or r[0] == "none" for r in rs):
                return ("direct" if worst[0] == "none" else worst[0], " | ".join(r[1] for r in rs if r[0] != "none"))
            return worst
        if ty == "Exists":
            u = e["table"]
            conds = conjuncts(e["condition"])
            anchors = [c for c in conds if c.get("type") == "Cmp" and c["op"] == "Eq" and is_session(c["value"])]
            corr = [(c["column"], outer_col(c["value"])) for c in conds
                    if c.get("type") == "Cmp" and c["op"] == "Eq" and outer_col(c["value"])]
            if not anchors:
                # The anchor may sit deeper (an OR, or U's own inheritance).
                rest = {"type": "And", "exprs": [c for c in conds if not (c.get("type") == "Cmp" and outer_col(c["value"]))]}
                inner = self.expr(u, rest, depth + 1) if rest["exprs"] else ("scatter", "no anchor")
                if not corr:
                    return ("scatter", f"uncorrelated EXISTS {u} (session-only gate)")
                if RANK[inner[0]] <= 1:
                    if self.root(u) == self.root(t):
                        return ("colocated", f"EXISTS {u} [{inner[1]}]")
                    return ("two-hop", f"EXISTS {u} on another root [{inner[1]}]")
                return (inner[0], f"EXISTS {u}: {inner[1]}")
            if not corr:
                return ("scatter", f"uncorrelated EXISTS {u} (session-only gate)")
            uc, tc = corr[0]
            self.directories.add((u, anchors[0]["column"]))
            if self.root(u) == self.root(t):
                return ("colocated", f"EXISTS {u}.{anchors[0]['column']} = session")
            # The anchor finds U rows; their uc values name T rows elsewhere.
            return ("two-hop", f"EXISTS {u} on another root ({self.root(u)} vs {self.root(t)})")
        if ty == "Inherits":
            if depth > 6:
                return ("scatter", "deep inherit")
            p = self.fk.get(t, {}).get(e["via_column"])
            if not p:
                return ("scatter", f"inherit via non-FK {e['via_column']}")
            if p == t:
                # A tree inside one partition (children shard with their
                # top ancestor): the recursion adds no partitions beyond the
                # ones the other arms already found.
                return ("none", "self-recursive within its root")
            r = self.route(p, depth + 1)
            if r[0] in ("none",):
                return r
            if self.parent.get(t, (None,))[0] == e["via_column"]:
                return ("inherit", f"via {e['via_column']} -> {p}: {r[0]}") if RANK[r[0]] <= 1 else (r[0], f"via {p}: {r[1]}")
            return ("two-hop", f"inherits {p} on another root") if RANK[r[0]] <= 2 else r
        if ty == "InheritsReferencing":
            r = self.route(e["source_table"], depth + 1)
            if RANK[r[0]] <= 2:
                return ("two-hop", f"referenced by {e['source_table']}")
            return r
        if ty in ("SessionCmp", "SessionIsNull", "SessionIsNotNull", "SessionContains", "SessionInList"):
            return ("scatter", "session-only gate")
        if ty == "Not":
            return ("scatter", "NOT")
        return ("scatter", f"unanchored {ty} {e.get('column','')}")

def conjuncts(e):
    return e["exprs"] if e.get("type") == "And" else [e]

def main(files):
    total = collections.Counter()
    rows = []
    for f in files:
        name = f.split("/")[-1][:-5]
        app = App(json.load(open(f)))
        counts = collections.Counter()
        detail = []
        for t in sorted(app.perms):
            if t.startswith("better_auth") or t.startswith("jazz_"):
                continue
            kind, why = app.route(t)
            counts[kind] += 1
            detail.append((t, kind, why, app.root(t)))
        total.update(counts)
        rows.append((name, counts, len(app.directories), detail))
    for name, counts, dirs, detail in rows:
        print(f"## {name}: " + ", ".join(f"{k} {v}" for k, v in sorted(counts.items(), key=lambda kv: RANK[kv[0]])) + f"; directories {dirs}")
        for t, kind, why, root in detail:
            print(f"   {t:28} {kind:9} root={root:20} {why}")
    print("TOTAL", dict(total))

if __name__ == "__main__":
    main(sys.argv[1:])
