#!/usr/bin/env python3
"""Synthetic shallow-history SQL reference. No Jazz wire/coordination emulation."""
import argparse
import hashlib
import json
from pathlib import Path
import sqlite3
import time


def scalar(value):
    kind, value = next(iter(value.items()))
    if kind == 'Nullable':
        return None if value is None else scalar(value)
    assert kind in {'Uuid', 'String', 'U64', 'U32', 'I64', 'F64', 'Bool', 'EnumTag'}, kind
    return int(value) if kind == 'Bool' else value


def quoted(name):
    return '"' + name.replace('"', '""') + '"'


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('fixture', type=Path)
    ap.add_argument('--sqlite', type=Path)
    ap.add_argument('--postgres', help='Dedicated empty database; creates sql_proxy schema')
    ap.add_argument('--out', type=Path, required=True)
    ap.add_argument('--rounds', type=int, default=5)
    args = ap.parse_args()
    data = json.loads(args.fixture.read_text())
    pg = bool(args.postgres)
    if pg:
        import psycopg
        db = psycopg.connect(args.postgres, autocommit=True)
        db.execute('CREATE SCHEMA sql_proxy')
        db.execute('SET client_encoding TO UTF8')
        db.execute('SET search_path=sql_proxy')
        version = db.execute('SELECT version()').fetchone()[0]
    else:
        assert args.sqlite and not args.sqlite.exists(), 'Use a fresh SQLite file'
        db = sqlite3.connect(args.sqlite)
        version = sqlite3.sqlite_version
    marker = '%s' if pg else '?'
    def many(sql, rows):
        with db.cursor() if pg else db as cur:
            if pg:
                cur.executemany(sql, rows)
            else:
                db.executemany(sql, rows)
    tables = {t: [] for t in data['expected']}
    for ordinal, write in enumerate(data['writes'], 1):
        cells = {k: scalar(v) for k, v in write['cells'].items()}
        tables[write['table']].append((write['id'], ordinal, cells))
    assert len({(w['table'], w['id']) for w in data['writes']}) == len(data['writes'])
    db.execute('CREATE TABLE transactions(tx BIGINT PRIMARY KEY, accepted INTEGER NOT NULL)')
    db.execute('CREATE TABLE parents(child BIGINT NOT NULL, parent BIGINT NOT NULL, PRIMARY KEY(child,parent))')
    db.execute('CREATE INDEX parents_parent ON parents(parent)')
    db.execute('CREATE TABLE bundles(version BIGINT PRIMARY KEY, table_name TEXT, row_id TEXT, payload TEXT)')
    many(f'INSERT INTO transactions VALUES ({marker},1)', [(i,) for i in range(1, len(data['writes'])+1)])
    columns = {}
    for table, rows in tables.items():
        cols = list(rows[0][2]) if rows else []
        columns[table] = cols
        types = {k: ('DOUBLE PRECISION' if isinstance(rows[0][2][k], float) else 'BIGINT' if isinstance(rows[0][2][k], int) else 'TEXT') for k in cols}
        decl = ''.join(f', {quoted(k)} {types[k]}' for k in cols)
        db.execute(f'CREATE TABLE {quoted("c_"+table)} (id TEXT PRIMARY KEY{decl})')
        db.execute(f'CREATE TABLE {quoted("h_"+table)} (id TEXT, version BIGINT PRIMARY KEY, tx BIGINT NOT NULL{decl})')
        db.execute(f'CREATE INDEX {quoted("h_"+table+"_id")} ON {quoted("h_"+table)}(id)')
        many(f'INSERT INTO {quoted("c_"+table)} VALUES ({",".join([marker]*(1+len(cols)))})', [(i,*c.values()) for i,v,c in rows])
        many(f'INSERT INTO {quoted("h_"+table)} VALUES ({",".join([marker]*(3+len(cols)))})', [(i,v,v,*c.values()) for i,v,c in rows])
        many(f'INSERT INTO bundles VALUES ({",".join([marker]*4)})', [(v,table,i,json.dumps(c,sort_keys=True,separators=(',',':'))) for i,v,c in rows])
        for col in cols:
            if col in {'parent_id', 'resource', 'team', 'member_id', 'target_id', 'user_id', 'group_id'}:
                for prefix in ['c_', 'h_']:
                    db.execute(f'CREATE INDEX {quoted(prefix+table+"_"+col)} ON {quoted(prefix+table)}({quoted(col)})')
        # Actual history selection is exercised even though no parent edge exists.
        db.execute(f'''CREATE VIEW {quoted('v_'+table)} AS
            SELECT h.* FROM {quoted('h_'+table)} h JOIN transactions t ON t.tx=h.tx AND t.accepted=1
            WHERE NOT EXISTS (SELECT 1 FROM parents p JOIN transactions successor ON successor.tx=p.child AND successor.accepted=1 WHERE p.parent=h.version)''')
    db.commit()
    db.execute('ANALYZE')
    resources = {r['table']: r for r in data['resources']}
    children = {r['child']: r for r in data['resources'] if r['child']}
    def query(table, history, witnesses):
        prefix = 'v_' if history else 'c_'
        q = lambda t: quoted(prefix+t)
        resource = resources.get(table) or children.get(table)
        if not resource:
            if witnesses:
                return f'SELECT b.* FROM {q(table)} r JOIN bundles b ON b.version=r.version'
            return f'SELECT id{"," if columns[table] else ""}{",".join(map(quoted,columns[table]))} FROM {q(table)}'
        # Gather is bounded at depth 8 and deduplicates reached group IDs.
        # UNION(id,depth) deliberately retains depth to honor the bound, then DISTINCT id.
        cte = f'''WITH RECURSIVE reach(id,depth) AS (
            SELECT group_id,0 FROM {q('group_access_edges')} WHERE user_id='{data['account']}'
            UNION SELECT e.target_id,r.depth+1 FROM reach r JOIN {q('group_entry')} e ON e.member_id=r.id
                JOIN {q('group')} g ON g.id=e.target_id WHERE e.administrator=0 AND r.depth<8
        ), reached AS (SELECT DISTINCT id FROM reach),
        allowed AS (SELECT DISTINCT a.resource FROM {q(resource['access'])} a JOIN reached g ON g.id=a.team WHERE a.administrator=0),
        output AS (SELECT r.* FROM {q(table)} r WHERE EXISTS (
            SELECT 1 FROM allowed a WHERE a.resource=r.{"parent_id" if table in children else "id"}))'''
        if not witnesses:
            return cte + f' SELECT id,{",".join(map(quoted,columns[table]))} FROM output'
        # Conservative, readable supporting set: output, parent, matching grants,
        # and the reachable permission graph. Not a claim of Jazz-identical provenance.
        parent_select = f"SELECT p.version FROM {q(resource['table'])} p JOIN output o ON o.parent_id=p.id" if table in children else 'SELECT version FROM output'
        parent_ids = 'SELECT DISTINCT parent_id FROM output' if table in children else 'SELECT id FROM output'
        return cte + f''', needed(version) AS (
            SELECT version FROM output UNION {parent_select}
            UNION SELECT a.version FROM {q(resource['access'])} a JOIN reached g ON a.team=g.id WHERE a.administrator=0 AND a.resource IN ({parent_ids})
            UNION SELECT s.version FROM {q('group_access_edges')} s WHERE s.user_id='{data['account']}'
            UNION SELECT e.version FROM {q('group_entry')} e JOIN reach r ON e.member_id=r.id WHERE e.administrator=0 AND r.depth<8
            UNION SELECT g.version FROM {q('group')} g JOIN reached r ON r.id=g.id
        ) SELECT b.* FROM needed n JOIN bundles b ON b.version=n.version'''
    receipts=[]
    expected_ids = {t: set(ids) for t,ids in data['expected'].items()}
    expected = {t: {i: tuple([i]+[c[k] for k in columns[t]]) for i,v,c in rows if i in expected_ids[t]} for t,rows in tables.items()}
    # Independent fixture oracle for the conservative support policy above.
    by_table = {t: {i: (v,c) for i,v,c in rows} for t,rows in tables.items()}
    direct = {i for i,v,c in tables['group_access_edges'] if c['user_id']==data['account']}
    reached = {by_table['group_access_edges'][i][1]['group_id'] for i in direct}
    traversed = set()
    for depth in range(8):
        next_groups = set(reached)
        for i,v,c in tables['group_entry']:
            if c['member_id'] in reached and c['administrator']==0 and c['target_id'] in by_table['group']:
                traversed.add(i)
                next_groups.add(c['target_id'])
        reached = next_groups
    witness_expected = {}
    bundles = {v: (v,t,i,json.dumps(c,sort_keys=True,separators=(',',':'))) for t,rows in tables.items() for i,v,c in rows}
    for table in tables:
        needed = {by_table[table][i][0] for i in expected_ids[table]}
        resource = resources.get(table) or children.get(table)
        if resource:
            parent_ids = {by_table[table][i][1]['parent_id'] for i in expected_ids[table]} if table in children else expected_ids[table]
            needed.update(by_table[resource['table']][i][0] for i in parent_ids)
            needed.update(v for i,v,c in tables[resource['access']] if c['resource'] in parent_ids and c['team'] in reached and c['administrator']==0)
            needed.update(by_table['group_access_edges'][i][0] for i in direct)
            needed.update(by_table['group_entry'][i][0] for i in traversed)
            needed.update(by_table['group'][i][0] for i in reached)
        witness_expected[table] = {v: bundles[v] for v in needed}
    plans={}
    queries_sql={}
    for round_no in range(args.rounds):
        # Rotate variant order, so history does not always inherit current's cache.
        modes=['current','history','witness']
        modes=modes[round_no%3:]+modes[:round_no%3]
        for mode in modes:
            times=[]; counts=[]; hashes=[]
            for table in sorted(tables):
                sql=query(table,mode!='current',mode=='witness')
                start=time.perf_counter_ns()
                result=db.execute(sql).fetchall()
                elapsed=(time.perf_counter_ns()-start)/1e6
                # Validation and hashes explicitly OUTSIDE measured execute+fetch.
                if mode!='witness':
                    assert {row[0]: tuple(row) for row in result} == expected[table], (mode,table,len(result),len(expected[table]))
                    assert len(result)==len(expected[table])
                else:
                    assert {r[0]: tuple(r) for r in result} == witness_expected[table], ('support',table)
                    assert len({r[0] for r in result})==len(result)
                    roots={r[2] for r in result if r[1]==table}
                    assert roots==set(expected[table]), (table,len(roots),len(expected[table]))
                digest=hashlib.sha256(json.dumps(sorted(result),separators=(',',':')).encode()).hexdigest()
                times.append((table,elapsed)); counts.append((table,len(result))); hashes.append((table,digest))
                if round_no==0 and table=='res_l_child_3':
                    queries_sql[mode]=sql
                    explain='EXPLAIN (ANALYZE, BUFFERS, TIMING OFF, FORMAT JSON) ' if pg else 'EXPLAIN QUERY PLAN '
                    plans[mode]=db.execute(explain+sql).fetchall()
            receipt={'round':round_no,'mode':mode,'total_ms':sum(t for _,t in times),'tables_ms':dict(times),'counts':dict(counts),'hashes':dict(hashes)}
            receipts.append(receipt)
            print(json.dumps({k:v for k,v in receipt.items() if k in ['round','mode','total_ms']}),flush=True)
    # Untimed sensitivity checks: execute the machinery instead of relying on
    # the all-accepted, zero-predecessor fixture to accidentally make it correct.
    db.commit()
    db.execute('BEGIN')
    root, version_id, cells = tables['res_l'][0]
    fresh = len(data['writes'])+1
    db.execute(f'INSERT INTO transactions VALUES ({fresh},0)')
    cols = ','.join(map(quoted, columns['res_l']))
    db.execute(f'INSERT INTO h_res_l SELECT id,{fresh},{fresh},{cols} FROM h_res_l WHERE version={version_id}')
    db.execute(f'INSERT INTO parents VALUES ({fresh},{version_id})')
    assert db.execute(f"SELECT version FROM v_res_l WHERE id='{root}'").fetchall()==[(version_id,)]
    db.execute(f'UPDATE transactions SET accepted=1 WHERE tx={fresh}')
    assert db.execute(f"SELECT version FROM v_res_l WHERE id='{root}'").fetchall()==[(fresh,)]
    db.execute('ROLLBACK')
    db.execute('BEGIN')
    db.execute("UPDATE transactions SET accepted=0 WHERE tx IN (SELECT tx FROM h_group_access_edges)")
    assert db.execute(query('res_l_child_3',True,False)).fetchall()==[]
    assert db.execute(query('res_l_child_3',False,False)).fetchall()!=[]
    db.execute('ROLLBACK')
    args.out.write_text(json.dumps({'engine':version,'fixture_sha256':hashlib.sha256(args.fixture.read_bytes()).hexdigest(),'rows':len(data['writes']),'queries':len(tables),'sensitivity':'unaccepted successor, accepted successor, permission seed withdrawal passed','receipts':receipts,'plans':plans,'dominant_queries':queries_sql},indent=2,default=str))
    db.close()

if __name__=='__main__':
    main()
