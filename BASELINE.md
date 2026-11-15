# Baseline on 2.0.0-alpha.64 (npm), 2026-11-15 07:15 UTC

```
===== t1-migrations
validate --check exit=0
PASS alice writes + reads task (enum 'done', renamed column) — {"id":"01a1de02-4861-73b4-8a6e-e63df6a72914","projectId":"01a1de02-4837-7bc7-83dd-fa055e0cd700","title":"T1","details":"d","status":"done"}
PASS bob (member) sees task
PASS carol (non-member) sees nothing
PASS carol can't add members
SMOKE PASS
===== t2-computed
validate --check exit=0
PASS bob reads 2 rows
PASS computed priority — high,low
PASS computed label — Fix (9)
PASS transform dueAt round-trips ISO date — 2026-12-01
PASS array + json
PASS bob can't update alice's task
SMOKE PASS
===== t3-permissions
validate --check exit=0
PASS editor sees both
PASS viewer sees only published
PASS outsider sees none
PASS staff claim sees both
PASS share grants friend the draft
PASS friend comment visible to ann (allowedTo.read)
PASS viewer can't insert
PASS locked doc can't be edited (whereOld ne)
PASS admin can't change own role
SMOKE PASS
===== t6-import-style
validate --check exit=0
PASS alice reads board with included notes (full slice) — ["via slice"]
PASS alice reads via notesOnly slice
PASS bob sees nothing
SMOKE PASS
===== t5-expo (backend smoke + web export)
validate --check exit=0
PASS alice reads note + 200 KB photo
PASS bob sees nothing
SMOKE PASS
expo export -p web exit=0
===== t7-enum-cases
validate --check exit=0
PASS ticket without kind round-trips
SMOKE PASS
===== t1 fixture store (.64 data)
PASS existing .64 store opens; alice still sees her task
SMOKE PASS
```

t4 (Vercel, jazz-eval-a64-next): validate --check exit 0; smoke 17/17 PASS (see #a64-gate-vercel-next).
