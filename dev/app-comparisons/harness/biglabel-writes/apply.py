#!/usr/bin/env python3
# Historical: patches the published jazz-tools 2.0.0-alpha.62 dist of the comparison's own Jazz copies
# (<hero>/jazz in the private comparisons repo). The upstream examples use the workspace jazz-tools,
# so these patches no longer apply as-is; patch packages/jazz-tools instead.
"""Patch (or restore) big-label/jazz's published jazz-tools 2.0.0-alpha.62 dist for biglabel-writes prototypes.
   python3 apply.py <pristine-dist> <name>[,<name>...]    names: debounce0, sync-outbound-first, tx-read-local
   sync-outbound-first and tx-read-local only act when localStorage["bl-fix"] names them (page code), so one build compares them.
   python3 apply.py <pristine-dist> none                  restores the pristine dist
Rebuild the app afterwards (serve.mjs does: next build)."""
import shutil, sys, os
here = os.path.dirname(os.path.abspath(__file__))
jt = os.path.realpath(os.path.join(here, "..", "..", "big-label/jazz/node_modules/jazz-tools"))
pristine, names = sys.argv[1], [n for n in sys.argv[2].split(",") if n != "none"]
ADAPTER = "runtime/native-runtime/native-runtime-adapter.js"
PUMP = "runtime/native-runtime/browser-worker-transport.js"
WORKER = "worker/jazz-broker-worker.js"
DB = "runtime/db.js"
GATE = '(() => { try { return (globalThis.localStorage?.getItem("bl-fix") ?? "").split(",").includes(%r); } catch { return false; } })()'
for rel in [ADAPTER, PUMP, WORKER, DB]:
    shutil.copyfile(os.path.join(pristine, rel), os.path.join(jt, "dist", rel))
def edit(rel, old, new, count=1):
    p = os.path.join(jt, "dist", rel); s = open(p).read()
    n = s.count(old)
    assert n == count, f"{rel}: expected {count} of {old!r}, found {n}"
    open(p, "w").write(s.replace(old, new))
for name in names:
    if name == "debounce0":
        # The runtime (page and SharedWorker) sends to the server on the next task, not after 16 ms.
        edit(ADAPTER, "const SERVER_PUMP_DEBOUNCE_MS = 16;", "const SERVER_PUMP_DEBOUNCE_MS = 0;")
        edit(WORKER, "var SERVER_PUMP_DEBOUNCE_MS = 16;", "var SERVER_PUMP_DEBOUNCE_MS = 0;")
    elif name == "sync-outbound-first":
        GATE_SYNC = GATE % "sync-outbound-first"
        # From profile/lf-delivery: the page posts its first outbound frame per task to the
        # worker synchronously, instead of queueing it behind React's render.
        edit(PUMP, """    scheduleOutboundDrain() {
        if (this.closed || this.outboundDrainScheduled)
            return;
""", """    scheduleOutboundDrain() {
        if (this.closed || this.outboundDrainScheduled)
            return;
        if (!this.syncDrainedThisTask && GATE_SYNC) {
            this.syncDrainedThisTask = true;
            setTimeout(() => {
                this.syncDrainedThisTask = false;
            }, 0);
            try {
                const frames = normalizeTransportFrames(this.transport.recvWireFrames());
                if (frames.length > 0)
                    this.sendFrames(frames);
                return;
            }
            catch (error) {
                globalThis.__jazzSyncOutboundErrors = [...(globalThis.__jazzSyncOutboundErrors ?? []), String(error)].slice(-5);
            }
        }
""".replace("GATE_SYNC", GATE_SYNC))
    elif name == "tx-read-local":
        # A serializable transaction's read with firstLoadRemoteWaitMs: 0 reads the local replica
        # (the internal local-only tier) instead of always fetching from the authority. The
        # authority re-runs the read at commit (sync.rs serialization_conflict), so a stale
        # replica can only cause a conflict.
        edit(DB, """        const rows = await client.queryInternal(translateQuery(builderJson, planningSchema, prepared.physical), {
            ...queryOptions,
            openTransactionId,
        }, this.context?.readSession);""", """        if (this.kind === "serializable" && options?.firstLoadRemoteWaitMs === 0 && """ + GATE % "tx-read-local" + """)
            queryOptions.tier = "local-only";
        const rows = await client.queryInternal(translateQuery(builderJson, planningSchema, prepared.physical), {
            ...queryOptions,
            openTransactionId,
        }, this.context?.readSession);""")
    else:
        raise SystemExit(f"unknown patch {name}")
print("jazz-tools dist:", ",".join(names) or "pristine")
