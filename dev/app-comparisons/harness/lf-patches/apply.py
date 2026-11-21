#!/usr/bin/env python3
# Historical: patches the published jazz-tools 2.0.0-alpha.62 dist of the comparison's own Jazz copies
# (<hero>/jazz in the private comparisons repo). The upstream examples use the workspace jazz-tools,
# so these patches no longer apply as-is; patch packages/jazz-tools instead.
"""Patch (or restore) the published jazz-tools 2.0.0-alpha.62 dist of an app for lf-delivery experiments.
   python3 apply.py <pristine-dist> <name>[,<name>...]   names: marks, sync-outbound, sync-outbound-first, defer-delivery, no-server-debounce
   python3 apply.py <pristine-dist> none                 restores the pristine dist"""
import shutil, sys, os, glob
here = os.path.dirname(os.path.abspath(__file__))
# APP=stage-plan/jazz patches that app's copy instead (default band-chat/jazz).
jt = os.path.realpath(os.path.join(here, "..", "..", os.environ.get("APP", "band-chat/jazz"), "node_modules", "jazz-tools"))
pristine, names = sys.argv[1], [n for n in sys.argv[2].split(",") if n != "none"]
for rel in ["runtime/native-runtime/native-runtime-adapter.js", "runtime/native-runtime/browser-worker-transport.js"]:
    shutil.copyfile(os.path.join(pristine, rel), os.path.join(jt, "dist", rel))
def edit(rel, old, new, count=1):
    p = os.path.join(jt, "dist", rel); s = open(p).read()
    n = s.count(old)
    assert n == count, f"{rel}: expected {count} of {old!r}, found {n}"
    open(p, "w").write(s.replace(old, new))
ADAPTER = "runtime/native-runtime/native-runtime-adapter.js"
PUMP = "runtime/native-runtime/browser-worker-transport.js"
M = 'globalThis.performance?.mark?.'
TRIPLE = """        this.pumpSubscriptions();
        this.scheduleServerPump();
        this.notifyPeerTransportWork();
"""
for name in names:
    if name == "marks":
        # Instrumentation only: where the commit, subscription delivery and the worker drain happen.
        edit(ADAPTER, "        const write = this.db.commitTransaction(openTransactionId, pending.kind);\n",
             f"        {M}('jazz:commit:native-start');\n        const write = this.db.commitTransaction(openTransactionId, pending.kind);\n        {M}('jazz:commit:native-end');\n")
        edit(ADAPTER, "    pumpSubscriptions() {\n        this.drainSubscriptionPool();\n",
             f"    pumpSubscriptions() {{\n        {M}('jazz:pumpSubscriptions');\n        this.drainSubscriptionPool();\n")
        edit(PUMP, "        this.outboundDrainScheduled = true;\n", f"        this.outboundDrainScheduled = true;\n        {M}('jazz:outbound-drain-scheduled');\n")
        edit(PUMP, "        const frames = normalizeTransportFrames(this.transport.recvWireFrames());\n",
             f"        const frames = normalizeTransportFrames(this.transport.recvWireFrames());\n        if (frames.length > 0) {M}('jazz:drain', {{ detail: frames.length }});\n")
        edit(PUMP, "        this.running = true;\n", f"        this.running = true;\n        {M}('jazz:pump');\n")
    elif name == "sync-outbound":
        # Post outbound frames to the worker as soon as the native transport announces
        # them (synchronously, during the core tick) instead of from a microtask that
        # queues behind React's useSyncExternalStore render. Falls back to the microtask
        # if the transport can't be read re-entrantly.
        edit(PUMP, """    scheduleOutboundDrain() {
        if (this.closed || this.outboundDrainScheduled)
            return;
""", """    scheduleOutboundDrain() {
        if (this.closed || this.outboundDrainScheduled)
            return;
        try {
            this.drainOutboundFrames();
            globalThis.__jazzSyncOutbound = (globalThis.__jazzSyncOutbound ?? 0) + 1;
            return;
        }
        catch (error) {
            globalThis.__jazzSyncOutboundErrors = [...(globalThis.__jazzSyncOutboundErrors ?? []), String(error)].slice(-5);
        }
""")
    elif name == "sync-outbound-first":
        # As sync-outbound, but at most once per task and only the wire frames: the first
        # frame of a turn (a user's write) leaves at once; anything later in the same task
        # (a startup burst) is batched by the existing microtask as before.
        edit(PUMP, """    scheduleOutboundDrain() {
        if (this.closed || this.outboundDrainScheduled)
            return;
""", """    scheduleOutboundDrain() {
        if (this.closed || this.outboundDrainScheduled)
            return;
        if (!this.syncDrainedThisTask) {
            this.syncDrainedThisTask = true;
            setTimeout(() => {
                this.syncDrainedThisTask = false;
            }, 0);
            try {
                const frames = normalizeTransportFrames(this.transport.recvWireFrames());
                if (frames.length > 0)
                    this.sendFrames(frames);
                globalThis.__jazzSyncOutbound = (globalThis.__jazzSyncOutbound ?? 0) + 1;
                return;
            }
            catch (error) {
                globalThis.__jazzSyncOutboundErrors = [...(globalThis.__jazzSyncOutboundErrors ?? []), String(error)].slice(-5);
            }
        }
""")
    elif name == "defer-delivery":
        # Deliver a local write to local subscriptions (and so to React) one task later,
        # after the microtask chain that ticks the core and posts the frame to the worker.
        edit(ADAPTER, TRIPLE, """        this.scheduleDeferredSubscriptionPump();
        this.scheduleServerPump();
        this.notifyPeerTransportWork();
""", count=3)
        edit(ADAPTER, "    pumpSubscriptions() {\n", """    scheduleDeferredSubscriptionPump() {
        if (this.deferredSubscriptionPump)
            return;
        this.deferredSubscriptionPump = true;
        setTimeout(() => {
            this.deferredSubscriptionPump = false;
            if (!this.closed)
                this.pumpSubscriptions();
        }, 0);
    }
    pumpSubscriptions() {\n""")
    elif name == "no-server-debounce":
        # The worker sends to the server on the next task instead of after a 16 ms debounce.
        edit(ADAPTER, "const SERVER_PUMP_DEBOUNCE_MS = 16;", "const SERVER_PUMP_DEBOUNCE_MS = 0;")
    else:
        raise SystemExit(f"unknown patch {name}")
print("jazz-tools dist:", ",".join(names) or "pristine")
