"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import {
  Check,
  Cloud,
  Loader2,
  Plane,
  Plus,
  Server,
  Smartphone,
  type LucideIcon,
} from "lucide-react";

import { cn } from "@/lib/cn";
import {
  type Anchors,
  DEFAULT_CORNER_RADIUS,
  Graph,
  type GraphNode,
  type GraphOverlayCtx,
  NodeAction,
  NodeFooter,
  NodeIcon,
  NodeShell,
  NodeSubtitle,
  NodeTitle,
  type Point,
  roundedPath,
  useDiagramTraces,
} from "./diagram";
import { PhoneChrome } from "./diagram/phone-chrome";
import { INITIAL_COLOR, pickNextColor } from "./colour";

type Tier = "local" | "edge" | "global";
const TIERS: Tier[] = ["local", "edge", "global"];

const TIER_LABEL: Record<Tier, string> = {
  local: "Local",
  edge: "Edge",
  global: "Global",
};

const TIER_DESCRIPTION: Record<Tier, string> = {
  local: "Durable on this device. No network round-trip required.",
  edge: "Durable on a regional sync server. Survives the device going offline.",
  global: "Durable in the global core. Replicated across regions.",
};

type NodeKey = "phone" | "edge" | "global" | "receiver";
// The three cards stacked on the right, each driven off the edge.
type CardKey = "global" | "edge" | "receiver";
const CARD_META: Record<CardKey, { icon: LucideIcon; title: string; subtitle: string }> = {
  global: { icon: Cloud, title: "Global", subtitle: "global core" },
  edge: { icon: Server, title: "Edge", subtitle: "regional sync server" },
  receiver: { icon: Smartphone, title: "Other device", subtitle: "receives updates" },
};

// hop key ⇒ the routed segment it animates along (built in connectorPaths).
type Hop = "edge" | "global" | "receiver";
const HOPS: Hop[] = ["edge", "global", "receiver"];

type EventPhase = "writing" | "settled";
type WriteEvent = {
  id: number;
  tier: Tier;
  value: string;
  phase: EventPhase;
  startedAt: number;
  // When non-null, the event is parked: no animation runs until the device
  // reconnects and queuedSince is cleared.
  queuedSince: number | null;
  // Flipped to true the moment the wave reaches the edge tier — at that
  // point the write is no longer durable only on the device.
  reachedEdge: boolean;
};

type Status =
  | { kind: "idle" }
  | { kind: "writing"; tier: Tier; eventId: number }
  | { kind: "settled"; tier: Tier; eventId: number };

type PulseState = { key: number };

const STAGE_MS = 850;
const TICK_LINGER_MS = 1500;
const FADE_MS = 600;
// Gap between consecutive queued writes when the device reconnects, so the
// user sees the propagation waves separated rather than blurred together.
const RECONNECT_STAGGER_MS = 280;
// Where the other-device branch leaves the edge's bottom edge, as a fraction
// of its half-width right of centre — far enough off the trunk to read as a
// fork, but inside the card so nothing leaves the measured layout.
const BRANCH_OFFSET = 0.6;

// Bespoke connector over the engine-measured node geometry: an L-shaped arm
// from the phone into the edge tier, a riser up to the global tier, and a
// riser down to the other device. The three hops animate along these paths.
// (Same pattern as LensDiagram, which also draws its own connector from
// `ctx.anchors`.)
function connectorPaths(
  anchors: Record<string, Anchors>,
  branchDx: number,
): Record<Hop, string> | null {
  const phone = anchors.phone;
  const edge = anchors.edge;
  const global = anchors.global;
  const receiver = anchors.receiver;
  if (!phone || !edge || !global || !receiver) return null;
  const armX = (phone.right + edge.left) / 2;
  // Trunk (phone → edge → global) runs up the column centreline. The other
  // device sits `branchDx` to the right (the card is shifted by the same
  // amount), so the branch is one clean side-step off the trunk into it. On
  // mobile branchDx is 0 ⇒ a straight riser, nothing leaves the layout.
  const branchY = (edge.bottom + receiver.top) / 2;
  const branchX = receiver.midX + branchDx;
  // Rounded elbows, matching the engine's own routed edges.
  const route = (pts: Point[]) => roundedPath(pts, DEFAULT_CORNER_RADIUS);
  return {
    edge: route([
      { x: phone.right, y: phone.midY },
      { x: armX, y: phone.midY },
      { x: armX, y: edge.midY },
      { x: edge.left, y: edge.midY },
    ]),
    global: route([
      { x: edge.midX, y: edge.top },
      { x: edge.midX, y: global.bottom },
    ]),
    receiver: route([
      { x: edge.midX, y: edge.bottom },
      { x: edge.midX, y: branchY },
      { x: branchX, y: branchY },
      { x: branchX, y: receiver.top },
    ]),
  };
}

function StatusBody({ status }: { status: Status }) {
  if (status.kind === "idle") {
    return <span className="text-fd-muted-foreground italic">Ready</span>;
  }
  if (status.kind === "writing") {
    return (
      <>
        <Loader2 className="h-3.5 w-3.5 animate-spin text-fd-primary" />
        <span className="text-fd-foreground">Awaiting {TIER_LABEL[status.tier]}…</span>
      </>
    );
  }
  return (
    <>
      <Check className="h-3.5 w-3.5 text-[#16a34a]" />
      <span className="text-fd-foreground">
        {status.tier === "local" ? "Settled locally" : `Settled at ${TIER_LABEL[status.tier]}`}
      </span>
    </>
  );
}

function AeroplaneToggle({ offline, onToggle }: { offline: boolean; onToggle: () => void }) {
  return (
    <button
      type="button"
      onClick={onToggle}
      aria-pressed={offline}
      className={cn(
        "rounded-md border px-2 py-1.5 text-[11px] font-medium flex items-center justify-between gap-2 transition-colors cursor-pointer",
        offline
          ? "border-amber-500/40 bg-amber-500/10 text-amber-700 dark:text-amber-300 dark:bg-amber-500/15"
          : "border-fd-border text-fd-muted-foreground hover:bg-fd-accent",
      )}
    >
      <span className="flex items-center gap-1.5">
        <Plane className="h-3 w-3" />
        Aeroplane mode
      </span>
      <span
        className={cn(
          "relative inline-block rounded-full transition-colors",
          offline ? "bg-amber-500" : "bg-fd-border",
        )}
        style={{ width: "28px", height: "14px" }}
      >
        <span
          className="absolute rounded-full bg-white shadow"
          style={{
            width: "10px",
            height: "10px",
            top: "2px",
            left: offline ? "16px" : "2px",
            transition: "left 200ms ease",
          }}
        />
      </span>
    </button>
  );
}

// Hex label + swatch, animating in on change. Themed (engine tokens) — used
// by both the cards and the phone's "what you wrote" row, which follow the
// host theme.
function ColourTag({ colour, compact }: { colour: string | null; compact?: boolean }) {
  const box = compact ? "0.85rem" : "0.9rem";
  const fg = "var(--diagram-fg)";
  const swatchBorder = "var(--diagram-border, #e4e4e7)";
  return (
    <span style={{ display: "inline-flex", alignItems: "center", gap: "0.375rem" }}>
      <span
        style={{
          fontSize: compact ? "0.6875rem" : "0.75rem",
          fontFamily: "var(--font-mono, ui-monospace, monospace)",
          color: fg,
          overflow: "hidden",
        }}
      >
        <span key={colour ?? "none"} className="diagram-tally" style={{ display: "inline-block" }}>
          {colour ?? "—"}
        </span>
      </span>
      <span
        style={{
          display: "inline-block",
          overflow: "hidden",
          borderRadius: "2px",
          border: `1px solid ${swatchBorder}`,
          height: box,
          width: box,
          flexShrink: 0,
        }}
      >
        <span
          key={`sw-${colour ?? "none"}`}
          className="diagram-tally"
          style={{
            display: "block",
            height: "100%",
            width: "100%",
            backgroundColor: colour ?? "transparent",
          }}
        />
      </span>
    </span>
  );
}

function PhoneScreen({
  tier,
  setTier,
  status,
  onWrite,
  offline,
  onToggleOffline,
  unsynced,
  colour,
}: {
  tier: Tier;
  setTier: (t: Tier) => void;
  status: Status;
  onWrite: () => void;
  offline: boolean;
  onToggleOffline: () => void;
  unsynced: number;
  colour: string | null;
}) {
  // A pending promise blocks further writes whether or not the device is
  // online — offline simply means it stays pending until reconnect.
  const locked = status.kind === "writing";
  return (
    <div className="flex flex-col gap-3 px-1 py-1">
      <AeroplaneToggle offline={offline} onToggle={onToggleOffline} />

      <div>
        <div className="text-[10px] uppercase tracking-wide text-fd-muted-foreground font-semibold mb-2">
          Wait for
        </div>
        <div className="grid grid-cols-3 rounded-lg border border-fd-border bg-fd-card p-1 gap-1">
          {TIERS.map((t) => (
            <button
              key={t}
              type="button"
              disabled={locked}
              onClick={() => setTier(t)}
              className={cn(
                "px-2 py-1 rounded text-xs font-medium transition-colors",
                tier === t
                  ? "bg-fd-primary text-fd-primary-foreground"
                  : "text-fd-foreground hover:bg-fd-accent",
                locked && tier !== t && "opacity-40",
                locked && "cursor-not-allowed",
              )}
            >
              {TIER_LABEL[t]}
            </button>
          ))}
        </div>
        <p className="text-[11px] text-fd-muted-foreground mt-2 italic text-balance min-h-[2.7em]">
          {TIER_DESCRIPTION[tier]}
        </p>
      </div>

      <div
        className="rounded-md flex items-center justify-center px-3 py-2 transition-colors"
        style={{ backgroundColor: colour ? `${colour}26` : undefined }}
      >
        <ColourTag colour={colour} compact />
      </div>

      <NodeAction disabled={locked} onClick={onWrite} aria-label="Write a new colour">
        {locked ? (
          <Loader2 className="h-3.5 w-3.5 animate-spin" />
        ) : (
          <Plus className="h-3.5 w-3.5" aria-hidden="true" />
        )}
        <span>{locked ? "Waiting…" : "New colour"}</span>
      </NodeAction>

      <div className="rounded-md bg-fd-muted/40 px-3 py-2 flex items-center justify-between text-xs">
        <span className="text-fd-muted-foreground">Status</span>
        <span className="flex items-center gap-1.5 font-medium">
          <StatusBody status={status} />
        </span>
      </div>

      <div
        className={cn(
          "rounded-md px-3 py-2 flex items-center justify-between text-xs transition-colors",
          unsynced > 0 ? "bg-amber-500/10" : "bg-fd-muted/40",
        )}
      >
        <span className="text-fd-muted-foreground">Local-only</span>
        <span
          className={cn(
            "font-medium tabular-nums",
            unsynced > 0 ? "text-amber-700 dark:text-amber-300" : "text-fd-muted-foreground italic",
          )}
        >
          {unsynced === 0 ? "all synced" : `${unsynced} write${unsynced === 1 ? "" : "s"}`}
        </span>
      </div>
    </div>
  );
}

// Edge / global / receiver card on the engine's node kit, showing the colour
// it currently holds. The pulse is dim-aware (a write past the awaited tier
// still lands, just faintly).
function Card({ k, pulse, colour }: { k: CardKey; pulse: PulseState; colour: string | null }) {
  const meta = CARD_META[k];
  const Icon = meta.icon;
  return (
    <NodeShell
      style={{
        width: "11rem",
        padding: "0.625rem 0.875rem",
        alignItems: "center",
        textAlign: "center",
        gap: "0.2rem",
        // The other device is a leaf the write is replicated out to, not part
        // of the authoritative durability spine — let it sit back a little.
        opacity: k === "receiver" ? 0.75 : 0.9,
      }}
    >
      {pulse.key > 0 && (
        <span
          key={pulse.key}
          className="diagram-pulse"
          style={{
            position: "absolute",
            inset: "-2px",
            borderRadius: "inherit",
            pointerEvents: "none",
          }}
        />
      )}
      <NodeIcon>
        <Icon />
      </NodeIcon>
      <NodeTitle>{meta.title}</NodeTitle>
      <NodeSubtitle>{meta.subtitle}</NodeSubtitle>
      <NodeFooter
        style={{
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          borderRadius: "0.25rem",
          padding: "0.2rem 0.4rem",
          marginTop: "0.4rem",
          backgroundColor: colour ? `${colour}26` : undefined,
          transition: "background-color 0.2s",
        }}
      >
        <ColourTag colour={colour} compact />
      </NodeFooter>
    </NodeShell>
  );
}

export function WriteTierDiagram() {
  const [tier, setTier] = useState<Tier>("edge");
  const [offline, setOffline] = useState(false);
  const [status, setStatus] = useState<Status>({ kind: "idle" });
  const [events, setEvents] = useState<WriteEvent[]>([]);
  const [colours, setColours] = useState<Record<NodeKey, string | null>>({
    phone: INITIAL_COLOR,
    edge: INITIAL_COLOR,
    global: INITIAL_COLOR,
    receiver: INITIAL_COLOR,
  });
  const [pulse, setPulse] = useState<Record<NodeKey, PulseState>>({
    phone: { key: 0 },
    edge: { key: 0 },
    global: { key: 0 },
    receiver: { key: 0 },
  });

  const traces = useDiagramTraces();
  const geomRef = useRef<GraphOverlayCtx | null>(null);
  const [geomReady, setGeomReady] = useState(false);
  // Desktop offset (px) of the other-device card off the trunk centreline,
  // measured from the edge card. Dropped to 0 on narrow screens so the shifted
  // card never leaves the layout (a transform isn't seen by the engine's
  // measurement, so an offset card overflows the frame on mobile).
  const [branchBase, setBranchBase] = useState(0);
  const [isNarrow, setIsNarrow] = useState(false);
  useEffect(() => {
    const mq = window.matchMedia("(max-width: 640px)");
    const sync = () => setIsNarrow(mq.matches);
    sync();
    mq.addEventListener("change", sync);
    return () => mq.removeEventListener("change", sync);
  }, []);
  const branchDx = isNarrow ? 0 : branchBase;

  const eventIdRef = useRef(0);
  const scheduledRef = useRef<Set<number>>(new Set());
  const timersRef = useRef<Set<ReturnType<typeof setTimeout>>>(new Set());
  // Live <path>s of in-flight traces, so a finished trace can fade out after a
  // linger (the engine's draw leaves it solid until we unmount the event).
  const pathElsRef = useRef<Record<string, SVGPathElement | null>>({});
  // Mirrors of colour state for handlers, plus the last event id each node has
  // applied (last-write-wins: an older queued write never clobbers a newer).
  const coloursRef = useRef<Record<NodeKey, string | null>>({
    phone: INITIAL_COLOR,
    edge: INITIAL_COLOR,
    global: INITIAL_COLOR,
    receiver: INITIAL_COLOR,
  });
  const lastSeenRef = useRef<Record<NodeKey, number>>({
    phone: 0,
    edge: 0,
    global: 0,
    receiver: 0,
  });

  // Latest events list, readable from event handlers without re-binding them.
  const eventsRef = useRef(events);
  useEffect(() => {
    eventsRef.current = events;
  });

  const after = useCallback((ms: number, fn: () => void) => {
    const t = setTimeout(() => {
      timersRef.current.delete(t);
      fn();
    }, ms);
    timersRef.current.add(t);
    return t;
  }, []);

  useEffect(() => {
    const timers = timersRef.current;
    return () => {
      timers.forEach(clearTimeout);
      timers.clear();
    };
  }, []);

  function pulseNode(node: NodeKey) {
    setPulse((p) => ({ ...p, [node]: { key: p[node].key + 1 } }));
  }

  // Last-write-wins: only the newest write to reach a node sticks.
  function applyColour(node: NodeKey, event: WriteEvent) {
    if (event.id <= lastSeenRef.current[node]) return;
    lastSeenRef.current = { ...lastSeenRef.current, [node]: event.id };
    coloursRef.current = { ...coloursRef.current, [node]: event.value };
    setColours((c) => ({ ...c, [node]: event.value }));
  }

  function scheduleStatusRevert(eventId: number) {
    after(TICK_LINGER_MS, () => {
      setStatus((s) => (s.kind === "settled" && s.eventId === eventId ? { kind: "idle" } : s));
    });
  }

  function markSettled(event: WriteEvent) {
    pulseNode("phone");
    setEvents((evts) => evts.map((e) => (e.id === event.id ? { ...e, phase: "settled" } : e)));
    setStatus((s) =>
      s.kind === "writing" && s.eventId === event.id
        ? { kind: "settled", tier: event.tier, eventId: event.id }
        : s,
    );
    scheduleStatusRevert(event.id);
  }

  const scheduleEvent = useCallback(
    (event: WriteEvent) => {
      const anchors = geomRef.current?.anchors;
      const paths = anchors ? connectorPaths(anchors, branchDx) : null;
      if (!paths) return false;

      const playHop = (hop: Hop, onArrive: () => void) =>
        traces.play({
          id: `${event.id}-${hop}`,
          d: paths[hop],
          durationMs: STAGE_MS,
          follow: true,
          onArrive,
        });

      // Hop 1: phone → edge. The dot's arrival is the moment the write
      // becomes durable on the sync server.
      playHop("edge", () => {
        applyColour("edge", event);
        pulseNode("edge");
        setEvents((evts) => evts.map((e) => (e.id === event.id ? { ...e, reachedEdge: true } : e)));
        if (event.tier === "edge") markSettled(event);
      });

      // One stage later the edge fans out: up to the global core, and across
      // to the other device — concurrently.
      after(STAGE_MS, () => {
        playHop("global", () => {
          applyColour("global", event);
          pulseNode("global");
          if (event.tier === "global") markSettled(event);
        });
        playHop("receiver", () => {
          applyColour("receiver", event);
          pulseNode("receiver");
        });
      });

      // Linger, fade the drawn paths, then drop the event (unmounts the SVG).
      after(2 * STAGE_MS + TICK_LINGER_MS, () => {
        for (const hop of HOPS) {
          const p = pathElsRef.current[`${event.id}-${hop}`];
          if (!p) continue;
          p.style.transition = `opacity ${FADE_MS}ms ease-out`;
          p.style.opacity = "0";
        }
      });
      after(2 * STAGE_MS + TICK_LINGER_MS + FADE_MS, () => {
        setEvents((evts) => evts.filter((e) => e.id !== event.id));
      });
      return true;
      // eslint-disable-next-line react-hooks/exhaustive-deps
    },
    [traces, after, branchDx],
  );

  // Schedule newly-added events once geometry is available. Queued (offline)
  // events are skipped until reconnect clears their queuedSince.
  useEffect(() => {
    if (!geomReady) return;
    for (const event of events) {
      if (event.queuedSince !== null) continue;
      if (scheduledRef.current.has(event.id)) continue;
      if (scheduleEvent(event)) scheduledRef.current.add(event.id);
    }
    const liveIds = new Set(events.map((e) => e.id));
    for (const id of Array.from(scheduledRef.current)) {
      if (!liveIds.has(id)) scheduledRef.current.delete(id);
    }
  }, [events, geomReady, scheduleEvent]);

  function runWrite() {
    if (status.kind === "writing") return;
    const id = ++eventIdRef.current;
    const isLocal = tier === "local";
    const value = pickNextColor(coloursRef.current.phone);
    const queuedSince = offline ? performance.now() : null;

    // The device has the write the instant you make it — colour and pulse
    // land locally regardless of the network or the awaited tier.
    lastSeenRef.current = { ...lastSeenRef.current, phone: id };
    coloursRef.current = { ...coloursRef.current, phone: value };
    setColours((c) => ({ ...c, phone: value }));
    pulseNode("phone");

    // Local is durable on the device itself — its promise resolves even
    // without the network, so the tick fires straight away. The propagation
    // to edge / global / the other device is what gets queued when offline.
    if (isLocal) {
      setStatus({ kind: "settled", tier, eventId: id });
      after(TICK_LINGER_MS, () => {
        setStatus((s) => (s.kind === "settled" && s.eventId === id ? { kind: "idle" } : s));
      });
    } else {
      setStatus({ kind: "writing", tier, eventId: id });
    }

    setEvents((evts) => [
      ...evts,
      {
        id,
        tier,
        value,
        phase: isLocal ? "settled" : "writing",
        startedAt: performance.now(),
        queuedSince,
        reachedEdge: false,
      },
    ]);
  }

  function toggleOffline() {
    if (!offline) {
      setOffline(true);
      return;
    }
    setOffline(false);
    // Release queued events one at a time so the user sees the propagation
    // waves as a sequence rather than a single chord.
    const queued = eventsRef.current
      .filter((e) => e.queuedSince !== null)
      .sort((a, b) => (a.queuedSince ?? 0) - (b.queuedSince ?? 0));
    queued.forEach((event, idx) => {
      after(idx * RECONNECT_STAGGER_MS, () => {
        setEvents((evts) => evts.map((e) => (e.id === event.id ? { ...e, queuedSince: null } : e)));
      });
    });
  }

  const onGeometry = useCallback((ctx: GraphOverlayCtx) => {
    geomRef.current = ctx;
    setGeomReady(true);
    const e = ctx.anchors.edge;
    if (e) {
      const base = (e.right - e.midX) * BRANCH_OFFSET;
      setBranchBase((p) => (Math.abs(p - base) < 0.5 ? p : base));
    }
  }, []);

  // Static resting connector + per-event, per-hop traces, drawn from the
  // engine-measured anchors and stroked in each write's colour.
  const overlay = useCallback(
    (ctx: GraphOverlayCtx) => {
      const conn = connectorPaths(ctx.anchors, branchDx);
      return (
        <>
          {conn && (
            <>
              {/* Authoritative durability spine: device → edge → global core,
                  drawn solid as the trunk. */}
              <path
                d={`${conn.edge} ${conn.global}`}
                fill="none"
                stroke="var(--diagram-edge, #9ca3af)"
                strokeWidth={1.5}
                strokeLinecap="round"
                strokeLinejoin="round"
              />
              {/* The other device hangs off the edge as a leaf — a replicated
                  copy, not part of the durability chain. Dashed + faded so the
                  hierarchy reads as a tree, not a symmetric fan-out. */}
              <path
                d={conn.receiver}
                fill="none"
                stroke="var(--diagram-edge, #9ca3af)"
                strokeWidth={1.5}
                strokeDasharray="3 4"
                strokeLinecap="round"
                strokeLinejoin="round"
                opacity={0.55}
              />
            </>
          )}
          <g>
            {events.flatMap((event) =>
              HOPS.map((hop) => {
                const id = `${event.id}-${hop}`;
                return (
                  <path
                    key={id}
                    ref={(el) => {
                      traces.pathRef(id)(el);
                      pathElsRef.current[id] = el;
                    }}
                    className="diagram-path"
                    d={conn?.[hop] ?? ""}
                    fill="none"
                    stroke={event.value}
                    strokeWidth={2}
                    strokeLinecap="round"
                    strokeLinejoin="round"
                  />
                );
              }),
            )}
          </g>
          <g>
            {events.flatMap((event) =>
              HOPS.map((hop) => {
                const id = `${event.id}-${hop}`;
                return (
                  <circle
                    key={id}
                    ref={traces.dotRef(id)}
                    className="diagram-dot"
                    r={5}
                    cx={0}
                    cy={0}
                    fill={event.value}
                    style={{ opacity: 0 }}
                  />
                );
              }),
            )}
          </g>
        </>
      );
    },
    [events, traces, branchDx],
  );

  // Phone on the left spanning all three rows; global (top), edge (middle)
  // and the other device (bottom) stacked on the right. No structural edges —
  // the overlay draws the connector itself from the measured anchors.
  const nodes: GraphNode[] = [
    {
      id: "phone",
      slot: { row: "1 / 4", col: 1 },
      content: (
        <PhoneChrome
          className="w-[clamp(13rem,46vw,17rem)]"
          pulseKey={pulse.phone.key}
          offline={offline}
        >
          <PhoneScreen
            tier={tier}
            setTier={setTier}
            status={status}
            onWrite={runWrite}
            offline={offline}
            onToggleOffline={toggleOffline}
            unsynced={events.filter((e) => !e.reachedEdge).length}
            colour={colours.phone}
          />
        </PhoneChrome>
      ),
    },
    {
      id: "global",
      slot: { row: 1, col: 2 },
      content: <Card k="global" pulse={pulse.global} colour={colours.global} />,
    },
    {
      id: "edge",
      slot: { row: 2, col: 2 },
      content: <Card k="edge" pulse={pulse.edge} colour={colours.edge} />,
    },
    {
      id: "receiver",
      slot: { row: 3, col: 2 },
      // Shifted to sit under its branch endpoint (branchDx). Visual only — the
      // engine measures the wrapper (still column-centred), and connectorPaths
      // routes to receiver.midX + branchDx, so card and branch stay locked.
      content: (
        <div style={{ transform: `translateX(${branchDx}px)` }}>
          <Card k="receiver" pulse={pulse.receiver} colour={colours.receiver} />
        </div>
      ),
    },
  ];

  return (
    <Graph
      eyebrow="Pick a tier"
      description={
        <>
          Each write picks a fresh colour. Choose how durable it must be before it's confirmed — it
          lands on this device at once, then syncs up through the tiers and across to the other
          device whenever the network is available; the promise resolves once it reaches the tier
          you picked. Turn on aeroplane mode to watch writes queue on the device and flush when you
          reconnect. Local always confirms instantly, even offline.
        </>
      }
      direction="LR"
      nodes={nodes}
      edges={[]}
      grid={{
        columns: "auto auto",
        rows: "auto auto auto",
        // Row gap fixed; column gap tightens on narrow screens so the phone +
        // tier column don't get pushed apart (and off-screen) on mobile.
        gap: "2rem clamp(0.75rem, 5vw, 3.5rem)",
      }}
      arrows={false}
      traces={traces}
      overlay={overlay}
      onGeometry={onGeometry}
    />
  );
}
