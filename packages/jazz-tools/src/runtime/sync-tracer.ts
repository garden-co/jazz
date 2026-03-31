/**
 * Sync message tracer for debugging and testing.
 *
 * Aggregates traces from multiple WASM/NAPI runtimes into a single view.
 * Each runtime is named (e.g. "alice", "bob") so the merged output shows
 * the full message flow between participants.
 *
 * ## Usage
 *
 * ```typescript
 * import { SyncTracer } from "jazz-tools";
 *
 * const tracer = new SyncTracer();
 *
 * // Name each participant's runtime
 * tracer.addRuntime("alice", aliceRuntime);
 * tracer.addRuntime("bob", bobRuntime);
 *
 * // ... perform operations ...
 *
 * console.log(tracer.tally());
 * // alice    => server  : ObjectUpdated (1)
 * // server   -> alice   : PersistenceAck (2)
 * // server   -> bob     : ObjectUpdated (1)
 *
 * console.log(tracer.dump());
 * // Full trace with timing and details
 * ```
 */

/** A single traced message (deserialized from Rust JSON). */
export interface SyncTracerMessage {
  seq: number;
  elapsed_ms: number;
  from: string;
  to: string;
  side: "send" | "recv";
  payload_type: string;
}

/** Any runtime that exposes sync tracer methods (WASM or NAPI). */
export interface TracableRuntime {
  enableSyncTracer(name?: string): void;
  syncTracerRegisterObject(objectId: string, name: string): void;
  syncTracerMessagesJson(): string | undefined;
  syncTracerTally(): string | undefined;
  syncTracerDump(): string | undefined;
  syncTracerSummary(): string | undefined;
  syncTracerTraceNormalized(): string | undefined;
  syncTracerClear(): void;
  syncTracerCount(): number;
}

export class SyncTracer {
  private runtimes: Map<string, TracableRuntime> = new Map();

  /**
   * Register a named runtime for tracing.
   * Enables the internal Rust tracer with the given name.
   */
  addRuntime(name: string, runtime: TracableRuntime): void {
    runtime.enableSyncTracer(name);
    this.runtimes.set(name, runtime);
  }

  /**
   * Name an object so traces show a human-readable label.
   *
   * ```typescript
   * const { id } = alice.insert("todos", { title: "buy milk", done: false });
   * tracer.registerObject(id, "buy-milk");
   * // trace now shows "obj:buy-milk" instead of "obj:019d3fc7"
   * ```
   */
  registerObject(objectId: string, name: string): void {
    for (const runtime of this.runtimes.values()) {
      runtime.syncTracerRegisterObject(objectId, name);
    }
  }

  /** Collect all messages from all runtimes, sorted by elapsed_ms then seq. */
  messages(): SyncTracerMessage[] {
    const all: SyncTracerMessage[] = [];
    for (const runtime of this.runtimes.values()) {
      const json = runtime.syncTracerMessagesJson();
      if (json) {
        try {
          const msgs: SyncTracerMessage[] = JSON.parse(json);
          all.push(...msgs);
        } catch {
          // ignore parse errors
        }
      }
    }
    // Sort by time, then by sequence within same time
    all.sort((a, b) => a.elapsed_ms - b.elapsed_ms || a.seq - b.seq);
    return all;
  }

  /** Grouped count summary — deterministic, stable across runs. */
  tally(): string {
    const msgs = this.messages();
    const groups = new Map<string, Map<string, number>>();

    for (const msg of msgs) {
      const arrow = msg.side === "send" ? "=>" : "->";
      const key = `${pad(msg.from, 8)} ${arrow} ${pad(msg.to, 8)}`;
      if (!groups.has(key)) groups.set(key, new Map());
      const types = groups.get(key)!;
      types.set(msg.payload_type, (types.get(msg.payload_type) ?? 0) + 1);
    }

    // Sort keys for determinism
    const sortedKeys = [...groups.keys()].sort();
    const lines: string[] = [];
    for (const key of sortedKeys) {
      const types = groups.get(key)!;
      const typeParts = [...types.entries()]
        .sort(([a], [b]) => a.localeCompare(b))
        .map(([type, count]) => `${type} (${count})`);
      lines.push(`${key}: ${typeParts.join(", ")}`);
    }
    if (lines.length === 0) return "";
    return lines.join("\n") + "\n";
  }

  /** One line per message: from -> to type. */
  summary(): string {
    const msgs = this.messages();
    if (msgs.length === 0) return "";
    return (
      msgs
        .map((m) => {
          const arrow = m.side === "send" ? "=>" : "->";
          return `${pad(m.from, 8)} ${arrow} ${pad(m.to, 8)} ${m.payload_type}`;
        })
        .join("\n") + "\n"
    );
  }

  /** Number of total messages across all runtimes. */
  count(): number {
    let total = 0;
    for (const runtime of this.runtimes.values()) {
      total += runtime.syncTracerCount();
    }
    return total;
  }

  /** Clear all messages in all runtimes. */
  clear(): void {
    for (const runtime of this.runtimes.values()) {
      runtime.syncTracerClear();
    }
  }

  /** Messages from a specific participant. */
  from(name: string): SyncTracerMessage[] {
    return this.messages().filter((m) => m.from === name);
  }

  /** Messages to a specific participant. */
  to(name: string): SyncTracerMessage[] {
    return this.messages().filter((m) => m.to === name);
  }

  /** Messages between two participants (either direction). */
  between(a: string, b: string): SyncTracerMessage[] {
    return this.messages().filter(
      (m) => (m.from === a && m.to === b) || (m.from === b && m.to === a),
    );
  }

  /** Messages of a specific payload type. */
  ofType(type: string): SyncTracerMessage[] {
    return this.messages().filter((m) => m.payload_type === type);
  }
}

function pad(s: string, width: number): string {
  return s.padEnd(width);
}
