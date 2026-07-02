import initBrokerWasm, { WasmBrokerCore } from "jazz-broker-wasm";
import { createRandomId } from "../runtime/browser-broker-protocol.js";
import {
  monitorWebLockRelease,
  stealAndReleaseWebLock,
  tryAcquireWebLock,
  type WebLockMonitor,
} from "../runtime/leader-lock.js";
import { JAZZ_BROKER_WASM_BASE64 } from "./jazz-broker-wasm-bytes.js";

type SharedWorkerGlobal = typeof globalThis & {
  onconnect: ((event: MessageEvent & { ports: MessagePort[] }) => void) | null;
};

type PortId = number;
type ProbeId = number;
type MonitorId = number;

type BrokerEvent = {
  kind: string;
  [key: string]: unknown;
};

type BrokerCommand = {
  kind: string;
  [key: string]: unknown;
};

type TimerKey = {
  kind: string;
  [key: string]: unknown;
};

const workerGlobal = globalThis as SharedWorkerGlobal;
const brokerInstanceId = createRandomId("broker");
const ports = new Map<PortId, MessagePort>();
const timers = new Map<string, ReturnType<typeof setTimeout>>();
const lockMonitors = new Map<MonitorId, WebLockMonitor>();
const queuedEvents: BrokerEvent[] = [];

let nextPortId = 1;
let core: WasmBrokerCore | null = null;
let coreReady = false;
let flushing = false;

void initializeCore();

workerGlobal.onconnect = (event) => {
  const port = event.ports[0];
  if (!port) return;

  const portId = nextPortId++;
  ports.set(portId, port);
  port.addEventListener("message", (messageEvent) => {
    const message = messageEvent.data;
    if (!message || typeof message !== "object") return;
    dispatchToCore({
      kind: "portMessage",
      portId,
      message,
    });
  });
  port.start();
};

async function initializeCore(): Promise<void> {
  try {
    const wasmBytes = JAZZ_BROKER_WASM_BASE64
      ? decodeBase64Bytes(JAZZ_BROKER_WASM_BASE64)
      : undefined;
    await initBrokerWasm(wasmBytes ? { module_or_path: wasmBytes } : undefined);
    core = new WasmBrokerCore(brokerInstanceId);
    coreReady = true;
    flushQueuedEvents();
  } catch (error) {
    console.error("[jazz-broker] failed to initialize broker wasm", error);
  }
}

function dispatchToCore(event: BrokerEvent): void {
  if (!coreReady || !core) {
    queuedEvents.push(event);
    return;
  }
  executeCommands(handleCoreEvent(core, event));
}

function handleCoreEvent(activeCore: WasmBrokerCore, event: BrokerEvent): BrokerCommand[] {
  try {
    return activeCore.handle(event, Date.now()) as BrokerCommand[];
  } catch (error) {
    // Malformed tab messages must not crash the SharedWorker; the JS broker
    // silently ignored anything it could not interpret.
    console.warn("[jazz-broker] dropping event the broker core could not process", error);
    return [];
  }
}

function flushQueuedEvents(): void {
  if (flushing) return;
  flushing = true;
  try {
    while (coreReady && core && queuedEvents.length > 0) {
      const event = queuedEvents.shift();
      if (!event) continue;
      executeCommands(core.handle(event, Date.now()) as BrokerCommand[]);
    }
  } finally {
    flushing = false;
  }
}

function executeCommands(commands: BrokerCommand[]): void {
  for (const command of commands) {
    switch (command.kind) {
      case "post":
        post(command.portId as PortId, command.message);
        break;
      case "closePort":
        closePort(command.portId as PortId);
        break;
      case "attachFollowerChannel":
        attachFollowerChannel(command);
        break;
      case "setTimer":
        setBrokerTimer(command.timer as TimerKey, command.delayMs as number);
        break;
      case "clearTimer":
        clearBrokerTimer(command.timer as TimerKey);
        break;
      case "probeLocks":
        void probeLocks(command.probeId as ProbeId, command.lockNames as string[]);
        break;
      case "stealLocks":
        void stealLocks(command.probeId as ProbeId, command.lockNames as string[]);
        break;
      case "monitorLock":
        monitorLock(command.monitorId as MonitorId, command.lockName as string);
        break;
      case "cancelLockMonitor":
        cancelLockMonitor(command.monitorId as MonitorId);
        break;
      case "warnStaleInstanceDrop":
        warnStaleInstanceDrop(command);
        break;
    }
  }
}

function post(portId: PortId, message: unknown): void {
  ports.get(portId)?.postMessage(message);
}

function closePort(portId: PortId): void {
  const port = ports.get(portId);
  ports.delete(portId);
  port?.close();
}

function attachFollowerChannel(command: BrokerCommand): void {
  const leaderPort = ports.get(command.leaderPortId as PortId);
  const followerPort = ports.get(command.followerPortId as PortId);
  if (!leaderPort || !followerPort) return;

  const channel = new MessageChannel();
  leaderPort.postMessage(
    {
      type: "attach-follower-port",
      brokerInstanceId,
      followerTabId: command.followerTabId,
      leadershipId: command.leadershipId,
      port: channel.port1,
    },
    [channel.port1],
  );
  followerPort.postMessage(
    {
      type: "use-follower-port",
      brokerInstanceId,
      leaderTabId: command.leaderTabId,
      leadershipId: command.leadershipId,
      port: channel.port2,
    },
    [channel.port2],
  );
}

function setBrokerTimer(timer: TimerKey, delayMs: number): void {
  const key = timerKey(timer);
  clearTimeout(timers.get(key));
  timers.set(
    key,
    setTimeout(() => {
      timers.delete(key);
      dispatchToCore({
        kind: "timerFired",
        timer,
      });
    }, delayMs),
  );
}

function clearBrokerTimer(timer: TimerKey): void {
  const key = timerKey(timer);
  const handle = timers.get(key);
  if (!handle) return;
  clearTimeout(handle);
  timers.delete(key);
}

async function probeLocks(probeId: ProbeId, lockNames: string[]): Promise<void> {
  const leases = await Promise.all(lockNames.map((lockName) => tryAcquireWebLock(lockName)));
  for (const lease of leases) {
    lease?.release();
  }
  dispatchToCore({
    kind: "locksProbeResult",
    probeId,
    allAcquired: leases.every((lease) => lease !== null),
  });
}

async function stealLocks(probeId: ProbeId, lockNames: string[]): Promise<void> {
  for (const lockName of lockNames) {
    await stealAndReleaseWebLock(lockName).catch(() => undefined);
  }
  dispatchToCore({
    kind: "locksStolen",
    probeId,
  });
}

function monitorLock(monitorId: MonitorId, lockName: string): void {
  cancelLockMonitor(monitorId);
  lockMonitors.set(
    monitorId,
    monitorWebLockRelease(lockName, {
      onGranted: () => {
        dispatchToCore({
          kind: "lockMonitorTriggered",
          monitorId,
        });
      },
      onError: () => {
        dispatchToCore({
          kind: "lockMonitorTriggered",
          monitorId,
        });
      },
    }),
  );
}

function cancelLockMonitor(monitorId: MonitorId): void {
  lockMonitors.get(monitorId)?.cancel();
  lockMonitors.delete(monitorId);
}

function warnStaleInstanceDrop(command: BrokerCommand): void {
  console.warn(
    `[jazz-broker] dropping "${String(command.messageType)}" from tab ${String(
      command.tabId,
    )}: stamped for broker instance ${String(
      command.stampedInstanceId,
    )}, current is ${brokerInstanceId}. This usually means tabs are running different jazz-tools versions against one broker.`,
  );
}

function timerKey(timer: TimerKey): string {
  return JSON.stringify(timer);
}

function decodeBase64Bytes(base64: string): Uint8Array {
  const binary = atob(base64);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i += 1) {
    bytes[i] = binary.charCodeAt(i);
  }
  return bytes;
}
