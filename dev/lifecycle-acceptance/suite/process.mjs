// One driver process and its line protocol (drivers/PROTOCOL.md).
import { spawn } from "node:child_process";
import { createInterface } from "node:readline";
import { cleanEnv } from "../build/consumer.mjs";
import { redact } from "./redact.mjs";

export const START_MS = 30_000;
export const CALL_MS = 30_000;

/** Resolves with `promise`'s value, or with `late()` after `ms`. */
export function within(promise, ms, late) {
  let timer;
  const timeout = new Promise((resolve) => {
    timer = setTimeout(() => resolve(late()), ms);
  });
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
}

export class DriverProcess {
  #child;
  #next = 1;
  #waiting = new Map();
  #stderr = "";
  #exited;
  #ended;

  constructor(driver) {
    const [command, ...args] = driver.cmd;
    this.#child = spawn(command, args, { env: cleanEnv(driver.env), stdio: ["pipe", "pipe", "pipe"] });
    this.#child.stdin.on("error", () => {});
    this.#child.stderr.on("data", (bytes) => {
      this.#stderr = (this.#stderr + bytes).slice(-4000);
    });
    this.#exited = new Promise((resolve) => {
      this.#child.on("error", (error) => resolve({ code: null, signal: error.message }));
      // close, not exit: every answer written before death has been read.
      this.#child.on("close", (code, signal) => resolve({ code, signal }));
    });
    this.#exited.then(({ code, signal }) => {
      this.#ended = `the process ended (${signal ?? code})${this.stderrTail()}`;
      for (const settle of this.#waiting.values())
        settle({ ok: false, kind: "harness.died", msg: this.#ended });
      this.#waiting.clear();
    });
    let announce;
    this.ready = new Promise((resolve) => (announce = resolve));
    createInterface({ input: this.#child.stdout }).on("line", (line) => {
      let message;
      try {
        message = JSON.parse(line);
      } catch {
        return; // a library's own print
      }
      if (message?.ready) return announce(true);
      this.#waiting.get(message?.id)?.(message);
      this.#waiting.delete(message?.id);
    });
  }

  get pid() {
    return this.#child.pid;
  }

  stderrTail() {
    const tail = redact(this.#stderr).trim().split("\n").slice(-4).join(" | ");
    return tail ? `; stderr: ${tail}` : "";
  }

  async started() {
    const ok = await within(
      Promise.race([this.ready, this.#exited.then(() => false)]),
      START_MS,
      () => false,
    );
    if (!ok) throw new Error(`the driver didn't start${this.stderrTail()}`);
    return this;
  }

  /** Starts a call: `{ id, reply }`. The reply always settles, at the latest after `timeout`. */
  send(op, args = {}, timeout = CALL_MS) {
    const id = this.#next++;
    const started = Date.now();
    const answered = new Promise((resolve) => {
      if (this.#ended) return resolve({ ok: false, kind: "harness.died", msg: this.#ended });
      this.#waiting.set(id, resolve);
      this.#child.stdin.write(`${JSON.stringify({ ...args, id, op })}\n`);
    });
    const reply = within(answered, timeout, () => {
      this.#waiting.delete(id);
      return { ok: false, kind: "harness.timeout", msg: `no answer within ${timeout} ms` };
    }).then((message) => ({ ...message, op, ms: Date.now() - started }));
    return { id, reply };
  }

  call(op, args, timeout) {
    return this.send(op, args, timeout).reply;
  }

  async exit() {
    this.#child.stdin.end();
    return within(this.#exited, 15_000, () => ({ code: null, signal: "still running after 15 s" }));
  }

  async kill() {
    this.#child.kill("SIGKILL"); // this PID only
    return within(this.#exited, 10_000, () => ({ code: null, signal: "still running after SIGKILL" }));
  }
}
