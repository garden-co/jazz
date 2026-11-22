// What one round of a rule gets, and how it says what happened.
import { randomUUID } from "node:crypto";
import { mkdirSync, rmSync } from "node:fs";
import { join } from "node:path";
import { setTimeout as sleep } from "node:timers/promises";
import { WORK } from "../lib/common.mjs";
import { DriverProcess } from "./process.mjs";
import { Proxy } from "./proxy.mjs";
import { redact } from "./redact.mjs";

/** The rule is broken. `tag` is the stable part, to count failures by. */
export class Broken extends Error {
  constructor(tag, detail = "") {
    super(redact(detail ? `${tag}: ${detail}` : tag));
    this.tag = tag;
  }
}

/** This language can't express the check, or the contract doesn't decide it. Never a PASS. */
export class NotCovered extends Error {}

export function show(reply) {
  if (reply.ok) return `ok ${JSON.stringify(reply.v)} in ${reply.ms} ms`;
  return redact(`${reply.kind} in ${reply.ms} ms: ${reply.msg}`);
}

/** The value of a call that has to succeed. */
export function value(reply, step) {
  if (reply.ok) return reply.v;
  if (reply.kind === "driver.unsupported") throw new NotCovered(`no public API for ${reply.op}`);
  throw new Broken(`${step}: ${reply.kind}`, reply.msg);
}

/** A call that has to fail, as one exact expected error (errors.mjs) and no other. */
export function refusal(reply, expected, step) {
  if (reply.ok) throw new Broken(`${step}: it succeeded, expected ${expected.name}`);
  if (reply.kind === "driver.unsupported") throw new NotCovered(`no public API for ${reply.op}`);
  if (!expected.matches(reply)) throw new Broken(`${step}: ${reply.kind}, expected ${expected.name}`, reply.msg);
}

export function equal(got, want, step) {
  if (JSON.stringify(got) !== JSON.stringify(want))
    throw new Broken(step, `got ${JSON.stringify(got)}, expected ${JSON.stringify(want)}`);
}

export class Round {
  #processes = new Set();
  #proxies = [];
  #base;
  notes = [];
  times = [];

  constructor({ lang, driver, stack }, index) {
    Object.assign(this, { lang, driver, stack, index, sleep });
    this.id = randomUUID().slice(0, 8);
    this.#base = join(WORK, "stores", this.id);
  }

  note(text) {
    this.notes.push(redact(text));
  }

  time(what, ms) {
    this.times.push([what, ms]);
  }

  dir(name = "store") {
    const dir = join(this.#base, name);
    mkdirSync(dir, { recursive: true });
    return dir;
  }

  async proxy() {
    const proxy = await Proxy.start(this.stack.backendPort);
    this.#proxies.push(proxy);
    return proxy;
  }

  async process() {
    const process = new DriverProcess(this.driver);
    this.#processes.add(process);
    return process.started();
  }

  async endCleanly(process) {
    const { code, signal } = await process.exit();
    if (code !== 0) throw new Broken("the process didn't exit cleanly", `${signal ?? code}${process.stderrTail()}`);
    this.#processes.delete(process);
  }

  async killed(process) {
    const { signal } = await process.kill();
    if (signal !== "SIGKILL") throw new Broken("the process wasn't killed", String(signal));
    this.#processes.delete(process);
  }

  open(process, options) {
    return process.call("open", this.openArgs(options));
  }

  openArgs({ h = "a", dir, proxy, key, token }) {
    return { h, dir, url: proxy.url, app: this.stack.appId, ...(key ? { key } : { token }) };
  }

  async cleanup() {
    for (const process of this.#processes) await process.kill();
    for (const proxy of this.#proxies) await proxy.stop();
    // Stores hold secrets (inspect-token-secret) and 100 rounds of them add up.
    rmSync(this.#base, { recursive: true, force: true });
  }
}
