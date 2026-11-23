// What the encrypted BandChat's browser tests share: real browsers (one
// storage profile each, so each is its own device), what they send and
// receive.
import { writeFile } from "node:fs/promises";
import {
  createServer,
  request as httpRequest,
  type IncomingMessage,
  type Server,
  type ServerResponse,
} from "node:http";
import { connect, type AddressInfo } from "node:net";
import type { Duplex } from "node:stream";
import type { Db } from "jazz-tools";
import {
  expect,
  type Browser,
  type BrowserContext,
  type Locator,
  type Page,
  type TestInfo,
} from "@playwright/test";

/** How long a step that waits on Jazz's background passes may take. */
export const T = 45_000;
export const PASSWORD = "correct horse battery";

/** A name unique to this run, so runs against one server never meet. */
export function unique(label: string): string {
  return `${label}-${Date.now().toString(36)}${Math.random().toString(36).slice(2, 6)}`;
}

export function emailOf(name: string): string {
  return `${name.toLowerCase()}@band-chat.test`;
}

/**
 * Everything one browser sent and received, as raw bytes: HTTP request and
 * response bodies and WebSocket frames, from its pages and from the workers
 * Jazz runs (the local-first client syncs from a SharedWorker). Each browser
 * reaches the app through its own recording proxy.
 */
export class Wire {
  readonly chunks: Buffer[] = [];
  private readonly tunnels = new Set<Duplex>();
  private outgoingPaused = false;
  private server: Server | undefined;

  /** Starts this browser's proxy; resolves with its address. */
  async listen(): Promise<string> {
    const server = createServer((request, response) => this.forward(request, response));
    server.on("connect", (request, client, head) => this.tunnel(request, client, head));
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    this.server = server;
    return `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
  }

  async close(): Promise<void> {
    this.resumeOutgoing();
    this.server?.closeAllConnections();
    await new Promise((resolve) => this.server?.close(resolve) ?? resolve(undefined));
  }

  /** A plain HTTP request, in the absolute form a browser sends a proxy. */
  private forward(request: IncomingMessage, response: ServerResponse): void {
    const target = new URL(request.url!);
    const body: Buffer[] = [];
    request.on("data", (chunk: Buffer) => body.push(chunk));
    request.on("end", () => {
      if (body.length) this.chunks.push(Buffer.concat(body));
      const upstream = httpRequest(
        target,
        { method: request.method, headers: request.headers },
        (answer) => {
          response.writeHead(answer.statusCode ?? 502, answer.headers);
          const received: Buffer[] = [];
          answer.on("data", (chunk: Buffer) => {
            received.push(chunk);
            response.write(chunk);
          });
          answer.on("end", () => {
            this.chunks.push(Buffer.concat(received));
            response.end();
          });
        },
      );
      upstream.on("error", () => response.destroy());
      upstream.end(Buffer.concat(body));
    });
  }

  /** A tunnel; browsers open WebSockets through a proxy this way. */
  private tunnel(request: IncomingMessage, client: Duplex, head: Buffer): void {
    const [host, port] = request.url!.split(":");
    const upstream = connect(Number(port), host!, () => {
      client.write("HTTP/1.1 200 Connection Established\r\n\r\n");
      if (head.length) upstream.write(head);
      this.tunnels.add(client);
      const outgoing = new FrameReader((payload) => this.chunks.push(payload));
      const incoming = new FrameReader((payload) => this.chunks.push(payload));
      client.on("data", (chunk: Buffer) => {
        outgoing.push(chunk);
        upstream.write(chunk);
      });
      upstream.on("data", (chunk: Buffer) => {
        incoming.push(chunk);
        client.write(chunk);
      });
      let open = true;
      const closed = () => {
        if (!open) return;
        open = false;
        this.tunnels.delete(client);
        client.destroy();
        upstream.destroy();
      };
      client.on("close", closed);
      upstream.on("close", closed);
      client.on("error", closed);
      upstream.on("error", closed);
      if (this.outgoingPaused) client.pause();
    });
    upstream.on("error", () => client.destroy());
  }

  /** Whether `text` appears in anything this browser sent or received. */
  saw(text: string): boolean {
    const needle = Buffer.from(text);
    return this.chunks.some((chunk) => chunk.includes(needle));
  }

  /** Whether a row ID appears, as text or as its 16 bytes. */
  sawId(id: string): boolean {
    const bytes = Buffer.from(id.replace(/-/g, ""), "hex");
    return this.saw(id) || this.chunks.some((chunk) => chunk.includes(bytes));
  }

  /** Hold this browser's outgoing sync without interrupting incoming data. */
  pauseOutgoing(): void {
    this.outgoingPaused = true;
    for (const tunnel of this.tunnels) tunnel.pause();
  }

  resumeOutgoing(): void {
    this.outgoingPaused = false;
    for (const tunnel of this.tunnels) tunnel.resume();
  }

  /** Open sync connections (tunnels) of this browser. */
  openSockets(): number {
    return this.tunnels.size;
  }
}

/**
 * Reads one direction of a WebSocket connection: the HTTP upgrade, then
 * frames, unmasked (a browser masks what it sends).
 */
class FrameReader {
  private buffer = Buffer.alloc(0);
  private inFrames = false;

  constructor(private readonly keep: (payload: Buffer) => void) {}

  push(chunk: Buffer): void {
    this.buffer = Buffer.concat([this.buffer, chunk]);
    if (!this.inFrames) {
      const end = this.buffer.indexOf("\r\n\r\n");
      if (end === -1) return;
      this.keep(this.buffer.subarray(0, end));
      this.buffer = this.buffer.subarray(end + 4);
      this.inFrames = true;
    }
    for (;;) {
      if (this.buffer.length < 2) return;
      const masked = (this.buffer[1]! & 0x80) !== 0;
      let length = this.buffer[1]! & 0x7f;
      let offset = 2;
      if (length === 126) {
        if (this.buffer.length < 4) return;
        length = this.buffer.readUInt16BE(2);
        offset = 4;
      } else if (length === 127) {
        if (this.buffer.length < 10) return;
        length = Number(this.buffer.readBigUInt64BE(2));
        offset = 10;
      }
      const mask = masked ? this.buffer.subarray(offset, offset + 4) : undefined;
      if (masked) offset += 4;
      if (this.buffer.length < offset + length) return;
      const payload = Buffer.from(this.buffer.subarray(offset, offset + length));
      if (mask) for (let i = 0; i < payload.length; i++) payload[i]! ^= mask[i % 4]!;
      this.keep(payload);
      this.buffer = this.buffer.subarray(offset + length);
    }
  }
}

export interface Device {
  context: BrowserContext;
  page: Page;
  wire: Wire;
}

/** The devices the running test opened, for its failure report. */
const opened: Device[] = [];

/**
 * After each test: on failure, saves a screenshot and the text of every page
 * the test opened; then closes its browsers and their proxies.
 */
export async function reportPages(testInfo: TestInfo): Promise<void> {
  const devices = opened.splice(0);
  try {
    if (testInfo.status !== testInfo.expectedStatus) await savePages(devices, testInfo);
  } finally {
    for (const device of devices) {
      await device.context.close().catch(() => undefined);
      await device.wire.close();
    }
  }
}

async function savePages(devices: Device[], testInfo: TestInfo): Promise<void> {
  for (const [index, device] of devices.entries())
    for (const [tab, page] of device.context.pages().entries()) {
      const name = `device-${index + 1}-tab-${tab + 1}`;
      await page
        .screenshot({ fullPage: true, path: testInfo.outputPath(`${name}.png`) })
        .catch(() => undefined);
      const text = await page
        .evaluate(() => `${location.href}\n${document.body.innerText}`)
        .catch((error: Error) => error.message);
      await writeFile(testInfo.outputPath(`${name}.txt`), text);
    }
}

/** A browser with its own storage profile: one device. */
export async function newDevice(browser: Browser): Promise<Device> {
  const wire = new Wire();
  // `<-loopback>`: the app runs on this machine, which browsers otherwise
  // reach without a proxy.
  const context = await browser.newContext({
    proxy: { server: await wire.listen(), bypass: "<-loopback>" },
  });
  const page = await context.newPage();
  const device = { context, page, wire };
  opened.push(device);
  return device;
}

/** Another tab of the same browser profile (after the last one closed). */
export async function reopen(device: Device): Promise<Page> {
  const page = await device.context.newPage();
  device.page = page;
  return page;
}

export async function signUp(page: Page, name: string, path = "/"): Promise<void> {
  await page.goto(path);
  await page.getByRole("button", { name: "Create an account" }).click();
  await page.getByLabel("Name").fill(name);
  await page.getByLabel("Email").fill(emailOf(name));
  await page.getByLabel("Password").fill(PASSWORD);
  await submitAuth(
    page,
    "Create account",
    page.getByRole("heading", { name: "Set up your profile" }),
  );
  await page.getByLabel("Display name").fill(name);
  await page.getByRole("button", { name: "Continue", exact: true }).click();
  await expect(page.getByRole("button", { name: "Account" })).toBeVisible({ timeout: T });
}

/** Signs in on the sign-in form, which shows on any page while signed out. */
export async function signIn(page: Page, name: string, path = "/"): Promise<void> {
  if (!(await signInHeading(page).isVisible())) await page.goto(path);
  await expect(signInHeading(page)).toBeVisible({ timeout: T });
  await page.getByLabel("Email").fill(emailOf(name));
  await page.getByLabel("Password").fill(PASSWORD);
  await submitAuth(page, "Sign in", page.getByRole("button", { name: "Account" }));
}

/**
 * Submits the sign-in or sign-up form. In production Better Auth allows three
 * sign-ins and sign-ups per ten seconds, and with no client IP forwarded every
 * browser shares that allowance, so a test that signs several browsers in
 * waits out a refusal and submits again.
 */
async function submitAuth(page: Page, button: string, done: Locator): Promise<void> {
  const limited = page.getByText(/too many requests/i);
  for (let attempt = 1; ; attempt++) {
    await page.getByRole("button", { name: button, exact: true }).click();
    await expect(done.or(limited).first()).toBeVisible({ timeout: T });
    if (await done.isVisible()) return;
    if (attempt === 4)
      throw new Error("Better Auth kept refusing the sign-in as too many requests");
    console.log(`[${button}] refused as too many requests; trying again in 10 s`);
    await page.waitForTimeout(10_000);
    if (await done.isVisible()) return;
  }
}

export function signInHeading(page: Page): Locator {
  return page.getByRole("heading", { name: "Sign in" });
}

export async function signOut(page: Page): Promise<void> {
  // The menu closes if the side bar re-renders while it is open; open it again.
  await expect(async () => {
    await page.keyboard.press("Escape");
    await page.getByRole("button", { name: "Account" }).click();
    await page.getByRole("menuitem", { name: "Sign out" }).click({ timeout: 2000 });
  }).toPass({ timeout: T });
  await expect(signInHeading(page)).toBeVisible({ timeout: T });
}

/** Better Auth's own answer: is this browser signed in at all? */
export async function hasSession(device: Device): Promise<boolean> {
  const response = await device.context.request.get("/api/auth/get-session");
  const body = await response.text();
  return response.ok() && body !== "null" && body.trim() !== "";
}

export async function createRoom(page: Page, name: string): Promise<string> {
  await page.getByRole("button", { name: "New room" }).click();
  const dialog = page.getByRole("dialog");
  await dialog.getByLabel("Room name").fill(name);
  await dialog.getByRole("button", { name: "Create room", exact: true }).click();
  await expect(page.getByRole("heading", { name, level: 2 })).toBeVisible({ timeout: T });
  await expect(page).toHaveURL(/[?&]room=/, { timeout: T });
  return new URL(page.url()).searchParams.get("room")!;
}

export async function openRoom(page: Page, name: string): Promise<void> {
  await roomList(page).getByText(name, { exact: true }).click();
  await expect(page.getByRole("heading", { name, level: 2 })).toBeVisible({ timeout: T });
}

/** The room list in the side navigation. */
export function roomList(page: Page): Locator {
  return page.getByRole("navigation");
}

export async function send(page: Page, text: string): Promise<void> {
  await page.getByRole("textbox", { name: "Message input" }).fill(text);
  await page.getByRole("button", { name: "Send", exact: true }).click();
  await expect(message(page, text)).toBeVisible({ timeout: T });
}

export function message(page: Page, text: string): Locator {
  return page.locator(".message-text", { hasText: text });
}

/** The row ID of a message shown on `page`. */
export async function messageId(page: Page, text: string): Promise<string> {
  const bubble = page.locator("[data-message-id]", { has: message(page, text) });
  return (await bubble.getAttribute("data-message-id"))!;
}

export function lockedBanner(page: Page): Locator {
  return page.getByText(/locked on this browser/);
}

export function waitingBanner(page: Page): Locator {
  return page.getByText("This browser is waiting for approval");
}

/** Opens Your profile, where the devices panel is. */
export async function openDevices(page: Page): Promise<Locator> {
  await expect(async () => {
    await page.keyboard.press("Escape");
    await page.getByRole("button", { name: "Account" }).click();
    await page.getByRole("menuitem", { name: "Edit profile" }).click({ timeout: 2000 });
  }).toPass({ timeout: T });
  const dialog = page.getByRole("dialog");
  await expect(dialog.getByRole("heading", { name: "Devices and recovery" })).toBeVisible({
    timeout: T,
  });
  return dialog;
}

export async function closeDialog(page: Page): Promise<void> {
  await page.keyboard.press("Escape");
  await expect(page.getByRole("dialog")).toHaveCount(0, { timeout: T });
}

export function deviceRows(dialog: Locator): Locator {
  return dialog.locator("li[data-device-id]");
}

export function thisBrowserRow(dialog: Locator): Locator {
  return deviceRows(dialog).filter({ hasText: "This browser" });
}

/** This browser's device ID, from its own row in the devices panel. */
export async function deviceIdOf(page: Page): Promise<string> {
  const dialog = await openDevices(page);
  const row = thisBrowserRow(dialog);
  await expect(row).toHaveCount(1, { timeout: T });
  const id = (await row.getAttribute("data-device-id"))!;
  await closeDialog(page);
  return id;
}

/** `active` approves `waiting` by typing the code the waiting browser shows. */
export async function approve(active: Page, waiting: Page): Promise<void> {
  await expect(waitingBanner(waiting).first()).toBeVisible({ timeout: T });
  const dialog = await openDevices(active);
  const row = deviceRows(dialog).filter({ hasText: "Waiting for approval" }).first();
  await row.getByRole("button", { name: "Approve" }).click();
  const code = await shownCode(waiting);
  await row.getByLabel("Code shown on the new browser").fill(code);
  await row.getByRole("button", { name: "Approve this code" }).click();
  await expect(waitingBanner(waiting)).toHaveCount(0, { timeout: T });
  await closeDialog(active);
}

/** The six-digit code a waiting browser shows once it has answered. */
export async function shownCode(waiting: Page): Promise<string> {
  const shown = waiting.getByText(/Type this code on your other device:/).first();
  await expect(shown).toBeVisible({ timeout: T });
  return (await shown.textContent())!.match(/(\d{6})/)![1]!;
}

/** Signs `device` in as `name`, a further browser of that user, and has `active` approve it. */
export async function addApprovedBrowser(
  browser: Browser,
  active: Page,
  name: string,
): Promise<Device> {
  const device = await newDevice(browser);
  await signIn(device.page, name);
  await approve(active, device.page);
  return device;
}

/** Asks to join through a room link; `creator` admits the request. */
export async function joinThroughLink(
  creator: Page,
  joiner: Page,
  roomId: string,
  joinerName: string,
): Promise<void> {
  await joiner.goto(`/?join=${roomId}`);
  await joiner.getByRole("button", { name: "Ask to join" }).click();
  await expect(joiner.getByText("Waiting for the room creator")).toBeVisible({ timeout: T });
  await admit(creator, joinerName);
}

export async function admit(creator: Page, joinerName: string): Promise<void> {
  await creator.getByRole("button", { name: /^Invite( \d+)?$/ }).click();
  const dialog = creator.getByRole("dialog");
  const request = dialog.getByRole("listitem").filter({ hasText: joinerName });
  await request.getByRole("button", { name: "Admit" }).click();
  await expect(dialog.getByRole("button", { name: "Admit" })).toHaveCount(0, { timeout: T });
  await expect(dialog.getByText("Key not delivered yet")).toHaveCount(0);
  await closeDialog(creator);
}

/** The Jazz account of the user signed in on `page`, from the open room's members list. */
export async function myAccountId(page: Page): Promise<string> {
  await page.getByRole("button", { name: "Room options" }).click();
  await page.getByRole("menuitem", { name: /^Members/ }).click();
  const me = page.getByRole("dialog").locator("[data-member-author]", { hasText: "(you)" });
  await expect(me).toHaveCount(1, { timeout: T });
  const id = (await me.getAttribute("data-member-author"))!;
  await closeDialog(page);
  return id;
}

/** Polls `read` until `accept` holds, failing with `what` after `timeout`. */
export async function until<V>(
  what: string,
  read: () => Promise<V>,
  accept: (value: V) => boolean,
  timeout = T,
): Promise<V> {
  const deadline = Date.now() + timeout;
  let last: V | undefined;
  for (;;) {
    last = await read();
    if (accept(last)) return last;
    if (Date.now() > deadline)
      throw new Error(`Timed out waiting for ${what}; last value: ${JSON.stringify(last)}`);
    await new Promise((resolve) => setTimeout(resolve, 500));
  }
}

/** Everything this origin keeps in the browser, as text. */
export async function pageStorage(page: Page): Promise<string> {
  return page.evaluate(async () => {
    const text = (value: unknown): string => {
      if (value instanceof ArrayBuffer) return new TextDecoder().decode(value);
      if (ArrayBuffer.isView(value))
        return new TextDecoder().decode(
          new Uint8Array(value.buffer, value.byteOffset, value.byteLength),
        );
      if (value instanceof Date) return value.toISOString();
      if (Array.isArray(value)) return value.map(text).join("\n");
      if (value && typeof value === "object")
        return Object.entries(value)
          .map(([key, inner]) => `${key}=${text(inner)}`)
          .join("\n");
      return String(value);
    };
    const parts: string[] = [];
    for (const store of [localStorage, sessionStorage])
      for (let i = 0; i < store.length; i++) {
        const key = store.key(i)!;
        parts.push(key, store.getItem(key) ?? "");
      }
    for (const { name } of await indexedDB.databases()) {
      if (!name) continue;
      const db = await new Promise<IDBDatabase>((resolve, reject) => {
        const open = indexedDB.open(name);
        open.onsuccess = () => resolve(open.result);
        open.onerror = () => reject(open.error);
      });
      for (const storeName of Array.from(db.objectStoreNames)) {
        const all = await new Promise<unknown[]>((resolve, reject) => {
          const request = db.transaction(storeName).objectStore(storeName).getAll();
          request.onsuccess = () => resolve(request.result);
          request.onerror = () => reject(request.error);
        });
        const keys = await new Promise<IDBValidKey[]>((resolve, reject) => {
          const request = db.transaction(storeName).objectStore(storeName).getAllKeys();
          request.onsuccess = () => resolve(request.result);
          request.onerror = () => reject(request.error);
        });
        parts.push(name, storeName, text(keys), text(all));
      }
      db.close();
    }
    for (const name of await caches.keys()) {
      const cache = await caches.open(name);
      parts.push(name);
      for (const request of await cache.keys()) {
        const response = await cache.match(request);
        parts.push(request.url, response ? await response.text() : "");
      }
    }
    return parts.join("\n");
  });
}

declare global {
  interface Window {
    bandChatMarkerWrites?: string[];
    bandChatKeyNotices?: KeyNotice[];
    holdBandChatSync: () => Promise<void>;
    bandChatRemovalAccepted?: boolean;
  }
}

export type KeyNotice = "decrypting" | "waiting for the key" | "locked";

/**
 * Records each locked-message notice the room shows from now on, in order, so
 * a test can wait for a notice however briefly it was shown: the decrypting
 * notice gives way to another after ten seconds.
 */
export async function watchKeyNotices(page: Page): Promise<void> {
  await page.evaluate(() => {
    const seen: KeyNotice[] = (window.bandChatKeyNotices = []);
    const record = () => {
      const text = document.body.textContent ?? "";
      const notice: KeyNotice | undefined = /Decrypting \d+ messages?…/.test(text)
        ? "decrypting"
        : /waiting for the room's key/.test(text)
          ? "waiting for the key"
          : /locked on this browser/.test(text)
            ? "locked"
            : undefined;
      if (notice && seen.at(-1) !== notice) seen.push(notice);
    };
    record();
    new MutationObserver(record).observe(document.body, {
      childList: true,
      subtree: true,
      characterData: true,
    });
  });
}

/** The notices recorded since {@link watchKeyNotices}. */
export async function keyNoticesSeen(page: Page): Promise<KeyNotice[]> {
  return page.evaluate(() => window.bandChatKeyNotices ?? []);
}

/** Count calls at the Db so a write refused locally still counts as an attempt. */
export async function watchMarkerWrites(page: Page): Promise<void> {
  await page.evaluate(() => {
    const db = [...(window.__jazzWindowStorageContexts__ ?? [])].at(-1)?.db;
    if (!db) throw new Error("The page has no Jazz Db to observe");
    const prototype = Object.getPrototypeOf(db) as Db;
    const writes: string[] = (window.bandChatMarkerWrites = []);
    const insert = prototype.insert;
    const update = prototype.update;
    prototype.insert = function (this: Db, ...args: Parameters<Db["insert"]>) {
      if (args[0]._table === "readMarkers")
        writes.push((args[1] as { lastReadAt: Date }).lastReadAt.toISOString());
      return Reflect.apply(insert, this, args);
    } as Db["insert"];
    prototype.update = function (this: Db, ...args: Parameters<Db["update"]>) {
      if (args[0]._table === "readMarkers")
        writes.push((args[2] as { lastReadAt: Date }).lastReadAt.toISOString());
      return Reflect.apply(update, this, args);
    } as Db["update"];
  });
}
