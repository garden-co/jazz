// The shared recording stage for the example walkthrough videos.
//
// Each person in a walkthrough is a real browser context with its own storage,
// identity and Jazz client, talking to the example's real sync server. Each
// device's screen is captured (CDP screencast) as timed frames while the
// story runs; afterwards the "stage" page composes them, frame by frame at a
// steady rate, into one video. Capturing and composing are separate so the
// recording never competes with the apps for the CPU (a live re-recording of
// the stage dropped motion to about 5 frames a second). The stage draws
// everything that isn't the app:
//
// - a device frame per pane: an OS menu bar (device name, Wi-Fi) above a slim
//   browser toolbar, so the app itself is never covered;
// - a Wi-Fi switch in that menu bar, outside the app and the browser chrome.
//   Turning it off cuts that device's network for real: every device reaches
//   the dev server through its own small proxy, and "Wi-Fi off" drops the
//   proxy's open connections (the sync WebSocket included) and refuses new
//   ones until it's back on (a walkthrough can keep its bundler's dev server
//   reachable, so hot reload doesn't reload the page). The browser context
//   goes offline too, so the page sees `navigator.onLine === false`;
// - subtitles in a band below the devices, in a rounded, bordered box;
// - title cards.
//
// `encodeRecording` (./encode.mjs) turns the composed video into the committed
// MP4 and poster.

import { spawn } from "node:child_process";
import { mkdir, readFile, rm, writeFile } from "node:fs/promises";
import http from "node:http";
import net from "node:net";
import { join } from "node:path";
import { chromium } from "playwright";

export const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// ---------------------------------------------------------------------------
// The network a device sees.

/**
 * An HTTP proxy for one device. While `online` is false it drops every open
 * connection and refuses new ones, the way turning Wi-Fi off would.
 */
async function startDeviceNetwork({ keepPorts = [] } = {}) {
  // Dev tooling (a bundler's hot-reload socket) isn't part of the app; a
  // walkthrough may keep it connected so the page doesn't reload itself.
  const kept = (port) => keepPorts.includes(Number(port));
  const sockets = new Map();
  const track = (socket, port) => {
    if (sockets.has(socket)) return;
    sockets.set(socket, Number(port));
    socket.on("close", () => sockets.delete(socket));
    socket.on("error", () => {});
  };
  let online = true;
  const server = http.createServer((req, res) => {
    let target;
    try {
      target = new URL(req.url);
    } catch {
      res.writeHead(400).end();
      return;
    }
    const port = target.port || 80;
    if (!online && !kept(port)) return req.socket.destroy();
    track(req.socket, port);
    const upstream = http.request(
      {
        host: target.hostname,
        port,
        path: target.pathname + target.search,
        method: req.method,
        headers: req.headers,
      },
      (response) => {
        res.writeHead(response.statusCode ?? 502, response.headers);
        response.pipe(res);
      },
    );
    upstream.on("socket", (socket) => track(socket, port));
    upstream.on("error", () => res.destroy());
    req.pipe(upstream);
  });
  // WebSockets (and anything else tunnelled) use CONNECT.
  server.on("connect", (req, client, head) => {
    const [host, port] = req.url.split(":");
    if (process.env.WALK_TRACE) console.log(`proxy CONNECT ${req.url} online=${online}`);
    if (!online && !kept(port)) return client.destroy();
    track(client, port);
    const upstream = net.connect(Number(port) || 80, host, () => {
      client.write("HTTP/1.1 200 Connection Established\r\n\r\n");
      if (head?.length) upstream.write(head);
      upstream.pipe(client);
      client.pipe(upstream);
    });
    track(upstream, port);
    const close = () => {
      upstream.destroy();
      client.destroy();
    };
    upstream.on("close", close);
    client.on("close", close);
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  return {
    server: `http://127.0.0.1:${server.address().port}`,
    get online() {
      return online;
    },
    setOnline(next) {
      online = next;
      if (!next)
        for (const [socket, port] of sockets)
          if (port === undefined || !kept(port)) socket.destroy();
    },
    close() {
      for (const socket of sockets.keys()) socket.destroy();
      return new Promise((resolve) => server.close(resolve));
    },
  };
}

// ---------------------------------------------------------------------------
// The cursor drawn inside each app page (headless recordings have none).

function installCursor({ color }) {
  const mount = () => {
    if (document.getElementById("__stage_cursor")) return;
    const cursor = document.createElement("div");
    cursor.id = "__stage_cursor";
    cursor.setAttribute("aria-hidden", "true");
    cursor.style.cssText =
      "position:fixed;left:0;top:0;z-index:2147483647;pointer-events:none;transform:translate(-60px,-60px);transition:transform 260ms cubic-bezier(.3,.7,.3,1);";
    cursor.innerHTML = `<svg width="22" height="28" viewBox="0 0 22 28" style="display:block;filter:drop-shadow(0 1px 2px rgba(0,0,0,.35))"><path d="M2 2 L2 23 L7.5 17.5 L11.5 26 L15 24.4 L11.2 16.2 L19 16.2 Z" fill="${color}" stroke="white" stroke-width="1.6" stroke-linejoin="round"/></svg>`;
    document.documentElement.append(cursor);
    const follow = (e) =>
      (cursor.style.transform = `translate(${e.clientX - 2}px,${e.clientY - 2}px)`);
    for (const type of ["mousemove", "dragover"]) addEventListener(type, follow, true);
    addEventListener(
      "mousedown",
      (e) => {
        const ripple = document.createElement("div");
        ripple.style.cssText = `position:fixed;left:${e.clientX - 16}px;top:${e.clientY - 16}px;width:32px;height:32px;border-radius:50%;border:3px solid ${color};z-index:2147483646;pointer-events:none;transition:transform 450ms ease-out,opacity 450ms ease-out;transform:scale(.3);opacity:.9;`;
        document.documentElement.append(ripple);
        requestAnimationFrame(() =>
          requestAnimationFrame(() => {
            ripple.style.transform = "scale(1.4)";
            ripple.style.opacity = "0";
          }),
        );
        setTimeout(() => ripple.remove(), 600);
      },
      true,
    );
  };
  if (document.readyState === "loading") addEventListener("DOMContentLoaded", mount);
  else mount();
}

// Dev-only overlays (Jazz's inspector toggle, Next.js's dev indicator) aren't
// part of the app.
function hideDevOverlay() {
  const style = document.createElement("style");
  style.textContent = "jazz-inspector-overlay,nextjs-portal{display:none!important}";
  if (document.documentElement) document.documentElement.append(style);
  else addEventListener("DOMContentLoaded", () => document.head.append(style));
}

// ---------------------------------------------------------------------------
// The stage page.

const fontFile = (weight) =>
  new URL(
    import.meta.resolve(`@garden-co/design/fonts/jazz/body-font-latin-${weight}-normal.woff2`),
  );

async function fontFaces() {
  const faces = [];
  for (const weight of [400, 700]) {
    const data = (await readFile(fontFile(weight))).toString("base64");
    faces.push(
      `@font-face{font-family:StageBody;font-weight:${weight};src:url(data:font/woff2;base64,${data}) format("woff2")}`,
    );
  }
  return faces.join("");
}

/** Heights of the drawn device frame above each pane's page. */
export const FRAME = { menuBar: 28, toolbar: 32 };

const WIFI_ON = `<svg viewBox="0 0 24 24" width="17" height="17" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round"><path d="M2.5 8.8a14 14 0 0 1 19 0"/><path d="M5.8 12.4a9.2 9.2 0 0 1 12.4 0"/><path d="M9.1 15.9a4.4 4.4 0 0 1 5.8 0"/><circle cx="12" cy="19.2" r="1.3" fill="currentColor" stroke="none"/></svg>`;
const WIFI_OFF = `<svg viewBox="0 0 24 24" width="17" height="17" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round"><path d="M2.5 8.8a14 14 0 0 1 19 0" opacity=".35"/><path d="M5.8 12.4a9.2 9.2 0 0 1 12.4 0" opacity=".35"/><path d="M9.1 15.9a4.4 4.4 0 0 1 5.8 0" opacity=".35"/><circle cx="12" cy="19.2" r="1.3" fill="currentColor" stroke="none" opacity=".35"/><path d="M4 3.5 20.5 20"/></svg>`;

function stageHtml({ width, height, band, fonts, captionSize, backdrop }) {
  return `<!doctype html><html><head><meta charset="utf-8"><style>${fonts}
*{box-sizing:border-box}
html,body{margin:0;width:${width}px;height:${height + band}px;overflow:hidden;background:${backdrop};font-family:StageBody,system-ui,sans-serif;-webkit-font-smoothing:antialiased}
.device{position:absolute;display:none;flex-direction:column;overflow:hidden;border-radius:10px;background:#000;box-shadow:0 0 0 1px rgba(255,255,255,.12),0 14px 40px rgba(0,0,0,.45)}
.device.fill{border-radius:0;box-shadow:none}
.device.phone{border-radius:30px;box-shadow:0 0 0 9px #0d0d0f,0 0 0 10px rgba(255,255,255,.14),0 16px 44px rgba(0,0,0,.55)}
.device.phone .toolbar,.device.phone .os,.device.phone .name{display:none}
.device.phone .menubar{padding:0 18px 0 22px;border-bottom:0}
.device.phone .menubar .clock{order:-1;color:#f1f2f4;font-weight:700}
.menubar{flex:none;height:${FRAME.menuBar}px;display:flex;align-items:center;gap:10px;padding:0 10px 0 12px;background:#1d1e22;color:#e8e9ec;font-size:13px;border-bottom:1px solid #000}
.menubar .os{width:12px;height:12px;border-radius:3px;background:linear-gradient(135deg,#9aa0aa,#5b616b)}
.menubar .name{font-weight:700;letter-spacing:.01em}
.menubar .spacer{flex:1}
.menubar .clock{color:#b9bcc3;font-variant-numeric:tabular-nums}
.wifi{display:flex;align-items:center;gap:6px;height:22px;padding:0 7px;border-radius:6px;color:#f1f2f4}
.wifi .state{font-size:12px;color:#ffb4a8;display:none}
.wifi.off .state{display:inline}
.wifi.open{background:rgba(255,255,255,.16)}
.toolbar{flex:none;height:${FRAME.toolbar}px;display:flex;align-items:center;gap:8px;padding:0 10px;background:#2a2c31;border-bottom:1px solid #18191c}
.toolbar .dot{width:10px;height:10px;border-radius:50%;background:#55585e}
.toolbar .url{flex:1;margin:0 8% 0 6%;height:22px;border-radius:6px;background:#3a3d43;color:#c9ccd1;font-size:12px;line-height:22px;text-align:center;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.screen{flex:1;position:relative;background:#000;overflow:hidden}
.screen img{display:block;width:100%;height:100%;object-fit:fill}
.screen .net{position:absolute;inset:0;pointer-events:none;box-shadow:inset 0 0 0 3px rgba(255,120,100,0);transition:box-shadow .3s}
.device.offline .screen .net{box-shadow:inset 0 0 0 3px rgba(255,120,100,.75)}
.popover{position:absolute;z-index:20;width:230px;padding:10px 12px;border-radius:12px;background:rgba(36,37,42,.97);border:1px solid rgba(255,255,255,.16);box-shadow:0 12px 32px rgba(0,0,0,.5);color:#eceef2;font-size:13px;opacity:0;transform:translateY(-4px);transition:opacity .18s,transform .18s;pointer-events:none}
.popover.show{opacity:1;transform:none}
.popover .row{display:flex;align-items:center;justify-content:space-between;font-weight:700}
.popover .sub{margin-top:6px;color:#a6a9b0;font-size:12px}
.switch{width:38px;height:22px;border-radius:11px;background:#34c759;position:relative;transition:background .2s}
.switch::after{content:"";position:absolute;top:2px;left:18px;width:18px;height:18px;border-radius:50%;background:#fff;transition:left .2s}
.switch.off{background:#5b5e66}
.switch.off::after{left:2px}
#cursor{position:absolute;left:0;top:0;z-index:40;pointer-events:none;opacity:0;transition:transform 420ms cubic-bezier(.3,.7,.3,1),opacity .25s}
#cursor .ripple{position:absolute;left:-14px;top:-14px;width:32px;height:32px;border-radius:50%;border:3px solid #fff;opacity:0}
#cursor.press .ripple{animation:ripple .5s ease-out}
@keyframes ripple{from{transform:scale(.3);opacity:.95}to{transform:scale(1.4);opacity:0}}
#band{position:absolute;left:0;right:0;top:${height}px;height:${band}px;display:flex;align-items:center;justify-content:center}
#cap{max-width:${Math.round(width * 0.86)}px;padding:${Math.round(captionSize * 0.5)}px ${Math.round(captionSize * 1.05)}px;border-radius:${Math.round(captionSize * 0.75)}px;background:rgba(14,15,18,.9);border:1.5px solid rgba(255,255,255,.34);box-shadow:0 8px 26px rgba(0,0,0,.4);color:#fff;font-size:${captionSize}px;line-height:1.3;text-align:center;opacity:0;transition:opacity .3s}
#title{position:absolute;inset:0;z-index:50;display:none;align-items:center;justify-content:center;flex-direction:column;color:#fff;background:${backdrop}}
#title h1{font-size:56px;margin:0 0 14px;font-weight:700}
#title p{font-size:23px;opacity:.82;margin:0;max-width:900px;text-align:center;line-height:1.35}
</style></head><body>
<div id="title"><h1></h1><p></p></div>
<div id="band"><div id="cap"></div></div>
<div id="cursor"><svg width="22" height="28" viewBox="0 0 22 28" style="display:block;filter:drop-shadow(0 1px 2px rgba(0,0,0,.5))"><path d="M2 2 L2 23 L7.5 17.5 L11.5 26 L15 24.4 L11.2 16.2 L19 16.2 Z" fill="#111" stroke="white" stroke-width="1.6" stroke-linejoin="round"/></svg><div class="ripple"></div></div>
</body></html>`;
}

/** Adds a device frame to the stage page (runs in the stage page). */
function mountDevice({ id, name, address, kind, wifiOn, wifiOff }) {
  const device = document.createElement("div");
  device.className = `device ${kind}`;
  device.id = `device-${id}`;
  device.innerHTML = `
    <div class="menubar"><span class="os"></span><span class="name"></span><span class="spacer"></span>
      <span class="wifi" data-wifi><span class="state">Offline</span><span class="icon">${wifiOn}</span></span>
      <span class="clock">9:41</span></div>
    <div class="toolbar"><span class="dot"></span><span class="dot"></span><span class="dot"></span><span class="url"></span></div>
    <div class="screen"><img alt=""><div class="net"></div></div>`;
  device.querySelector(".name").textContent = name;
  device.querySelector(".url").textContent = address;
  const popover = document.createElement("div");
  popover.className = "popover";
  popover.id = `wifi-${id}`;
  popover.innerHTML = `<div class="row"><span>Wi-Fi</span><span class="switch" data-switch></span></div><div class="sub">Venue Wi-Fi</div>`;
  document.body.append(device, popover);
  device.dataset.wifiOn = wifiOn;
  device.dataset.wifiOff = wifiOff;
}

// ---------------------------------------------------------------------------

export class Stage {
  /**
   * width/height: the area the devices share. Subtitles get their own band
   * below it (`captionBand`, default about four lines of `captionSize`), so
   * they never cover a device. `webgl` turns on software WebGL.
   * executablePath: optional Chromium build.
   */
  static async launch({
    width = 1280,
    height = 800,
    captionSize = 22,
    captionBand,
    backdrop = "#000",
    fps = 30,
    webgl = false,
    videoDir,
    executablePath = process.env.CHROMIUM_PATH || undefined,
  } = {}) {
    await mkdir(join(videoDir, "frames"), { recursive: true });
    // Software WebGL only for apps that need it (a globe): it slows down
    // every other page's rendering, and so the captured frame rate.
    const browser = await chromium.launch({
      executablePath,
      args: webgl
        ? ["--use-gl=angle", "--use-angle=swiftshader", "--enable-unsafe-swiftshader"]
        : [],
    });
    const stage = new Stage();
    const band = captionBand ?? Math.round((captionSize * 4.2) / 2) * 2;
    Object.assign(stage, { browser, width, height, band, captionSize, backdrop, fps, videoDir });
    stage.devices = new Map();
    stage.ops = [];
    stage.frameCount = 0;
    return stage;
  }

  /**
   * A device: its own browser context (own storage and identity) behind its
   * own network, in dark mode unless `colorScheme` says otherwise. Returns
   * the page. `name` is shown in a laptop's menu bar; `kind: "phone"` draws a
   * phone with a status bar and no browser toolbar.
   */
  async device(
    id,
    {
      name = id,
      address = "",
      color = "#3b82f6",
      kind = "laptop",
      viewport,
      colorScheme = "dark",
      keepPorts = [],
      contextOptions = {},
    } = {},
  ) {
    const network = await startDeviceNetwork({ keepPorts });
    const context = await this.browser.newContext({
      viewport: viewport ?? { width: 1280, height: 800 },
      colorScheme,
      // Loopback normally bypasses proxies; "<-loopback>" sends it through ours.
      proxy: { server: network.server, bypass: "<-loopback>" },
      ...contextOptions,
    });
    await context.addInitScript(installCursor, { color });
    await context.addInitScript(hideDevOverlay);
    const page = await context.newPage();
    page.on("pageerror", (e) => console.log(`[${id}] pageerror:`, String(e).slice(0, 300)));
    this.devices.set(id, {
      id,
      name,
      address,
      kind,
      context,
      page,
      network,
      cdp: null,
      frames: [],
      writes: [],
      wifi: true,
    });
    return page;
  }

  /**
   * Starts capturing. Call once every device is set up. Each device's frames
   * are kept with the time they arrived; the stage page (frames, subtitles,
   * Wi-Fi menu, cursor) runs live for layout, and every change to it is
   * logged. `finish()` then renders the video frame by frame at a steady
   * `fps`, so the capture never competes with the apps for the CPU.
   */
  async start() {
    this.stageContext = await this.browser.newContext({
      viewport: { width: this.width, height: this.height + this.band },
    });
    this.stagePage = await this.stageContext.newPage();
    this.html = stageHtml({
      width: this.width,
      height: this.height,
      band: this.band,
      fonts: await fontFaces(),
      captionSize: this.captionSize,
      backdrop: this.backdrop,
    });
    await this.stagePage.setContent(this.html);
    await this.stagePage.evaluate(() => document.fonts.ready);
    this.startedAt = Date.now();
    for (const d of this.devices.values()) {
      await this.#ui(mountDevice, {
        id: d.id,
        name: d.name,
        address: d.address,
        kind: d.kind,
        wifiOn: WIFI_ON,
        wifiOff: WIFI_OFF,
      });
      await this.#cast(d);
    }
  }

  /** Runs `fn(arg)` in the stage page now, and logs it for the render. */
  #ui(fn, arg) {
    this.ops.push({ t: Date.now(), fn: fn.toString(), arg });
    return this.stagePage.evaluate(fn, arg);
  }

  /** Marks the moment the walkthrough begins; the video starts here. */
  roll() {
    this.rolledAt = Date.now();
  }

  /** Marks the current moment as the poster frame. */
  poster() {
    this.posterAt = (Date.now() - (this.rolledAt ?? this.startedAt)) / 1000;
  }

  async #cast(d) {
    d.cdp = await d.page.context().newCDPSession(d.page);
    // Keeps at most one frame per output frame: a frame that arrives sooner
    // waits, and is replaced if a newer one comes before it's due.
    const gap = 1000 / this.fps;
    const store = () => {
      clearTimeout(d.timer);
      d.timer = undefined;
      if (!d.pending) return;
      const { t, data } = d.pending;
      d.pending = undefined;
      d.lastStored = t;
      const file = join(this.videoDir, "frames", `${d.id}-${this.frameCount++}.jpg`);
      d.frames.push({ t, file });
      d.writes.push(writeFile(file, Buffer.from(data, "base64")));
    };
    d.cdp.on("Page.screencastFrame", (frame) => {
      d.cdp.send("Page.screencastFrameAck", { sessionId: frame.sessionId }).catch(() => {});
      d.pending = { t: Date.now(), data: frame.data };
      const wait = (d.lastStored ?? 0) + gap - Date.now();
      if (wait <= 0) store();
      else d.timer ??= setTimeout(store, wait);
    });
    await d.cdp.send("Page.startScreencast", {
      format: "jpeg",
      quality: 82,
      maxWidth: 2000,
      maxHeight: 2000,
    });
  }

  /** Re-attaches the screencast after a device's page loads a new document. */
  async recast(id) {
    const d = this.devices.get(id);
    try {
      await d.cdp.send("Page.stopScreencast");
    } catch {}
    await this.#cast(d);
  }

  /**
   * Shows devices at the given boxes (stage px, including the drawn frame).
   * Each: { id, x, y, w, h, scale = 1, fill = false }. The page's viewport
   * becomes the box's screen area divided by `scale`, so the app lays itself
   * out for that size.
   */
  async show(panes) {
    for (const p of panes) {
      const d = this.devices.get(p.id);
      const scale = p.scale ?? 1;
      const width = Math.round(p.w / scale);
      const frame = d.kind === "phone" ? FRAME.menuBar : FRAME.menuBar + FRAME.toolbar;
      const height = Math.round((p.h - frame) / scale);
      const current = d.page.viewportSize();
      if (!current || current.width !== width || current.height !== height)
        await d.page.setViewportSize({ width, height });
    }
    await this.#ui((panes) => {
      const ids = panes.map((p) => p.id);
      for (const el of document.querySelectorAll(".device"))
        if (!ids.includes(el.id.slice(7))) el.style.display = "none";
      for (const p of panes) {
        const el = document.getElementById(`device-${p.id}`);
        Object.assign(el.style, {
          display: "flex",
          left: `${p.x}px`,
          top: `${p.y}px`,
          width: `${p.w}px`,
          height: `${p.h}px`,
        });
        el.classList.toggle("fill", !!p.fill);
      }
    }, panes);
    this.layout = panes;
    await sleep(350);
  }

  /** One device filling the device area. */
  full(id) {
    return this.show([{ id, x: 0, y: 0, w: this.width, h: this.height, fill: true }]);
  }

  /** Two devices side by side, each laid out at `1 / scale` of its box. */
  split(a, b, { scale = 0.66, pad = 14, gap = 14 } = {}) {
    const w = Math.floor((this.width - pad * 2 - gap) / 2);
    const h = this.height - pad * 2;
    return this.show([
      { id: a, x: pad, y: pad, w, h, scale },
      { id: b, x: pad + w + gap, y: pad, w, h, scale },
    ]);
  }

  /** Shows a subtitle in the band below the devices (empty text hides it). */
  async caption(text, ms = 0) {
    if (process.env.WALK_TRACE)
      console.log(`${((Date.now() - (this.rolledAt ?? 0)) / 1000).toFixed(1)}s caption ${text}`);
    await this.#ui((text) => {
      const cap = document.getElementById("cap");
      if (text) cap.textContent = text;
      cap.style.opacity = text ? "1" : "0";
    }, text || "");
    if (ms) await sleep(ms);
  }

  async title(heading, text, ms = 2500) {
    await this.#ui(
      ([heading, text]) => {
        const title = document.getElementById("title");
        title.querySelector("h1").textContent = heading;
        title.querySelector("p").textContent = text;
        title.style.display = "flex";
      },
      [heading, text],
    );
    await sleep(ms);
    await this.#ui(() => (document.getElementById("title").style.display = "none"));
  }

  /** Glides the stage's own cursor (outside every app) to a point and clicks. */
  async #stageClick(x, y) {
    await this.#ui(
      ([x, y]) => {
        const cursor = document.getElementById("cursor");
        cursor.style.opacity = "1";
        cursor.style.transform = `translate(${x - 2}px,${y - 2}px)`;
      },
      [x, y],
    );
    await sleep(560);
    await this.#ui(() => {
      const cursor = document.getElementById("cursor");
      cursor.classList.remove("press");
      void cursor.offsetWidth;
      cursor.classList.add("press");
    });
    await sleep(220);
  }

  /**
   * Turns a device's Wi-Fi off or on from its menu bar: the stage cursor
   * opens the Wi-Fi menu and flips the switch. Off drops that device's
   * connections (sync included) and keeps it offline until it's turned back on.
   */
  async wifi(id, on) {
    const d = this.devices.get(id);
    const icon = await this.stagePage.locator(`#device-${id} [data-wifi]`).boundingBox();
    const startX = icon.x + icon.width / 2 - 60;
    await this.#ui(
      ([x, y]) => {
        const cursor = document.getElementById("cursor");
        cursor.style.transition = "none";
        cursor.style.transform = `translate(${x}px,${y}px)`;
        void cursor.offsetWidth;
        cursor.style.transition = "";
      },
      [startX, icon.y + 90],
    );
    await this.#stageClick(icon.x + icon.width / 2, icon.y + icon.height / 2);
    const popover = await this.#ui(
      ({ id, x, y }) => {
        const pop = document.getElementById(`wifi-${id}`);
        const left = Math.min(x - 200, document.body.clientWidth - 240);
        pop.style.left = `${Math.max(8, left)}px`;
        pop.style.top = `${y + 6}px`;
        document.querySelector(`#device-${id} [data-wifi]`).classList.add("open");
        pop.classList.add("show");
        const sw = pop.querySelector("[data-switch]").getBoundingClientRect();
        return { x: sw.x + sw.width / 2, y: sw.y + sw.height / 2 };
      },
      { id, x: icon.x + icon.width, y: icon.y + icon.height },
    );
    await sleep(350);
    await this.#stageClick(popover.x, popover.y);
    // The real cut (or reconnect) happens as the switch flips.
    d.network.setOnline(on);
    await d.context.setOffline(!on);
    d.wifi = on;
    await this.#ui(
      ({ id, on }) => {
        const device = document.getElementById(`device-${id}`);
        const pop = document.getElementById(`wifi-${id}`);
        pop.querySelector("[data-switch]").classList.toggle("off", !on);
        pop.querySelector(".sub").textContent = on ? "Venue Wi-Fi" : "Not connected";
        const wifi = device.querySelector("[data-wifi]");
        wifi.classList.toggle("off", !on);
        wifi.querySelector(".icon").innerHTML = on ? device.dataset.wifiOn : device.dataset.wifiOff;
        device.classList.toggle("offline", !on);
      },
      { id, on },
    );
    await sleep(500);
    await this.#ui((id) => {
      document.getElementById(`wifi-${id}`).classList.remove("show");
      document.querySelector(`#device-${id} [data-wifi]`).classList.remove("open");
      document.getElementById("cursor").style.opacity = "0";
    }, id);
    await sleep(250);
  }

  /**
   * Stops capturing and renders the video: from `roll()` to now, at a steady
   * `fps`, each frame shows every device's latest frame and the stage as it
   * was at that moment (its CSS transitions seeked to that time). Returns
   * { path, trimStart, posterAt } for encodeRecording.
   */
  async finish() {
    const end = Date.now();
    for (const d of this.devices.values()) {
      await d.cdp?.send("Page.stopScreencast").catch(() => {});
      clearTimeout(d.timer);
      await Promise.all(d.writes);
      await d.context.close().catch(() => {});
      await d.network.close();
    }
    const from = this.rolledAt ?? this.startedAt;
    if (process.env.WALK_TRACE)
      for (const d of this.devices.values()) {
        // Busiest second of captured frames, a ceiling for on-screen motion.
        let best = 0;
        for (let i = 0, j = 0; j < d.frames.length; j++) {
          while (d.frames[j].t - d.frames[i].t >= 1000) i++;
          best = Math.max(best, j - i + 1);
        }
        console.log(`${d.id}: ${d.frames.length} frames, busiest second ${best}`);
      }
    const path = join(this.videoDir, "stage.mp4");
    const started = Date.now();
    const frames = await this.#render(from, end, path);
    if (process.env.WALK_TRACE)
      console.log(`rendered ${frames} frames in ${((Date.now() - started) / 1000).toFixed(1)} s`);
    await this.browser.close();
    return { path, trimStart: 0, posterAt: this.posterAt };
  }

  async #render(from, end, path) {
    const page = this.stagePage;
    await page.setContent(this.html);
    await page.evaluate(() => document.fonts.ready);
    // Replays logged stage changes; CSS transitions they start are paused and
    // seeked to the frame's time instead of running in real time.
    await page.evaluate(() => {
      // Returns whether anything is still moving at time t.
      window.__seek = (t) => {
        let moving = false;
        for (const a of document.getAnimations()) {
          if (a.__start === undefined) {
            a.__start = window.__opTime;
            a.pause();
          }
          const at = Math.max(0, t - a.__start);
          a.currentTime = at;
          const end = a.effect?.getComputedTiming().endTime ?? 0;
          if (at < end) moving = true;
        }
        return moving;
      };
    });
    const ffmpeg = spawn(
      "ffmpeg",
      [
        "-y",
        "-loglevel",
        "error",
        "-f",
        "image2pipe",
        "-c:v",
        "mjpeg",
        "-framerate",
        String(this.fps),
        "-i",
        "-",
        "-c:v",
        "libx264",
        "-preset",
        "veryfast",
        "-crf",
        "14",
        "-pix_fmt",
        "yuv420p",
        path,
      ],
      { stdio: ["pipe", "ignore", "inherit"] },
    );
    const done = new Promise((resolve, reject) =>
      ffmpeg.on("exit", (code) => (code ? reject(new Error(`ffmpeg exited ${code}`)) : resolve())),
    );
    const cdp = await page.context().newCDPSession(page);
    const shown = new Map();
    let op = 0;
    let n = 0;
    let last;
    let moving = true;
    for (let t = from; t <= end; t = from + (++n * 1000) / this.fps) {
      let changed = moving;
      if (op < this.ops.length && this.ops[op].t <= t) changed = true;
      while (op < this.ops.length && this.ops[op].t <= t) {
        const { t: opTime, fn, arg } = this.ops[op++];
        await page.evaluate(
          ([opTime, fn, arg]) => {
            window.__opTime = opTime;
            // eslint-disable-next-line no-new-func
            new Function(`return (${fn})`)()(arg);
            window.__seek(opTime);
          },
          [opTime, fn, arg],
        );
      }
      const changes = [];
      for (const d of this.devices.values()) {
        let latest;
        for (const f of d.frames) {
          if (f.t > t) break;
          latest = f;
        }
        if (latest && shown.get(d.id) !== latest.file) {
          shown.set(d.id, latest.file);
          changes.push([d.id, (await readFile(latest.file)).toString("base64")]);
        }
      }
      if (changes.length) changed = true;
      if (changed || !last) {
        moving = await page.evaluate(
          async ([t, changes]) => {
            const moving = window.__seek(t);
            await Promise.all(
              changes.map(([id, data]) => {
                const img = document.querySelector(`#device-${id} .screen img`);
                img.src = `data:image/jpeg;base64,${data}`;
                return img.decode().catch(() => {});
              }),
            );
            return moving;
          },
          [t, changes],
        );
        const { data } = await cdp.send("Page.captureScreenshot", {
          format: "jpeg",
          quality: 92,
          optimizeForSpeed: true,
        });
        last = Buffer.from(data, "base64");
      }
      if (!ffmpeg.stdin.write(last))
        await new Promise((resolve) => ffmpeg.stdin.once("drain", resolve));
    }
    ffmpeg.stdin.end();
    await done;
    return n;
  }

  async abort(debugDir) {
    if (debugDir) {
      await mkdir(debugDir, { recursive: true });
      for (const d of this.devices.values())
        await d.page.screenshot({ path: `${debugDir}/${d.id}.png` }).catch(() => {});
    }
    await this.browser.close().catch(() => {});
    for (const d of this.devices.values()) await d.network.close().catch(() => {});
  }

  async cleanup() {
    await rm(this.videoDir, { recursive: true, force: true });
  }
}

// ---------------------------------------------------------------------------
// Driving a page the way a person would, with the drawn cursor.

export async function pointAt(page, locator, { steps = 8, hover = 320 } = {}) {
  await locator.waitFor({ state: "visible", timeout: 30_000 });
  await locator.scrollIntoViewIfNeeded().catch(() => {});
  const box = await locator.boundingBox();
  if (box) await page.mouse.move(box.x + box.width / 2, box.y + box.height / 2, { steps });
  await sleep(hover);
  return box;
}

/**
 * Glides the drawn cursor to the element and clicks it. `direct` clicks with
 * the mouse where the element is, skipping Playwright's actionability checks,
 * which are slow on pages that animate every frame (a WebGL globe).
 */
export async function click(page, locator, { after = 400, direct = false, ...options } = {}) {
  const started = Date.now();
  const box = await pointAt(page, locator, options);
  if (direct && box) await page.mouse.click(box.x + box.width / 2, box.y + box.height / 2);
  else await locator.click();
  if (process.env.WALK_TRACE) console.log(`click ${locator} ${Date.now() - started} ms`);
  await sleep(after);
}

export async function type(page, locator, text, { delay = 55, clear = false, ...options } = {}) {
  await click(page, locator, { after: 150, ...options });
  if (clear) await locator.fill("");
  await locator.pressSequentially(text, { delay });
  await sleep(300);
}
