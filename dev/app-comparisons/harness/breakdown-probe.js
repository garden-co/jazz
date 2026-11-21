// Injected by breakdown.mjs into every page (addInitScript) and every
// SharedWorker (CDP, while it waits for the debugger, before its first line).
// Observes from outside, without patching Jazz: WebAssembly compile and
// instantiate calls, WebSocket frames, MessagePort traffic, console calls,
// and (pages) DOM conditions. Everything is timestamped with
// performance.timeOrigin + performance.now() (epoch ms).
//
// Wrapped as a function taking { realm, checks }; breakdown.mjs calls it.
function benchProbe({ realm, checks = [] }) {
  if (globalThis.__bench) return;
  const now = () => performance.timeOrigin + performance.now();
  const B = (globalThis.__bench = { realm, ws: [], ports: [], console: [], needle: null, counts: {} });
  const mark = (name, detail) => {
    try {
      performance.mark(name, { detail });
    } catch {}
  };
  const once = new Set();
  const markOnce = (name, detail) => {
    if (once.has(name)) return;
    once.add(name);
    mark(name, detail);
  };
  const latin1 = new TextDecoder("latin1");
  // Does a message (string, bytes, or a structured-clone object) carry the needle?
  const has = (value, depth = 0) => {
    const needle = B.needle;
    if (!needle || value == null || depth > 8) return false;
    if (typeof value === "string") return value.includes(needle);
    if (value instanceof ArrayBuffer) return latin1.decode(new Uint8Array(value)).includes(needle);
    if (ArrayBuffer.isView(value)) {
      return latin1.decode(new Uint8Array(value.buffer, value.byteOffset, value.byteLength)).includes(needle);
    }
    if (typeof Blob !== "undefined" && value instanceof Blob) return false;
    if (typeof value === "object") {
      for (const key in value) if (has(value[key], depth + 1)) return true;
    }
    return false;
  };
  const size = (value) =>
    typeof value === "string" ? value.length : value?.byteLength ?? value?.size ?? undefined;
  const typeOf = (value) =>
    value && typeof value === "object" && typeof value.type === "string" ? value.type : undefined;

  // WebAssembly: compile/instantiate timings (wasm-bindgen goes through these).
  const W = globalThis.WebAssembly;
  if (W) {
    for (const name of ["instantiateStreaming", "instantiate", "compileStreaming", "compile"]) {
      const orig = W[name];
      if (typeof orig !== "function") continue;
      W[name] = function (...args) {
        const n = (B.counts[`wasm:${name}`] = (B.counts[`wasm:${name}`] ?? 0) + 1);
        mark(`wasm:${name}:start`, { n });
        return orig.apply(this, args).then((result) => {
          mark(`wasm:${name}:end`, { n });
          markOnce("wasm:ready");
          return result;
        });
      };
    }
  }

  // WebSocket frames, with whether they carry the needle.
  const WS = globalThis.WebSocket;
  if (WS) {
    const origSend = WS.prototype.send;
    WS.prototype.send = function (data) {
      B.ws.push({ t: now(), dir: "out", bytes: size(data), needle: has(data) || undefined });
      markOnce("ws:first-send");
      return origSend.call(this, data);
    };
    globalThis.WebSocket = new Proxy(WS, {
      construct(target, args) {
        const socket = Reflect.construct(target, args);
        const n = (B.counts.ws = (B.counts.ws ?? 0) + 1);
        mark("ws:create", { n, url: String(args[0]) });
        socket.addEventListener("open", () => mark("ws:open", { n }));
        socket.addEventListener("close", (e) => mark("ws:close", { n, code: e.code }));
        socket.addEventListener("message", (e) => {
          B.ws.push({ t: now(), dir: "in", bytes: size(e.data), needle: has(e.data) || undefined });
          markOnce("ws:first-recv");
        });
        return socket;
      },
    });
  }

  // MessagePort / SharedWorker port traffic (page <-> worker).
  const MP = globalThis.MessagePort;
  if (MP) {
    const origPost = MP.prototype.postMessage;
    MP.prototype.postMessage = function (data, ...rest) {
      const needle = has(data) || undefined;
      B.ports.push({ t: now(), dir: "out", type: typeOf(data), needle });
      if (needle) performance.mark("probe:port-out-needle");
      return origPost.call(this, data, ...rest);
    };
    const record = (e) =>
      B.ports.push({ t: now(), dir: "in", type: typeOf(e.data), needle: has(e.data) || undefined });
    const origAdd = MP.prototype.addEventListener;
    MP.prototype.addEventListener = function (type, listener, ...rest) {
      if (type === "message" && !this.__benchListening) {
        this.__benchListening = true;
        origAdd.call(this, "message", record);
      }
      return origAdd.call(this, type, listener, ...rest);
    };
    const desc = Object.getOwnPropertyDescriptor(MP.prototype, "onmessage");
    if (desc?.set) {
      Object.defineProperty(MP.prototype, "onmessage", {
        ...desc,
        set(fn) {
          if (!this.__benchListening) {
            this.__benchListening = true;
            origAdd.call(this, "message", record);
          }
          desc.set.call(this, fn);
        },
      });
    }
  }
  if (globalThis.SharedWorker) {
    const SW = globalThis.SharedWorker;
    globalThis.SharedWorker = new Proxy(SW, {
      construct(target, args) {
        mark("page:shared-worker-created", { url: String(args[0]) });
        return Reflect.construct(target, args);
      },
    });
  }
  if (realm === "worker") {
    globalThis.addEventListener?.("connect", () => mark("worker:connect"));
  }

  // Console, for pages (workers' console comes through CDP with timestamps).
  if (realm === "page") {
    for (const level of ["log", "info", "warn", "error", "debug", "trace"]) {
      const orig = console[level];
      console[level] = function (...args) {
        try {
          const text = args
            .map((a) => {
              if (typeof a === "string") return a;
              if (a instanceof Error) return `${a.name}: ${a.message}`;
              try {
                return JSON.stringify(a);
              } catch {
                return String(a);
              }
            })
            .join(" ");
          B.console.push({ t: now(), level, text: text.slice(0, 4000) });
        } catch {}
        return orig.apply(this, args);
      };
    }
  }

  // DOM conditions: each check { name, cond } marks `name` whenever cond()
  // returns a new truthy key (e.g. "100 cards", or the needle that appeared).
  if (realm === "page" && checks.length) {
    const compiled = checks.map((c) => ({ name: c.name, cond: new Function(c.cond), last: undefined }));
    const run = () => {
      for (const c of compiled) {
        let key;
        try {
          key = c.cond();
        } catch {}
        if (key && key !== c.last) {
          c.last = key;
          mark(c.name, { key });
        }
      }
    };
    B.runChecks = run;
    const start = () => {
      new MutationObserver(run).observe(document, { subtree: true, childList: true, characterData: true, attributes: true });
      run();
    };
    if (document.documentElement) start();
    else document.addEventListener("readystatechange", start, { once: true });
  }

  // Long tasks (pages): main-thread work of 50 ms or more.
  B.longtasks = [];
  if (realm === "page" && typeof PerformanceObserver !== "undefined") {
    try {
      new PerformanceObserver((list) => {
        for (const e of list.getEntries()) B.longtasks.push({ t: performance.timeOrigin + e.startTime, ms: e.duration });
      }).observe({ type: "longtask", buffered: true });
    } catch {}
  }

  // What breakdown.mjs reads back.
  B.collect = () => ({
    realm,
    url: String(location.href),
    timeOrigin: performance.timeOrigin,
    navigation: realm === "page" ? performance.getEntriesByType("navigation").map((e) => e.toJSON()) : [],
    marks: performance.getEntriesByType("mark").map((m) => ({ name: m.name, t: m.startTime, detail: m.detail ?? undefined })),
    resources: performance
      .getEntriesByType("resource")
      .map((r) => ({ name: r.name, start: r.startTime, responseEnd: r.responseEnd, bytes: r.transferSize })),
    ws: B.ws,
    ports: B.ports,
    console: B.console,
    counts: B.counts,
    longtasks: B.longtasks,
  });
}
