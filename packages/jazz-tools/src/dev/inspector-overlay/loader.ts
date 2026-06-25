import { createRelay } from "./relay.js";

const PANEL_STATE_KEY = "jazz-inspector-overlay:open";
const readOpen = () => {
  try {
    return localStorage.getItem(PANEL_STATE_KEY) === "1";
  } catch {
    return false;
  }
};
const writeOpen = (open: boolean) => {
  try {
    localStorage.setItem(PANEL_STATE_KEY, open ? "1" : "0");
  } catch {
    /* ignore */
  }
};

function mount(): void {
  const w = window as unknown as Record<string, unknown>;
  if (w.__jazzInspectorOverlayMounted) return;
  w.__jazzInspectorOverlayMounted = true;

  const container = document.createElement("div");
  container.id = "jazz-inspector-overlay";
  container.style.cssText =
    "position:fixed;bottom:16px;right:16px;z-index:2147483647;font-family:system-ui,sans-serif;";

  const toggle = document.createElement("button");
  toggle.textContent = "⚡";
  toggle.setAttribute("aria-label", "Toggle Jazz inspector");
  toggle.style.cssText =
    "width:40px;height:40px;border-radius:50%;border:none;cursor:pointer;font-size:18px;box-shadow:0 2px 8px rgba(0,0,0,.25);background:#111;color:#fff;";

  const panel = document.createElement("div");
  panel.style.cssText =
    "position:fixed;bottom:64px;right:16px;width:480px;height:640px;max-width:90vw;max-height:80vh;background:#fff;border:1px solid #ddd;border-radius:8px;overflow:hidden;box-shadow:0 8px 32px rgba(0,0,0,.3);resize:both;display:none;";

  const iframe = document.createElement("iframe");
  // The embedded Vite build emits embedded.html, not index.html.
  iframe.src = "/__jazz/embedded/embedded.html";
  iframe.style.cssText = "width:100%;height:100%;border:none;";
  panel.appendChild(iframe);

  let open = readOpen();
  const apply = () => (panel.style.display = open ? "block" : "none");
  const setOpen = (next: boolean) => {
    open = next;
    writeOpen(open);
    apply();
  };
  toggle.addEventListener("click", () => setOpen(!open));
  // Alt+Shift+J toggles.
  window.addEventListener("keydown", (e) => {
    if (e.altKey && e.shiftKey && e.key.toLowerCase() === "j") setOpen(!open);
  });
  apply();

  container.appendChild(panel);
  container.appendChild(toggle);
  document.body.appendChild(container);

  // Relay wired regardless of panel visibility: the iframe announces/subscribes on load.
  const relay = createRelay({
    topWindow: window,
    iframeWindow: iframe.contentWindow!,
    origin: window.location.origin,
  });
  window.addEventListener("message", (event) => relay.handle(event));
}

if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", mount);
else mount();
