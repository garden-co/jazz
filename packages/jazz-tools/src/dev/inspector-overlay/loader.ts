import { createRelay } from "./relay.js";

const OPEN_KEY = "jazz-inspector-overlay:open";
const HEIGHT_KEY = "jazz-inspector-overlay:height";
const MIN_HEIGHT = 200;
const DEFAULT_RATIO = 0.42;

const readOpen = (): boolean => {
  try {
    return localStorage.getItem(OPEN_KEY) === "1";
  } catch {
    return false;
  }
};
const writeOpen = (open: boolean): void => {
  try {
    localStorage.setItem(OPEN_KEY, open ? "1" : "0");
  } catch {
    /* ignore */
  }
};
const readHeight = (): number | null => {
  try {
    const raw = localStorage.getItem(HEIGHT_KEY);
    const n = raw == null ? NaN : Number(raw);
    return Number.isFinite(n) ? n : null;
  } catch {
    return null;
  }
};
const writeHeight = (h: number): void => {
  try {
    localStorage.setItem(HEIGHT_KEY, String(Math.round(h)));
  } catch {
    /* ignore */
  }
};

const maxHeight = () => Math.round(window.innerHeight * 0.92);
const clampHeight = (h: number) => Math.max(MIN_HEIGHT, Math.min(h, maxHeight()));

// Scoped styles. Everything is namespaced under #jazz-inspector-overlay so the
// overlay can't leak into — or be restyled by — the host app. The chrome is
// tuned to the inspector's own dark theme (bg #0f141b, borders #1c2430, accent
// #345273) and docks to the bottom edge like browser devtools.
const STYLE = `
#jazz-inspector-overlay {
  position: fixed; inset: 0; z-index: 2147483647; pointer-events: none;
  font-family: Inter, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
}
#jazz-inspector-overlay *, #jazz-inspector-overlay *::before, #jazz-inspector-overlay *::after {
  box-sizing: border-box;
}
#jazz-inspector-overlay .jzov-toggle {
  position: fixed; bottom: 16px; right: 16px; pointer-events: auto;
  display: flex; align-items: center; justify-content: center;
  width: 44px; height: 44px; padding: 8px; margin: 0;
  border-radius: 50%; border: 1px solid #1c2430; background: #161b24; cursor: pointer;
  -webkit-tap-highlight-color: transparent;
  box-shadow: 0 4px 14px rgba(0,0,0,.4), 0 1px 2px rgba(0,0,0,.3);
  transition: transform .18s cubic-bezier(.22,1,.36,1), background-color .18s ease,
    border-color .18s ease, box-shadow .18s ease;
}
#jazz-inspector-overlay .jzov-toggle:hover {
  background: #1c2430; border-color: #345273; box-shadow: 0 6px 20px rgba(0,0,0,.45);
}
#jazz-inspector-overlay .jzov-toggle:active { transform: scale(.93); }
#jazz-inspector-overlay .jzov-toggle:focus-visible { outline: 2px solid #5b8fc7; outline-offset: 2px; }
#jazz-inspector-overlay .jzov-toggle[hidden] { display: none; }
#jazz-inspector-overlay .jzov-toggle svg { width: 100%; height: 100%; display: block; }
#jazz-inspector-overlay .jzov-dock {
  position: fixed; left: 0; right: 0; bottom: 0; width: 100%; pointer-events: auto;
  display: flex; flex-direction: column;
  background: #0f141b; border-top: 1px solid #1c2430;
  box-shadow: 0 -14px 44px rgba(0,0,0,.5);
  transform: translateY(100%); visibility: hidden;
  transition: transform .24s cubic-bezier(.22,1,.36,1), visibility 0s linear .24s;
}
#jazz-inspector-overlay .jzov-dock[data-open="true"] {
  transform: translateY(0); visibility: visible;
  transition: transform .24s cubic-bezier(.22,1,.36,1), visibility 0s;
}
#jazz-inspector-overlay .jzov-bar {
  flex: 0 0 33px; height: 33px; display: flex; align-items: center; gap: 10px;
  padding: 0 8px 0 12px; background: #0f141b; border-bottom: 1px solid #1c2430;
  cursor: ns-resize; user-select: none; touch-action: none;
}
#jazz-inspector-overlay .jzov-brand { display: flex; align-items: center; gap: 8px; pointer-events: none; }
#jazz-inspector-overlay .jzov-brand svg { width: 17px; height: 17px; display: block; }
#jazz-inspector-overlay .jzov-title {
  font-size: 12px; font-weight: 600; letter-spacing: .01em; color: #9ca8b9; white-space: nowrap;
}
#jazz-inspector-overlay .jzov-grip { flex: 1 1 auto; height: 100%; }
#jazz-inspector-overlay .jzov-close {
  flex: 0 0 auto; display: flex; align-items: center; justify-content: center;
  width: 26px; height: 26px; padding: 0; border-radius: 6px; border: 1px solid transparent;
  background: transparent; color: #9ca8b9; cursor: pointer;
  transition: background-color .15s ease, color .15s ease;
}
#jazz-inspector-overlay .jzov-close:hover { background: #1c2430; color: #dbe1ea; }
#jazz-inspector-overlay .jzov-close:focus-visible { outline: 2px solid #5b8fc7; outline-offset: 1px; }
#jazz-inspector-overlay .jzov-close svg { width: 15px; height: 15px; display: block; }
#jazz-inspector-overlay .jzov-frame {
  flex: 1 1 auto; width: 100%; min-height: 0; border: 0; display: block; background: #0f141b;
}
@media (prefers-reduced-motion: reduce) {
  #jazz-inspector-overlay .jzov-toggle { transition: background-color .01s, border-color .01s; }
  #jazz-inspector-overlay .jzov-toggle:active { transform: none; }
  #jazz-inspector-overlay .jzov-dock { transition: visibility 0s; transform: translateY(100%); }
  #jazz-inspector-overlay .jzov-dock[data-open="true"] { transform: translateY(0); transition: visibility 0s; }
}`;

// The Jazz logo mark (cropped from the wordmark), in brand blue.
const JAZZ_MARK =
  '<svg viewBox="16 19 122 117" fill="none" aria-hidden="true" focusable="false">' +
  '<path fill-rule="evenodd" clip-rule="evenodd" fill="#146AFF" d="' +
  "M136.179 44.8277C136.179 44.8277 136.179 44.8277 136.179 44.8276V21.168C117.931 28.5527 97.9854 32.6192 77.0897 32.6192C65.1466 32.6192 53.5138 31.2908 42.331 28.7737V51.4076C42.331 51.4076 42.331 51.4076 42.331 51.4076V81.1508C41.2955 80.4385 40.1568 79.8458 38.9405 79.3915C36.1732 78.358 33.128 78.0876 30.1902 78.6145C27.2524 79.1414 24.5539 80.4419 22.4358 82.3516C20.3178 84.2613 18.8754 86.6944 18.291 89.3433C17.7066 91.9921 18.0066 94.7377 19.1528 97.2329C20.2991 99.728 22.2403 101.861 24.7308 103.361C27.2214 104.862 30.1495 105.662 33.1448 105.662H33.1455C33.6061 105.662 33.8365 105.662 34.0314 105.659C44.5583 105.449 53.042 96.9656 53.2513 86.4386C53.2534 86.3306 53.2544 86.2116 53.2548 86.0486H53.2552V85.7149L53.2552 85.5521V82.0762L53.2552 53.1993C61.0533 54.2324 69.0092 54.7656 77.0897 54.7656C77.6696 54.7656 78.2489 54.7629 78.8276 54.7574V110.696C77.792 109.983 76.6533 109.391 75.437 108.936C72.6697 107.903 69.6246 107.632 66.6867 108.159C63.7489 108.686 61.0504 109.987 58.9323 111.896C56.8143 113.806 55.3719 116.239 54.7875 118.888C54.2032 121.537 54.5031 124.283 55.6494 126.778C56.7956 129.273 58.7368 131.405 61.2273 132.906C63.7179 134.406 66.646 135.207 69.6414 135.207C70.1024 135.207 70.3329 135.207 70.5279 135.203C81.0548 134.994 89.5385 126.51 89.7478 115.983C89.7517 115.788 89.7517 115.558 89.7517 115.097V111.621L89.7517 54.3266C101.962 53.4768 113.837 51.4075 125.255 48.2397V80.9017C124.219 80.1894 123.081 79.5966 121.864 79.1424C119.097 78.1089 116.052 77.8384 113.114 78.3653C110.176 78.8922 107.478 80.1927 105.36 82.1025C103.242 84.0122 101.799 86.4453 101.215 89.0941C100.631 91.743 100.931 94.4886 102.077 96.9837C103.223 99.4789 105.164 101.612 107.655 103.112C110.145 104.612 113.073 105.413 116.069 105.413C116.53 105.413 116.76 105.413 116.955 105.409C127.482 105.2 135.966 96.7164 136.175 86.1895C136.179 85.9945 136.179 85.764 136.179 85.3029V81.8271L136.179 44.8277Z" +
  '"/></svg>';

const CLOSE_SVG =
  '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" ' +
  'aria-hidden="true" focusable="false"><path d="M6 6l12 12M18 6 6 18"/></svg>';

function mount(): void {
  const w = window as unknown as Record<string, unknown>;
  if (w.__jazzInspectorOverlayMounted) return;
  w.__jazzInspectorOverlayMounted = true;

  const style = document.createElement("style");
  style.id = "jazz-inspector-overlay-style";
  style.textContent = STYLE;
  document.head.appendChild(style);

  const container = document.createElement("div");
  container.id = "jazz-inspector-overlay";

  const dock = document.createElement("div");
  dock.className = "jzov-dock";
  dock.id = "jazz-inspector-overlay-dock";
  dock.setAttribute("role", "dialog");
  dock.setAttribute("aria-label", "Jazz inspector");
  dock.style.height =
    clampHeight(readHeight() ?? Math.round(window.innerHeight * DEFAULT_RATIO)) + "px";

  const bar = document.createElement("div");
  bar.className = "jzov-bar";
  bar.setAttribute("aria-label", "Drag to resize the inspector");

  const brand = document.createElement("div");
  brand.className = "jzov-brand";
  brand.innerHTML = JAZZ_MARK + '<span class="jzov-title">Inspector</span>';

  const grip = document.createElement("div");
  grip.className = "jzov-grip";

  const closeBtn = document.createElement("button");
  closeBtn.type = "button";
  closeBtn.className = "jzov-close";
  closeBtn.setAttribute("aria-label", "Close inspector");
  closeBtn.title = "Close (Esc)";
  closeBtn.innerHTML = CLOSE_SVG;

  bar.append(brand, grip, closeBtn);

  const iframe = document.createElement("iframe");
  iframe.className = "jzov-frame";
  iframe.title = "Jazz inspector";
  // The embedded Vite build emits embedded.html, not index.html.
  iframe.src = "/__jazz/embedded/embedded.html";

  dock.append(bar, iframe);

  const toggle = document.createElement("button");
  toggle.type = "button";
  toggle.className = "jzov-toggle";
  toggle.title = "Jazz inspector (Alt+Shift+J)";
  toggle.setAttribute("aria-label", "Open Jazz inspector");
  toggle.setAttribute("aria-haspopup", "dialog");
  toggle.setAttribute("aria-controls", dock.id);
  toggle.setAttribute("aria-expanded", "false");
  toggle.innerHTML = JAZZ_MARK;

  let open = readOpen();
  const apply = () => {
    dock.dataset.open = open ? "true" : "false";
    toggle.hidden = open;
    toggle.setAttribute("aria-expanded", open ? "true" : "false");
  };
  const setOpen = (next: boolean) => {
    if (open === next) return;
    open = next;
    writeOpen(open);
    apply();
    if (!open) toggle.focus();
  };

  toggle.addEventListener("click", () => setOpen(true));
  closeBtn.addEventListener("click", () => setOpen(false));
  window.addEventListener("keydown", (e) => {
    if (e.altKey && e.shiftKey && e.key.toLowerCase() === "j") {
      e.preventDefault();
      setOpen(!open);
    } else if (e.key === "Escape" && open) {
      setOpen(false);
    }
  });

  // Drag the top bar to resize the dock height (from the top edge).
  bar.addEventListener("pointerdown", (e) => {
    if (closeBtn.contains(e.target as Node)) return;
    e.preventDefault();
    bar.setPointerCapture(e.pointerId);
    const onMove = (ev: PointerEvent) => {
      dock.style.height = clampHeight(window.innerHeight - ev.clientY) + "px";
    };
    const onUp = () => {
      bar.removeEventListener("pointermove", onMove);
      bar.removeEventListener("pointerup", onUp);
      writeHeight(dock.getBoundingClientRect().height);
    };
    bar.addEventListener("pointermove", onMove);
    bar.addEventListener("pointerup", onUp);
  });

  // Keep the dock within the viewport when it shrinks.
  window.addEventListener("resize", () => {
    dock.style.height = clampHeight(dock.getBoundingClientRect().height) + "px";
  });

  apply();
  container.append(dock, toggle);
  document.body.appendChild(container);

  // Relay wired regardless of dock visibility: the iframe announces/subscribes on load.
  const relay = createRelay({
    topWindow: window,
    iframeWindow: iframe.contentWindow!,
    origin: window.location.origin,
  });
  window.addEventListener("message", (event) => relay.handle(event));
}

if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", mount);
else mount();
