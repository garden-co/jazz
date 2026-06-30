import { installInspectorHost } from "./host-bridge.js";
import type { Db } from "../../runtime/db.js";

const ELEMENT_NAME = "jazz-inspector-overlay";

const OPEN_KEY = "jazz-inspector-overlay:open";
const HEIGHT_KEY = "jazz-inspector-overlay:height";
const TOGGLE_POS_KEY = "jazz-inspector-overlay:toggle-pos";
// Set from the inspector's Settings panel (rendered in the same-origin iframe);
// see packages/inspector/src/utility/overlay-settings.ts. When true, the toggle
// button is hidden and the overlay opens only via the keyboard shortcut. Stored
// as JSON by the iframe, hence the `=== "true"` parse below.
const HIDE_TOGGLE_KEY = "jazz-inspector-overlay:hide-toggle";
// postMessage type the inspector's in-iframe Close button posts to the top
// window to dismiss the dock; see packages/inspector/src/utility/overlay-settings.ts.
const CLOSE_MESSAGE_TYPE = "jazz-inspector-overlay:close";
const MIN_HEIGHT = 200;
const DEFAULT_RATIO = 0.42;
const TOGGLE_SIZE = 44;
const EDGE_MARGIN = 8;
const DRAG_THRESHOLD = 4;

// localStorage may throw (private mode / disabled), so every access is wrapped.
// One read/write pair carries the try/catch; each accessor is a one-liner.
const readLS = <T>(key: string, parse: (raw: string) => T, fallback: T): T => {
  try {
    const raw = localStorage.getItem(key);
    return raw == null ? fallback : parse(raw);
  } catch {
    return fallback;
  }
};
const writeLS = (key: string, value: string): void => {
  try {
    localStorage.setItem(key, value);
  } catch {
    /* ignore */
  }
};

const readOpen = (): boolean => readLS(OPEN_KEY, (raw) => raw === "1", false);
const writeOpen = (open: boolean): void => writeLS(OPEN_KEY, open ? "1" : "0");

const readHideToggle = (): boolean => readLS(HIDE_TOGGLE_KEY, (raw) => raw === "true", false);

const readHeight = (): number | null =>
  readLS(
    HEIGHT_KEY,
    (raw) => {
      const n = Number(raw);
      return Number.isFinite(n) ? n : null;
    },
    null,
  );
const writeHeight = (h: number): void => writeLS(HEIGHT_KEY, String(Math.round(h)));

const maxHeight = () => Math.round(window.innerHeight * 0.92);
const clampHeight = (h: number) => Math.max(MIN_HEIGHT, Math.min(h, maxHeight()));

const readTogglePos = (): { left: number; top: number } | null =>
  readLS(
    TOGGLE_POS_KEY,
    (raw) => {
      const p = JSON.parse(raw) as { left?: unknown; top?: unknown };
      return typeof p.left === "number" && typeof p.top === "number"
        ? { left: p.left, top: p.top }
        : null;
    },
    null,
  );
const writeTogglePos = (left: number, top: number): void =>
  writeLS(TOGGLE_POS_KEY, JSON.stringify({ left: Math.round(left), top: Math.round(top) }));
const clampToggle = (left: number, top: number): { left: number; top: number } => ({
  left: Math.max(EDGE_MARGIN, Math.min(left, window.innerWidth - TOGGLE_SIZE - EDGE_MARGIN)),
  top: Math.max(EDGE_MARGIN, Math.min(top, window.innerHeight - TOGGLE_SIZE - EDGE_MARGIN)),
});

// pointerdown → capture → move-until-up, with cleanup on release. Shared by the
// draggable toggle and the resize bar. `start` may return false to ignore the
// gesture (wrong button, click landed on a child control). The pointerdown
// listener is tied to `signal` so it's removed when the overlay tears down.
interface DragHandlers {
  start?: (e: PointerEvent) => boolean | void;
  move: (e: PointerEvent) => void;
  end?: () => void;
}
function onDrag(el: HTMLElement, handlers: DragHandlers, signal: AbortSignal): void {
  el.addEventListener(
    "pointerdown",
    (e) => {
      if (handlers.start?.(e) === false) return;
      el.setPointerCapture(e.pointerId);
      const move = (ev: PointerEvent) => handlers.move(ev);
      const up = () => {
        el.removeEventListener("pointermove", move);
        el.removeEventListener("pointerup", up);
        el.removeEventListener("pointercancel", up);
        handlers.end?.();
      };
      el.addEventListener("pointermove", move);
      el.addEventListener("pointerup", up);
      el.addEventListener("pointercancel", up);
    },
    { signal },
  );
}

// Styles live in the shadow root, so selectors need no host-app namespacing —
// the shadow boundary keeps the overlay from leaking into (or being restyled
// by) the page. The chrome mirrors the inspector's dark theme; because the
// shadow tree is a separate document from the iframe, it can't import the
// inspector's tokens.css, so the same semantic token names are redeclared on
// :host below (kept in sync by hand) and used throughout the chrome.
const STYLE = `
:host {
  --jz-bg: #0f141b;        /* dock / bar / frame */
  --jz-surface: #161b24;   /* launcher button fill */
  --jz-border: #1c2430;    /* borders + subtle hover fill */
  --jz-accent: #345273;    /* hover edge */
  --jz-focus: #5b8fc7;     /* focus ring */
  --jz-muted: #9ca8b9;     /* title / close icon */
  --jz-ink: #dbe1ea;       /* close hover text */
  position: fixed; inset: 0; z-index: 2147483647; pointer-events: none;
  font-family: Inter, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
}
*, *::before, *::after { box-sizing: border-box; }
.jzov-toggle {
  position: fixed; bottom: 16px; right: 16px; pointer-events: auto;
  display: flex; align-items: center; justify-content: center;
  width: 44px; height: 44px; padding: 8px; margin: 0;
  border-radius: 8px; border: 1px solid var(--jz-border); background: var(--jz-surface); cursor: pointer;
  -webkit-tap-highlight-color: transparent; touch-action: none;
  /* Two-layer elevation: a tight contact line plus a soft ambient lift. */
  box-shadow: 0 1px 3px rgba(0,0,0,.30), 0 6px 16px rgba(0,0,0,.34);
  transition-property: transform, background-color, border-color, box-shadow;
  transition-duration: .18s;
  transition-timing-function: cubic-bezier(.22,1,.36,1);
}
.jzov-toggle:hover {
  background: var(--jz-border); border-color: var(--jz-accent);
  /* Lift on hover: taller ambient, plus a faint brand-blue glow tying the mark to the chrome. */
  box-shadow: 0 1px 3px rgba(0,0,0,.30), 0 10px 26px rgba(0,0,0,.40), 0 0 18px rgba(20,106,255,.14);
}
.jzov-toggle:hover svg { filter: brightness(1.12); }
.jzov-toggle:active {
  /* Gentler press than a hard shrink; settle the elevation as it's depressed. */
  transform: scale(.95);
  box-shadow: 0 1px 2px rgba(0,0,0,.30), 0 3px 9px rgba(0,0,0,.34);
}
.jzov-toggle:focus-visible { outline: 2px solid var(--jz-focus); outline-offset: 2px; }
.jzov-toggle[hidden] { display: none; }
.jzov-toggle svg {
  width: 100%; height: 100%; display: block;
  /* The mark's ink centroid sits up-and-right of its viewBox center; nudge it onto the optical center. */
  transform: translate(-1.4px, 1.1px);
  transition: filter .18s cubic-bezier(.22,1,.36,1);
}
.jzov-dock {
  position: fixed; left: 0; right: 0; bottom: 0; width: 100%; pointer-events: auto;
  display: flex; flex-direction: column;
  background: var(--jz-bg); border-top: 1px solid var(--jz-border);
  box-shadow: 0 -14px 44px rgba(0,0,0,.5);
  transform: translateY(100%); visibility: hidden;
  transition: transform .24s cubic-bezier(.22,1,.36,1), visibility 0s linear .24s;
}
.jzov-dock[data-open="true"] {
  transform: translateY(0); visibility: visible;
  transition: transform .24s cubic-bezier(.22,1,.36,1), visibility 0s;
}
/* Slim drag-to-resize strip in place of the old title bar. A centred grip line
   fades in on hover to signal it's draggable. */
.jzov-resize {
  flex: 0 0 7px; height: 7px; position: relative;
  background: var(--jz-bg); border-bottom: 1px solid var(--jz-border);
  cursor: ns-resize; user-select: none; touch-action: none;
}
.jzov-resize::after {
  content: ""; position: absolute; left: 50%; top: 50%;
  width: 36px; height: 2px; border-radius: 1px; transform: translate(-50%, -50%);
  background: var(--jz-muted); opacity: 0; transition: opacity .15s ease;
}
.jzov-resize:hover::after { opacity: .5; }
.jzov-frame {
  flex: 1 1 auto; width: 100%; min-height: 0; border: 0; display: block; background: var(--jz-bg);
}
@media (prefers-reduced-motion: reduce) {
  .jzov-toggle { transition: background-color .01s, border-color .01s; }
  .jzov-toggle:active { transform: none; }
  .jzov-toggle svg { transition: none; }
  .jzov-dock { transition: visibility 0s; transform: translateY(100%); }
  .jzov-dock[data-open="true"] { transform: translateY(0); transition: visibility 0s; }
}`;

// The Jazz logo mark (cropped from the wordmark), in brand blue.
const JAZZ_MARK =
  '<svg viewBox="16 19 122 117" fill="none" aria-hidden="true" focusable="false">' +
  '<path fill-rule="evenodd" clip-rule="evenodd" fill="#146AFF" d="' +
  "M136.179 44.8277C136.179 44.8277 136.179 44.8277 136.179 44.8276V21.168C117.931 28.5527 97.9854 32.6192 77.0897 32.6192C65.1466 32.6192 53.5138 31.2908 42.331 28.7737V51.4076C42.331 51.4076 42.331 51.4076 42.331 51.4076V81.1508C41.2955 80.4385 40.1568 79.8458 38.9405 79.3915C36.1732 78.358 33.128 78.0876 30.1902 78.6145C27.2524 79.1414 24.5539 80.4419 22.4358 82.3516C20.3178 84.2613 18.8754 86.6944 18.291 89.3433C17.7066 91.9921 18.0066 94.7377 19.1528 97.2329C20.2991 99.728 22.2403 101.861 24.7308 103.361C27.2214 104.862 30.1495 105.662 33.1448 105.662H33.1455C33.6061 105.662 33.8365 105.662 34.0314 105.659C44.5583 105.449 53.042 96.9656 53.2513 86.4386C53.2534 86.3306 53.2544 86.2116 53.2548 86.0486H53.2552V85.7149L53.2552 85.5521V82.0762L53.2552 53.1993C61.0533 54.2324 69.0092 54.7656 77.0897 54.7656C77.6696 54.7656 78.2489 54.7629 78.8276 54.7574V110.696C77.792 109.983 76.6533 109.391 75.437 108.936C72.6697 107.903 69.6246 107.632 66.6867 108.159C63.7489 108.686 61.0504 109.987 58.9323 111.896C56.8143 113.806 55.3719 116.239 54.7875 118.888C54.2032 121.537 54.5031 124.283 55.6494 126.778C56.7956 129.273 58.7368 131.405 61.2273 132.906C63.7179 134.406 66.646 135.207 69.6414 135.207C70.1024 135.207 70.3329 135.207 70.5279 135.203C81.0548 134.994 89.5385 126.51 89.7478 115.983C89.7517 115.788 89.7517 115.558 89.7517 115.097V111.621L89.7517 54.3266C101.962 53.4768 113.837 51.4075 125.255 48.2397V80.9017C124.219 80.1894 123.081 79.5966 121.864 79.1424C119.097 78.1089 116.052 77.8384 113.114 78.3653C110.176 78.8922 107.478 80.1927 105.36 82.1025C103.242 84.0122 101.799 86.4453 101.215 89.0941C100.631 91.743 100.931 94.4886 102.077 96.9837C103.223 99.4789 105.164 101.612 107.655 103.112C110.145 104.612 113.073 105.413 116.069 105.413C116.53 105.413 116.76 105.413 116.955 105.409C127.482 105.2 135.966 96.7164 136.175 86.1895C136.179 85.9945 136.179 85.764 136.179 85.3029V81.8271L136.179 44.8277Z" +
  '"/></svg>';

// Static structure built once into the shadow root, then wired up by ref. The
// embedded Vite build emits embedded.html, not index.html. There's no title bar:
// the slim top strip is just a resize grip, and Close lives in the inspector's
// own top bar (inside the iframe), which posts a close message up to here.
const TEMPLATE = `
<div class="jzov-dock" id="jzov-dock" role="dialog" aria-label="Jazz inspector">
  <div class="jzov-resize" aria-hidden="true" title="Drag to resize"></div>
  <iframe class="jzov-frame" title="Jazz inspector" src="/__jazz/embedded/embedded.html"></iframe>
</div>
<button type="button" class="jzov-toggle"
  title="Jazz inspector — click to open, drag to move (Alt+Shift+J)"
  aria-label="Open Jazz inspector" aria-haspopup="dialog" aria-controls="jzov-dock" aria-expanded="false"
>${JAZZ_MARK}</button>`;

// Constructed once and shared by adoptedStyleSheets, so the (large) CSS is
// parsed a single time rather than re-injected as a <style> per mount.
let sheet: CSSStyleSheet | undefined;
function overlayStyleSheet(): CSSStyleSheet {
  if (!sheet) {
    sheet = new CSSStyleSheet();
    sheet.replaceSync(STYLE);
  }
  return sheet;
}

// The host app's Db, set by startInspectorOverlay() before the element mounts,
// so connectedCallback can publish the host handle for the iframe.
let hostDb: Db | undefined;

// The overlay chrome: a floating toggle + a bottom dock hosting the inspector
// iframe, isolated from the host page by its shadow root. All listeners are
// registered against #ac.signal so disconnectedCallback() removes them at once.
class JazzInspectorOverlay extends HTMLElement {
  #ac: AbortController | undefined;

  connectedCallback(): void {
    // Build the shadow tree once (keeps the iframe alive across moves), but
    // re-wire every (re)connect with a fresh AbortController so the element
    // isn't left dead if it's ever removed and re-added to the DOM.
    let root = this.shadowRoot;
    if (!root) {
      root = this.attachShadow({ mode: "open" });
      root.adoptedStyleSheets = [overlayStyleSheet()];
      root.innerHTML = TEMPLATE;
    }
    this.#ac?.abort();
    this.#ac = new AbortController();
    const { signal } = this.#ac;

    const dock = root.querySelector<HTMLDivElement>(".jzov-dock")!;
    const resize = root.querySelector<HTMLDivElement>(".jzov-resize")!;
    const iframe = root.querySelector<HTMLIFrameElement>(".jzov-frame")!;
    const toggle = root.querySelector<HTMLButtonElement>(".jzov-toggle")!;

    dock.style.height =
      clampHeight(readHeight() ?? Math.round(window.innerHeight * DEFAULT_RATIO)) + "px";

    // The toggle is draggable; restore a saved position (default CSS bottom-right).
    let togglePos = readTogglePos();
    const applyTogglePos = (pos: { left: number; top: number } | null): void => {
      if (!pos) return;
      const clamped = clampToggle(pos.left, pos.top);
      toggle.style.left = clamped.left + "px";
      toggle.style.top = clamped.top + "px";
      toggle.style.right = "auto";
      toggle.style.bottom = "auto";
      togglePos = clamped;
    };
    applyTogglePos(togglePos);

    let open = readOpen();
    // When the user opts into keyboard-only mode, the toggle stays hidden even
    // while the dock is closed; the shortcut is then the only way to open it.
    let hideToggle = readHideToggle();
    const apply = (): void => {
      dock.dataset.open = open ? "true" : "false";
      toggle.hidden = open || hideToggle;
      toggle.setAttribute("aria-expanded", open ? "true" : "false");
    };
    const setOpen = (next: boolean): void => {
      if (open === next) return;
      open = next;
      writeOpen(open);
      apply();
      // Return focus to the toggle on close, but only if it's actually visible.
      if (!open && !toggle.hidden) toggle.focus();
    };

    // Drag to reposition; a click that didn't drag opens the inspector.
    let dragMoved = false;
    let offsetX = 0;
    let offsetY = 0;
    let startX = 0;
    let startY = 0;
    onDrag(
      toggle,
      {
        start: (e) => {
          if (e.pointerType === "mouse" && e.button !== 0) return false;
          dragMoved = false;
          const rect = toggle.getBoundingClientRect();
          offsetX = e.clientX - rect.left;
          offsetY = e.clientY - rect.top;
          startX = e.clientX;
          startY = e.clientY;
        },
        move: (ev) => {
          if (!dragMoved && Math.hypot(ev.clientX - startX, ev.clientY - startY) < DRAG_THRESHOLD)
            return;
          dragMoved = true;
          applyTogglePos({ left: ev.clientX - offsetX, top: ev.clientY - offsetY });
        },
        end: () => {
          if (dragMoved && togglePos) writeTogglePos(togglePos.left, togglePos.top);
        },
      },
      signal,
    );
    toggle.addEventListener(
      "click",
      (e) => {
        if (dragMoved) {
          // Suppress the click the browser fires after a drag-release.
          e.preventDefault();
          e.stopPropagation();
          dragMoved = false;
          return;
        }
        setOpen(true);
      },
      { signal },
    );
    window.addEventListener(
      "keydown",
      (e) => {
        // Match the physical key via `code`, not `key`: on macOS, holding Option
        // (Alt) composes a special character, so `e.key` for Alt+Shift+J is not
        // "j". `e.code === "KeyJ"` is layout- and modifier-independent.
        if (e.altKey && e.shiftKey && e.code === "KeyJ") {
          e.preventDefault();
          setOpen(!open);
        } else if (e.key === "Escape" && open) {
          setOpen(false);
        }
      },
      { signal },
    );

    // The Settings panel (same-origin iframe) writes HIDE_TOGGLE_KEY; the
    // storage event fires here in the top window. A null key means the whole
    // store was cleared, so re-read in that case too.
    window.addEventListener(
      "storage",
      (e) => {
        if (e.key !== null && e.key !== HIDE_TOGGLE_KEY) return;
        const next = readHideToggle();
        if (next === hideToggle) return;
        hideToggle = next;
        apply();
      },
      { signal },
    );

    // Drag the slim top strip to resize the dock height (from the top edge).
    onDrag(
      resize,
      {
        start: (e) => {
          e.preventDefault();
        },
        move: (ev) => {
          dock.style.height = clampHeight(window.innerHeight - ev.clientY) + "px";
        },
        end: () => {
          writeHeight(dock.getBoundingClientRect().height);
        },
      },
      signal,
    );

    // Keep the dock within the viewport when it shrinks.
    window.addEventListener(
      "resize",
      () => {
        dock.style.height = clampHeight(dock.getBoundingClientRect().height) + "px";
        if (togglePos) applyTogglePos(togglePos);
      },
      { signal },
    );

    apply();

    // Publish the host handle + push the active-subscription list to the iframe.
    // The overlay reads the config off window.__jazzInspectorHost and opens its
    // own worker connection; we only push the stack-less subscription list.
    if (hostDb) {
      const disposeHost = installInspectorHost(
        hostDb,
        iframe.contentWindow!,
        window.location.origin,
      );
      signal.addEventListener("abort", () => disposeHost(), { once: true });
    }

    // The in-iframe Close button posts up here (same-origin).
    window.addEventListener(
      "message",
      (event) => {
        if (
          event.origin === window.location.origin &&
          (event.data as { type?: unknown } | null)?.type === CLOSE_MESSAGE_TYPE
        ) {
          setOpen(false);
        }
      },
      { signal },
    );
  }

  disconnectedCallback(): void {
    this.#ac?.abort();
  }
}

function mount(): void {
  if (!customElements.get(ELEMENT_NAME)) {
    customElements.define(ELEMENT_NAME, JazzInspectorOverlay);
  }
  if (!document.querySelector(ELEMENT_NAME)) {
    document.body.appendChild(document.createElement(ELEMENT_NAME));
  }
}

/**
 * Start the inspector for an app db: record the db and mount the overlay UI
 * (floating toggle + bottom dock + iframe). The element's connectedCallback then
 * publishes the host handle (window.__jazzInspectorHost) and pushes the active
 * subscription list to the iframe; the overlay opens its own worker connection
 * from the published config. Idempotent. No-op at module load — providers call
 * this from a dev-only dynamic import, so it's absent from prod builds.
 */
export function startInspectorOverlay(db: object): void {
  hostDb = db as Db;
  mount();
}
