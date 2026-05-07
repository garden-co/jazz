/**
 * MapController — hand-rolled canvas-2D dot globe.
 *
 * Resolution-independent renderer:
 *   - A Fibonacci sphere of ~50,000 points, masked against an embedded
 *     world map for land/sea, projected to 2D each frame and drawn as
 *     CSS-pixel-sized dots that stay crisp at any zoom.
 *   - Per-leg route arcs traced as great-circle slerp polylines lifted
 *     radially with a sine bump, so the arc stays above the sphere
 *     everywhere — short hops stay flat, long hops arch more.
 *   - A DOM overlay layer with one absolutely-positioned wrapper per stop,
 *     transformed each frame to the projected screen position. Each wrapper
 *     holds a clickable pin button (firing `stopClick`) and a text label.
 *
 * We don't use CSS anchor positioning — the timing race between Vue
 * rendering anchored elements and us creating the anchors leaves anchor()
 * unresolved in practice. Imperative DOM transforms are universal and
 * predictable.
 *
 * The embedded land-mask raster is MIT-licensed (originally shipped with
 * the cobe library), inlined as a base64 data URL so we don't add a
 * network fetch.
 */

import { computeRouteLine } from "./route-line";
import { unprojectGlobe } from "./sphere-unproject";

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

export type StopMapData = {
  id: string;
  name: string;
  lng: number;
  lat: number;
};

type MapClickEvent = { lng: number; lat: number; x: number; y: number };
type StopClickEvent = { stopId: string };
type MapClickListener = (e: MapClickEvent) => void;
type StopClickListener = (e: StopClickEvent) => void;

type MapControllerOptions = {
  container: string | HTMLElement;
};

type FlyToOptions = {
  duration?: number;
  /** Globe scale at the destination. Defaults to the current scale. */
  scale?: number;
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

// Embedded land mask (256x128 binary PNG, MIT — from `cobe`).
const LAND_MASK_DATA_URL =
  "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAQAAAACAAQAAAADMzoqnAAAECklEQVR42u3VsW4jRRzH8d94gzfF4Q0VQaC4vBLTRTp0mze4ggfAPAE5XQEFsGNAVIjwBrmW7h7gJE+giKjyABTZE4g06LKJETdRJvtD65kdz6yduKABiW+TVfzRf2bXYxtcE/59YJCz6YdbgQF6ACSRrwYKYImmh5PbwOewlV3wlQNbAN6SEExjUOO+BU0aCSnxReHABUlK4YFQeJeUT3da8IIkZ6NGoSnFY5KsMoVzMKfECUnqxgPYRArarmUCndHwzIEaQEpg5xVdBXROl8mpAQx5dUgPiHoYAAkg5w3JABR06byGAVgcRGAz5bznj6phBQNRFwyqgdxebH6gshJAesWoFhgYpApAFoG8BIZ/fEhSox5jDjQXmV0Ar5XJfAIrALi3URVs09gHIL4XJCkLC5LH9JWiArABFCSrQjdgkBzRJ0WJeUOSNyQAfJJwUSWUBRlJQ8oGHATACGlBynnzy2kEYLNjrxouigD8BZcgOeVPqh12RtufaCN5wCPVDpvQ9lsIrqndsJtDcWqBCpf4hWN7OdWHBw58FwIaNOU/n1TpMW2DFaD48cmr4185T8NHkpUFX749pQPVdgRKC/DGoQPVeAEKv+WHvY8OOWNTPRp5kHuwSf8wzXtVBKR7YwEH9H3lQUaypUfSATOALyVNu5vZJW31Bnx98nkLfDUWJaz6ixvm+RIQRdl3kmRxxiaDoGnZW4CpPfkaQadlcPim1xOSvETQo7Lv75enVAXJ3xGUlony4KQBBWUM1NiDc6qhyS8RgQs18OCMMtPDaAUIyg0PZkRWDqs+wnKJBTDI1Js6BolegOsKmUxNDBAAKqQyMQmidhegBlLZ+wwKYdv5M/8x1khkb1cgKqP2H+MKyV5vS+whrE8DQDgAlUAoRBX056EElJCjJVACeJBZgNfVp+iCCm4RBWCgKsRxASSA9KgDhDtCiTuMyfHsKXzhC6wNAIjjWb8LKAOA2ctk3FmCOlgKFy8f1N0JJtgsxinYnVAHt4t3gPzZXSCTyCWCQmBT91QE3B5yarSN40dNHYPka4TlDhTUI8zLvl0JSL3vZn6DsCFZOeB2yROEpR68sECQQA++xIGCR2X7DwlEoLRgUrZrqlUg50S1uy43YqDcN6UFBVkhAjWiCV2Q0jgQPdplMKxvBXodcOfAwJYvgdL+1etA1YJJfBcZlQV7sO1i2gHoNiyxtQ5sBsCgWyoxCHiFFd2L5nUTCqMAqGUgsQ9f5kCcCiZgRYkMgMTd5WsB1rTzj0Em14BE4r+QxN1lCEsVur2PoF5Wbg8RJXR4djgvBgauhLywoEZQrt1KKRdVS4CdlJ8qafyP+9KIj/nE/d7kKwH9jgS72e9DV+kvfTWgct4ZyP8Byb8BPG7MaaIIkAQAAAAASUVORK5CYII=";

const SPHERE_SAMPLES = 50000;
const MAP_DOT_RADIUS_PX = 1.0;
const STOP_MARKER_RADIUS_PX = 5.5;
const PATH_LINE_WIDTH_PX = 2;
const PATH_SAMPLES_PER_LEG = 32;

const DEFAULT_THETA = 0.2;
const ROTATION_SPEED = 0.003;
const DRAG_SENSITIVITY = 0.005;
const DRAG_THRESHOLD_PX = 5;
const THETA_LIMIT = Math.PI / 2 - 0.05;
const FLY_DURATION_DEFAULT = 1500;
const ZOOM_MIN = 0.6;
const ZOOM_MAX = 3.5;
const ZOOM_WHEEL_FACTOR = 0.0015;

const GLOBE_FIT = 0.75;
const ARC_APEX = 0.15;

const COLOR_OCEAN_INNER = "rgba(15, 19, 32, 1)";
const COLOR_OCEAN_OUTER = "rgba(8, 9, 14, 1)";
const COLOR_LIMB_GLOW = "rgba(255, 45, 123, 0.18)";
const COLOR_STOP = "#ff2d7b";
const COLOR_PATH = "#00e5cc";

type Vec3 = [number, number, number];

type TweenState = {
  fromVec: Vec3;
  toVec: Vec3;
  angle: number;
  fromScale: number;
  toScale: number;
  midScale?: number;
  startTime: number;
  duration: number;
  resolve: () => void;
};

type PointerStart = {
  x: number;
  y: number;
  phi: number;
  theta: number;
};

type RouteLeg = { points: Vec3[] };

type StopElement = {
  wrapper: HTMLDivElement;
  button: HTMLButtonElement;
  label: HTMLSpanElement;
};

// ---------------------------------------------------------------------------
// MapController
// ---------------------------------------------------------------------------

export class MapController {
  private container: HTMLElement;
  private canvas: HTMLCanvasElement;
  private ctx: CanvasRenderingContext2D;
  private overlay: HTMLDivElement;
  private mapClickListeners: MapClickListener[] = [];
  private stopClickListeners: StopClickListener[] = [];

  private phi = 0;
  private theta = DEFAULT_THETA;
  private scale = 1;

  private spherePoints: Vec3[];
  private landFlags: Uint8Array;
  private stops: StopMapData[] = [];
  private stopVecs: Vec3[] = [];
  private routeLegs: RouteLeg[] = [];
  /** Reused per-frame, bucketed by depth tier to limit fillStyle churn. */
  private dotBuckets: Array<Array<number>> = Array.from({ length: 8 }, () => []);

  private stopElements: Map<string, StopElement> = new Map();

  private autoRotate = false;
  private tweenState: TweenState | null = null;
  private tourAbort: AbortController | null = null;

  private pointerStart: PointerStart | null = null;
  private dragging = false;

  private rafId: number | null = null;
  private resizeObserver: ResizeObserver;
  private destroyed = false;
  private readyPromise: Promise<void>;
  private bufferZoomFactor = 1;

  constructor(options: MapControllerOptions) {
    const container =
      typeof options.container === "string"
        ? document.getElementById(options.container)
        : options.container;
    if (!container) throw new Error(`MapController: container not found`);
    this.container = container;
    if (getComputedStyle(this.container).position === "static") {
      this.container.style.position = "relative";
    }

    this.canvas = document.createElement("canvas");
    this.canvas.style.cssText =
      "position: absolute; inset: 0; width: 100%; height: 100%; cursor: grab; touch-action: none;";
    this.container.appendChild(this.canvas);

    const ctx = this.canvas.getContext("2d");
    if (!ctx) throw new Error("MapController: 2D canvas context unavailable");
    this.ctx = ctx;

    this.applyCanvasSize();

    this.overlay = document.createElement("div");
    this.overlay.style.cssText = "position: absolute; inset: 0; pointer-events: none; z-index: 5;";
    this.container.appendChild(this.overlay);

    this.spherePoints = fibonacciSphere(SPHERE_SAMPLES);
    this.landFlags = new Uint8Array(SPHERE_SAMPLES).fill(1);
    this.readyPromise = this.loadLandMask().catch(() => {
      /* keep going with all-land fallback */
    });

    this.tick();

    this.canvas.addEventListener("pointerdown", this.handlePointerDown);
    this.canvas.addEventListener("pointermove", this.handlePointerMove);
    this.canvas.addEventListener("pointerup", this.handlePointerUp);
    this.canvas.addEventListener("pointercancel", this.handlePointerUp);
    this.canvas.addEventListener("wheel", this.handleWheel, { passive: false });

    this.resizeObserver = new ResizeObserver(() => this.applyCanvasSize());
    this.resizeObserver.observe(this.container);
  }

  // -----------------------------------------------------------------------
  // Events
  // -----------------------------------------------------------------------

  on(event: "mapClick", fn: MapClickListener): void;
  on(event: "stopClick", fn: StopClickListener): void;
  on(event: "mapClick" | "stopClick", fn: MapClickListener | StopClickListener): void {
    if (event === "mapClick") this.mapClickListeners.push(fn as MapClickListener);
    else if (event === "stopClick") this.stopClickListeners.push(fn as StopClickListener);
  }

  private emitMapClick(e: MapClickEvent): void {
    for (const fn of this.mapClickListeners) fn(e);
  }

  private emitStopClick(e: StopClickEvent): void {
    for (const fn of this.stopClickListeners) fn(e);
  }

  // -----------------------------------------------------------------------
  // Public API
  // -----------------------------------------------------------------------

  whenReady(): Promise<void> {
    return this.readyPromise;
  }

  setStops(stops: StopMapData[], dates?: Date[]): void {
    this.stops = stops;
    this.stopVecs = stops.map((s) => latLngToVec3(s.lat, s.lng));
    this.syncStopElements(stops);

    const stopsWithDates = stops.map((s, i) => ({
      ...s,
      date: dates?.[i] ?? new Date(0),
    }));
    const route = computeRouteLine(
      stopsWithDates.map((s) => ({ date: s.date, lng: s.lng, lat: s.lat })),
    );

    this.routeLegs = [];
    if (route) {
      for (let i = 1; i < route.coordinates.length; i++) {
        const [lngA, latA] = route.coordinates[i - 1];
        const [lngB, latB] = route.coordinates[i];
        this.routeLegs.push(buildLeg(latLngToVec3(latA, lngA), latLngToVec3(latB, lngB)));
      }
    }
  }

  flyTo(target: { lng: number; lat: number }, opts: FlyToOptions = {}): Promise<void> {
    const { phi, theta } = latLngToPhiTheta(target.lat, target.lng);
    return this.tweenCamera(
      phi,
      theta,
      opts.scale ?? this.scale,
      opts.duration ?? FLY_DURATION_DEFAULT,
    );
  }

  async tour(stops: StopMapData[]): Promise<void> {
    if (stops.length === 0) return;

    this.stopRotation();
    this.tourAbort?.abort();
    this.tourAbort = new AbortController();
    const signal = this.tourAbort.signal;

    const CLOSE = 1.6;
    const FAR = 1.1;

    await this.flyTo(stops[0], { duration: 2000, scale: CLOSE });
    if (!signal.aborted) await sleep(1800, signal);

    for (let i = 1; i < stops.length; i++) {
      if (signal.aborted) break;
      const target = latLngToPhiTheta(stops[i].lat, stops[i].lng);
      await this.tweenCamera(target.phi, target.theta, CLOSE, 2600, FAR);
      if (signal.aborted) break;
      await sleep(1800, signal);
    }

    this.tourAbort = null;
  }

  stopTour(): void {
    if (!this.tourAbort) return;
    this.tourAbort.abort();
    this.tourAbort = null;
    this.cancelActiveTween();
    if (this.scale !== 1) {
      this.tweenCamera(this.phi, this.theta, 1, 600);
    }
  }

  startRotation(): void {
    this.autoRotate = true;
  }

  stopRotation(): void {
    this.autoRotate = false;
  }

  destroy(): void {
    this.destroyed = true;
    this.stopRotation();
    if (this.tourAbort) {
      this.tourAbort.abort();
      this.tourAbort = null;
    }
    this.cancelActiveTween();
    if (this.rafId !== null) cancelAnimationFrame(this.rafId);
    this.resizeObserver.disconnect();
    this.canvas.removeEventListener("pointerdown", this.handlePointerDown);
    this.canvas.removeEventListener("pointermove", this.handlePointerMove);
    this.canvas.removeEventListener("pointerup", this.handlePointerUp);
    this.canvas.removeEventListener("pointercancel", this.handlePointerUp);
    this.canvas.removeEventListener("wheel", this.handleWheel);
    for (const el of this.stopElements.values()) el.wrapper.remove();
    this.stopElements.clear();
    this.overlay.remove();
    this.canvas.remove();
    this.mapClickListeners = [];
    this.stopClickListeners = [];
  }

  // -----------------------------------------------------------------------
  // Internal: render loop
  // -----------------------------------------------------------------------

  private tick = (): void => {
    if (this.destroyed) return;

    if (this.tweenState) {
      const elapsed = performance.now() - this.tweenState.startTime;
      const t = Math.min(1, elapsed / this.tweenState.duration);
      const eased = easeInOutCubic(t);
      const { fromVec, toVec, angle, fromScale, toScale, midScale } = this.tweenState;
      const [vx, vy, vz] = slerpVec3(fromVec, toVec, eased, angle);
      this.theta = Math.asin(clamp(vy, -1, 1));
      const rawPhi = Math.atan2(-vx, -vz);
      this.phi = this.phi + shortestAngleDelta(this.phi, rawPhi);
      const lerp = fromScale + (toScale - fromScale) * eased;
      if (midScale === undefined) {
        this.scale = lerp;
      } else {
        const dip = (midScale - 0.5 * (fromScale + toScale)) * 4 * eased * (1 - eased);
        this.scale = lerp + dip;
      }
      if (t >= 1) {
        const done = this.tweenState;
        this.tweenState = null;
        done.resolve();
      }
    } else if (this.autoRotate && !this.dragging) {
      this.phi += ROTATION_SPEED;
    }

    const desiredZoom = Math.max(1, this.scale);
    if (Math.abs(desiredZoom - this.bufferZoomFactor) > 0.1) {
      this.applyCanvasSize();
    }

    this.render();

    this.rafId = requestAnimationFrame(this.tick);
  };

  private render(): void {
    const ctx = this.ctx;
    const cssW = this.container.clientWidth;
    const cssH = this.container.clientHeight;
    if (cssW === 0 || cssH === 0) return;

    ctx.clearRect(0, 0, cssW, cssH);

    const cf = Math.cos(this.phi);
    const sf = Math.sin(this.phi);
    const ct = Math.cos(this.theta);
    const st = Math.sin(this.theta);

    const radius = (Math.min(cssW, cssH) / 2) * this.scale * GLOBE_FIT;
    const cx = cssW / 2;
    const cy = cssH / 2;

    // 1. Atmospheric limb glow (soft halo just outside the disc).
    const glow = ctx.createRadialGradient(cx, cy, radius * 0.94, cx, cy, radius * 1.18);
    glow.addColorStop(0, "rgba(255, 45, 123, 0)");
    glow.addColorStop(0.4, COLOR_LIMB_GLOW);
    glow.addColorStop(1, "rgba(255, 45, 123, 0)");
    ctx.fillStyle = glow;
    ctx.fillRect(0, 0, cssW, cssH);

    // 2. Earth disc — dark with a slight centre-bright shading so the sphere
    // reads as a body, not a transparent dot field in space.
    const ocean = ctx.createRadialGradient(cx, cy, 0, cx, cy, radius);
    ocean.addColorStop(0, COLOR_OCEAN_INNER);
    ocean.addColorStop(1, COLOR_OCEAN_OUTER);
    ctx.fillStyle = ocean;
    ctx.beginPath();
    ctx.arc(cx, cy, radius, 0, 2 * Math.PI);
    ctx.fill();

    // 3. Land dots, alpha modulated by camera-facing depth so the limb is
    // dimmer than the centre — fakes diffuse lighting without a shader.
    // Bucketed by depth tier so we batch fillStyle changes.
    const r = MAP_DOT_RADIUS_PX;
    const d = r * 2;
    const buckets = this.dotBuckets;
    const bins = buckets.length;
    for (let b = 0; b < bins; b++) buckets[b].length = 0;
    for (let i = 0; i < this.spherePoints.length; i++) {
      if (!this.landFlags[i]) continue;
      const [px, py, pz] = this.spherePoints[i];
      const x1 = cf * px + sf * pz;
      const y1 = py;
      const z1 = -sf * px + cf * pz;
      const y2 = ct * y1 - st * z1;
      const z2 = st * y1 + ct * z1;
      if (z2 < 0) continue;
      const sx = cx + x1 * radius;
      const sy = cy - y2 * radius;
      const bin = Math.min(bins - 1, Math.floor(z2 * bins));
      const arr = buckets[bin];
      arr.push(sx, sy);
    }
    for (let b = 0; b < bins; b++) {
      const arr = buckets[b];
      if (arr.length === 0) continue;
      const alpha = 0.18 + 0.7 * ((b + 0.5) / bins);
      ctx.fillStyle = `rgba(245, 242, 237, ${alpha.toFixed(3)})`;
      for (let i = 0; i < arr.length; i += 2) {
        ctx.fillRect(arr[i] - r, arr[i + 1] - r, d, d);
      }
    }

    // 2. Route arcs
    if (this.routeLegs.length > 0) {
      ctx.strokeStyle = COLOR_PATH;
      ctx.lineWidth = PATH_LINE_WIDTH_PX;
      ctx.lineJoin = "round";
      ctx.lineCap = "round";
      for (const leg of this.routeLegs) {
        ctx.beginPath();
        let started = false;
        for (let s = 0; s < leg.points.length; s++) {
          const [px, py, pz] = leg.points[s];

          const x1 = cf * px + sf * pz;
          const y1 = py;
          const z1 = -sf * px + cf * pz;
          const y2 = ct * y1 - st * z1;
          const z2 = st * y1 + ct * z1;
          const distSq = x1 * x1 + y2 * y2;
          // Arc points are visible if on the front of the globe OR outside
          // the visible disc — this lets tall arcs that loop above the horizon
          // render correctly.
          if (z2 < 0 && distSq < 1) {
            started = false;
            continue;
          }
          const sx = cx + x1 * radius;
          const sy = cy - y2 * radius;
          if (!started) {
            ctx.moveTo(sx, sy);
            started = true;
          } else {
            ctx.lineTo(sx, sy);
          }
        }
        ctx.stroke();
      }
    }

    // 3. Stop markers + DOM overlay positions
    for (let i = 0; i < this.stops.length; i++) {
      const stop = this.stops[i];
      const [px, py, pz] = this.stopVecs[i];
      const x1 = cf * px + sf * pz;
      const y1 = py;
      const z1 = -sf * px + cf * pz;
      const y2 = ct * y1 - st * z1;
      const z2 = st * y1 + ct * z1;
      const visible = z2 >= 0;

      const sx = cx + x1 * radius;
      const sy = cy - y2 * radius;

      if (visible) {
        ctx.fillStyle = COLOR_STOP;
        ctx.beginPath();
        ctx.arc(sx, sy, STOP_MARKER_RADIUS_PX, 0, 2 * Math.PI);
        ctx.fill();
      }

      const el = this.stopElements.get(stop.id);
      if (!el) continue;
      if (visible) {
        el.wrapper.style.transform = `translate3d(${sx}px, ${sy}px, 0)`;
        el.wrapper.style.opacity = "1";
        el.wrapper.style.pointerEvents = "auto";
      } else {
        el.wrapper.style.opacity = "0";
        el.wrapper.style.pointerEvents = "none";
      }
    }
  }

  // -----------------------------------------------------------------------
  // Internal: anchor div lifecycle
  // -----------------------------------------------------------------------

  private syncStopElements(stops: StopMapData[]): void {
    const present = new Set(stops.map((s) => s.id));
    for (const [id, el] of this.stopElements) {
      if (!present.has(id)) {
        el.wrapper.remove();
        this.stopElements.delete(id);
      }
    }
    for (const stop of stops) {
      const existing = this.stopElements.get(stop.id);
      if (existing) {
        if (existing.label.textContent !== stop.name) existing.label.textContent = stop.name;
        existing.button.setAttribute("aria-label", stop.name);
        continue;
      }

      const wrapper = document.createElement("div");
      wrapper.style.cssText =
        "position: absolute; left: 0; top: 0; transform: translate3d(-9999px, -9999px, 0); will-change: transform; opacity: 0;";

      const button = document.createElement("button");
      button.className = "stop-pin";
      button.type = "button";
      button.setAttribute("aria-label", stop.name);
      const stopId = stop.id;
      button.addEventListener("click", () => this.emitStopClick({ stopId }));

      const label = document.createElement("span");
      label.className = "stop-label";
      label.textContent = stop.name;

      wrapper.appendChild(button);
      wrapper.appendChild(label);
      this.overlay.appendChild(wrapper);
      this.stopElements.set(stop.id, { wrapper, button, label });
    }
  }

  // -----------------------------------------------------------------------
  // Internal: land mask
  // -----------------------------------------------------------------------

  private async loadLandMask(): Promise<void> {
    const img = await loadImage(LAND_MASK_DATA_URL);
    const cnv = document.createElement("canvas");
    cnv.width = img.width;
    cnv.height = img.height;
    const c = cnv.getContext("2d", { willReadFrequently: true });
    if (!c) return;
    c.drawImage(img, 0, 0);
    const data = c.getImageData(0, 0, img.width, img.height).data;

    for (let i = 0; i < this.spherePoints.length; i++) {
      const [x, y, z] = this.spherePoints[i];
      const lat = Math.asin(Math.max(-1, Math.min(1, y)));
      const lng = Math.atan2(-z, x);
      const u = lng / (2 * Math.PI) + 0.5;
      const v = 0.5 - lat / Math.PI;
      const ix = Math.min(img.width - 1, Math.max(0, Math.floor(u * img.width)));
      const iy = Math.min(img.height - 1, Math.max(0, Math.floor(v * img.height)));
      const idx = (iy * img.width + ix) * 4;
      this.landFlags[i] = data[idx] > 128 ? 1 : 0;
    }
  }

  // -----------------------------------------------------------------------
  // Internal: tween
  // -----------------------------------------------------------------------

  private tweenCamera(
    toPhi: number,
    toTheta: number,
    toScale: number,
    duration: number,
    midScale?: number,
  ): Promise<void> {
    this.cancelActiveTween();

    const fromVec = phiThetaToVec(this.phi, this.theta);
    const toVec = phiThetaToVec(toPhi, clamp(toTheta, -THETA_LIMIT, THETA_LIMIT));
    const angle = angleBetween(fromVec, toVec);
    const fromScale = this.scale;

    return new Promise<void>((resolve) => {
      this.tweenState = {
        fromVec,
        toVec,
        angle,
        fromScale,
        toScale,
        midScale,
        startTime: performance.now(),
        duration,
        resolve,
      };
    });
  }

  private cancelActiveTween(): void {
    if (this.tweenState) {
      const cancelled = this.tweenState;
      this.tweenState = null;
      cancelled.resolve();
    }
  }

  private abortTour(): void {
    if (!this.tourAbort) return;
    this.tourAbort.abort();
    this.tourAbort = null;
  }

  // -----------------------------------------------------------------------
  // Internal: pointer + wheel
  // -----------------------------------------------------------------------

  private handleWheel = (e: WheelEvent): void => {
    e.preventDefault();
    this.stopRotation();
    this.abortTour();
    this.cancelActiveTween();
    const delta = -e.deltaY * ZOOM_WHEEL_FACTOR;
    this.scale = clamp(this.scale * Math.exp(delta), ZOOM_MIN, ZOOM_MAX);
  };

  private handlePointerDown = (e: PointerEvent): void => {
    this.pointerStart = {
      x: e.clientX,
      y: e.clientY,
      phi: this.phi,
      theta: this.theta,
    };
    this.dragging = false;
    this.canvas.setPointerCapture(e.pointerId);
    this.canvas.style.cursor = "grabbing";
    this.stopRotation();
    this.abortTour();
    this.cancelActiveTween();
  };

  private handlePointerMove = (e: PointerEvent): void => {
    if (!this.pointerStart) return;
    const dx = e.clientX - this.pointerStart.x;
    const dy = e.clientY - this.pointerStart.y;
    if (!this.dragging && Math.hypot(dx, dy) < DRAG_THRESHOLD_PX) return;
    this.dragging = true;
    this.phi = this.pointerStart.phi - dx * DRAG_SENSITIVITY;
    this.theta = clamp(this.pointerStart.theta - dy * DRAG_SENSITIVITY, -THETA_LIMIT, THETA_LIMIT);
  };

  private handlePointerUp = (e: PointerEvent): void => {
    this.canvas.style.cursor = "grab";
    if (this.canvas.hasPointerCapture(e.pointerId)) {
      this.canvas.releasePointerCapture(e.pointerId);
    }
    if (!this.pointerStart) return;
    const wasDragging = this.dragging;
    this.pointerStart = null;
    this.dragging = false;
    if (wasDragging) return;

    const rect = this.canvas.getBoundingClientRect();
    const cssX = e.clientX - rect.left;
    const cssY = e.clientY - rect.top;
    const result = unprojectGlobe({
      x: cssX,
      y: cssY,
      width: rect.width,
      height: rect.height,
      phi: this.phi,
      theta: this.theta,
      scale: this.scale,
    });
    if (!result) return;
    this.emitMapClick({ lat: result.lat, lng: result.lng, x: cssX, y: cssY });
  };

  // -----------------------------------------------------------------------
  // Internal: sizing
  // -----------------------------------------------------------------------

  private applyCanvasSize(): void {
    const dpr = window.devicePixelRatio || 1;
    const w = this.container.clientWidth;
    const h = this.container.clientHeight;
    const z = Math.max(1, this.scale);
    this.bufferZoomFactor = z;
    const px = dpr * z;
    this.canvas.width = Math.max(1, Math.round(w * px));
    this.canvas.height = Math.max(1, Math.round(h * px));
    // Draw in CSS pixels regardless of buffer scale.
    this.ctx.setTransform(px, 0, 0, px, 0, 0);
  }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function latLngToVec3(lat: number, lng: number): Vec3 {
  const latRad = (lat * Math.PI) / 180;
  const lngRad = (lng * Math.PI) / 180;
  const c = Math.cos(latRad);
  return [c * Math.cos(lngRad), Math.sin(latRad), -c * Math.sin(lngRad)];
}

function latLngToPhiTheta(lat: number, lng: number): { phi: number; theta: number } {
  return {
    phi: -((lng * Math.PI) / 180) - Math.PI / 2,
    theta: (lat * Math.PI) / 180,
  };
}

function phiThetaToVec(phi: number, theta: number): Vec3 {
  const ct = Math.cos(theta);
  return [-ct * Math.sin(phi), Math.sin(theta), -ct * Math.cos(phi)];
}

function fibonacciSphere(n: number): Vec3[] {
  const out: Vec3[] = [];
  const inc = Math.PI * (3 - Math.sqrt(5));
  for (let i = 0; i < n; i++) {
    const y = 1 - (i / Math.max(1, n - 1)) * 2;
    const r = Math.sqrt(Math.max(0, 1 - y * y));
    const t = inc * i;
    out.push([Math.cos(t) * r, y, Math.sin(t) * r]);
  }
  return out;
}

/**
 * Build a route leg as a polyline that follows the great-circle path between
 * v1 and v2 (slerp), with each sample radially lifted by a sine bump so the
 * arc stays above the sphere everywhere — not just at the apex. Apex height
 * scales with leg length, so short hops stay flat and long hops arch more.
 */
function buildLeg(v1: Vec3, v2: Vec3): RouteLeg {
  const angle = angleBetween(v1, v2);
  const apex = ARC_APEX * Math.sin(angle / 2);
  const N = PATH_SAMPLES_PER_LEG;
  const points: Vec3[] = new Array(N + 1);
  for (let i = 0; i <= N; i++) {
    const t = i / N;
    const [vx, vy, vz] = slerpVec3(v1, v2, t, angle);
    const lift = 1 + apex * Math.sin(Math.PI * t);
    points[i] = [vx * lift, vy * lift, vz * lift];
  }
  return { points };
}

function shortestAngleDelta(from: number, to: number): number {
  let d = to - from;
  while (d > Math.PI) d -= 2 * Math.PI;
  while (d < -Math.PI) d += 2 * Math.PI;
  return d;
}

function easeInOutCubic(t: number): number {
  return t < 0.5 ? 4 * t * t * t : 1 - Math.pow(-2 * t + 2, 3) / 2;
}

function clamp(v: number, lo: number, hi: number): number {
  return Math.max(lo, Math.min(hi, v));
}

function angleBetween(a: Vec3, b: Vec3): number {
  return Math.acos(clamp(a[0] * b[0] + a[1] * b[1] + a[2] * b[2], -1, 1));
}

function slerpVec3(v1: Vec3, v2: Vec3, t: number, angle: number): Vec3 {
  const sinAngle = Math.sin(angle);
  if (sinAngle < 1e-6) return [v1[0], v1[1], v1[2]];
  const s1 = Math.sin((1 - t) * angle) / sinAngle;
  const s2 = Math.sin(t * angle) / sinAngle;
  return [s1 * v1[0] + s2 * v2[0], s1 * v1[1] + s2 * v2[1], s1 * v1[2] + s2 * v2[2]];
}

function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    if (signal?.aborted) return resolve();
    const timer = setTimeout(resolve, ms);
    signal?.addEventListener(
      "abort",
      () => {
        clearTimeout(timer);
        resolve();
      },
      { once: true },
    );
  });
}

function loadImage(src: string): Promise<HTMLImageElement> {
  return new Promise((resolve, reject) => {
    const img = new Image();
    img.onload = () => resolve(img);
    img.onerror = () => reject(new Error("failed to load image"));
    img.src = src;
  });
}
