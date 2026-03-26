/**
 * MapController — wraps all MapLibre GL JS interactions for the WorldTour app.
 *
 * Design goal: make the common caller (App.vue) trivial. One import, one
 * constructor, a handful of methods. All GeoJSON plumbing, layer management,
 * globe projection, cursor handling, and tour animation are internal concerns.
 */

import maplibregl from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";
import { computeRouteLine } from "./route-line";

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

export type StopMapData = {
  id: string;
  name: string;
  lng: number;
  lat: number;
};

/** Emitted when the user clicks a stop dot on the map. */
export type StopClickEvent = { stopId: string };

/** Emitted when the user clicks empty space on the map. */
export type MapClickEvent = { lng: number; lat: number; x: number; y: number };

export type MapControllerEvents = {
  stopClick: (e: StopClickEvent) => void;
  mapClick: (e: MapClickEvent) => void;
};

export type MapControllerOptions = {
  /** DOM element or CSS selector for the map container. */
  container: string | HTMLElement;
  /** MapLibre style URL. Defaults to Carto dark-matter. */
  styleUrl?: string;
  /** Initial camera. Sensible defaults are provided. */
  center?: [number, number];
  zoom?: number;
  pitch?: number;
};

// ---------------------------------------------------------------------------
// Internal constants
// ---------------------------------------------------------------------------

const DEFAULT_STYLE = "https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json";

const ACCENT_PRIMARY = "#ff2d7b";
const ACCENT_SECONDARY = "#00e5cc";
const TEXT_COLOR = "#f5f2ed";
const HALO_COLOR = "#08090d";

// ---------------------------------------------------------------------------
// MapController
// ---------------------------------------------------------------------------

export class MapController {
  private map: maplibregl.Map;
  private listeners: { [K in keyof MapControllerEvents]?: MapControllerEvents[K][] } = {};
  private tourAbort: AbortController | null = null;
  private ready: Promise<void>;
  private rotationFrame: number | null = null;
  private rotating = false;

  constructor(options: MapControllerOptions) {
    this.map = new maplibregl.Map({
      container: options.container,
      style: options.styleUrl ?? DEFAULT_STYLE,
      center: options.center ?? [0, 20],
      zoom: options.zoom ?? 1.8,
      pitch: options.pitch ?? 30,
      bearing: 0,
    });

    this.ready = new Promise<void>((resolve) => {
      this.map.on("style.load", () => {
        this.map.setProjection({ type: "globe" });
      });
      this.map.on("load", () => {
        this.setupClickHandlers();
        resolve();
      });
    });
  }

  // -----------------------------------------------------------------------
  // Events
  // -----------------------------------------------------------------------

  on<K extends keyof MapControllerEvents>(event: K, fn: MapControllerEvents[K]): void {
    (this.listeners[event] ??= []).push(fn);
  }

  off<K extends keyof MapControllerEvents>(event: K, fn: MapControllerEvents[K]): void {
    const list = this.listeners[event];
    if (!list) return;
    const idx = list.indexOf(fn);
    if (idx >= 0) list.splice(idx, 1);
  }

  private emit<K extends keyof MapControllerEvents>(
    event: K,
    ...args: Parameters<MapControllerEvents[K]>
  ): void {
    for (const fn of this.listeners[event] ?? []) {
      (fn as (...a: unknown[]) => void)(...args);
    }
  }

  // -----------------------------------------------------------------------
  // Public API
  // -----------------------------------------------------------------------

  /** Wait for the map to be fully loaded. Resolves immediately if already loaded. */
  whenReady(): Promise<void> {
    return this.ready;
  }

  /**
   * Replace all stops and the route line in one call.
   *
   * Call this whenever your stop data changes — the controller handles
   * source creation on first call and data updates thereafter.
   */
  setStops(stops: StopMapData[], dates?: Date[]): void {
    const features = stops.map((s) => ({
      type: "Feature" as const,
      properties: { id: s.id, name: s.name },
      geometry: { type: "Point" as const, coordinates: [s.lng, s.lat] },
    }));
    const stopsGeoJson = { type: "FeatureCollection" as const, features };

    // Build route line from date-sorted stops
    const routeStops = stops.map((s, i) => ({
      date: dates?.[i] ?? new Date(0),
      lng: s.lng,
      lat: s.lat,
    }));
    const routeLine = computeRouteLine(routeStops);
    const routeGeoJson = routeLine
      ? { type: "Feature" as const, geometry: routeLine, properties: {} }
      : {
          type: "Feature" as const,
          geometry: { type: "LineString" as const, coordinates: [] as [number, number][] },
          properties: {},
        };

    this.upsertStopsSource(stopsGeoJson);
    this.upsertRouteSource(routeGeoJson);
  }

  /** Smooth camera flight to a stop. */
  flyTo(
    target: { lng: number; lat: number },
    opts?: { zoom?: number; pitch?: number; bearing?: number; duration?: number },
  ): Promise<void> {
    return new Promise<void>((resolve) => {
      this.map.flyTo({
        center: [target.lng, target.lat],
        zoom: opts?.zoom ?? 5,
        pitch: opts?.pitch ?? 40,
        bearing: opts?.bearing ?? 0,
        duration: opts?.duration ?? 1500,
      });
      this.map.once("moveend", () => resolve());
    });
  }

  /**
   * Animate a guided tour through the given stops in order.
   *
   * Returns a promise that resolves when the tour completes or is cancelled.
   * Call `stopTour()` to abort early.
   */
  async tour(stops: StopMapData[]): Promise<void> {
    if (stops.length === 0) return;

    this.tourAbort = new AbortController();
    const signal = this.tourAbort.signal;

    // Fly to first stop
    await this.flyTo(stops[0], { zoom: 4, pitch: 50, duration: 2000 });
    await sleep(800, signal);

    for (let i = 1; i < stops.length; i++) {
      if (signal.aborted) break;

      const from: [number, number] = [stops[i - 1].lng, stops[i - 1].lat];
      const to: [number, number] = [stops[i].lng, stops[i].lat];

      // Linear interpolation along the segment
      const steps = 80;
      for (let s = 0; s <= steps; s++) {
        if (signal.aborted) break;
        const t = s / steps;
        const zoomEase = 3.5 + 1.5 * (1 - Math.sin(t * Math.PI));
        this.map.jumpTo({
          center: [from[0] + (to[0] - from[0]) * t, from[1] + (to[1] - from[1]) * t],
          zoom: zoomEase,
          pitch: 45,
        });
        await sleep(40, signal);
      }

      if (signal.aborted) break;

      // Settle at the stop
      await this.flyTo(stops[i], { zoom: 5, pitch: 45, duration: 800 });
      await sleep(1200, signal);
    }

    this.tourAbort = null;
  }

  /** Cancel a running tour. No-op if no tour is active. */
  stopTour(): void {
    this.tourAbort?.abort();
    this.tourAbort = null;
  }

  /** Whether a tour animation is currently running. */
  get isTouring(): boolean {
    return this.tourAbort !== null && !this.tourAbort.signal.aborted;
  }

  /** Clean up the map instance. Call when unmounting. */
  startRotation(): void {
    if (this.rotating) return;
    this.rotating = true;
    const spin = () => {
      if (!this.rotating) return;
      const center = this.map.getCenter();
      center.lng += 0.03;
      this.map.jumpTo({ center });
      this.rotationFrame = requestAnimationFrame(spin);
    };
    this.rotationFrame = requestAnimationFrame(spin);

    this.map.on("mousedown", this.pauseRotation);
    this.map.on("touchstart", this.pauseRotation);
  }

  stopRotation(): void {
    this.rotating = false;
    if (this.rotationFrame !== null) {
      cancelAnimationFrame(this.rotationFrame);
      this.rotationFrame = null;
    }
    this.map.off("mousedown", this.pauseRotation);
    this.map.off("touchstart", this.pauseRotation);
  }

  private pauseRotation = (): void => {
    this.stopRotation();
  };

  destroy(): void {
    this.stopRotation();
    this.stopTour();
    this.map.remove();
  }

  // -----------------------------------------------------------------------
  // Internal: click handling
  // -----------------------------------------------------------------------

  private setupClickHandlers(): void {
    // Stop dot click
    this.map.on("click", "stops-layer", (e) => {
      if (!e.features?.length) return;
      const stopId = e.features[0].properties?.id;
      if (stopId) this.emit("stopClick", { stopId });
    });

    // Cursor management
    this.map.on("mouseenter", "stops-layer", () => {
      this.map.getCanvas().style.cursor = "pointer";
    });
    this.map.on("mouseleave", "stops-layer", () => {
      this.map.getCanvas().style.cursor = "";
    });

    // Empty-area click
    this.map.on("click", (e) => {
      const hits = this.map.queryRenderedFeatures(e.point, { layers: ["stops-layer"] });
      if (hits.length > 0) return; // handled by layer click above
      this.emit("mapClick", {
        lng: e.lngLat.lng,
        lat: e.lngLat.lat,
        x: e.point.x,
        y: e.point.y,
      });
    });
  }

  // -----------------------------------------------------------------------
  // Internal: source/layer management
  // -----------------------------------------------------------------------

  private upsertStopsSource(data: GeoJSON.FeatureCollection): void {
    const existing = this.map.getSource("stops") as maplibregl.GeoJSONSource | undefined;
    if (existing) {
      existing.setData(data);
      return;
    }

    this.map.addSource("stops", { type: "geojson", data });

    this.map.addLayer({
      id: "stops-pulse",
      type: "circle",
      source: "stops",
      paint: {
        "circle-radius": 12,
        "circle-color": ACCENT_PRIMARY,
        "circle-opacity": 0.2,
        "circle-blur": 1,
      },
    });

    this.map.addLayer({
      id: "stops-layer",
      type: "circle",
      source: "stops",
      paint: {
        "circle-radius": 6,
        "circle-color": ACCENT_PRIMARY,
        "circle-stroke-width": 2,
        "circle-stroke-color": TEXT_COLOR,
      },
    });

    this.map.addLayer({
      id: "stops-labels",
      type: "symbol",
      source: "stops",
      layout: {
        "text-field": ["get", "name"],
        "text-size": 12,
        "text-offset": [0, -1.5],
        "text-anchor": "bottom",
        "text-font": ["Open Sans Bold"],
        "text-max-width": 10,
      },
      paint: {
        "text-color": TEXT_COLOR,
        "text-halo-color": HALO_COLOR,
        "text-halo-width": 1.5,
      },
    });
  }

  private upsertRouteSource(data: GeoJSON.Feature): void {
    const existing = this.map.getSource("route") as maplibregl.GeoJSONSource | undefined;
    if (existing) {
      existing.setData(data);
      return;
    }

    this.map.addSource("route", { type: "geojson", data });
    this.map.addLayer(
      {
        id: "route-line",
        type: "line",
        source: "route",
        paint: {
          "line-color": ACCENT_SECONDARY,
          "line-width": 3,
        },
      },
      "stops-pulse",
    );
  }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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
