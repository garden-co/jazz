"use client";

import { useSyncExternalStore } from "react";

/**
 * Hash routes keep every page inside the one authenticated Jazz provider that
 * `app/page.tsx` mounts after sign-in.
 */
export type Route =
  | { page: "overview" }
  | { page: "artists" }
  | { page: "artist"; id: string }
  | { page: "releases" }
  | { page: "release"; id: string }
  | { page: "catalogues" }
  | { page: "catalogue"; id: string }
  | { page: "teams" }
  | { page: "team"; id: string }
  | { page: "people" }
  | { page: "settings" };

export const href = {
  overview: "#/",
  artists: "#/artists",
  artist: (id: string) => `#/artists/${id}`,
  releases: "#/releases",
  release: (id: string) => `#/releases/${id}`,
  catalogues: "#/catalogues",
  catalogue: (id: string) => `#/catalogues/${id}`,
  teams: "#/teams",
  team: (id: string) => `#/teams/${id}`,
  people: "#/people",
  settings: "#/settings",
};

const collections = {
  artists: "artist",
  releases: "release",
  catalogues: "catalogue",
  teams: "team",
} as const;

export function parseRoute(hash: string): Route {
  const [section, id] = hash.replace(/^#\/?/, "").split("/");
  if (section === "people" || section === "settings") return { page: section };
  if (section && section in collections) {
    const collection = section as keyof typeof collections;
    return id ? { page: collections[collection], id } : { page: collection };
  }
  return { page: "overview" };
}

function subscribe(onChange: () => void) {
  window.addEventListener("hashchange", onChange);
  return () => window.removeEventListener("hashchange", onChange);
}

export function useRoute(): Route {
  const hash = useSyncExternalStore(
    subscribe,
    () => window.location.hash,
    () => "",
  );
  return parseRoute(hash);
}

export function navigate(to: string) {
  window.location.hash = to;
}
