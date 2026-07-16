"use client";

import { useEffect, useState, useSyncExternalStore } from "react";
import { Chip, type RenderedOn } from "./Chip";

const subscribe = () => () => {};

function useRenderedOn(): RenderedOn {
  const hydrated = useSyncExternalStore(
    subscribe,
    () => true,
    () => false,
  );
  const [clientOnly] = useState(hydrated);

  if (clientOnly) return "client";
  return hydrated ? "hydrated" : "server";
}

export function RenderChip() {
  return <Chip renderedOn={useRenderedOn()} />;
}

export function ClientChip() {
  const [mounted, setMounted] = useState(false);
  useEffect(() => setMounted(true), []);
  return mounted ? <RenderChip /> : null;
}
