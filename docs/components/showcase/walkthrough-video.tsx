"use client";

import { useEffect, useRef, useState } from "react";
import { watchAutoplay } from "./autoplay";

type Props = {
  src: string;
  poster: string;
  className?: string;
  "aria-label"?: string;
  /** Show the browser's controls (always shown with reduced motion). */
  controls?: boolean;
  width?: number;
  height?: number;
};

/** A muted, looping walkthrough video that plays only while it is in view. */
export function WalkthroughVideo({ controls = false, ...props }: Props) {
  const ref = useRef<HTMLVideoElement>(null);
  const [reducedMotion, setReducedMotion] = useState(false);
  useEffect(() => {
    const query = window.matchMedia("(prefers-reduced-motion: reduce)");
    setReducedMotion(query.matches);
    const update = () => setReducedMotion(query.matches);
    query.addEventListener("change", update);
    return () => query.removeEventListener("change", update);
  }, []);
  useEffect(() => {
    const video = ref.current;
    if (!video) return;
    if (reducedMotion) video.pause();
    return watchAutoplay(video, { reducedMotion });
  }, [reducedMotion]);
  return (
    <video
      ref={ref}
      {...props}
      controls={controls || reducedMotion}
      muted
      loop
      playsInline
      preload="metadata"
    />
  );
}
