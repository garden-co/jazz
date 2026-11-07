"use client";

import { useEffect, useRef, useState } from "react";
import { AppLink } from "@/components/design/app-link";
import { watchAutoplay } from "@/components/showcase/autoplay";

export type ReelItem = {
  id: string;
  title: string;
  subtitle: string;
  src: string;
  poster: string;
  label: string;
};

/**
 * Example walkthroughs: one video beside a list of examples. Each video
 * plays to its end and then the next example starts; picking an item plays it.
 */
export function ExampleReel({ items, moreHref }: { items: ReelItem[]; moreHref: string }) {
  const ref = useRef<HTMLVideoElement>(null);
  const [index, setIndex] = useState(0);
  const [progress, setProgress] = useState(0);
  const [reducedMotion, setReducedMotion] = useState(false);
  // Set when the next render should start playing the newly selected video.
  const playNext = useRef(false);

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

  useEffect(() => {
    setProgress(0);
    if (!playNext.current) return;
    playNext.current = false;
    ref.current?.play().catch(() => {});
  }, [index]);

  const select = (next: number, play: boolean) => {
    playNext.current = play;
    if (next === index) {
      const video = ref.current;
      if (video && play) {
        video.currentTime = 0;
        video.play().catch(() => {});
      }
      return;
    }
    setIndex(next);
  };

  const item = items[index];
  if (!item) return null;

  return (
    <div className="home-reel">
      <div className="home-reel-stage">
        <video
          ref={ref}
          className="home-reel-video"
          src={item.src}
          poster={item.poster}
          aria-label={item.label}
          width={1280}
          height={892}
          controls={reducedMotion}
          muted
          playsInline
          preload="metadata"
          onTimeUpdate={(event) => {
            const video = event.currentTarget;
            if (video.duration) setProgress(video.currentTime / video.duration);
          }}
          onEnded={() => select((index + 1) % items.length, !reducedMotion)}
        />
      </div>
      <div className="home-reel-side">
        <ol className="home-reel-list">
          {items.map((entry, i) => (
            <li key={entry.id}>
              <button
                type="button"
                className="home-reel-item"
                aria-current={i === index ? "true" : undefined}
                onClick={() => select(i, true)}
              >
                <span className="home-reel-title">{entry.title}</span>
                <span className="home-reel-subtitle">{entry.subtitle}</span>
                {i === index ? (
                  <span
                    className="home-reel-progress"
                    style={{ transform: `scaleX(${progress})` }}
                    aria-hidden="true"
                  />
                ) : null}
              </button>
            </li>
          ))}
          <li>
            <AppLink href={moreHref} className="home-reel-item home-reel-more">
              <span className="home-reel-title">More examples and benchmarks →</span>
            </AppLink>
          </li>
        </ol>
      </div>
    </div>
  );
}
