/**
 * Plays a muted walkthrough video while it is in view and pauses it when it
 * leaves. With reduced motion it never starts on its own. A video the viewer
 * paused while it was in view stays paused until it leaves view again.
 */

export type AutoplayVideo = Pick<
  HTMLVideoElement,
  "play" | "pause" | "paused" | "addEventListener" | "removeEventListener"
>;

type Observer = { observe(target: Element): void; disconnect(): void };
export type ObserverFactory = (
  callback: (entries: { isIntersecting: boolean; intersectionRatio: number }[]) => void,
  options: { threshold: number },
) => Observer;

/** Share of the video that must be visible before it plays. */
export const VISIBLE_SHARE = 0.5;

export function watchAutoplay(
  video: AutoplayVideo,
  {
    reducedMotion,
    observe = (callback, options) => new IntersectionObserver(callback, options),
  }: { reducedMotion: boolean; observe?: ObserverFactory },
): () => void {
  if (reducedMotion) return () => {};
  let ours = false;
  let viewerPaused = false;
  let visible = false;
  const onPause = () => {
    if (!ours && visible) viewerPaused = true;
    ours = false;
  };
  video.addEventListener("pause", onPause);
  const observer = observe(
    (entries) => {
      const entry = entries[entries.length - 1];
      if (!entry) return;
      visible = entry.isIntersecting && entry.intersectionRatio >= VISIBLE_SHARE;
      if (visible) {
        if (!viewerPaused && video.paused) video.play().catch(() => {});
      } else {
        viewerPaused = false;
        if (!video.paused) {
          ours = true;
          video.pause();
        }
      }
    },
    { threshold: VISIBLE_SHARE },
  );
  observer.observe(video as unknown as Element);
  return () => {
    observer.disconnect();
    video.removeEventListener("pause", onPause);
  };
}
