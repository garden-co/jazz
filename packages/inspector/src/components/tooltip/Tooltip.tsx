import { useRef, type ReactNode } from "react";
import styles from "./Tooltip.module.css";

const SHOW_DELAY_MS = 300;
const GAP = 6;
const EDGE = 4;

interface TooltipProps {
  label: string;
  /** Preferred side; flips automatically when it wouldn't fit. */
  placement?: "top" | "bottom";
  children: ReactNode;
}

/**
 * Hover/focus tooltip built on the native Popover API. The bubble is a
 * `popover="manual"` element, so it renders in the browser's top layer and
 * escapes the inspector's many `overflow: hidden` panels — a plain absolutely
 * positioned tooltip would be clipped by the toolbar or grid.
 *
 * Listeners live on a wrapping span (not the trigger) so the tooltip still fires
 * for disabled buttons, which don't emit pointer events themselves. Positioning
 * is done in JS rather than CSS anchor positioning for Safari/Firefox support.
 */
export function Tooltip({ label, placement = "top", children }: TooltipProps) {
  const wrapRef = useRef<HTMLSpanElement>(null);
  const popoverRef = useRef<HTMLDivElement>(null);
  const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const position = () => {
    const wrap = wrapRef.current;
    const popover = popoverRef.current;
    if (!wrap || !popover) return;
    const anchor = wrap.getBoundingClientRect();
    const bubble = popover.getBoundingClientRect();

    let top = placement === "top" ? anchor.top - bubble.height - GAP : anchor.bottom + GAP;
    if (top < EDGE) top = anchor.bottom + GAP;
    if (top + bubble.height > window.innerHeight - EDGE) top = anchor.top - bubble.height - GAP;

    const left = Math.min(
      Math.max(EDGE, anchor.left + anchor.width / 2 - bubble.width / 2),
      window.innerWidth - bubble.width - EDGE,
    );

    popover.style.left = `${Math.round(left)}px`;
    popover.style.top = `${Math.round(top)}px`;
  };

  const show = () => {
    const popover = popoverRef.current;
    if (!popover || typeof popover.showPopover !== "function") return;
    if (timerRef.current) clearTimeout(timerRef.current);
    timerRef.current = setTimeout(() => {
      try {
        popover.showPopover();
        position();
      } catch {
        // showPopover throws if already open or detached — ignore.
      }
    }, SHOW_DELAY_MS);
  };

  const hide = () => {
    if (timerRef.current) clearTimeout(timerRef.current);
    try {
      popoverRef.current?.hidePopover?.();
    } catch {
      // hidePopover throws if not open — ignore.
    }
  };

  return (
    <span
      ref={wrapRef}
      className={styles.wrap}
      onPointerEnter={show}
      onPointerLeave={hide}
      onPointerDownCapture={hide}
      onFocusCapture={show}
      onBlurCapture={hide}
    >
      {children}
      <div ref={popoverRef} popover="manual" role="tooltip" aria-hidden className={styles.tooltip}>
        {label}
      </div>
    </span>
  );
}
