import type { ReactNode } from "react";
import type { Art, Hue } from "@/schema";

/**
 * Generated product imagery: a small line drawing per product kind, coloured
 * with the theme's hue tokens so it follows light and dark mode. The seed
 * catalogue is synthetic, so there are no photos to ship.
 */
export function ProductArt({ art, hue, label }: { art: Art; hue: Hue; label: string }) {
  return (
    <svg
      className="product-art"
      viewBox="0 0 160 120"
      role="img"
      aria-label={label}
      data-hue={hue}
      preserveAspectRatio="xMidYMid meet"
    >
      <rect className="product-art-ground" x="0" y="0" width="160" height="120" />
      <g className="product-art-line" fill="none" strokeLinecap="round" strokeLinejoin="round">
        {DRAWINGS[art]}
      </g>
    </svg>
  );
}

const DRAWINGS: Record<Art, ReactNode> = {
  guitar: (
    <>
      <path d="M52 84c-10-2-16 8-10 16s22 8 28 0c3-4 8-4 12-2 6 3 14 0 14-8s-8-12-14-10c-4 1-8 0-10-3-4-6-14-6-18 0-2 3-1 6-2 7z" />
      <path d="M86 78l46-46" />
      <path d="M128 30l8-8 6 6-8 8z" />
      <circle cx="68" cy="88" r="5" />
      <path d="M78 82l-6 6" />
    </>
  ),
  bass: (
    <>
      <path d="M48 88c-8-2-14 8-8 15s22 6 27-1c3-4 7-4 11-2 6 3 13-1 13-8s-8-11-13-9c-4 1-7-1-9-4-4-6-13-5-16 1-1 3-2 6-5 8z" />
      <path d="M82 80l54-54" />
      <path d="M132 24l10-8 6 6-8 10z" />
      <path d="M60 94l8-8M66 98l8-8" />
    </>
  ),
  keys: (
    <>
      <rect x="22" y="46" width="116" height="36" rx="4" />
      <path d="M34 46v36M46 46v36M58 46v36M70 46v36M82 46v36M94 46v36M106 46v36M118 46v36M130 46v36" />
      <path d="M40 46v20M52 46v20M76 46v20M88 46v20M100 46v20M124 46v20" strokeWidth="5" />
    </>
  ),
  synth: (
    <>
      <rect x="22" y="36" width="116" height="52" rx="4" />
      <path d="M22 66h116" />
      <path d="M34 66v22M46 66v22M58 66v22M70 66v22M82 66v22M94 66v22M106 66v22M118 66v22M130 66v22" />
      <circle cx="40" cy="50" r="5" />
      <circle cx="60" cy="50" r="5" />
      <circle cx="80" cy="50" r="5" />
      <path d="M100 52h28M100 46h16" />
    </>
  ),
  drum: (
    <>
      <ellipse cx="80" cy="46" rx="42" ry="12" />
      <path d="M38 46v32c0 7 19 12 42 12s42-5 42-12V46" />
      <path d="M48 56l8 30M72 58l0 32M96 58l-4 32M114 54l-6 30" />
      <path d="M106 20l-20 22M120 26l-28 18" />
    </>
  ),
  cymbal: (
    <>
      <path d="M26 58c18-12 90-12 108 0-18 8-90 8-108 0z" />
      <path d="M72 50c2-4 14-4 16 0" />
      <path d="M80 62v38M66 100h28" />
      <path d="M44 56c10-4 62-4 72 0" />
    </>
  ),
  mic: (
    <>
      <rect x="64" y="18" width="32" height="48" rx="16" />
      <path d="M64 34h32M64 44h32M64 54h32" />
      <path d="M54 52c0 18 12 26 26 26s26-8 26-26" />
      <path d="M80 78v20M64 100h32" />
    </>
  ),
  headphones: (
    <>
      <path d="M42 76V64c0-22 17-38 38-38s38 16 38 38v12" />
      <rect x="32" y="70" width="20" height="30" rx="6" />
      <rect x="108" y="70" width="20" height="30" rx="6" />
    </>
  ),
  amp: (
    <>
      <rect x="34" y="22" width="92" height="80" rx="4" />
      <path d="M34 40h92" />
      <circle cx="80" cy="72" r="22" />
      <circle cx="80" cy="72" r="8" />
      <circle cx="48" cy="31" r="3" />
      <circle cx="60" cy="31" r="3" />
      <circle cx="72" cy="31" r="3" />
      <path d="M58 16h44" />
    </>
  ),
  pedal: (
    <>
      <rect x="52" y="20" width="56" height="82" rx="6" />
      <circle cx="66" cy="36" r="6" />
      <circle cx="94" cy="36" r="6" />
      <circle cx="80" cy="54" r="6" />
      <circle cx="80" cy="84" r="8" />
      <path d="M52 30h-12M108 30h12" />
    </>
  ),
  cable: (
    <>
      <path d="M34 34c30 0 18 30 46 30s20 30 46 30" />
      <rect x="18" y="28" width="16" height="12" rx="3" />
      <path d="M18 34h-8" />
      <rect x="126" y="88" width="16" height="12" rx="3" />
      <path d="M142 94h8" />
    </>
  ),
  strings: (
    <>
      <circle cx="80" cy="60" r="36" />
      <circle cx="80" cy="60" r="28" />
      <circle cx="80" cy="60" r="20" />
      <circle cx="80" cy="60" r="12" />
      <path d="M116 60c10 0 18 10 20 24" />
    </>
  ),
  picks: (
    <>
      <path d="M40 40c12-8 32-8 40 0s-8 36-20 44c-12-8-28-36-20-44z" />
      <path d="M80 36c12-8 32-8 40 0s-8 36-20 44c-12-8-28-36-20-44z" />
      <path d="M62 60c12-8 32-8 40 0s-8 36-20 44c-12-8-28-36-20-44z" />
    </>
  ),
};
