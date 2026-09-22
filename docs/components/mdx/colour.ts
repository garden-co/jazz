// Colour-propagation payload policy, shared by the animated sync diagrams
// (tier-sync & write-tier). The diagram engine owns the propagation
// *mechanism*; choosing the next colour to write is diagram *policy*, so it
// lives here alongside the definitions rather than inside the engine.

const MIN_HUE_GAP = 40;
export const INITIAL_COLOR = "#146aff";

function hexToHue(hex: string): number | null {
  const m = /^#([0-9a-f]{6})$/i.exec(hex);
  if (!m) return null;
  const n = parseInt(m[1], 16);
  const r = ((n >> 16) & 0xff) / 255;
  const g = ((n >> 8) & 0xff) / 255;
  const b = (n & 0xff) / 255;
  const max = Math.max(r, g, b);
  const min = Math.min(r, g, b);
  const d = max - min;
  if (d === 0) return 0;
  let h: number;
  if (max === r) h = ((g - b) / d) % 6;
  else if (max === g) h = (b - r) / d + 2;
  else h = (r - g) / d + 4;
  h *= 60;
  return h < 0 ? h + 360 : h;
}

function hslToHex(h: number, s: number, l: number): string {
  const sat = s / 100;
  const lit = l / 100;
  const c = (1 - Math.abs(2 * lit - 1)) * sat;
  const hh = (h % 360) / 60;
  const x = c * (1 - Math.abs((hh % 2) - 1));
  const [r1, g1, b1] =
    hh < 1
      ? [c, x, 0]
      : hh < 2
        ? [x, c, 0]
        : hh < 3
          ? [0, c, x]
          : hh < 4
            ? [0, x, c]
            : hh < 5
              ? [x, 0, c]
              : [c, 0, x];
  const m = lit - c / 2;
  const to255 = (v: number) =>
    Math.round((v + m) * 255)
      .toString(16)
      .padStart(2, "0");
  return `#${to255(r1)}${to255(g1)}${to255(b1)}`;
}

function hueDistance(a: number, b: number): number {
  const d = Math.abs(a - b) % 360;
  return d > 180 ? 360 - d : d;
}

// A fresh, saturated colour whose hue is at least MIN_HUE_GAP away from the
// current one, so consecutive writes are always visibly distinct.
export function pickNextColor(current: string | null): string {
  const prev = current ? hexToHue(current) : null;
  let hue = Math.floor(Math.random() * 360);
  if (prev !== null) {
    for (let i = 0; i < 8 && hueDistance(hue, prev) < MIN_HUE_GAP; i++) {
      hue = Math.floor(Math.random() * 360);
    }
  }
  const saturation = 75 + Math.floor(Math.random() * 20);
  return hslToHex(hue, saturation, 50);
}
