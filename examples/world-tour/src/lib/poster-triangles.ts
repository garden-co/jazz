export interface Triangle {
  points: string;
  fill: string;
  opacity: number;
}

export interface GridOptions {
  cols: number;
  rows: number;
  seed: number;
  palette: string[];
}

const W = 595;
const H = 842;

export function hash(n: number): number {
  const x = Math.sin(n * 127.1 + 311.7) * 43758.5453;
  return x - Math.floor(x);
}

export function blendHex(a: string, b: string, t: number): string {
  const pa = [
    parseInt(a.slice(1, 3), 16),
    parseInt(a.slice(3, 5), 16),
    parseInt(a.slice(5, 7), 16),
  ];
  const pb = [
    parseInt(b.slice(1, 3), 16),
    parseInt(b.slice(3, 5), 16),
    parseInt(b.slice(5, 7), 16),
  ];
  const r = Math.round(pa[0] + (pb[0] - pa[0]) * t);
  const g = Math.round(pa[1] + (pb[1] - pa[1]) * t);
  const bl = Math.round(pa[2] + (pb[2] - pa[2]) * t);
  return `#${r.toString(16).padStart(2, "0")}${g.toString(16).padStart(2, "0")}${bl.toString(16).padStart(2, "0")}`;
}

export function seedFromString(s: string): number {
  return s.split("").reduce((a, c) => a + c.charCodeAt(0), 0);
}

export function pickPalette(seed: number, palettes: string[][]): string[] {
  return palettes[seed % palettes.length];
}

export function generateTriangles(opts: GridOptions): Triangle[] {
  const { cols, rows, seed, palette } = opts;
  const cellW = W / cols;
  const cellH = H / rows;
  const tris: Triangle[] = [];

  const pts: [number, number][][] = [];
  for (let r = 0; r <= rows; r++) {
    pts[r] = [];
    for (let c = 0; c <= cols; c++) {
      const edge = r === 0 || r === rows || c === 0 || c === cols;
      const jx = edge ? 0 : (hash(seed + r * 17 + c * 31) - 0.5) * cellW * 0.7;
      const jy = edge ? 0 : (hash(seed + r * 53 + c * 97) - 0.5) * cellH * 0.7;
      pts[r][c] = [c * cellW + jx, r * cellH + jy];
    }
  }

  for (let r = 0; r < rows; r++) {
    for (let c = 0; c < cols; c++) {
      const tl = pts[r][c];
      const tr = pts[r][c + 1];
      const bl = pts[r + 1][c];
      const br = pts[r + 1][c + 1];
      const h1 = hash(seed + r * 7 + c * 13);
      const h2 = hash(seed + r * 11 + c * 23 + 100);

      const lerp = (t: number) => {
        const idx = t * (palette.length - 1);
        const lo = Math.floor(idx);
        const hi = Math.min(lo + 1, palette.length - 1);
        return lo === hi ? palette[lo] : blendHex(palette[lo], palette[hi], idx - lo);
      };

      tris.push({
        points: `${tl[0]},${tl[1]} ${tr[0]},${tr[1]} ${bl[0]},${bl[1]}`,
        fill: lerp(h1),
        opacity: 0.4 + h1 * 0.5,
      });
      tris.push({
        points: `${tr[0]},${tr[1]} ${br[0]},${br[1]} ${bl[0]},${bl[1]}`,
        fill: lerp(h2),
        opacity: 0.4 + h2 * 0.5,
      });
    }
  }

  return tris;
}
