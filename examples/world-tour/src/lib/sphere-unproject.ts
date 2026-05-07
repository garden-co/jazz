/**
 * Project a canvas-space click back onto the globe and recover the
 * geographic coordinate the user clicked on.
 *
 * The renderer maps `[lat, lng]` to a unit-sphere point as
 *   p = (cos lat * cos lng, sin lat, -cos lat * sin lng)
 * and renders it under the rotation `M = Rx(theta) * Ry(phi)`, where
 *   Ry(a) = [[cos a, 0, sin a], [0, 1, 0], [-sin a, 0, cos a]]
 *   Rx(a) = [[1, 0, 0], [0, cos a, -sin a], [0, sin a, cos a]]
 * In particular, `phi=0, theta=0` puts (lat=0, lng=-90) at the visible centre.
 *
 * Returns `null` if the click is outside the rendered globe disc.
 */

export type UnprojectInput = {
  x: number;
  y: number;
  width: number;
  height: number;
  phi: number;
  theta: number;
  scale?: number;
};

export function unprojectGlobe(input: UnprojectInput): { lat: number; lng: number } | null {
  const { x, y, width, height, phi, theta, scale = 1 } = input;

  const cx = width / 2;
  const cy = height / 2;
  const radius = (Math.min(width, height) / 2) * scale;

  // Click in unit-sphere coordinates (canvas y points down → world y points up).
  const px = (x - cx) / radius;
  const py = -(y - cy) / radius;

  const r2 = px * px + py * py;
  if (r2 > 1) return null;

  const pz = Math.sqrt(1 - r2);

  // Inverse tilt: Rx(-theta).
  const ct = Math.cos(theta);
  const st = Math.sin(theta);
  const y1 = py * ct + pz * st;
  const z1 = -py * st + pz * ct;

  // Inverse rotation: Ry(-phi).
  const cp = Math.cos(phi);
  const sp = Math.sin(phi);
  const ox = px * cp - z1 * sp;
  const oz = px * sp + z1 * cp;

  const lat = (Math.asin(y1) * 180) / Math.PI;
  const lng = (Math.atan2(-oz, ox) * 180) / Math.PI;

  return { lat, lng };
}
