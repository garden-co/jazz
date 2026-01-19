import { GridRoot } from "./schema";

/**
 * Generate a random hex color
 */
export function randomColor(): string {
  const hex = Math.floor(Math.random() * 16777215)
    .toString(16)
    .padStart(6, "0");
  return `#${hex}`;
}

/**
 * Generate random padding data of specified size
 */
export function generatePadding(minBytes: number, maxBytes: number): string {
  const size = minBytes + Math.floor(Math.random() * (maxBytes - minBytes + 1));
  const chars =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
  let result = "";
  for (let i = 0; i < size; i++) {
    result += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  return result;
}

/**
 * Generate an NxN grid of PixelCells with random colors and padding
 */
export function generateGrid(
  size: number,
  minPaddingBytes: number,
  maxPaddingBytes: number,
) {
  // Create the grid
  const grid = GridRoot.create({
    size,
    cells: [],
  });

  const totalCells = size * size;

  const cells = grid.cells;

  const done = nonBlocking(() => {
    for (let i = 0; i < totalCells; i++) {
      cells.$jazz.push({
        color: randomColor(),
        padding: generatePadding(minPaddingBytes, maxPaddingBytes),
      });
    }

    return Promise.all(cells.map((cell) => cell.$jazz.waitForSync()));
  });

  return { grid, done };
}

function nonBlocking<T>(callback: () => T) {
  return new Promise<T>((resolve) => {
    requestAnimationFrame(() => {
      queueMicrotask(() => {
        resolve(callback());
      });
    });
  });
}
