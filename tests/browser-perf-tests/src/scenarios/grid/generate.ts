import { GridRoot, PixelCell } from "./schema";

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
  // Create the cells list first
  const cells = GridRoot.shape.cells.create([]);

  // Create the grid
  const grid = GridRoot.create({
    size,
    cells,
  });

  // Populate cells asynchronously to avoid blocking
  const done = new Promise<void>((resolve) => {
    setTimeout(() => {
      const totalCells = size * size;
      for (let i = 0; i < totalCells; i++) {
        cells.$jazz.push({
          color: randomColor(),
          padding: generatePadding(minPaddingBytes, maxPaddingBytes),
        });
      }

      Promise.all(cells.map((cell) => cell.$jazz.waitForSync())).then(() =>
        resolve(),
      );
    }, 10);
  });

  return { grid, done };
}
