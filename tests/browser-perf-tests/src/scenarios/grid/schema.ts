import { co, z } from "jazz-tools";

/**
 * PixelCell: Each cell in the grid
 * - color: hex color like "#FF5733"
 * - padding: random data for payload testing
 */
export const PixelCell = co.map({
  color: z.string(),
  padding: z.string(),
});
export type PixelCell = co.loaded<typeof PixelCell>;

/**
 * GridRoot: The root container for the NxN grid
 * - size: N (grid is NxN)
 * - cells: flat list of PixelCells, accessed as [row * N + col]
 */
export const GridRoot = co
  .map({
    size: z.number(),
    cells: co.list(PixelCell),
  })
  .withPermissions({
    onCreate: (newGroup) => {
      newGroup.makePublic();
    },
  });
export type GridRoot = co.loaded<typeof GridRoot>;
