import { z } from "jazz-tools";

export const Vec2 = z.object({
  x: z.number(),
  y: z.number(),
});
export type Vec2 = z.infer<typeof Vec2>;

export const Cursor = z.object({
  position: Vec2,
});
export type Cursor = z.infer<typeof Cursor>;

export const Camera = z.object({
  position: Vec2,
});
export type Camera = z.infer<typeof Camera>;

export const RemoteCursor = z.object({
  ...Cursor,
  id: z.string(),
  color: z.string(),
  name: z.string(),
  isRemote: z.literal(true),
  isDragging: z.boolean(),
});
export type RemoteCursor = z.infer<typeof RemoteCursor>;

export const ViewBox = z.object({
  x: z.number(),
  y: z.number(),
  width: z.number(),
  height: z.number(),
});
export type ViewBox = z.infer<typeof ViewBox>;
