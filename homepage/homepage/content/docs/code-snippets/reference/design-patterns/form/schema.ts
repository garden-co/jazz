import { co, z } from "jazz-tools";
// [!code hide]
const AddOns = co.map({
  // [!code hide]
  name: z.string(),
  // [!code hide]
});

export const BubbleTeaOrder = co.map({
  name: z.string(),
  // [!code hide]
  addOns: co.list(AddOns),
  // [!code hide]
  instructions: co.plainText(),
});
export type BubbleTeaOrder = co.loaded<typeof BubbleTeaOrder>;

export const PartialBubbleTeaOrder = BubbleTeaOrder.partial();
export type PartialBubbleTeaOrder = co.loaded<typeof PartialBubbleTeaOrder>;
