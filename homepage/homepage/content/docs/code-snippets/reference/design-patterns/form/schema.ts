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

export const PartialBubbleTeaOrder = BubbleTeaOrder.partial();
// [!code hide:6]
export const JazzAccount = co.account({
  root: co.map({
    orders: co.list(BubbleTeaOrder),
  }),
  profile: co.profile(),
});
