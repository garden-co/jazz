import { co, z } from "jazz-tools";

export const BubbleTeaAddOnTypes = [
  "Pearl",
  "Lychee jelly",
  "Red bean",
  "Brown sugar",
  "Taro",
] as const;

export const BubbleTeaBaseTeaTypes = [
  "Black",
  "Oolong",
  "Jasmine",
  "Thai",
] as const;

export const ListOfBubbleTeaAddOns = co.list(
  z.literal([...BubbleTeaAddOnTypes]),
);
export type ListOfBubbleTeaAddOns = co.loaded<typeof ListOfBubbleTeaAddOns>;

function hasAddOnsChanges(list?: ListOfBubbleTeaAddOns | null) {
  return list && Object.entries(list.$jazz.raw.insertions).length > 0;
}

export const BubbleTeaOrder = co.map({
  baseTea: z.literal([...BubbleTeaBaseTeaTypes]),
  addOns: ListOfBubbleTeaAddOns,
  deliveryDate: z.date(),
  withMilk: z.boolean(),
  instructions: co.optional(co.plainText()),
});
export type BubbleTeaOrder = co.loaded<typeof BubbleTeaOrder>;

export const DraftBubbleTeaOrder = BubbleTeaOrder.partial();
export type DraftBubbleTeaOrder = co.loaded<typeof DraftBubbleTeaOrder>;

export function validateDraftOrder(order: DraftBubbleTeaOrder) {
  const errors: string[] = [];

  if (!order.baseTea) {
    errors.push("Please select your preferred base tea.");
  }
  if (!order.deliveryDate) {
    errors.push("Plese select a delivery date.");
  }

  return { errors };
}

export function hasChanges(order?: DraftBubbleTeaOrder | null) {
  return (
    !!order &&
    (Object.keys(order.$jazz.getEdits()).length > 1 ||
      hasAddOnsChanges(order.addOns))
  );
}

/** The root is an app-specific per-user private `CoMap`
 *  where you can store top-level objects for that user */
export const AccountRoot = co.map({
  draft: DraftBubbleTeaOrder,
  orders: co.list(BubbleTeaOrder),
});

export const JazzAccount = co
  .account({
    root: AccountRoot,
    profile: co.profile(),
  })
  .withMigration((account) => {
    if (!account.root) {
      account.$jazz.set("root", {
        draft: { addOns: [], instructions: "" },
        orders: [],
      });
    }
  });
