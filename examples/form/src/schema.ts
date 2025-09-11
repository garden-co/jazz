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

export const ListOfBubbleTeaAddOns = co.list(z.literal(BubbleTeaAddOnTypes));
export type ListOfBubbleTeaAddOns = co.loaded<typeof ListOfBubbleTeaAddOns>;

export const BubbleTeaOrder = co.map({
  baseTea: z.literal([...BubbleTeaBaseTeaTypes]),
  addOns: ListOfBubbleTeaAddOns,
  deliveryDate: z.date(),
  withMilk: z.boolean(),
  instructions: co.plainText(),
});
export type BubbleTeaOrder = co.loaded<typeof BubbleTeaOrder>;

export const DraftBubbleTeaOrder = BubbleTeaOrder.partial({
  baseTea: true,
  deliveryDate: true,
  withMilk: true,
});
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

export function getLastDraftId(root: AccountRoot) {
  if (root.$jazz.refs.draft?.id) return root.$jazz.refs.draft.id;

  const draft = DraftBubbleTeaOrder.create({
    addOns: [],
    instructions: "",
  });

  root.$jazz.set("draft", draft);

  return draft.$jazz.id;
}

export function hasChanges(
  order?: co.loaded<
    typeof DraftBubbleTeaOrder,
    { addOns: true; instructions: true }
  > | null,
) {
  if (!order) return false;

  return (
    order.addOns.length > 0 ||
    order.instructions.length > 0 ||
    order.baseTea ||
    order.deliveryDate ||
    order.withMilk
  );
}

/** The root is an app-specific per-user private `CoMap`
 *  where you can store top-level objects for that user */
export const AccountRoot = co.map({
  draft: DraftBubbleTeaOrder.optional(),
  orders: co.list(BubbleTeaOrder),
});
export type AccountRoot = co.loaded<typeof AccountRoot>;

export const JazzAccount = co
  .account({
    root: AccountRoot,
    profile: co.profile(),
  })
  .withMigration((account) => {
    if (!account.$jazz.has("root")) {
      account.$jazz.set("root", {
        orders: [],
      });
    }
  });
