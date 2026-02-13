import { co, z, setDefaultValidationMode } from "jazz-tools";

setDefaultValidationMode("strict");

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

export const BubbleTeaOrder = co
  .map({
    baseTea: z.literal([...BubbleTeaBaseTeaTypes]),
    addOns: ListOfBubbleTeaAddOns,
    deliveryDate: z.date(),
    withMilk: z.boolean(),
    instructions: co.plainText(),
  })
  .resolved({
    addOns: true,
    instructions: true,
  });
export type BubbleTeaOrder = co.loaded<typeof BubbleTeaOrder>;

export const PartialBubbleTeaOrder = BubbleTeaOrder.partial({
  baseTea: true,
  deliveryDate: true,
  withMilk: true,
}).resolved({
  addOns: true,
  instructions: true,
});
export type PartialBubbleTeaOrder = co.loaded<typeof PartialBubbleTeaOrder>;

export function validatePartialBubbleTeaOrder(order: PartialBubbleTeaOrder) {
  const errors: string[] = [];

  if (!order.baseTea) {
    errors.push("Please select your preferred base tea.");
  }
  if (!order.deliveryDate) {
    errors.push("Plese select a delivery date.");
  }

  return { errors };
}

/** The root is an app-specific per-user private `CoMap`
 *  where you can store top-level objects for that user */
export const AccountRoot = co.map({
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

export const AccountWithOrders = JazzAccount.resolved({
  root: {
    orders: {
      $each: BubbleTeaOrder.resolveQuery,
    },
  },
});
