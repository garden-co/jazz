import { Loaded, co, z } from "jazz-tools";

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

export const ListOfBubbleTeaAddOns = co
  .list(z.literal([...BubbleTeaAddOnTypes]))
  .withHelpers((Self) => ({
    hasChanges(list?: Loaded<typeof Self> | null) {
      return list && Object.entries(list._raw.insertions).length > 0;
    },
  }));

export const BubbleTeaOrder = co.map({
  baseTea: z.literal([...BubbleTeaBaseTeaTypes]),
  addOns: ListOfBubbleTeaAddOns,
  deliveryDate: z.date(),
  withMilk: z.boolean(),
  instructions: z.optional(co.plainText()),
});

export const DraftBubbleTeaOrder = co
  .map({
    baseTea: z.optional(z.literal([...BubbleTeaBaseTeaTypes])),
    addOns: z.optional(ListOfBubbleTeaAddOns),
    deliveryDate: z.optional(z.date()),
    withMilk: z.optional(z.boolean()),
    instructions: z.optional(co.plainText()),
  })
  .withHelpers((Self) => ({
    hasChanges(order: Loaded<typeof Self> | undefined) {
      return (
        !!order &&
        (Object.keys(order._edits).length > 1 ||
          ListOfBubbleTeaAddOns.hasChanges(order.addOns))
      );
    },

    validate(order: Loaded<typeof Self>) {
      const errors: string[] = [];

      if (!order.baseTea) {
        errors.push("Please select your preferred base tea.");
      }
      if (!order.deliveryDate) {
        errors.push("Plese select a delivery date.");
      }

      return { errors };
    },
  }));

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
      const orders = co.list(BubbleTeaOrder).create([], account);
      const draft = DraftBubbleTeaOrder.create(
        {
          addOns: ListOfBubbleTeaAddOns.create([], account),
          instructions: co.plainText().create("", account),
        },
        account,
      );

      account.root = AccountRoot.create({ draft, orders }, account);
    }
  });
