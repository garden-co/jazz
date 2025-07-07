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

export const ListOfBubbleTeaAddOns = co.list(z.enum(BubbleTeaAddOnTypes));

export const BubbleTeaOrder = co.map({
  baseTea: z.enum(BubbleTeaBaseTeaTypes),
  addOns: ListOfBubbleTeaAddOns,
  deliveryDate: z.date(),
  withMilk: z.boolean(),
  instructions: z.optional(co.plainText()),
});

/** The root is an app-specific per-user private `CoMap`
 *  where you can store top-level objects for that user */
export const AccountRoot = co.map({
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
      account.root = AccountRoot.create({ orders }, account);
    }
  });
