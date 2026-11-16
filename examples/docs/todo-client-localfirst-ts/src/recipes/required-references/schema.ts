import { schema as s } from "jazz-tools";

// #region schema
export const schema = s.defineSchema({
  customers: s.table(
    { name: s.string() },
    {
      orders: s.reverse("orders", "customer"),
    },
  ),
  orders: s.table(
    { customerId: s.uuid() },
    {
      customer: s.rel("customers", "customerId"),
    },
  ),
});
// #endregion schema

// #region permissions
const { definePermissions } = schema;

export const permissions = definePermissions({
  customers: {},
  orders: {},
});
// #endregion permissions
