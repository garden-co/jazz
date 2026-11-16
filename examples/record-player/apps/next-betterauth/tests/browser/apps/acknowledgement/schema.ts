import { schema as s } from "jazz-tools";

export const schema = s.defineSchema({
  receipts: s.table({ label: s.string() }),
});

const { definePermissions } = schema;

export const permissions = definePermissions({
  receipts: {
    read: true,
    insert: true,
  },
});
