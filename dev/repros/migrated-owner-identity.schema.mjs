import { schema as s } from "../../packages/jazz-tools/dist/index.js";

export const before = { entries: s.table({ text: s.string() }, {}) };
export const after = { ...before, controls: s.table({ value: s.string() }, {}) };
export const oldApp = s.defineApp(before);
export const newApp = s.defineApp(after);
export const oldPermissions = s.definePermissions(oldApp, ({ policy }) => {
  policy.entries.allowRead.always();
  policy.entries.allowInsert.always();
});
export const newPermissions = s.definePermissions(newApp, ({ policy }) => {
  policy.entries.allowRead.always();
  policy.entries.allowInsert.always();
  policy.controls.allowRead.always();
});
export const migration = s.defineMigration({
  from: before,
  to: after,
  createTables: { controls: true },
});
