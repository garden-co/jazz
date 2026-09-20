import { schema } from "../../src/index.js";

// Name-blind public fixture for #2363's two forward-reference carriers.
export const liveEdgeApp = schema.defineApp({
  parents: schema.table(
    { name: schema.string() },
    { itemsViaParent: schema.reverse("items", "parent") },
  ),
  authors: schema.table(
    { name: schema.string() },
    { itemsViaAuthor: schema.reverse("items", "author") },
  ),
  labels: schema.table(
    { name: schema.string() },
    { itemsViaLabel: schema.reverse("items", "label") },
  ),
  items: schema.table(
    {
      title: schema.string(),
      parent_id: schema.uuid(),
      author_id: schema.uuid(),
      label_id: schema.uuid(),
    },
    {
      parent: schema.rel("parents", "parent_id"),
      author: schema.rel("authors", "author_id"),
      label: schema.rel("labels", "label_id"),
    },
  ),
  unrelated: schema.table({ value: schema.string() }, {}),
});
export const liveEdgePermissions = schema.definePermissions(liveEdgeApp, ({ policy }) => [
  policy.parents.allowRead.always(),
  policy.parents.allowInsert.always(),
  policy.authors.allowRead.always(),
  policy.authors.allowInsert.always(),
  policy.labels.allowRead.always(),
  policy.labels.allowInsert.always(),
  policy.items.allowRead.always(),
  policy.items.allowInsert.always(),
  policy.unrelated.allowRead.always(),
  policy.unrelated.allowInsert.always(),
]);
export interface LiveEdgeSeed {
  parentId: string;
  authorId: string;
  labelId: string;
  itemId: string;
}
