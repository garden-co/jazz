# Column Metadata

## What

Allow attaching arbitrary key/value metadata to schema columns, like Zod's `.meta()`, so UI code can drive itself off the schema as a single source of truth (impossible to forget to render a field).

## Notes

Example shape from the user:

```ts
const schema = {
  tasks: s.table({
    description: s.string().meta({
      component: "text",
      helper: "Write a brief description of the task",
      formColumns: 12,
      tableWidth: 250,
    }),
  }),
};
```

Reference: https://zod.dev/metadata?id=meta#meta — classic Jazz had this and the user's UI was tightly coupled to the schema as a result.
