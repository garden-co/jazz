# Jazz Vanilla JS Reference

Minimal setup and data patterns for non-framework environments.

## Core Setup

```ts
import { JazzBrowserContextManager } from "jazz-tools/browser";

await new JazzBrowserContextManager().createContext({
  sync: {
    peer: `wss://cloud.jazz.tools?key=${apiKey}`,
    when: "always"
  }
});
```

## Schema Definition

```ts
import { co, z } from "jazz-tools";

const ToDo = co.map({ title: z.string(), completed: z.boolean() });
const ToDoList = co.list(ToDo);
```

## Data Lifecycle

### Reading & Subscribing
Use the schema to subscribe by ID.

```ts
const unsubscribe = ToDoList.subscribe(
  listId,
  { resolve: { $each: true } },
  (list) => {
    // list is an array-like co.loaded object
    console.log(list.length);
  }
);
```

Returns an unsubscribe function which **must** be called when tearing down to avoid memory leaks.

### Loading (One-off)
```ts
const list = await ToDoList.load(listId);
```

### Creating & Mutating
```ts
// Create
const newList = ToDoList.create([{ title: "Task", completed: false }]);
await newList.$jazz.waitForSync();

// Mutate
item.$jazz.set("completed", true);
list.$jazz.push({ title: "New", completed: false });
```

## Image Handling

```ts
import { createImage, loadImage } from "jazz-tools/media";

// Create ImageDefinition
const imageDef = await createImage(file, { placeholder: "blur" });

// Access ID
const id = imageDef.$jazz.id;

// Load image
const loadedImage = await loadImage(imageDefinitionOrId);
if (loadedImage === null) {
  throw new Error("Image not found");
}

// Render image
const img = document.createElement("img");
img.width = loadedImage.width;
img.height = loadedImage.height;
img.src = URL.createObjectURL(loadedImage.image.toBlob()!);
img.onload = () => URL.revokeObjectURL(img.src);
```

More details available here: <https://jazz.tools/docs/react/core-concepts/covalues/imagedef.md>

## Key APIs

*   `JazzBrowserContextManager.createContext(options)`
*   `CoValueSchema.subscribe(id, options, callback)`
*   `CoValueSchema.load(id)`
*   `createImage(file, options)`
*   `loadImage(imageDefinitionOrId)`


