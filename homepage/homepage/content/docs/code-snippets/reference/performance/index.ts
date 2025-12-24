import { co, z } from "jazz-tools";

// #region ImplicitVsExplicit
const SubSubItem = co.map({
  name: z.string(),
});
const SubItem = co.map({
  subSubItem: SubSubItem,
});
const Item = co.map({
  subItem: SubItem,
});

// Implicit CoValue creation
// Results in Group extension for subItem and subSubItem's owners.
const item = Item.create({
  subItem: {
    subSubItem: {
      name: "Example",
    },
  },
});

// Explicit CoValue creation
// Does not result in Group extension.
// @ts-expect-error Redeclared
const fasterItem = Item.create({
  subItem: SubItem.create({
    subSubItem: SubSubItem.create({
      name: "Example",
    }),
  }),
});

// Alternative
const subSubItem = SubSubItem.create({ name: "Example" });
const subItem = SubItem.create({ subSubItem: subSubItem });
// @ts-expect-error Redeclared
const fasterItem = Item.create({ subItem: subItem });
// #endregion

// #region ZObjectsZTuples
const Sprite = co.map({
  position: z.object({ x: z.number(), y: z.number() }),
});

const Location = co.map({
  position: z.tuple([z.number(), z.number()]),
});

const mySprite = Sprite.create({ position: { x: 10, y: 10 } });
mySprite.$jazz.set("position", { x: 20, y: 20 });
// You cannot update 'x' and 'y' independently, only replace the whole object

const myLocation = Location.create({ position: [26.052, -80.209] });
myLocation.$jazz.set("position", [-33.868, -63.987]);
// Note: you cannot replace a single array element, only replace the whole tuple
// #endregion

// #region InitWasmAsync
import { initWasm } from "jazz-tools/wasm";

await initWasm();

// Your code here...
// #endregion
