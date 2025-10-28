import { co, z } from "jazz-tools";
const Inventory = co.record(z.string(), z.number());

// #region BracketNotation
const inventory = Inventory.create({
  tomatoes: 48,
  peppers: 24,
  basil: 12,
});

console.log(inventory["tomatoes"]); // 48
// #endregion
