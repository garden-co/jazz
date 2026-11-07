import { describe, expect, it } from "vitest";
import { columnTransformDependencies } from "./column-transforms.js";

describe("column transform dependency plans", () => {
  it("rejects a self-cycle before building the plan", () => {
    expect(() =>
      columnTransformDependencies({
        price: { dependsOn: ["price"] },
      }),
    ).toThrow('Cyclic transform dependency at "price".');
  });

  it("rejects a transitive cycle before building the plan", () => {
    expect(() =>
      columnTransformDependencies({
        price: { dependsOn: ["currency"] },
        currency: { dependsOn: ["scale"] },
        scale: { dependsOn: ["price"] },
      }),
    ).toThrow('Cyclic transform dependency at "price".');
  });
});
