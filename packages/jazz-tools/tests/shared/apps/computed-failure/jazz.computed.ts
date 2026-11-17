import type { AppComputed } from "./jazz.generated.js";

export const computed = {
  rows: {
    failure: (): string => {
      throw new Error("computed failure");
    },
  },
} satisfies AppComputed;
