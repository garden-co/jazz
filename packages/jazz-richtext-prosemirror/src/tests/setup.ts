import * as matchers from "@testing-library/jest-dom/matchers";
import { expect } from "vitest";

expect.extend(matchers);

declare module "vitest" {
  interface Assertion<T = any>
    extends matchers.TestingLibraryMatchers<T, void> {}
}
