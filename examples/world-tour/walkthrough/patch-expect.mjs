/**
 * Preload patch: seed the jest-matchers symbol with the full shape
 * that both @vitest/expect and @playwright/test expect, and mark
 * the property as configurable so neither crashes on redefinition.
 *
 * Workaround for vitest 4.x + @playwright/test collision where both
 * try to define a non-configurable globalThis[Symbol.for("$$jest-matchers-object")].
 * See: https://github.com/vitest-dev/vitest/discussions/4289
 * Remove this once vitest or playwright resolve the incompatibility.
 */
const symbol = Symbol.for("$$jest-matchers-object");
if (!(symbol in globalThis)) {
  Object.defineProperty(globalThis, symbol, {
    value: {
      matchers: Object.create(null),
      customEqualityTesters: [],
      state: {
        assertionCalls: 0,
        expectedAssertionsNumber: null,
        isExpectingAssertions: false,
        numPassingAsserts: 0,
        suppressedErrors: [],
      },
    },
    writable: true,
    configurable: true,
  });
}
