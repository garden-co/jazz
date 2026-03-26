/**
 * Preload patch: seed the jest-matchers symbol with the full shape
 * that both @vitest/expect and @playwright/test expect, and mark
 * the property as configurable so neither crashes on redefinition.
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
