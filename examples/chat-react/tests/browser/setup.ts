const benignResizeObserverMessage = "ResizeObserver loop completed with undelivered notifications.";
const originalConsoleError = console.error;

// Chromium reports this benign layout-observer condition as an ErrorEvent with
// no `error` value. Vitest converts that exact event into a console error; keep
// every other browser error and console error visible.
console.error = (...args: Parameters<typeof console.error>) => {
  const [first] = args;
  if (
    args.length === 1 &&
    first instanceof Error &&
    first.message === benignResizeObserverMessage
  ) {
    return;
  }

  originalConsoleError(...args);
};
