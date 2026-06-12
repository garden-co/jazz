export function polyfillGlobal<T>(name: string, getValue: () => T): void {
  Object.defineProperty(globalThis, name, {
    configurable: true,
    enumerable: true,
    get: getValue,
  });
}
