// Override conflicting Element interface from worker-configuration.d.ts in the wrangler tests
// This ensures the native DOM Element.append() method is correctly typed
declare global {
  interface Element {
    append(...nodes: (Node | string)[]): void;
  }
  interface HTMLElement {
    append(...nodes: (Node | string)[]): void;
  }
}

export { };

