// Minimal ambient type for `tiged` — it ships no types. Only the subset
// we actually use (default export: factory returning an object with .clone).
declare module "tiged" {
  interface TigedEmitter {
    clone(dest: string): Promise<void>;
  }

  interface TigedOptions {
    disableCache?: boolean;
    force?: boolean;
    verbose?: boolean;
  }

  function tiged(src: string, options?: TigedOptions): TigedEmitter;

  export default tiged;
}
