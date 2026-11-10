/**
 * The framework providers used to open the Inspector overlay on their own in
 * development, and took `autoAttachDevTools` to opt out. They no longer do:
 * a page mounts the overlay itself with `mountInspector(db)` from
 * `jazz-tools/inspector`. Refuse the old prop (plain JavaScript, a Vue
 * template, a cast), so it can't slip through into the client config.
 */
export function refuseAutoAttachDevTools(provider: string, props: object): void {
  const given = props as Record<string, unknown>;
  if (given.autoAttachDevTools === undefined && given["auto-attach-dev-tools"] === undefined)
    return;
  throw new Error(
    `${provider} takes no autoAttachDevTools: providers no longer open the Inspector overlay. ` +
      'Remove the prop. To show the overlay, call mountInspector(db) from "jazz-tools/inspector" ' +
      "in development.",
  );
}

/** The prop type the providers declare, so TypeScript refuses it too. */
export interface NoAutoAttachDevTools {
  /**
   * Removed: providers no longer open the Inspector overlay. Call
   * `mountInspector(db)` from `jazz-tools/inspector` in development instead.
   */
  autoAttachDevTools?: never;
}
