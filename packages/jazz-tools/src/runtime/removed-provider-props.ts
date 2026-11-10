/**
 * The framework providers used to open the Inspector overlay on their own in
 * development, and took `autoAttachDevTools` to opt out. They no longer do:
 * a page mounts the overlay itself with `mountInspector(db)` from
 * `jazz-tools/inspector`. Refuse the old prop (plain JavaScript, a Vue
 * template, a cast), so it can't slip through into the client config.
 */
export function refuseAutoAttachDevTools(provider: string, props: object): void {
  if (!Object.hasOwn(props, "autoAttachDevTools") && !Object.hasOwn(props, "auto-attach-dev-tools"))
    return;
  throw new Error(
    `${provider} takes no autoAttachDevTools: providers no longer open the Inspector overlay. ` +
      'Remove the prop. To show the overlay, call mountInspector(db) from "jazz-tools/inspector" ' +
      "in development.",
  );
}
