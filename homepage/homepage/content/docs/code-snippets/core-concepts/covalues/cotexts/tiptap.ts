// @ts-expect-error package not installed
import { Extension } from "@tiptap/core";
import { CoRichText } from "jazz-tools";
import { createJazzPlugin } from "jazz-tools/prosemirror";

export interface JazzSyncOptions {
  /** The CoRichText instance to synchronize with */
  coRichText: CoRichText;
  /** Configuration options for the plugin */
  config?: Parameters<typeof createJazzPlugin>[1];
}

export const JazzSyncExtension = Extension.create<JazzSyncOptions>({
  name: "jazzSync",

  addOptions() {
    return {
      // TipTap treats extension options as optional...
      coRichText: undefined as any,
      config: {},
    };
  },

  addProseMirrorPlugins() {
    // ...so we check that it exists at runtime.
    if (!this.options.coRichText) {
      throw new Error("JazzSyncExtension requires a CoRichText value");
    }
    return [createJazzPlugin(this.options.coRichText, this.options.config)];
  },
});
