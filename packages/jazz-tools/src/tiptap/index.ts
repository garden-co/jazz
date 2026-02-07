import { Extension } from "@tiptap/core";
import type { Plugin } from "@tiptap/pm/state";
import type { CoRichText } from "jazz-tools";
import { createJazzPlugin } from "jazz-tools/prosemirror";

export interface JazzSyncOptions {
  /** The CoRichText instance to synchronize with */
  coRichText: CoRichText | null;
  /** Configuration options for the plugin */
  config?: Parameters<typeof createJazzPlugin>[1];
}

export const JazzSyncExtension = Extension.create<JazzSyncOptions>({
  name: "jazzSync",

  addOptions() {
    return {
      coRichText: null,
      config: {},
    };
  },

  addProseMirrorPlugins(): Plugin[] {
    const { coRichText, config } = this.options;

    if (!coRichText) {
      throw new Error("JazzSyncExtension requires a CoRichText value");
    }

    return [createJazzPlugin(coRichText, config)];
  },
});
