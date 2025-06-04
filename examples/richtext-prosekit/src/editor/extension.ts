import { createJazzPlugin } from "jazz-richtext-prosemirror";
import { defineBasicExtension } from "prosekit/basic";
import { definePlugin, union } from "prosekit/core";
import {
  defineCodeBlock,
  defineCodeBlockShiki,
} from "prosekit/extensions/code-block";
import { defineHorizontalRule } from "prosekit/extensions/horizontal-rule";
import { defineMention } from "prosekit/extensions/mention";
import { definePlaceholder } from "prosekit/extensions/placeholder";
import {
  type ReactNodeViewComponent,
  defineReactNodeView,
} from "prosekit/react";

import { JazzPluginConfig } from "jazz-richtext-prosemirror/src/lib/plugin";
import type { CoRichText } from "jazz-tools";
import CodeBlockView from "./code-block-view";
import ImageView from "./image-view";
import { defineImageFileHandlers } from "./upload-file";

export function defineExtension(jazz: {
  coRichText?: CoRichText;
  config?: JazzPluginConfig;
}) {
  return union(
    defineBasicExtension(),
    definePlaceholder({ placeholder: "Press / for commands..." }),
    defineMention(),
    defineCodeBlock(),
    defineCodeBlockShiki(),
    defineHorizontalRule(),
    defineReactNodeView({
      name: "codeBlock",
      contentAs: "code",
      component: CodeBlockView satisfies ReactNodeViewComponent,
    }),
    defineReactNodeView({
      name: "image",
      component: ImageView satisfies ReactNodeViewComponent,
    }),
    defineImageFileHandlers(),
    definePlugin(createJazzPlugin(jazz.coRichText, jazz.config)),
  );
}

export type EditorExtension = ReturnType<typeof defineExtension>;
