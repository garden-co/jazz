// @ts-expect-error package not installed
import { createEditor, definePlugin } from "prosekit/core";
import { createJazzPlugin } from "jazz-tools/prosemirror";
import { co } from "jazz-tools";

const coRichText = co.richText().create("");
const extension = definePlugin(createJazzPlugin(coRichText))
export const editor = createEditor({ extension });