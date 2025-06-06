import "prosekit/basic/style.css";
import "prosekit/basic/typography.css";

import { createEditor } from "prosekit/core";
import { ProseKit } from "prosekit/react";
import { memo, useMemo } from "react";

import { CoRichText } from "jazz-tools";
import BlockHandle from "./block-handle";
import { defineExtension } from "./extension";
import InlineMenu from "./inline-menu";
import SlashMenu from "./slash-menu";
import TagMenu from "./tag-menu";
import Toolbar from "./toolbar";
import UserMenu from "./user-menu";

interface EditorProps {
  coRichText?: CoRichText;
}

function EditorComponent({ coRichText }: EditorProps) {
  const editor = useMemo(() => {
    const extension = defineExtension({ coRichText });
    return createEditor({ extension });
  }, [coRichText]);

  return (
    <ProseKit editor={editor}>
      <div className="box-border h-full w-full min-h-72 overflow-y-hidden overflow-x-hidden rounded-md border border-solid border-gray-200 dark:border-gray-700 shadow flex flex-col color-black dark:color-white  bg-white">
        <Toolbar />
        <div className="relative bg-white w-full flex-1 box-border overflow-y-scroll">
          <div
            ref={editor.mount}
            className="ProseMirror bg-white box-border min-h-full px-[max(4rem,_calc(50%-20rem))] py-8 outline-none outline-0 [&_span[data-mention=user]]:text-blue-500 [&_span[data-mention=tag]]:text-violet-500"
          ></div>
          <InlineMenu />
          <SlashMenu />
          <UserMenu />
          <TagMenu />
          <BlockHandle />
        </div>
      </div>
    </ProseKit>
  );
}

const EditorMemo = memo(EditorComponent);

export default EditorMemo;
