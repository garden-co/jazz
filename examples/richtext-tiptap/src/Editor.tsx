import { EditorContent, useEditor } from "@tiptap/react";
import StarterKit from "@tiptap/starter-kit";
import clsx from "clsx";
import { JazzSyncExtension } from "jazz-richtext-tiptap";
import { CoRichText } from "jazz-tools";
import { BoldIcon, ItalicIcon, type LucideIcon } from "lucide-react";
import { useCallback } from "react";

export function Editor({ value }: { value: CoRichText }) {
  const extensions = [
    StarterKit,
    // Jazz sync!
    JazzSyncExtension.configure({ coRichText: value }),
  ];

  const editor = useEditor({ extensions });

  const toggleBold = useCallback(() => {
    editor?.chain().focus().toggleBold().run();
  }, [editor]);

  const toggleItalic = useCallback(() => {
    editor?.chain().focus().toggleItalic().run();
  }, [editor]);

  const isBold = editor?.isActive("bold");
  const isItalic = editor?.isActive("italic");

  return (
    <div className="max-w-2xl bg-white rounded-xl shadow-md border border-black/10 w-full">
      <div className="flex p-4 gap-2 border-b border-black/10">
        <EditorMenuToggleButton
          onClick={toggleBold}
          isActive={isBold}
          icon={BoldIcon}
        />
        <EditorMenuToggleButton
          onClick={toggleItalic}
          isActive={isItalic}
          icon={ItalicIcon}
        />
      </div>

      <EditorContent
        editor={editor}
        className="prose prose-sm sm:prose lg:prose-lg xl:prose-xl [&>.ProseMirror]:active:ring-0 [&>.ProseMirror]:p-4"
      />
    </div>
  );
}

function EditorMenuToggleButton({
  onClick,
  isActive,
  icon: Icon,
}: {
  onClick: () => void;
  isActive: boolean | undefined;
  icon: LucideIcon;
}) {
  return (
    <button
      onClick={onClick}
      className={clsx(
        "w-8 h-8 flex items-center justify-center rounded-md text-sm font-medium",
        isActive ? "bg-black text-white" : "bg-gray-100 text-black/70",
      )}
    >
      <Icon size="1.25em" />
    </button>
  );
}
