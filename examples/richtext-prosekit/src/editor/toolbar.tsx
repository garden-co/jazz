import { useEditor } from "prosekit/react";

import Button from "./button";
import type { EditorExtension } from "./extension";
import { ImageUploadPopover } from "./image-upload-popover";

export default function Toolbar() {
  const editor = useEditor<EditorExtension>({ update: true });

  return (
    <div className="z-2 box-border border-gray-200 dark:border-gray-800 border-solid border-l-0 border-r-0 border-t-0 border-b flex flex-wrap gap-1 p-2 items-center">
      <Button
        pressed={false}
        disabled={!editor.commands.undo.canExec()}
        onClick={editor.commands.undo}
        tooltip="Undo"
      >
        <div className="i-lucide-undo-2 h-5 w-5" />
      </Button>

      <Button
        pressed={false}
        disabled={!editor.commands.redo.canExec()}
        onClick={editor.commands.redo}
        tooltip="Redo"
      >
        <div className="i-lucide-redo-2 h-5 w-5" />
      </Button>

      <Button
        pressed={editor.marks.bold.isActive()}
        disabled={!editor.commands.toggleBold.canExec()}
        onClick={editor.commands.toggleBold}
        tooltip="Bold"
      >
        <div className="i-lucide-bold h-5 w-5" />
      </Button>

      <Button
        pressed={editor.marks.italic.isActive()}
        disabled={!editor.commands.toggleItalic.canExec()}
        onClick={editor.commands.toggleItalic}
        tooltip="Italic"
      >
        <div className="i-lucide-italic h-5 w-5" />
      </Button>

      <Button
        pressed={editor.marks.underline.isActive()}
        disabled={!editor.commands.toggleUnderline.canExec()}
        onClick={editor.commands.toggleUnderline}
        tooltip="Underline"
      >
        <div className="i-lucide-underline h-5 w-5" />
      </Button>

      <Button
        pressed={editor.marks.strike.isActive()}
        disabled={!editor.commands.toggleStrike.canExec()}
        onClick={editor.commands.toggleStrike}
        tooltip="Strike"
      >
        <div className="i-lucide-strikethrough h-5 w-5" />
      </Button>

      <Button
        pressed={editor.marks.code.isActive()}
        disabled={!editor.commands.toggleCode.canExec()}
        onClick={editor.commands.toggleCode}
        tooltip="Code"
      >
        <div className="i-lucide-code h-5 w-5" />
      </Button>

      <Button
        pressed={editor.nodes.codeBlock.isActive()}
        disabled={
          !editor.commands.insertCodeBlock.canExec({ language: "javascript" })
        }
        onClick={() =>
          editor.commands.insertCodeBlock({ language: "javascript" })
        }
        tooltip="Code Block"
      >
        <div className="i-lucide-square-code h-5 w-5" />
      </Button>

      <Button
        pressed={editor.nodes.heading.isActive({ level: 1 })}
        disabled={!editor.commands.toggleHeading.canExec({ level: 1 })}
        onClick={() => editor.commands.toggleHeading({ level: 1 })}
        tooltip="Heading 1"
      >
        <div className="i-lucide-heading-1 h-5 w-5" />
      </Button>

      <Button
        pressed={editor.nodes.heading.isActive({ level: 2 })}
        disabled={!editor.commands.toggleHeading.canExec({ level: 2 })}
        onClick={() => editor.commands.toggleHeading({ level: 2 })}
        tooltip="Heading 2"
      >
        <div className="i-lucide-heading-2 h-5 w-5" />
      </Button>

      <Button
        pressed={editor.nodes.heading.isActive({ level: 3 })}
        disabled={!editor.commands.toggleHeading.canExec({ level: 3 })}
        onClick={() => editor.commands.toggleHeading({ level: 3 })}
        tooltip="Heading 3"
      >
        <div className="i-lucide-heading-3 h-5 w-5" />
      </Button>

      <Button
        pressed={editor.nodes.horizontalRule.isActive()}
        disabled={!editor.commands.insertHorizontalRule.canExec()}
        onClick={() => editor.commands.insertHorizontalRule()}
        tooltip="Divider"
      >
        <div className="i-lucide-minus h-5 w-5"></div>
      </Button>

      <Button
        pressed={editor.nodes.list.isActive({ kind: "bullet" })}
        disabled={!editor.commands.toggleList.canExec({ kind: "bullet" })}
        onClick={() => editor.commands.toggleList({ kind: "bullet" })}
        tooltip="Bullet List"
      >
        <div className="i-lucide-list h-5 w-5" />
      </Button>

      <Button
        pressed={editor.nodes.list.isActive({ kind: "ordered" })}
        disabled={!editor.commands.toggleList.canExec({ kind: "ordered" })}
        onClick={() => editor.commands.toggleList({ kind: "ordered" })}
        tooltip="Ordered List"
      >
        <div className="i-lucide-list-ordered h-5 w-5" />
      </Button>

      <Button
        pressed={editor.nodes.list.isActive({ kind: "task" })}
        disabled={!editor.commands.toggleList.canExec({ kind: "task" })}
        onClick={() => editor.commands.toggleList({ kind: "task" })}
        tooltip="Task List"
      >
        <div className="i-lucide-list-checks h-5 w-5" />
      </Button>

      <Button
        pressed={editor.nodes.list.isActive({ kind: "toggle" })}
        disabled={!editor.commands.toggleList.canExec({ kind: "toggle" })}
        onClick={() => editor.commands.toggleList({ kind: "toggle" })}
        tooltip="Toggle List"
      >
        <div className="i-lucide-list-collapse h-5 w-5" />
      </Button>

      <Button
        pressed={false}
        disabled={!editor.commands.indentList.canExec()}
        tooltip="Increase indentation"
        onClick={() => editor.commands.indentList()}
      >
        <div className="i-lucide-indent-increase h-5 w-5" />
      </Button>

      <Button
        pressed={false}
        disabled={!editor.commands.dedentList.canExec()}
        tooltip="Decrease indentation"
        onClick={() => editor.commands.dedentList()}
      >
        <div className="i-lucide-indent-decrease h-5 w-5" />
      </Button>

      <ImageUploadPopover
        disabled={!editor.commands.insertImage.canExec()}
        tooltip="Insert Image"
      >
        <div className="i-lucide-image h-5 w-5" />
      </ImageUploadPopover>
    </div>
  );
}
