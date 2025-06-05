import {
  BlockHandleAdd,
  BlockHandleDraggable,
  BlockHandlePopover,
} from "prosekit/react/block-handle";

export default function BlockHandle() {
  return (
    <BlockHandlePopover className="flex items-center flex-row box-border justify-center transition border-0 [&:not([data-state])]:hidden will-change-transform data-[state=open]:animate-in data-[state=closed]:animate-out data-[state=open]:fade-in-0 data-[state=closed]:fade-out-0 data-[state=open]:zoom-in-95 data-[state=closed]:zoom-out-95 data-[state=open]:animate-duration-150 data-[state=closed]:animate-duration-200">
      <BlockHandleAdd className="flex items-center box-border justify-center h-[1.5em] w-[1.5em] hover:bg-gray-100 dark:hover:bg-gray-800 rounded text-gray-500/50 dark:text-gray-500/50 cursor-pointer">
        <div className="i-lucide-plus h-5 w-5" />
      </BlockHandleAdd>
      <BlockHandleDraggable className="flex items-center box-border justify-center h-[1.5em] w-[1.2em] hover:bg-gray-100 dark:hover:bg-gray-800 rounded text-gray-500/50 dark:text-gray-500/50 cursor-grab">
        <div className="i-lucide-grip-vertical h-5 w-5" />
      </BlockHandleDraggable>
    </BlockHandlePopover>
  );
}
