import { useEditor } from "prosekit/react";
import {
  PopoverContent,
  PopoverRoot,
  PopoverTrigger,
} from "prosekit/react/popover";
import { type FC, type ReactNode, useState } from "react";

import Button from "./button";
import type { EditorExtension } from "./extension";

export const ImageUploadPopover: FC<{
  tooltip: string;
  disabled: boolean;
  children: ReactNode;
}> = ({ tooltip, disabled, children }) => {
  const [open, setOpen] = useState(false);
  const [webUrl, setWebUrl] = useState("");
  const [objectUrl, setObjectUrl] = useState("");
  const url = webUrl || objectUrl;

  const editor = useEditor<EditorExtension>();

  const handleFileChange: React.ChangeEventHandler<HTMLInputElement> = (
    event,
  ) => {
    const file = event.target.files?.[0];

    if (file) {
      setObjectUrl(URL.createObjectURL(file));
      setWebUrl("");
    } else {
      setObjectUrl("");
    }
  };

  const handleWebUrlChange: React.ChangeEventHandler<HTMLInputElement> = (
    event,
  ) => {
    const url = event.target.value;

    if (url) {
      setWebUrl(url);
      setObjectUrl("");
    } else {
      setWebUrl("");
    }
  };

  const deferResetState = () => {
    setTimeout(() => {
      setWebUrl("");
      setObjectUrl("");
    }, 300);
  };

  const handleSubmit = () => {
    editor.commands.insertImage({ src: url });
    deferResetState();
    setOpen(false);
  };

  const handleOpenChange = (open: boolean) => {
    if (!open) {
      deferResetState();
    }
    setOpen(open);
  };

  return (
    <PopoverRoot open={open} onOpenChange={handleOpenChange}>
      <PopoverTrigger>
        <Button pressed={open} disabled={disabled} tooltip={tooltip}>
          {children}
        </Button>
      </PopoverTrigger>

      <PopoverContent className="flex flex-col gap-y-4 p-6 text-sm w-sm z-10 box-border rounded-lg border border-gray-200 dark:border-gray-800 bg-white dark:bg-gray-950 shadow-lg [&:not([data-state])]:hidden will-change-transform data-[state=open]:animate-in data-[state=closed]:animate-out data-[state=open]:fade-in-0 data-[state=closed]:fade-out-0 data-[state=open]:zoom-in-95 data-[state=closed]:zoom-out-95 data-[state=open]:animate-duration-150 data-[state=closed]:animate-duration-200 data-[side=bottom]:slide-in-from-top-2 data-[side=bottom]:slide-out-to-top-2 data-[side=left]:slide-in-from-right-2 data-[side=left]:slide-out-to-right-2 data-[side=right]:slide-in-from-left-2 data-[side=right]:slide-out-to-left-2 data-[side=top]:slide-in-from-bottom-2 data-[side=top]:slide-out-to-bottom-2">
        {objectUrl ? null : (
          <>
            <label>Embed Link</label>
            <input
              className="flex h-9 rounded-md w-full bg-white dark:bg-gray-950 px-3 py-2 text-sm placeholder:text-gray-500 dark:placeholder:text-gray-500 transition border box-border border-gray-200 dark:border-gray-800 border-solid ring-0 ring-transparent focus-visible:ring-2 focus-visible:ring-gray-900 dark:focus-visible:ring-gray-300 focus-visible:ring-offset-0 outline-none focus-visible:outline-none file:border-0 file:bg-transparent file:text-sm file:font-medium disabled:cursor-not-allowed disabled:opacity-50"
              placeholder="Paste the image link..."
              type="url"
              value={webUrl}
              onChange={handleWebUrlChange}
            />
          </>
        )}

        {webUrl ? null : (
          <>
            <label>Upload</label>
            <input
              className="flex h-9 rounded-md w-full bg-white dark:bg-gray-950 px-3 py-2 text-sm placeholder:text-gray-500 dark:placeholder:text-gray-500 transition border box-border border-gray-200 dark:border-gray-800 border-solid ring-0 ring-transparent focus-visible:ring-2 focus-visible:ring-gray-900 dark:focus-visible:ring-gray-300 focus-visible:ring-offset-0 outline-none focus-visible:outline-none file:border-0 file:bg-transparent file:text-sm file:font-medium disabled:cursor-not-allowed disabled:opacity-50"
              accept="image/*"
              type="file"
              onChange={handleFileChange}
            />
          </>
        )}

        {url ? (
          <button
            className="inline-flex items-center justify-center whitespace-nowrap rounded-md text-sm font-medium ring-offset-white dark:ring-offset-gray-950 transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-gray-900 dark:focus-visible:ring-gray-300 focus-visible:ring-offset-2 disabled:pointer-events-none disabled:opacity-50 border-0 bg-gray-900 dark:bg-gray-50 text-gray-50 dark:text-gray-900 hover:bg-gray-900/90 dark:hover:bg-gray-50/90 h-10 px-4 py-2 w-full"
            onClick={handleSubmit}
          >
            Insert Image
          </button>
        ) : null}
      </PopoverContent>
    </PopoverRoot>
  );
};
