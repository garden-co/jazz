import clsx from "clsx";
import { CoPlainText, ImageDefinition, Account } from "jazz-tools";
import { Image, useCoState } from "jazz-tools/react";
import {
  ImageIcon,
  PencilIcon,
  ReplyIcon,
  SendIcon,
  XIcon,
} from "lucide-react";
import { useId, useRef } from "react";
import { inIframe } from "@/util.ts";

export function AppContainer(props: { children: React.ReactNode }) {
  return (
    <div className="flex flex-col justify-between w-screen h-screen bg-stone-100 dark:bg-stone-925 dark:text-white">
      {props.children}
    </div>
  );
}

export function TopBar(props: { children: React.ReactNode }) {
  return (
    <div
      className={clsx(
        "px-3 pt-2 pb-3 bg-stone-100 w-full flex justify-center items-center gap-2 dark:bg-transparent dark:border-stone-900",
        inIframe &&
          "absolute top-0 left-0 right-0 z-100 from-25% from-stone-100 to-stone-100/0 dark:from-stone-925 dark:to-stone-925/0 bg-gradient-to-b",
      )}
    >
      {props.children}
    </div>
  );
}

export function ChatBody(props: { children: React.ReactNode }) {
  return (
    <div
      className={clsx(
        "flex-1 overflow-y-auto flex flex-col-reverse",
        inIframe && "no-scrollbar",
      )}
      role="application"
    >
      {props.children}
    </div>
  );
}

export function EmptyChatMessage() {
  return (
    <div className="h-full text-base text-stone-500 flex items-center justify-center px-3 md:text-2xl">
      Start a conversation below.
    </div>
  );
}

export function BubbleContainer(props: {
  children: React.ReactNode;
  fromMe: boolean | undefined;
  messageId?: string;
}) {
  const align = props.fromMe ? "items-end" : "items-start";
  return (
    <div
      className={`${align} flex flex-col m-3`}
      role="row"
      data-message-id={props.messageId}
    >
      {props.children}
    </div>
  );
}

export function BubbleBody(props: {
  children: React.ReactNode;
  fromMe: boolean | undefined;
}) {
  return (
    <div
      className={clsx(
        "line-clamp-10 text-ellipsis whitespace-pre-wrap",
        "rounded-2xl overflow-hidden shadow-sm p-1",
        props.fromMe
          ? "bg-white dark:bg-stone-900 dark:text-white"
          : "bg-blue text-white",
      )}
    >
      {props.children}
    </div>
  );
}

export function BubbleText(props: {
  text: CoPlainText | string;
  className?: string;
}) {
  return (
    <p className={clsx("px-2 leading-relaxed", props.className)}>
      {props.text}
    </p>
  );
}

export function BubbleImage(props: { image: ImageDefinition }) {
  return (
    <Image
      imageId={props.image.$jazz.id}
      className="h-auto max-h-80 max-w-full rounded-t-xl mb-1 object-contain"
      height="original"
      width="original"
    />
  );
}

export function BubbleInfo(props: { by: string | undefined; madeAt: number }) {
  const by = useCoState(Account, props.by, { resolve: { profile: true } });
  return (
    <div className="text-xs text-neutral-500 mb-1.5 h-4">
      {by.$isLoaded
        ? by.profile.name +
          " · " +
          new Date(props.madeAt).toLocaleTimeString("en-US", {
            hour12: false,
          })
        : ""}
    </div>
  );
}

export function InputBar(props: { children: React.ReactNode }) {
  return (
    <div className="px-3 pb-3 pt-1 bg-stone-100 mt-auto flex flex-col gap-1 dark:bg-transparent dark:border-stone-900">
      {props.children}
    </div>
  );
}

export function ImageInput({
  onImageChange,
}: {
  onImageChange?: (event: React.ChangeEvent<HTMLInputElement>) => void;
}) {
  const inputRef = useRef<HTMLInputElement>(null);

  const onUploadClick = () => {
    inputRef.current?.click();
  };

  return (
    <>
      <button
        type="button"
        aria-label="Send image"
        title="Send image"
        onClick={onUploadClick}
        className="text-stone-500 dark:text-stone-400 h-10 w-10 grid place-items-center cursor-pointer rounded-full hover:bg-stone-100 hover:text-stone-800 dark:hover:bg-stone-900 dark:hover:text-stone-200 transition-colors"
      >
        <ImageIcon size={20} strokeWidth={1.5} />
      </button>

      <label className="sr-only">
        Image
        <input
          ref={inputRef}
          type="file"
          accept="image/png, image/jpeg, image/gif"
          onChange={onImageChange}
        />
      </label>
    </>
  );
}

export function TextInput(props: {
  onSubmit: (text: string) => void;
  onCancel?: () => void;
  inputRef?: React.RefObject<HTMLInputElement | null>;
  placeholder?: string;
}) {
  const inputId = useId();
  const internalRef = useRef<HTMLInputElement>(null);
  const inputRef = props.inputRef ?? internalRef;

  const handleSubmit = () => {
    const input = inputRef.current;
    if (!input?.value) return;
    props.onSubmit(input.value);
    input.value = "";
  };

  return (
    <div className="flex-1 relative">
      <label className="sr-only" htmlFor={inputId}>
        Type a message and press Enter
      </label>
      <input
        ref={inputRef}
        id={inputId}
        className="rounded-full h-10 px-4 border border-stone-400 block w-full placeholder:text-stone-500 dark:bg-stone-925 dark:text-white dark:border-stone-900"
        placeholder={props.placeholder ?? "Message"}
        maxLength={2048}
        onKeyDown={({ key }) => {
          if (key === "Enter") handleSubmit();
          else if (key === "Escape" && props.onCancel) props.onCancel();
        }}
      />

      <button
        type="button"
        onClick={handleSubmit}
        aria-label="Send message"
        title="Send message"
        className="text-stone-500 dark:text-stone-400 absolute right-1 top-1/2 -translate-y-1/2 h-8 w-8 grid place-items-center cursor-pointer rounded-full hover:bg-stone-100 hover:text-stone-800 dark:hover:bg-stone-900 dark:hover:text-stone-200 transition-colors"
      >
        <SendIcon className="size-4" />
      </button>
    </div>
  );
}

export function BubbleActions(props: {
  fromMe: boolean | undefined;
  onReply: () => void;
  onEdit?: () => void;
}) {
  return (
    <div className="absolute -top-3 right-1 opacity-0 group-hover:opacity-100 focus-within:opacity-100 transition-opacity flex bg-white dark:bg-stone-800 rounded-full shadow-sm border border-stone-200 dark:border-stone-700">
      <button
        type="button"
        onClick={props.onReply}
        className="p-1.5 text-stone-500 hover:text-stone-800 dark:text-stone-400 dark:hover:text-stone-200 hover:bg-stone-100 dark:hover:bg-stone-700 rounded-full transition-colors cursor-pointer"
        title="Reply"
        aria-label="Reply to message"
      >
        <ReplyIcon size={14} />
      </button>
      {props.onEdit && (
        <button
          type="button"
          onClick={props.onEdit}
          className="p-1.5 text-stone-500 hover:text-stone-800 dark:text-stone-400 dark:hover:text-stone-200 hover:bg-stone-100 dark:hover:bg-stone-700 rounded-full transition-colors cursor-pointer"
          title="Edit"
          aria-label="Edit message"
        >
          <PencilIcon size={14} />
        </button>
      )}
    </div>
  );
}

export function ReplyPreviewBubble(props: {
  text: string;
  fromMe: boolean;
  onClick?: () => void;
}) {
  return (
    <div
      className={clsx(
        "mx-2 mt-1.5 px-2 py-1 text-xs rounded border-l-2 truncate",
        props.fromMe
          ? "opacity-70 bg-black/5 dark:bg-white/10 border-blue"
          : "opacity-80 bg-white/15 border-white/50",
        props.onClick && "cursor-pointer",
      )}
      onClick={props.onClick}
    >
      {props.text}
    </div>
  );
}

export function InputBarBanner(props: {
  label: string;
  text?: string;
  onCancel: () => void;
}) {
  return (
    <div className="flex items-center gap-2 px-3 py-1.5 text-xs text-stone-600 dark:text-stone-400 bg-stone-200/50 dark:bg-stone-800/50 rounded-lg">
      <span className="font-medium shrink-0">{props.label}</span>
      {props.text && (
        <span className="truncate flex-1 text-stone-500 dark:text-stone-500">
          {props.text}
        </span>
      )}
      <button
        type="button"
        onClick={props.onCancel}
        className="ml-auto shrink-0 text-stone-400 hover:text-stone-700 dark:hover:text-stone-200 cursor-pointer transition-colors"
        title="Cancel"
        aria-label="Cancel"
      >
        <XIcon size={14} />
      </button>
    </div>
  );
}
