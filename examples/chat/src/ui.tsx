import clsx from "clsx";
import { CoPlainText, ImageDefinition } from "jazz-tools";
import { Image } from "jazz-tools/react";
import { ImageIcon, SendIcon } from "lucide-react";
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
}) {
  const align = props.fromMe ? "items-end" : "items-start";
  return (
    <div className={`${align} flex flex-col m-3`} role="row">
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
        "rounded-2xl overflow-hidden max-w-[calc(100%-5rem)] shadow-sm p-1",
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
      className="h-auto max-h-80 max-w-full rounded-t-xl mb-1"
      height="original"
      width="original"
    />
  );
}

export function BubbleInfo(props: { by: string | undefined; madeAt: Date }) {
  return (
    <div className="text-xs text-neutral-500 mb-1.5">
      {props.by} ·{" "}
      {props.madeAt.toLocaleTimeString("en-US", {
        hour12: false,
      })}
    </div>
  );
}

export function InputBar(props: { children: React.ReactNode }) {
  return (
    <div className="px-3 pb-3 pt-1 bg-stone-100 mt-auto flex gap-1 dark:bg-transparent dark:border-stone-900">
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

export function TextInput(props: { onSubmit: (text: string) => void }) {
  const inputId = useId();
  const inputRef = useRef<HTMLInputElement>(null);

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
        placeholder="Message"
        maxLength={2048}
        onKeyDown={({ key }) => {
          if (key !== "Enter") return;
          handleSubmit();
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
