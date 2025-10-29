"use client";

import { Icon } from "@garden-co/design-system/src/components/atoms/Icon";
import { Label } from "@garden-co/design-system/src/components/atoms/Label";
import QRCode from "qrcode";
import clsx from "clsx";
import Link from "next/link";
import { Prose } from "@garden-co/design-system/src/components/molecules/Prose";

import {
  IframeHTMLAttributes,
  useEffect,
  useLayoutEffect,
  useMemo,
  useState,
} from "react";

function StatusBar() {
  const currentTime = new Date().toLocaleTimeString("en-US", {
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  });

  return (
    <div className="flex items-center justify-between bg-[#f5f3f3] px-3 py-2 text-sm font-bold text-black dark:bg-stone-925 dark:text-white">
      {currentTime}
      <svg
        xmlns="http://www.w3.org/2000/svg"
        width="80"
        height="18"
        fill="currentColor"
        viewBox="0 0 80 18"
      >
        <path
          fillRule="evenodd"
          d="M19.528 4.033c0-.633-.477-1.146-1.066-1.146h-1.067c-.59 0-1.067.513-1.067 1.146v9.934c0 .633.478 1.146 1.067 1.146h1.067c.589 0 1.066-.513 1.066-1.146zm-7.434 1.3h1.067c.589 0 1.066.525 1.066 1.173v7.434c0 .648-.477 1.173-1.066 1.173h-1.067c-.59 0-1.067-.525-1.067-1.173V6.506c0-.648.478-1.174 1.067-1.174M7.762 7.98H6.696c-.59 0-1.067.532-1.067 1.189v4.755c0 .656.477 1.188 1.067 1.188h1.066c.59 0 1.067-.532 1.067-1.188V9.17c0-.657-.478-1.189-1.067-1.189m-5.3 2.446H1.394c-.59 0-1.067.524-1.067 1.171v2.344c0 .647.478 1.171 1.067 1.171H2.46c.59 0 1.067-.524 1.067-1.171v-2.344c0-.647-.477-1.171-1.067-1.171M36.1 5.302c2.487 0 4.879.923 6.681 2.576a.355.355 0 0 0 .487-.004l1.297-1.263a.34.34 0 0 0-.003-.494c-4.73-4.375-12.195-4.375-16.926 0a.342.342 0 0 0-.003.494l1.298 1.263c.133.13.35.132.486.004 1.803-1.654 4.195-2.576 6.683-2.576m-.004 4.22c1.358 0 2.667.512 3.673 1.436.136.131.35.129.483-.006l1.287-1.32a.367.367 0 0 0-.005-.518 7.9 7.9 0 0 0-10.873 0 .367.367 0 0 0-.005.519l1.287 1.319a.343.343 0 0 0 .483.006 5.43 5.43 0 0 1 3.67-1.435m2.525 2.794a.4.4 0 0 1-.103.28l-2.176 2.456a.32.32 0 0 1-.242.112.32.32 0 0 1-.242-.112l-2.177-2.455a.4.4 0 0 1-.102-.28.4.4 0 0 1 .113-.277c1.39-1.314 3.426-1.314 4.816 0 .07.071.11.17.113.276"
          clipRule="evenodd"
        />
        <path
          d="M71.17 14.5v1h-12v-1zm5.5-5.5c0-1.039 0-1.767-.04-2.338-.04-.56-.113-.894-.223-1.152a3.3 3.3 0 0 0-1.747-1.747c-.258-.11-.591-.184-1.152-.223-.57-.04-1.299-.04-2.338-.04h-12c-1.04 0-1.767 0-2.338.04-.56.04-.894.113-1.152.223a3.3 3.3 0 0 0-1.747 1.747c-.11.258-.184.591-.223 1.152-.04.57-.04 1.299-.04 2.338s0 1.768.04 2.338c.04.561.113.894.223 1.152.334.787.96 1.413 1.747 1.748.258.11.591.183 1.152.222.57.04 1.299.04 2.338.04v1l-1.358-.005c-1.192-.016-1.92-.08-2.524-.338a4.3 4.3 0 0 1-2.19-2.085l-.085-.19c-.343-.806-.343-1.831-.343-3.882 0-1.922 0-2.943.282-3.727l.06-.155a4.3 4.3 0 0 1 2.087-2.19l.19-.085c.604-.257 1.331-.322 2.523-.338L59.17 2.5h12c2.05 0 3.076 0 3.882.343a4.3 4.3 0 0 1 2.275 2.275c.343.806.343 1.832.343 3.882s0 3.076-.343 3.882l-.086.19a4.3 4.3 0 0 1-2.19 2.085l-.154.061c-.784.282-1.805.282-3.727.282v-1c1.04 0 1.767 0 2.338-.04.56-.039.894-.113 1.152-.223a3.3 3.3 0 0 0 1.747-1.747c.11-.258.184-.591.223-1.152.04-.57.04-1.299.04-2.338"
          opacity=".35"
        />
        <path
          d="M78.67 7.281v4.076a2.21 2.21 0 0 0 1.328-2.038c0-.89-.523-1.693-1.328-2.038"
          opacity=".4"
        />
        <path d="M54.67 8.5c0-1.4 0-2.1.272-2.635a2.5 2.5 0 0 1 1.093-1.092C56.57 4.5 57.27 4.5 58.67 4.5h13c1.4 0 2.1 0 2.635.273a2.5 2.5 0 0 1 1.092 1.092c.273.535.273 1.235.273 2.635v1c0 1.4 0 2.1-.273 2.635a2.5 2.5 0 0 1-1.092 1.093c-.535.272-1.235.272-2.635.272h-13c-1.4 0-2.1 0-2.635-.272a2.5 2.5 0 0 1-1.093-1.093C54.67 11.6 54.67 10.9 54.67 9.5z" />
      </svg>
    </div>
  );
}

function Iframe(
  props: IframeHTMLAttributes<HTMLIFrameElement> & {
    user: string;
    className?: string;
  },
) {
  const { src, user, className } = props;

  return (
    <div
      className={clsx(
        "relative w-full overflow-hidden rounded-[28px] border-[6px] border-black shadow-[0px_0px_0px_3px_rgba(0,0,0,0.15)] dark:bg-black dark:shadow-[0px_0px_0px_3px_rgba(255,255,255,0.2)]",
        className,
      )}
    >
      <StatusBar />
      <iframe
        {...props}
        src={src}
        title="Jazz chat demo"
        className="w-full"
        width="200"
        height="390"
        allowFullScreen
      />
    </div>
  );
}

export function ChatDemoSection() {
  const [chatId, setChatId] = useState<string | undefined>();

  const user1 = "Alice";
  const user2 = "Bob";

  const [server1, setServer1] = useState<string | null>();
  const [server2, setServer2] = useState<string | null>();
  const [shareUrl, setShareUrl] = useState<string | null>();
  const [qrCode, setQrCode] = useState<string | null>();

  let [copyCount, setCopyCount] = useState(0);
  let copied = copyCount > 0;

  useEffect(() => {
    if (copyCount > 0) {
      let timeout = setTimeout(() => setCopyCount(0), 1000);
      return () => {
        clearTimeout(timeout);
      };
    }
  }, [copyCount]);

  useLayoutEffect(() => {
    if (typeof window === "undefined") return;

    const isLocal = window.location.hostname === "localhost";

    if (chatId) {
      const shareServer = isLocal
        ? "http://localhost:5173"
        : "https://chat.jazz.tools";
      const url = `${shareServer}/${chatId}`;
      setShareUrl(url);

      QRCode.toDataURL(url, {
        errorCorrectionLevel: "L",
      }).then((dataUrl) => {
        setQrCode(dataUrl);
      });

      return; // Once the chatId is set, we don't need to listen for messages anymore
    }

    setServer1(
      (isLocal ? "http://localhost:5173" : "https://jazz-chat-1.vercel.app") +
        `?user=${user1}`,
    );
    setServer2(
      (isLocal ? "http://localhost:5174" : "https://jazz-chat-2.vercel.app") +
        `?user=${user2}`,
    );

    if (!server1 || !server2) return;

    const server1Url = new URL(server1);

    const listener = (e: MessageEvent) => {
      const isValidOrigin = e.origin === server1Url.origin;

      if (e.data.type === "navigate" && isValidOrigin) {
        setChatId(new URL(e.data.url).hash);
      }
    };
    window.addEventListener("message", listener);
    return () => {
      window.removeEventListener("message", listener);
    };
  }, [chatId, server1, server2]);

  const server2WithSameChatId = useMemo(() => {
    if (chatId && server2) {
      const server2Url = new URL(server2);
      server2Url.hash = chatId;
      return server2Url.toString();
    }

    return null;
  }, [chatId, server2]);

  if (!server1) return null;

  const copyUrl = () => {
    if (shareUrl) {
      navigator.clipboard.writeText(shareUrl).then(() => {
        setCopyCount((count) => count + 1);
      });
    }
  };

  return (
    <div className="container grid items-start md:grid-cols-12">
      <div className="md:col-span-7">
        <div className="grid items-start gap-4 sm:grid-cols-2 sm:gap-0">
          <Iframe
            src={server1}
            user={user1}
            title="Jazz chat demo user 1"
            className="chat-1 md:-mt-12"
          />
          {server2WithSameChatId && (
            <Iframe
              src={server2WithSameChatId}
              user={user2}
              title="Jazz chat demo user 2"
              className="chat-2 md:-mt-2"
            />
          )}
        </div>

        <Prose className="hidden md:block">
          <p className="mt-4 text-center text-sm">
            A chat app with image upload in ~300 lines of{" "}
            <Link
              href="https://github.com/garden-co/jazz/tree/main/examples/chat"
              target="_blank"
            >
              client-side code
              <span className="relative -left-0.5 -top-0.5 -mr-2 inline-block text-muted">
                ⌝
              </span>
            </Link>
            .
          </p>
        </Prose>
      </div>
      {chatId && shareUrl && (
        <div className="mt-6 flex flex-col justify-between gap-6 text-center md:col-span-4 md:col-start-9 md:mt-0">
          <div>
            <h2 className="mb-2 hidden font-display text-xl font-semibold tracking-tight text-stone-950 dark:text-white md:block md:text-2xl">
              Scan the QR code
            </h2>
            <p className="text-balance">
              Add your device to the live demo. Messages update everywhere
              instantly.
            </p>
          </div>

          {qrCode && (
            <img
              src={qrCode}
              className="mx-auto hidden size-48 rounded-lg border md:block"
              alt="Scan this QR code to join the chat"
            />
          )}
          <div className="hidden items-center gap-2 md:flex">
            <div className="h-px w-full border-t" />
            <p className="whitespace-nowrap">or copy the URL</p>
            <div className="h-px w-full border-t" />
          </div>
          <div className="relative w-full sm:mx-auto sm:max-w-xl">
            <Label
              label="To join the chat, copy the URL"
              htmlFor="shareUrl"
              className="sr-only"
            />
            <input
              id="shareUrl"
              className="h-10 w-full rounded-md border bg-transparent pl-3 pr-10"
              type="text"
              value={shareUrl}
              onClick={(e) => e.currentTarget.select()}
              onBlur={(e) => e.currentTarget.setSelectionRange(0, 0)}
              readOnly
            />
            <button
              type="button"
              className="absolute right-0 top-0 p-3 text-primary dark:text-blue-400"
              onClick={copyUrl}
            >
              {copied ? (
                <Icon name="check" size="xs" />
              ) : (
                <Icon name="copy" size="xs" />
              )}
              <span className="sr-only">Copy URL</span>
            </button>
          </div>
        </div>
      )}
    </div>
  );
}
