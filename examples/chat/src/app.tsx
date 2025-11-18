import { apiKey } from "@/apiKey.ts";
import { getRandomUsername, inIframe, onChatLoad } from "@/util.ts";
import { useIframeHashRouter } from "hash-slash";
import { co, getLoadedOrUndefined, Group } from "jazz-tools";
import { JazzInspector } from "jazz-tools/inspector";
import { JazzReactProvider, useAccount, useLogOut } from "jazz-tools/react";
import { StrictMode, useId, useMemo, useState, useEffect, useRef } from "react";
import { createRoot } from "react-dom/client";
import Jazzicon from "react-jazzicon";
import { ChatScreen } from "./chatScreen.tsx";
import { Chat, Message } from "./schema.ts";
import { ThemeProvider } from "./themeProvider.tsx";
import { AppContainer, TopBar } from "./ui.tsx";

function stringToSeed(str: string): number {
  let hash = 0;
  for (let i = 0; i < str.length; i++) {
    const char = str.charCodeAt(i);
    hash = (hash << 5) - hash + char;
    hash = hash & hash;
  }
  return Math.abs(hash);
}

const AccountWithProfile = co.account().resolved({
  profile: true,
});

export function App() {
  const me = useAccount(AccountWithProfile);
  const logOut = useLogOut();
  const router = useIframeHashRouter();
  const inputId = useId();
  const [localValue, setLocalValue] = useState("");
  const [inputWidth, setInputWidth] = useState(120);
  const spanRef = useRef<HTMLSpanElement>(null);

  const profile = getLoadedOrUndefined(me)?.profile;

  const avatarSeed = useMemo(() => {
    if (!me?.$jazz?.id) return 0;
    return stringToSeed(me.$jazz.id);
  }, [me?.$jazz?.id]);

  useEffect(() => {
    setLocalValue(profile?.name ?? "");
  }, [profile?.name]);

  useEffect(() => {
    if (spanRef.current) {
      const width = spanRef.current.offsetWidth;
      setInputWidth(Math.max(width + 4, 20));
    }
  }, [localValue]);

  const createChat = () => {
    if (!me) return;
    const group = Group.create();
    group.makePublic("writer");
    const chat = Chat.create([], group);

    chat.$jazz.push(Message.create({ text: "Hello world" }, group));

    router.navigate("/#/chat/" + chat.$jazz.id);

    // for https://jazz.tools marketing site demo only
    onChatLoad(chat);
  };

  const usernamePlaceholder = "Set username";

  return (
    <AppContainer>
      <TopBar>
        <label htmlFor={inputId} className="inline-flex">
          <Jazzicon diameter={28} seed={avatarSeed} />
          <span className="sr-only">Username</span>
        </label>
        <div className="relative">
          <span
            ref={spanRef}
            className="absolute invisible whitespace-pre text-lg"
            aria-hidden="true"
          >
            {localValue || usernamePlaceholder}
          </span>
          <input
            type="text"
            id={inputId}
            value={localValue}
            style={{ width: `${inputWidth}px` }}
            className="bg-transparent text-lg outline-none min-w-0 max-w-full"
            onChange={(e) => {
              setLocalValue(e.target.value);
              if (!profile) return;
              profile.$jazz.set("name", e.target.value);
            }}
            placeholder={usernamePlaceholder}
          />
        </div>
        {!inIframe && (
          <button
            type="button"
            className="cursor-pointer ml-auto"
            onClick={logOut}
          >
            Log out
          </button>
        )}
      </TopBar>
      {router.route({
        "/": () => createChat() as never,
        "/chat/:id": (id) => <ChatScreen chatID={id} />,
      })}
    </AppContainer>
  );
}

const url = new URL(window.location.href);
const defaultProfileName = url.searchParams.get("user") ?? getRandomUsername();

createRoot(document.getElementById("root")!).render(
  <ThemeProvider>
    <StrictMode>
      <JazzReactProvider
        authSecretStorageKey="examples/chat"
        sync={{
          peer: `wss://cloud.jazz.tools/?key=${apiKey}`,
        }}
        defaultProfileName={defaultProfileName}
      >
        <App />
        {!inIframe && <JazzInspector />}
      </JazzReactProvider>
    </StrictMode>
  </ThemeProvider>,
);
