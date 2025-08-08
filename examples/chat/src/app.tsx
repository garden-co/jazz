import { useIframeHashRouter } from "hash-slash";
import { Group } from "jazz-tools";
import { JazzInspector } from "jazz-tools/inspector";
import { JazzReactProvider, useAccount } from "jazz-tools/react";
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { apiKey } from "@/apiKey.ts";
import { getRandomUsername, inIframe, onChatLoad } from "@/util.ts";
import { ChatScreen } from "./chatScreen.tsx";
import { Chat } from "./schema.ts";
import { ThemeProvider } from "./themeProvider.tsx";
import { AppContainer, TopBar } from "./ui.tsx";

export function App() {
  const { me, logOut } = useAccount();
  const router = useIframeHashRouter();

  const createChat = () => {
    if (!me) return;
    const group = Group.create();
    group.makePublic("writer");
    const chat = Chat.create([], group);
    router.navigate("/#/chat/" + chat.id);

    // for https://jazz.tools marketing site demo only
    onChatLoad(chat);
  };

  return (
    <AppContainer>
      <TopBar>
        <input
          type="text"
          value={me?.profile?.name ?? ""}
          className="bg-transparent"
          onChange={(e) => {
            if (!me?.profile) return;
            me.profile.name = e.target.value;
          }}
          placeholder="Set username"
        />
        {!inIframe && <button onClick={logOut}>Log out</button>}
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
        sync={{
          peer: `wss://cloud.jazz.tools/?key=${apiKey}`,
        }}
        defaultProfileName={defaultProfileName}
      >
        <App />
        <JazzInspector />
      </JazzReactProvider>
    </StrictMode>
  </ThemeProvider>,
);
