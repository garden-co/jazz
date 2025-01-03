import { inIframe, onChatLoad } from "@/util.ts";
import { useIframeHashRouter } from "hash-slash";
import { useAccount } from "jazz-react";
import { Group, ID } from "jazz-tools";
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { ChatScreen } from "./chatScreen.tsx";
import { JazzAndAuth } from "./jazz.tsx";
import { Chat } from "./schema.ts";
import { ThemeProvider } from "./themeProvider.tsx";
import { AppContainer, TopBar } from "./ui.tsx";

export function App() {
  const { me, logOut } = useAccount();
  const router = useIframeHashRouter();

  const createChat = () => {
    if (!me) return;
    const group = Group.create({ owner: me });
    group.addMember("everyone", "writer");
    const chat = Chat.create([], { owner: group });
    router.navigate("/#/chat/" + chat.id);

    // for https://jazz.tools marketing site demo only
    onChatLoad(chat);
  };

  return (
    <AppContainer>
      <TopBar>
        <p>{me?.profile?.name}</p>
        {!inIframe && <button onClick={logOut}>Log out</button>}
      </TopBar>
      {router.route({
        "/": () => createChat() as never,
        "/chat/:id": (id) => <ChatScreen chatID={id as ID<Chat>} />,
      })}
    </AppContainer>
  );
}

createRoot(document.getElementById("root")!).render(
  <ThemeProvider>
    <StrictMode>
      <JazzAndAuth>
        <App />
      </JazzAndAuth>
    </StrictMode>
  </ThemeProvider>,
);
