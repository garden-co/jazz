import * as React from "react";
import { co, z } from "jazz-tools";
import { MyAppAccount } from "./schema";

const Chat = co.map({});

class ChatPreview extends React.Component<{ chat: co.loaded<typeof Chat> }> {}

// #region Main
import { useAccount } from "jazz-tools/react";

function DashboardPageComponent() {
  const me = useAccount(MyAppAccount, {
    resolve: {
      profile: true,
      root: {
        myChats: { $each: true },
      },
    },
  });

  return (
    <div>
      <h1>Dashboard</h1>
      {me.$isLoaded ? (
        <div>
          <p>Logged in as {me.profile.name}</p>
          <h2>My chats</h2>
          {me.root.myChats.map((chat) => (
            <ChatPreview key={chat.$jazz.id} chat={chat} />
          ))}
        </div>
      ) : (
        "Loading..."
      )}
    </div>
  );
}
// #endregion
