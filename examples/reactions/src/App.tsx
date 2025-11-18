import { useIframeHashRouter } from "hash-slash";
import { Account, Group } from "jazz-tools";
import { useAccount, useLogOut } from "jazz-tools/react";
import { ReactionsScreen } from "./ReactionsScreen.tsx";
import { Reactions } from "./schema.ts";

function App() {
  const me = useAccount(Account, {
    resolve: {
      profile: true,
    },
  });
  const logOut = useLogOut();
  const router = useIframeHashRouter();

  const createReactions = () => {
    const group = Group.create();
    group.addMember("everyone", "writer");
    const chat = Reactions.create([], { owner: group });
    router.navigate("/#/reactions/" + chat.$jazz.id);
  };

  if (!me.$isLoaded) {
    return (
      <div className="flex-1 flex justify-center items-center">Loading...</div>
    );
  }

  return (
    <>
      <header>
        <nav className="container">
          <span>
            You're logged in as <strong>{me?.profile?.name}</strong>
          </span>
          <button className="btn" onClick={() => logOut()}>
            Log out
          </button>
        </nav>
      </header>

      <main className="container">
        {router.route({
          "/": () => createReactions() as never,
          "/reactions/:id": (id) => <ReactionsScreen id={id} />,
        })}
      </main>
    </>
  );
}

export default App;
