import { useIframeHashRouter } from "hash-slash";
import { Group, ID } from "jazz-tools";
import { ReactionsScreen } from "./ReactionsScreen.tsx";
import { useAccount } from "./main";
import { Reactions } from "./schema.ts";

function App() {
  const { me, logOut } = useAccount();
  const router = useIframeHashRouter();

  const createReactions = () => {
    if (!me) return;
    const group = Group.create({ owner: me });
    group.addMember("everyone", "writer");
    const chat = Reactions.create([], { owner: group });
    router.navigate("/#/reactions/" + chat.id);
  };

  return (
    <div className="container">
      <nav>
        <span>
          You're logged in as <strong>{me?.profile?.name}</strong>
        </span>
        <button className="btn" onClick={() => logOut()}>
          Logout
        </button>
      </nav>

      {router.route({
        "/": () => createReactions() as never,
        "/reactions/:id": (id) => <ReactionsScreen id={id as ID<Reactions>} />,
      })}
    </div>
  );
}

export default App;
