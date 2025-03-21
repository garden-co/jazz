import React from "react";
import ReactDOM from "react-dom/client";
import {
  Link,
  RouterProvider,
  createHashRouter,
  useNavigate,
} from "react-router-dom";
import "./index.css";

import {
  JazzProvider,
  PasskeyAuthBasicUI,
  useAcceptInvite,
  useAccount,
} from "jazz-react";

import { PetAccount, PetPost } from "./1_schema.ts";
import { NewPetPostForm } from "./3_NewPetPostForm.tsx";
import { RatePetPostUI } from "./4_RatePetPostUI.tsx";
import { apiKey } from "./apiKey.ts";
import {
  Button,
  ThemeProvider,
  TitleAndLogo,
} from "./basicComponents/index.ts";

const peer =
  (new URL(window.location.href).searchParams.get(
    "peer",
  ) as `ws://${string}`) ?? `wss://cloud.jazz.tools/?key=${apiKey}`;

const appName = "Jazz Rate My Pet Example";

declare module "jazz-react" {
  interface Register {
    Account: PetAccount;
  }
}

/** Walkthrough: The top-level provider `<JazzProvider/>`
 *
 *  This shows how to use the top-level provider `<JazzProvider/>`,
 *  which provides the rest of the app with a `LocalNode` (used through `useCoState` later),
 *  based on `PasskeyAuth` that uses PassKeys (aka WebAuthn) to store a user's account secret
 *  - no backend needed. */
ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <ThemeProvider>
      <TitleAndLogo name={appName} />
      <div className="flex flex-col h-full items-center justify-start gap-10 pt-10 pb-10 px-5">
        <JazzProvider
          sync={{
            peer,
          }}
          AccountSchema={PetAccount}
        >
          <PasskeyAuthBasicUI appName={appName}>
            <App />
          </PasskeyAuthBasicUI>
        </JazzProvider>
      </div>
    </ThemeProvider>
  </React.StrictMode>,
);

/** Walkthrough: Creating pet posts & routing in `<App/>`
 *
 *  <App> is the main app component, handling client-side routing based
 *  on the CoValue ID (CoID) of our PetPost, stored in the URL hash
 *  - which can also contain invite links.
 */
export default function App() {
  const { logOut } = useAccount();

  const router = createHashRouter([
    {
      path: "/",
      element: <PostOverview />,
    },
    {
      path: "/new",
      element: <NewPetPostForm />,
    },
    {
      path: "/pet/:petPostId",
      element: <RatePetPostUI />,
    },
    {
      path: "/invite/*",
      element: <AcceptInvite />,
    },
  ]);

  return (
    <>
      <RouterProvider router={router} />

      <Button
        onClick={() => router.navigate("/").then(() => logOut())}
        variant="outline"
      >
        Log out
      </Button>
    </>
  );
}

function AcceptInvite() {
  const navigate = useNavigate();
  useAcceptInvite({
    invitedObjectSchema: PetPost,
    onAccept: (petPostID) => navigate("/pet/" + petPostID),
  });

  return <p>Accepting invite...</p>;
}

export function PostOverview() {
  const { me } = useAccount();

  const myPosts = me?.root?.posts;

  return (
    <>
      {myPosts?.length ? (
        <>
          <h1>My posts</h1>
          {myPosts.map(
            (post) =>
              post && (
                <Link key={post.id} to={"/pet/" + post.id}>
                  {post.name}
                </Link>
              ),
          )}
        </>
      ) : undefined}
      <Link to="/new">New post</Link>
    </>
  );
}
