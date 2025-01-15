import { useIframeHashRouter } from "hash-slash";
import { useAccount } from "jazz-react";
import { DocumentComponent } from "jazz-react-prosemirror";
import { CoRichText, ID } from "jazz-tools";

export function App() {
  const { me, logOut } = useAccount();

  const createDocument = () => {
    if (!me) return;

    // Create a new document
    const doc = CoRichText.createFromPlainText("", { owner: me });

    // Update URL after document is created
    setTimeout(() => {
      location.hash = "/doc/" + doc.id;
    }, 100);

    return <div>Creating document...</div>;
  };

  return (
    <div className="flex flex-col items-center w-screen h-screen p-2 dark:bg-black dark:text-white">
      <div className="rounded mb-5 px-2 py-1 text-sm self-end">
        {me?.profile?.name} · <button onClick={logOut}>Log Out</button>
      </div>
      {useIframeHashRouter().route({
        "/": () => createDocument(),
        "/doc/:id": (id) => (
          <div className="border">
            <DocumentComponent docID={id as ID<CoRichText>} />
          </div>
        ),
      })}
    </div>
  );
}

export default App;
