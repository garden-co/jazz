import { createSSRJazzAgent } from "jazz-tools/ssr";
import { TodoItem } from "./schema";
//[!code hide]
const apiKey = "";

// In order to avoid creating a new agent for every page, if you have more than one page which will be rendered server-side, the agent can be exported from a single centralised module.
export const jazzSSR = createSSRJazzAgent({
  peer: `wss://cloud.jazz.tools/?key=${apiKey}`,
});

export default async function ServerSidePage(props: {
  params: Promise<{ itemId: string }>;
}) {
  const { itemId } = await props.params;
  const item = await TodoItem.load(itemId, {
    loadAs: jazzSSR,
  });

  return (
    <div className="flex flex-col items-center justify-center h-screen gap-4">
      <h1 className="text-2xl font-bold">SSR rendering example with Jazz</h1>
      <div className="text-sm text-gray-500 w-1/2 text-center">
        This is a server component!
      </div>
      {item.$isLoaded &&
        <div className="text-sm">Item title "{item.title}"</div>
      }
    </div>
  );
}