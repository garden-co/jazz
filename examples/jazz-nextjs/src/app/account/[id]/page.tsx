import { TodoList } from "@/app/TodoList";
import { jazzSSR } from "@/jazzSSR";
import { JazzAccount } from "@/schema";

export default async function ServerSidePage(props: {
  params: Promise<{ id: string }>;
}) {
  const { id } = await props.params;
  const account = await JazzAccount.load(id, {
    loadAs: jazzSSR,
  });

  if (!account.$isLoaded) {
    return <div>{account.$jazz.loadingState}</div>;
  }

  return (
    <>
      <div className="flex flex-col items-center justify-center h-screen gap-4">
        <TodoList id={account.$jazz.id} preloaded={account.$jazz.export()} />
      </div>
      <footer className="text-sm text-gray-500 absolute bottom-0 left-0 right-0 text-center p-4">
        This list is server-side rendered and works without JavaScript. With
        JavaScript enabled, the list is updated in real-time.
      </footer>
    </>
  );
}
