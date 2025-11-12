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
      <div className="fixed top-4 left-1/2 -translate-x-1/2 pointer-events-none z-50 flex flex-col items-center gap-2">
        <div className="w-0 h-0 border-l-[10px] border-r-[10px] border-b-[14px] border-l-transparent border-r-transparent border-b-blue-500" />
        <span className="rounded-full bg-white/90 px-3 py-1 text-sm font-semibold text-blue-600 shadow-md">
          Share the URL
        </span>
      </div>
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
