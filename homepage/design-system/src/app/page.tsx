import { ViewsLayout } from "./views/ViewsLayout";
import Colors from "./views/colors/page";

export default function Home() {
  return (
    <main>
      <div className="col-span-8 overflow-y-scroll">
        <ViewsLayout>
          <h1 className="text-2xl font-bold mt-4">Colors</h1>
          <Colors />
        </ViewsLayout>
      </div>
    </main>
  );
}
