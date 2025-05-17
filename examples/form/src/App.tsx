import { useIframeHashRouter } from "hash-slash";
import { CreateOrder } from "./CreateOrder.tsx";
import { EditOrder } from "./EditOrder.tsx";
import { Orders } from "./Orders.tsx";

function App() {
  const router = useIframeHashRouter();

  return (
    <>
      <main className="container py-8 space-y-8">
        {router.route({
          "/": () => <Orders />,
          "/order": () => <CreateOrder />,
          "/order/:id": (id) => <EditOrder id={id} />,
        })}
      </main>
    </>
  );
}

export default App;
