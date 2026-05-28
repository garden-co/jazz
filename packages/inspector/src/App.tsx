import { InspectorRouterProvider, createInspectorRouter } from "./createInspectorRouter";

const router = createInspectorRouter();

export default function App() {
  return <InspectorRouterProvider router={router} />;
}
