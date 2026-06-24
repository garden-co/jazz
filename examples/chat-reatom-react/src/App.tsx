import { reatomComponent } from "@reatom/react";
import { layoutRoute } from "@/routes";

export const App = reatomComponent(() => layoutRoute.render(), "App");
