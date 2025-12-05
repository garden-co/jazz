import { JazzBrowserContextManager } from "jazz-tools/browser";
import { Festival, JazzFestAccount } from "./schema";
import { FestivalComponent } from "./FestivalComponent";
import { NewBandComponent } from "./NewBandComponent";
import { assertLoaded } from "jazz-tools";

// @ts-expect-error Not a real vite app
const apiKey = import.meta.env.VITE_JAZZ_API_KEY;
const contextManager = new JazzBrowserContextManager<typeof JazzFestAccount>();
await contextManager.createContext({
  sync: {
    peer: `wss://cloud.jazz.tools?key=${apiKey}`,
  },
});

function getCurrentAccount() {
  const context = contextManager.getCurrentValue();
  if (!context || !("me" in context)) {
    throw new Error("");
  }

  return context.me;
}

// @ts-expect-error This is baseline now: https://web.dev/blog/baseline-urlpattern
const festivalRoute = new URLPattern({ pathname: "/festival/:festivalId" });
const result = festivalRoute.exec(location.href);
let { festivalId } = result?.pathname?.groups ?? {};
let festival;
if (festivalId) {
  festival = await Festival.load(festivalId);
} else {
  const me = getCurrentAccount();
  const account = await JazzFestAccount.load(me.$jazz.id);
  if (!account.$isLoaded) throw new Error("Account is not loaded");
  account.migrate();
  const myAccount = await account.$jazz.ensureLoaded({
    resolve: {
      root: {
        myFestival: true,
      },
    },
  });
  window.location.href = `/festival/${myAccount.root.myFestival.$jazz.id}`;
  festival = myAccount.root.myFestival;
}

assertLoaded(festival);
const festivalComponent = FestivalComponent(festival);
const newBand = NewBandComponent(festival);
const app = document.querySelector<HTMLDivElement>("#app")!;
app.append(festivalComponent, newBand);
