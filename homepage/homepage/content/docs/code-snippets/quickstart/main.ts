// #region Context
import { JazzBrowserContextManager } from 'jazz-tools/browser';
import { JazzFestAccount } from './schema';

// @ts-expect-error Not a real Vite app
const apiKey = import.meta.env.VITE_JAZZ_API_KEY;
const contextManager = new JazzBrowserContextManager<typeof JazzFestAccount>();
await contextManager.createContext({
  sync: {
    peer: `wss://cloud.jazz.tools?key=${apiKey}`
  },
});

function getCurrentAccount() {
  const context = contextManager.getCurrentValue();
  if (!context || !("me" in context)) {
    throw new Error("");
  }

  return context.me;
}
// #endregion

// #region AddBand
const me = getCurrentAccount();
const account = await JazzFestAccount.load(me.$jazz.id);
if (!account.$isLoaded) throw new Error("Account is not loaded");
account.migrate();
const myAccount = await account.$jazz.ensureLoaded({
  resolve: { root: { myFestival: true } },
});

const form = document.createElement('form');
const input = Object.assign(document.createElement('input'), {
  type: 'text',
  name: 'band',
  placeholder: 'Band name'
});
const button = Object.assign(document.createElement('button'), {
  name: 'band',
  innerText: 'Add',
  onclick: async (e: Event) => {
    e.preventDefault(); // Prevent navigation
    if (!myAccount.$isLoaded) return;
    myAccount.root.myFestival.$jazz.push({ name: input.value });
    input.value = '';
  }
});

form.append(input, button);
// #endregion

// #region Display
const bandList = document.createElement('ul');
const unsubscribe = myAccount.root.myFestival.$jazz.subscribe((festival) => {
  if (!festival.$isLoaded) throw new Error("Festival not loaded");

  const bandElements = festival
    .map((band) => {
      if (!band.$isLoaded) return;
      const bandElement = document.createElement("li");
      bandElement.innerText = band.name;
      return bandElement;
    })
    .filter((band) => band !== undefined);

  bandList.replaceChildren(...bandElements);
});
// #endregion

// #region Page
const app = document.querySelector<HTMLDivElement>('#app')!;
app.append(form, bandList);
// #endregion

