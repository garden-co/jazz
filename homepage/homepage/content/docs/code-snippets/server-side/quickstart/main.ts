// #region Context
import { JazzBrowserContextManager } from 'jazz-tools/browser';
import { announceBand } from "./announceBandSchema";

//[!code hide:1]
const apiKey = '';
await new JazzBrowserContextManager().createContext({
  sync: {
    peer: `wss://cloud.jazz.tools?key=${apiKey}`,
  },
});
// #endregion

// #region Page
const app = document.querySelector<HTMLDivElement>('#app')!;
const bandList = document.createElement('ul');
const form = document.createElement('form');
const input = Object.assign(document.createElement('input'), {
  type: 'text',
  name: 'band'
});
const button = Object.assign(document.createElement('button'), {
  name: 'band',
  innerText: 'Announce Band',
  onclick: async () => {
    const bandListResponse = await announceBand.send({
      band: { name: input.value },
    });
    input.value = ""
    if (bandListResponse.bandList.$isLoaded) {
      bandList?.replaceChildren(...bandListResponse
        .bandList
        .map(band => {
          return Object.assign(document.createElement('li'), {
            innerText: band.name
          })
        })
      )
    }
  }
});

form.append(input, button);
app.append(form, bandList);
// #endregion