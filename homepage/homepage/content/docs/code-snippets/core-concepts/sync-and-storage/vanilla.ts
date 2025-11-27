import { JazzBrowserContextManager } from 'jazz-tools/browser';

// [!code hide:1]
const apiKey = "you@example.com";
await new JazzBrowserContextManager().createContext({
  sync: {
    peer: `wss://cloud.jazz.tools?key=${apiKey}`,
  },
});