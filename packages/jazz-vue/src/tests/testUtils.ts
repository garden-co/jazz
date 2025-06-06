import type { Account, AnonymousJazzAgent } from "jazz-tools";
import { createApp, defineComponent, h } from "vue";
import { JazzTestProvider } from "../testing";

// biome-ignore lint/suspicious/noExplicitAny: Complex generic typing with Jazz framework internals
export const withJazzTestSetup = <C extends (...args: any[]) => any>(
  composable: C,
  {
    account,
    isAuthenticated,
  }: {
    account?: Account | { guest: AnonymousJazzAgent };
    isAuthenticated?: boolean;
  } = {},
) => {
  // biome-ignore lint/suspicious/noImplicitAnyLet: Not sure what the type is
  let result;

  const wrapper = defineComponent({
    setup() {
      result = composable();
      // suppress missing template warning
      return () => {};
    },
  });

  const app = createApp({
    setup() {
      return () =>
        h(
          JazzTestProvider,
          {
            account,
            isAuthenticated,
          },
          {
            default: () => h(wrapper),
          },
        );
    },
  });

  app.mount(document.createElement("div"));
  // return the result and the app instance
  // for testing provide/unmount
  return [result, app] as [ReturnType<C>, ReturnType<typeof createApp>];
};

export function waitFor(callback: () => void) {
  return new Promise<void>((resolve, reject) => {
    const checkPassed = () => {
      try {
        return { ok: callback(), error: null };
      } catch (error) {
        return { ok: false, error };
      }
    };

    let retries = 0;

    const interval = setInterval(() => {
      const { ok, error } = checkPassed();

      if (ok !== false) {
        clearInterval(interval);
        resolve();
      }

      if (++retries > 10) {
        clearInterval(interval);
        reject(error);
      }
    }, 100);
  });
}
