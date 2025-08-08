import { render as renderSvelte } from "@testing-library/svelte";
import { Component, ComponentProps } from "svelte";
import { TestJazzContextManager } from "../../testing";
import { Account, AnonymousJazzAgent } from "../../tools";
import { JAZZ_AUTH_CTX, JAZZ_CTX } from "../jazz.svelte";

type JazzExtendedOptions = {
  account: Account | { guest: AnonymousJazzAgent };
};

const render = <T extends Component>(
  component: T,
  props: ComponentProps<T>,
  jazzOptions: JazzExtendedOptions,
) => {
  const ctx = TestJazzContextManager.fromAccountOrGuest(jazzOptions.account);

  return renderSvelte(
    // @ts-expect-error Svelte new Component type is not compatible with @testing-library/svelte
    component,
    {
      props,
      context: new Map<any, any>([
        [JAZZ_CTX, { current: ctx.getCurrentValue() }],
        [JAZZ_AUTH_CTX, ctx.getAuthSecretStorage()],
      ]),
    },
  );
};

export * from "@testing-library/svelte";

export { render };
