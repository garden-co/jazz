import type { Account, AnonymousJazzAgent } from "jazz-tools";
import { TestJazzContextManager } from "jazz-tools/testing";
import { provide } from "vue";
import { type PropType, defineComponent, ref } from "vue";
import { JazzAuthContextSymbol, JazzContextSymbol } from "./provider.js";

export const JazzTestProvider = defineComponent({
  name: "JazzTestProvider",
  props: {
    account: {
      type: Object as PropType<Account | { guest: AnonymousJazzAgent }>,
      required: false,
    },
    isAuthenticated: {
      type: Boolean,
      required: false,
    },
  },
  setup(props, { slots }) {
    const contextManager = TestJazzContextManager.fromAccountOrGuest(
      props.account,
      {
        isAuthenticated: props.isAuthenticated,
      },
    );

    provide(JazzContextSymbol, ref(contextManager.getCurrentValue()));
    provide(JazzAuthContextSymbol, contextManager.getAuthSecretStorage());

    return () => slots.default?.();
  },
});

export {
  createJazzTestAccount,
  createJazzTestGuest,
  linkAccounts,
  setActiveAccount,
  setupJazzTestSync,
} from "jazz-tools/testing";
