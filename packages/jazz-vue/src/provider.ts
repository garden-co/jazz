import {
  BrowserContext,
  BrowserGuestContext,
  createJazzBrowserContext,
} from "jazz-browser";
import { Account, AccountClass, AuthMethod } from "jazz-tools";
/* eslint-disable @typescript-eslint/no-explicit-any */
import {
  PropType,
  defineComponent,
  onMounted,
  onUnmounted,
  provide,
  ref,
  watch,
} from "vue";

export const logoutHandler = ref<() => void>();

// eslint-disable-next-line @typescript-eslint/no-empty-object-type
export interface Register {}

export type RegisteredAccount = Register extends { Account: infer Acc }
  ? Acc
  : Account;

export const JazzContextSymbol = Symbol("JazzContext");

export const JazzProvider = defineComponent({
  name: "JazzProvider",
  props: {
    auth: {
      type: [String, Object] as PropType<AuthMethod | "guest">,
      required: true,
    },
    AccountSchema: {
      type: Object as PropType<AccountClass<RegisteredAccount>>,
      required: false,
    },
    peer: {
      type: String as PropType<`wss://${string}` | `ws://${string}`>,
      required: true,
    },
    storage: {
      type: String as PropType<"indexedDB" | "singleTabOPFS">,
      default: undefined,
    },
  },
  setup(props, { slots }) {
    const ctx = ref<
      BrowserContext<RegisteredAccount> | BrowserGuestContext | undefined
    >(undefined);

    const key = ref(0);

    provide(JazzContextSymbol, ctx);

    const initializeContext = async () => {
      if (ctx.value) {
        ctx.value.done?.();
        ctx.value = undefined;
      }

      try {
        const context = await createJazzBrowserContext<RegisteredAccount>(
          props.auth === "guest"
            ? { peer: props.peer, storage: props.storage, guest: true }
            : {
                AccountSchema: props.AccountSchema,
                auth: props.auth,
                peer: props.peer,
                storage: props.storage,
                guest: false,
              },
        );

        ctx.value = {
          ...context,
          logOut: () => {
            logoutHandler.value?.();
            context.logOut();
            key.value += 1;
          },
        };
      } catch (e) {
        console.error("Error creating Jazz browser context:", e);
      }
    };

    onMounted(() => {
      void initializeContext();
    });

    watch(
      () => key.value,
      async () => {
        await initializeContext();
      },
    );

    onUnmounted(() => {
      if (ctx.value) ctx.value.done?.();
    });

    return () => (ctx.value ? slots.default?.() : null);
  },
});
