import type {
  Account,
  AccountClass,
  AnyAccountSchema,
  CoValueFromRaw,
  JazzContextType,
  SyncConfig,
} from "jazz-tools";
import { JazzBrowserContextManager } from "jazz-tools/browser";
import {
  type PropType,
  computed,
  defineComponent,
  markRaw,
  onUnmounted,
  provide,
  ref,
  shallowRef,
  watch,
} from "vue";

export const logoutHandler = ref<() => void>();

export const JazzContextSymbol = Symbol("JazzContext");
export const JazzContextManagerSymbol = Symbol("JazzContextManager");
export const JazzAuthContextSymbol = Symbol("JazzAuthContext");

export const JazzVueProvider = defineComponent({
  name: "JazzProvider",
  props: {
    AccountSchema: {
      type: Function as unknown as PropType<
        (AccountClass<Account> & CoValueFromRaw<Account>) | AnyAccountSchema
      >,
      required: false,
    },
    guestMode: {
      type: Boolean,
      default: false,
    },
    sync: {
      type: Object as PropType<SyncConfig>,
      required: true,
    },
    storage: {
      type: String as PropType<"indexedDB">,
      default: undefined,
    },
    defaultProfileName: {
      type: String,
      required: false,
    },
    onLogOut: {
      type: Function as PropType<() => void>,
      required: false,
    },
    logOutReplacement: {
      type: Function as PropType<() => void>,
      required: false,
    },
    onAnonymousAccountDiscarded: {
      type: Function as PropType<(anonymousAccount: Account) => Promise<void>>,
      required: false,
    },
    enableSSR: {
      type: Boolean,
      default: false,
    },
  },
  setup(props, { slots }) {
    const contextManager = markRaw(
      new JazzBrowserContextManager<
        (AccountClass<Account> & CoValueFromRaw<Account>) | AnyAccountSchema
      >({
        useAnonymousFallback: props.enableSSR,
      }),
    );

    // Use shallowRef to avoid deep reactivity on Jazz objects which can cause proxy errors
    const ctx = shallowRef<JazzContextType<any>>();

    provide(JazzContextSymbol, ctx);
    provide(JazzContextManagerSymbol, ref(contextManager));
    provide(
      JazzAuthContextSymbol,
      markRaw(contextManager.getAuthSecretStorage()),
    );

    // Create stable callback references like React's useRefCallback
    const logOutReplacementCallback = () => props.logOutReplacement?.();

    const logoutReplacementActive = computed(() =>
      Boolean(props.logOutReplacement),
    );

    watch(
      () => ({
        peer: props.sync.peer,
        syncWhen: props.sync.when,
        storage: props.storage,
        guestMode: props.guestMode,
      }),
      async () => {
        contextManager
          .createContext({
            AccountSchema: props.AccountSchema,
            guestMode: props.guestMode,
            sync: props.sync,
            storage: props.storage,
            defaultProfileName: props.defaultProfileName,
            onLogOut: props.onLogOut,
            logOutReplacement: logoutReplacementActive.value
              ? logOutReplacementCallback
              : undefined,
            onAnonymousAccountDiscarded: props.onAnonymousAccountDiscarded,
          })
          .catch((error) => {
            console.error("Error creating Jazz browser context:", error);
          });
      },
      { immediate: true },
    );

    // Set up context manager subscription immediately, not waiting for onMounted
    // This ensures we catch context changes (like logout) that happen before mounting
    const cleanup = contextManager.subscribe(() => {
      const currentValue = contextManager.getCurrentValue();
      // Use markRaw to prevent Vue from making Jazz objects reactive
      ctx.value = currentValue ? markRaw(currentValue) : undefined;
    });

    onUnmounted(() => {
      cleanup();
    });

    onUnmounted(() => {
      if (ctx.value) ctx.value.done?.();

      // https://github.com/rizen/jazz-vue-vamp/blob/main/src/provider.ts#L167
      // Only call done() in production, not in development (for HMR)
      if (process.env.NODE_ENV !== "development") {
        contextManager.done();
      }
    });

    return () => (ctx.value ? slots.default?.() : null);
  },
});

/**
 * Alias to JazzVueProvider to be consistent with React yet backwards compatible
 * @deprecated Use JazzVueProvider instead
 */
export const JazzProvider = JazzVueProvider;
