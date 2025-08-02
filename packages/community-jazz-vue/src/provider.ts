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
  defineComponent,
  markRaw,
  nextTick,
  onUnmounted,
  provide,
  ref,
  shallowRef,
  toRaw,
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
      type: [Function, Object] as unknown as PropType<
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

    // Use shallowRef to avoid deep reactivity on Jazz objects
    const ctx = shallowRef<JazzContextType<any>>();

    provide(JazzContextSymbol, ctx);
    provide(JazzContextManagerSymbol, ref(contextManager));
    provide(
      JazzAuthContextSymbol,
      markRaw(contextManager.getAuthSecretStorage()),
    );

    // Simple context creation flag to prevent rapid recreation
    let contextCreationInProgress = false;

    // Watch for prop changes and recreate context when needed
    watch(
      [
        () => props.sync.peer,
        () => props.sync.when,
        () => props.storage,
        () => props.guestMode,
        () => props.AccountSchema,
      ],
      async (newValues, oldValues) => {
        // Prevent rapid re-creation during initialization
        if (contextCreationInProgress) return;

        // Efficient O(1) change detection - skip if no actual changes
        if (
          oldValues &&
          newValues[0] === oldValues[0] && // sync.peer
          newValues[1] === oldValues[1] && // sync.when
          newValues[2] === oldValues[2] && // storage
          newValues[3] === oldValues[3] && // guestMode
          newValues[4] === oldValues[4] // AccountSchema
        ) {
          return;
        }

        contextCreationInProgress = true;

        try {
          await contextManager.createContext({
            AccountSchema: props.AccountSchema,
            guestMode: props.guestMode,
            sync: props.sync,
            storage: props.storage,
            defaultProfileName: props.defaultProfileName,
            onLogOut: props.onLogOut,
            logOutReplacement: props.logOutReplacement,
            onAnonymousAccountDiscarded: props.onAnonymousAccountDiscarded,
          });
        } catch (error) {
          console.error("Error creating Jazz browser context:", error);
        } finally {
          contextCreationInProgress = false;
        }
      },
      { immediate: true },
    );

    // Set up context manager subscription with complete isolation
    let lastValue: any = undefined;
    let isUpdating = false;

    const cleanup = contextManager.subscribe(() => {
      if (isUpdating) return;

      isUpdating = true;
      try {
        const rawContextManager = toRaw(contextManager);
        let currentValue = rawContextManager.getCurrentValue();

        if (currentValue === lastValue) return;

        // Use markRaw to prevent Vue reactivity
        if (currentValue && typeof currentValue === "object") {
          currentValue = markRaw(toRaw(currentValue));
        }

        lastValue = currentValue;

        // Use nextTick to defer context update and avoid sync reactive loops
        nextTick(() => {
          ctx.value = currentValue;
        });
      } catch (error) {
        console.error("Error in context manager subscription:", error);
        nextTick(() => {
          ctx.value = undefined;
        });
      } finally {
        isUpdating = false;
      }
    });

    onUnmounted(() => {
      cleanup();
    });

    onUnmounted(() => {
      if (ctx.value) ctx.value.done?.();

      // Only call done() in production, not in development (for HMR)
      if (process.env.NODE_ENV !== "development") {
        contextManager.done();
      }
    });

    // Simple render function - render children when context exists
    return () => (ctx.value ? slots.default?.() : null);
  },
});

/**
 * Alias to JazzVueProvider for backward compatibility
 * @deprecated Use JazzVueProvider instead
 */
export const JazzProvider = JazzVueProvider;
