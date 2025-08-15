import type {
  Account,
  AccountClass,
  AnyAccountSchema,
  CoValueFromRaw,
  SyncConfig,
} from "jazz-tools";
import {
  InMemoryKVStore,
  JazzClerkAuth,
  KvStoreContext,
  type MinimalClerkClient,
} from "jazz-tools";
import { LocalStorageKVStore } from "jazz-tools/browser";
import { type PropType, defineComponent, h, onMounted, ref } from "vue";
import { JazzVueProvider } from "../provider.js";
import { useClerkAuth } from "./useClerkAuth.js";

function setupKvStore() {
  KvStoreContext.getInstance().initialize(
    typeof window === "undefined"
      ? new InMemoryKVStore()
      : new LocalStorageKVStore(),
  );
}

const RegisterClerkAuth = defineComponent({
  name: "RegisterClerkAuth",
  props: {
    clerk: {
      type: Object as PropType<MinimalClerkClient>,
      required: true,
    },
  },
  setup(props, { slots }) {
    useClerkAuth(props.clerk);
    return () => slots.default?.();
  },
});

export const JazzVueProviderWithClerk = defineComponent({
  name: "JazzVueProviderWithClerk",
  props: {
    clerk: {
      type: Object as PropType<MinimalClerkClient>,
      required: true,
    },
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
    onAnonymousAccountDiscarded: {
      type: Function as PropType<(anonymousAccount: any) => Promise<void>>,
      required: false,
    },
    enableSSR: {
      type: Boolean,
      default: false,
    },
  },
  setup(props, { slots }) {
    const isLoaded = ref(false);

    onMounted(async () => {
      try {
        setupKvStore();
        await JazzClerkAuth.initializeAuth(props.clerk);
        isLoaded.value = true;
      } catch (error) {
        console.error("Jazz + Clerk initialization error:", error);
        // Still render even if auth init fails
        isLoaded.value = true;
      }
    });

    return () => {
      if (!isLoaded.value) {
        return null;
      }

      // Destructure props to exclude 'clerk' which JazzVueProvider doesn't accept
      const { clerk, ...jazzProviderProps } = props;

      return h(
        JazzVueProvider,
        {
          ...jazzProviderProps,
          logOutReplacement: clerk.signOut,
        },
        {
          default: () =>
            h(
              RegisterClerkAuth,
              { clerk },
              { default: () => slots.default?.() },
            ),
        },
      );
    };
  },
});
