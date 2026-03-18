import { computed, defineComponent, h, ref, watch, type PropType } from "vue";
import type { LocalAuthMode } from "../runtime/context.js";
import {
  createSyntheticUserProfile,
  loadSyntheticUserStore,
  saveSyntheticUserStore,
  setActiveSyntheticProfile,
  type StorageLike,
  type SyntheticUserProfile,
  type SyntheticUserStore,
} from "../synthetic-users.js";

export interface SyntheticUserSwitcherProps {
  appId: string;
  storage?: StorageLike;
  storageKey?: string;
  defaultMode?: LocalAuthMode;
  reloadOnSwitch?: boolean;
  onProfileChange?: (profile: SyntheticUserProfile) => void;
}

function getActiveProfile(store: SyntheticUserStore): SyntheticUserProfile {
  const fallbackProfile = store.profiles[0];
  if (!fallbackProfile) {
    throw new Error("Synthetic user store must contain at least one profile.");
  }
  return store.profiles.find((profile) => profile.id === store.activeProfileId) ?? fallbackProfile;
}

export const SyntheticUserSwitcher = defineComponent({
  name: "SyntheticUserSwitcher",
  props: {
    appId: {
      type: String,
      required: true,
    },
    storage: {
      type: Object as PropType<StorageLike | undefined>,
      default: undefined,
    },
    storageKey: {
      type: String,
      default: undefined,
    },
    defaultMode: {
      type: String as PropType<LocalAuthMode | undefined>,
      default: undefined,
    },
    reloadOnSwitch: {
      type: Boolean,
      default: true,
    },
    onProfileChange: {
      type: Function as PropType<((profile: SyntheticUserProfile) => void) | undefined>,
      default: undefined,
    },
  },
  setup(props, { attrs }) {
    const store = ref<SyntheticUserStore>(
      loadSyntheticUserStore(props.appId, {
        storage: props.storage,
        storageKey: props.storageKey,
        defaultMode: props.defaultMode,
      }),
    );

    watch(
      () => [props.appId, props.storage, props.storageKey, props.defaultMode] as const,
      () => {
        store.value = loadSyntheticUserStore(props.appId, {
          storage: props.storage,
          storageKey: props.storageKey,
          defaultMode: props.defaultMode,
        });
      },
    );

    const activeProfile = computed(() => getActiveProfile(store.value));

    const applyStore = (nextStore: SyntheticUserStore, triggerReload: boolean) => {
      saveSyntheticUserStore(props.appId, nextStore, {
        storage: props.storage,
        storageKey: props.storageKey,
        defaultMode: props.defaultMode,
      });
      store.value = nextStore;
      props.onProfileChange?.(getActiveProfile(nextStore));

      if (triggerReload && props.reloadOnSwitch && typeof window !== "undefined") {
        window.location.reload();
      }
    };

    const handleSwitch = (event: Event) => {
      const target = event.target as HTMLSelectElement;
      const nextStore = setActiveSyntheticProfile(props.appId, target.value, {
        storage: props.storage,
        storageKey: props.storageKey,
        defaultMode: props.defaultMode,
      });
      store.value = nextStore;
      props.onProfileChange?.(getActiveProfile(nextStore));

      if (props.reloadOnSwitch && typeof window !== "undefined") {
        window.location.reload();
      }
    };

    const handleModeChange = (event: Event) => {
      const target = event.target as HTMLSelectElement;
      const mode = target.value as LocalAuthMode;
      const nextStore: SyntheticUserStore = {
        ...store.value,
        profiles: store.value.profiles.map((profile) =>
          profile.id === store.value.activeProfileId ? { ...profile, mode } : profile,
        ),
      };
      applyStore(nextStore, false);
    };

    const handleAddProfile = () => {
      const suggestedName = `User ${store.value.profiles.length + 1}`;
      const rawName =
        typeof window !== "undefined"
          ? window.prompt("New synthetic user name", suggestedName)
          : suggestedName;
      if (rawName === null) {
        return;
      }

      const profile = createSyntheticUserProfile(rawName.trim() || suggestedName, "demo");
      const nextStore: SyntheticUserStore = {
        activeProfileId: profile.id,
        profiles: [...store.value.profiles, profile],
      };
      applyStore(nextStore, true);
    };

    const handleRemoveProfile = () => {
      if (store.value.profiles.length <= 1) {
        return;
      }

      const nextProfiles = store.value.profiles.filter(
        (profile) => profile.id !== store.value.activeProfileId,
      );
      const nextActiveProfile = nextProfiles[0];
      if (!nextActiveProfile) {
        return;
      }
      const nextStore: SyntheticUserStore = {
        activeProfileId: nextActiveProfile.id,
        profiles: nextProfiles,
      };
      applyStore(nextStore, true);
    };

    return () =>
      h("div", attrs, [
        h("label", [
          "Synthetic User ",
          h(
            "select",
            {
              value: store.value.activeProfileId,
              onChange: handleSwitch,
            },
            store.value.profiles.map((profile) =>
              h(
                "option",
                { key: profile.id, value: profile.id },
                `${profile.name} (${profile.mode})`,
              ),
            ),
          ),
        ]),
        " ",
        h("label", [
          "Mode ",
          h(
            "select",
            {
              value: activeProfile.value.mode,
              onChange: handleModeChange,
            },
            [
              h("option", { value: "anonymous" }, "anonymous"),
              h("option", { value: "demo" }, "demo"),
            ],
          ),
        ]),
        " ",
        h(
          "button",
          {
            type: "button",
            onClick: handleAddProfile,
          },
          "Add",
        ),
        " ",
        h(
          "button",
          {
            type: "button",
            disabled: store.value.profiles.length <= 1,
            onClick: handleRemoveProfile,
          },
          "Remove",
        ),
      ]);
  },
});
