import { CoID, LocalNode, RawCoValue } from "cojson";
import { type PropType, defineComponent, ref, watch } from "vue";
import { Button } from "../ui/button.js";
import { resolveCoValue, useResolvedCoValue } from "./use-resolve-covalue.js";

export const AccountOrGroupText = defineComponent({
  name: "AccountOrGroupText",
  props: {
    coId: {
      type: String as any,
      required: true,
    },
    node: {
      type: Object as PropType<LocalNode>,
      required: true,
    },
    showId: {
      type: Boolean,
      default: false,
    },
    onClick: {
      type: Function as PropType<(name?: string) => void>,
    },
  },
  setup(props) {
    const { snapshot, extendedType } = useResolvedCoValue(
      props.coId,
      props.node,
    );
    const name = ref<string | null>(null);

    watch(
      [snapshot, () => props.node, extendedType],
      async () => {
        if (
          snapshot.value &&
          typeof snapshot.value === "object" &&
          "profile" in snapshot.value
        ) {
          const profileId = snapshot.value.profile as CoID<RawCoValue>;
          const profileResult = await resolveCoValue(profileId, props.node);
          if (
            profileResult.snapshot &&
            typeof profileResult.snapshot === "object" &&
            "name" in profileResult.snapshot
          ) {
            name.value = profileResult.snapshot.name as string;
          }
        }
      },
      { immediate: true },
    );

    return () => {
      if (!snapshot.value) return <span>Loading...</span>;
      if (extendedType.value !== "account" && extendedType.value !== "group") {
        return <span>CoID is not an account or group</span>;
      }

      const displayName =
        extendedType.value === "account" ? name.value || "Account" : "Group";
      const displayText = props.showId
        ? `${displayName} <${props.coId}>`
        : displayName;

      if (props.onClick) {
        return (
          <Button variant="link" onClick={() => props.onClick?.(displayName)}>
            {displayText}
          </Button>
        );
      }

      return <>{displayText}</>;
    };
  },
});
