import {
  CoID,
  LocalNode,
  RawBinaryCoStream,
  RawCoStream,
  RawCoValue,
} from "cojson";
import { computed, onUnmounted, ref, watch } from "vue";
import { detectCoStreamType } from "./co-stream-view.js";

export type CoJsonType = "comap" | "costream" | "colist" | "coplaintext";
export type ExtendedCoJsonType =
  | "image"
  | "record"
  | "account"
  | "group"
  | "file";

type JSON = string | number | boolean | null | JSON[] | { [key: string]: JSON };
type JSONObject = { [key: string]: JSON };

type ResolvedImageDefinition = {
  originalSize: [number, number];
  placeholderDataURL?: string;
  [res: `${number}x${number}`]: RawBinaryCoStream["id"];
};

// Type guard for browser image
export const isBrowserImage = (
  coValue: JSONObject,
): coValue is ResolvedImageDefinition => {
  return "originalSize" in coValue && "placeholderDataURL" in coValue;
};

export type ResolvedGroup = {
  readKey: string;
  [key: string]: JSON;
};

export const isGroup = (coValue: JSONObject): coValue is ResolvedGroup => {
  return "readKey" in coValue;
};

export type ResolvedAccount = {
  profile: {
    name: string;
  };
  [key: string]: JSON;
};

export const isAccount = (coValue: JSONObject): coValue is ResolvedAccount => {
  return isGroup(coValue) && "profile" in coValue;
};

export async function resolveCoValue(
  coValueId: CoID<RawCoValue>,
  node: LocalNode,
): Promise<
  | {
      value: RawCoValue;
      snapshot: JSONObject;
      type: CoJsonType | null;
      extendedType: ExtendedCoJsonType | undefined;
    }
  | {
      value: undefined;
      snapshot: "unavailable";
      type: null;
      extendedType: undefined;
    }
> {
  const value = await node.load(coValueId);

  if (value === "unavailable") {
    return {
      value: undefined,
      snapshot: "unavailable",
      type: null,
      extendedType: undefined,
    };
  }

  const snapshot = value.toJSON() as JSONObject;
  const type = value.type as CoJsonType;

  // Determine extended type
  let extendedType: ExtendedCoJsonType | undefined;

  if (type === "comap") {
    if (isBrowserImage(snapshot)) {
      extendedType = "image";
    } else if (isAccount(snapshot)) {
      extendedType = "account";
    } else if (isGroup(snapshot)) {
      extendedType = "group";
    }
  }

  return {
    value,
    snapshot,
    type,
    extendedType,
  };
}

function subscribeToCoValue(
  coValueId: CoID<RawCoValue>,
  node: LocalNode,
  callback: (result: Awaited<ReturnType<typeof resolveCoValue>>) => void,
) {
  return node.subscribe(coValueId, (value) => {
    if (value === "unavailable") {
      callback({
        value: undefined,
        snapshot: "unavailable",
        type: null,
        extendedType: undefined,
      });
    } else {
      const snapshot = value.toJSON() as JSONObject;
      const type = value.type as CoJsonType;
      let extendedType: ExtendedCoJsonType | undefined;

      if (type === "comap") {
        if (isBrowserImage(snapshot)) {
          extendedType = "image";
        } else if (isAccount(snapshot)) {
          extendedType = "account";
        } else if (isGroup(snapshot)) {
          extendedType = "group";
        }
      } else if (type === "costream") {
        const coStream = detectCoStreamType(value as RawCoStream);

        if (coStream.type === "binary") {
          extendedType = "file";
        }
      }

      callback({
        value,
        snapshot,
        type,
        extendedType,
      });
    }
  });
}

export function useResolvedCoValue(
  coValueId: CoID<RawCoValue>,
  node: LocalNode,
) {
  const result = ref<Awaited<ReturnType<typeof resolveCoValue>>>();

  let unsubscribe: (() => void) | undefined;

  const setupSubscription = () => {
    if (unsubscribe) {
      unsubscribe();
    }
    unsubscribe = subscribeToCoValue(coValueId, node, (newResult) => {
      result.value = newResult;
    });
  };

  watch([() => coValueId, () => node], setupSubscription, { immediate: true });

  onUnmounted(() => {
    if (unsubscribe) {
      unsubscribe();
    }
  });

  const resolvedValue = computed(
    () =>
      result.value || {
        value: undefined,
        snapshot: undefined,
        type: undefined,
        extendedType: undefined,
      },
  );

  return {
    value: computed(() => resolvedValue.value.value),
    snapshot: computed(() => resolvedValue.value.snapshot),
    type: computed(() => resolvedValue.value.type),
    extendedType: computed(() => resolvedValue.value.extendedType),
  };
}

export function useResolvedCoValues(
  coValueIds: CoID<RawCoValue>[],
  node: LocalNode,
) {
  const results = ref<Awaited<ReturnType<typeof resolveCoValue>>[]>([]);
  let unsubscribes: (() => void)[] = [];

  const setupSubscriptions = () => {
    // Clean up existing subscriptions
    unsubscribes.forEach((unsubscribe) => unsubscribe());
    unsubscribes = [];

    coValueIds.forEach((coValueId, index) => {
      const unsubscribe = subscribeToCoValue(coValueId, node, (newResult) => {
        const newResults = results.value.slice(0, coValueIds.length);
        newResults[index] = newResult as any;
        results.value = newResults;
      });
      unsubscribes.push(unsubscribe);
    });
  };

  watch([() => coValueIds, () => node], setupSubscriptions, {
    immediate: true,
  });

  onUnmounted(() => {
    unsubscribes.forEach((unsubscribe) => unsubscribe());
  });

  return computed(() => results.value);
}
