import type { CoID, JsonObject, LocalNode, RawCoValue } from "cojson";
import { markRaw, onUnmounted, ref } from "vue";

export interface ResolvedCoValue {
  id: string;
  type: string;
  extendedType?: string;
  snapshot: JsonObject | "unavailable";
  value?: RawCoValue;
  properties: Array<{ key: string; value: any; isCoValueRef: boolean }>;
}

// Type detection helpers (from React Inspector)
const isGroup = (coValue: JsonObject): boolean => {
  return "readKey" in coValue;
};

const isAccount = (coValue: JsonObject): boolean => {
  return isGroup(coValue) && "profile" in coValue;
};

const isBrowserImage = (coValue: JsonObject): boolean => {
  return "originalSize" in coValue && "placeholderDataURL" in coValue;
};

export function useCoValueResolver(localNode?: LocalNode) {
  const loading = ref(false);
  const error = ref<string | null>(null);
  const subscriptions = new Set<() => void>();

  const resolveCoValue = async (
    coValueId: string,
  ): Promise<ResolvedCoValue | null> => {
    if (!localNode) {
      throw new Error("LocalNode is required for CoValue resolution");
    }

    loading.value = true;
    error.value = null;

    try {
      // Use real Jazz CoValue resolution like React Inspector
      const value = await localNode.load(coValueId as CoID<RawCoValue>);

      if (value === "unavailable") {
        return {
          id: coValueId,
          type: "unavailable",
          snapshot: "unavailable",
          properties: [],
        };
      }

      // Get JSON snapshot like React Inspector
      const snapshot = value.toJSON() as JsonObject;
      const type = value.type as string;

      // Determine extended type like React Inspector
      let extendedType: string | undefined;

      if (type === "comap") {
        if (isBrowserImage(snapshot)) {
          extendedType = "image";
        } else if (isAccount(snapshot)) {
          extendedType = "account";
        } else if (isGroup(snapshot)) {
          extendedType = "group";
        }
      } else if (type === "colist") {
        extendedType = "list";
      } else if (type === "coplaintext") {
        extendedType = "plaintext";
      }

      // Process properties like React Inspector
      const properties = [];

      if (snapshot && typeof snapshot === "object") {
        for (const [key, value] of Object.entries(snapshot)) {
          // Filter out internal keys like React Inspector does
          if (
            extendedType === "account" &&
            (key === "readKey" ||
              key.startsWith("sealer_z") ||
              key.startsWith("key_z"))
          ) {
            continue;
          }

          const isCoValueRef = isCoValueId(value);
          properties.push({
            key,
            value,
            isCoValueRef,
          });
        }
      }

      const resolvedData: ResolvedCoValue = {
        id: coValueId,
        type,
        extendedType,
        snapshot,
        value,
        properties,
      };

      // Mark the resolved data as raw to prevent Vue reactivity issues
      return markRaw(resolvedData);
    } catch (err) {
      error.value =
        err instanceof Error ? err.message : "Failed to resolve CoValue";
      return null;
    } finally {
      loading.value = false;
    }
  };

  // Subscribe to CoValue changes like React Inspector
  const subscribeToCoValue = (
    coValueId: string,
    callback: (result: ResolvedCoValue | null) => void,
  ) => {
    if (!localNode) return () => {};

    const unsubscribe = localNode.subscribe(
      coValueId as CoID<RawCoValue>,
      (value) => {
        if (value === "unavailable") {
          callback({
            id: coValueId,
            type: "unavailable",
            snapshot: "unavailable",
            properties: [],
          });
        } else {
          const snapshot = value.toJSON() as JsonObject;
          const type = value.type as string;
          let extendedType: string | undefined;

          if (type === "comap") {
            if (isBrowserImage(snapshot)) {
              extendedType = "image";
            } else if (isAccount(snapshot)) {
              extendedType = "account";
            } else if (isGroup(snapshot)) {
              extendedType = "group";
            }
          } else if (type === "colist") {
            extendedType = "list";
          } else if (type === "coplaintext") {
            extendedType = "plaintext";
          }

          const properties = [];
          if (snapshot && typeof snapshot === "object") {
            for (const [key, value] of Object.entries(snapshot)) {
              if (
                extendedType === "account" &&
                (key === "readKey" ||
                  key.startsWith("sealer_z") ||
                  key.startsWith("key_z"))
              ) {
                continue;
              }

              const isCoValueRef = isCoValueId(value);
              properties.push({ key, value, isCoValueRef });
            }
          }

          callback(
            markRaw({
              id: coValueId,
              type,
              extendedType,
              snapshot,
              value,
              properties,
            }),
          );
        }
      },
    );

    subscriptions.add(unsubscribe);
    return unsubscribe;
  };

  const isCoValueId = (value: unknown): boolean => {
    return (
      typeof value === "string" &&
      value.startsWith("co_") &&
      !value.includes("inviteSecret")
    );
  };

  // Cleanup subscriptions on unmount
  onUnmounted(() => {
    subscriptions.forEach((unsubscribe) => unsubscribe());
    subscriptions.clear();
  });

  return {
    loading,
    error,
    resolveCoValue,
    subscribeToCoValue,
    isCoValueId,
  };
}
