import {
  CoID,
  LocalNode,
  RawBinaryCoStream,
  RawCoStream,
  RawCoValue,
} from "cojson";
import { useEffect, useState } from "react";
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
  const [result, setResult] =
    useState<Awaited<ReturnType<typeof resolveCoValue>>>();

  useEffect(() => {
    let isMounted = true;
    const unsubscribe = subscribeToCoValue(coValueId, node, (newResult) => {
      if (isMounted) {
        setResult(newResult);
      }
    });

    return () => {
      isMounted = false;
      unsubscribe();
    };
  }, [coValueId, node]);

  return (
    result || {
      value: undefined,
      snapshot: undefined,
      type: undefined,
      extendedType: undefined,
    }
  );
}

export function useResolvedCoValues(
  coValueIds: CoID<RawCoValue>[],
  node: LocalNode,
) {
  const [results, setResults] = useState<
    Awaited<ReturnType<typeof resolveCoValue>>[]
  >([]);

  useEffect(() => {
    let isMounted = true;
    const unsubscribes: (() => void)[] = [];

    coValueIds.forEach((coValueId, index) => {
      const unsubscribe = subscribeToCoValue(coValueId, node, (newResult) => {
        if (isMounted) {
          setResults((prevResults) => {
            const newResults = prevResults.slice(0, coValueIds.length);
            newResults[index] = newResult;
            return newResults;
          });
        }
      });
      unsubscribes.push(unsubscribe);
    });

    return () => {
      isMounted = false;
      unsubscribes.forEach((unsubscribe) => unsubscribe());
    };
  }, [coValueIds, node]);

  return results;
}
