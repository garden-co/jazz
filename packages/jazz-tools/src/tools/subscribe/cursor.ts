import { base58 } from "@scure/base";
import { cojsonInternals, RawCoID, Stringified } from "cojson";
import {
  CoValueCursor,
  CoValueErrorState,
  CoValueLoadingState,
  DecodedCoValueCursor,
} from "./types.js";
import { z } from "zod/v4";
import type { RefsToResolve } from "../coValues/deepLoading.js";

const cursorSchema = z.object({
  version: z.literal(1),
  rootId: z.string<RawCoID>(),
  resolveFingerprint: z.record(z.string(), z.any()),
  frontiers: z.record(z.string<RawCoID>(), z.record(z.string(), z.number())),
  valueErrors: z
    .record(
      z.string<RawCoID>(),
      z.enum([
        CoValueLoadingState.UNAVAILABLE,
        CoValueLoadingState.UNAUTHORIZED,
        CoValueLoadingState.DELETED,
      ]) satisfies z.ZodEnum<Record<CoValueErrorState, CoValueErrorState>>,
    )
    .optional(),
}) satisfies z.ZodType<DecodedCoValueCursor>;

export class CursorError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "CursorError";
  }
}

export const encodeCursor = (
  decodedCursor: Omit<DecodedCoValueCursor, "resolveFingerprint"> & {
    resolveFingerprint: DecodedCoValueCursor["resolveFingerprint"] | boolean;
  },
): CoValueCursor => {
  const textEncoder = new TextEncoder();
  return `cursor_z${base58.encode(
    textEncoder.encode(
      cojsonInternals.stableStringify({
        ...decodedCursor,
        resolveFingerprint:
          decodedCursor.resolveFingerprint === true ||
          !decodedCursor.resolveFingerprint
            ? {}
            : decodedCursor.resolveFingerprint,

        // remove empty valueErrors to reduce the cursor size
        ...(decodedCursor.valueErrors &&
        Object.keys(decodedCursor.valueErrors).length === 0
          ? { valueErrors: undefined }
          : {}),
      }),
    ),
  )}`;
};

export const decodeAndValidateCursor = ({
  rootId,
  resolve,
  cursor,
}: {
  cursor: CoValueCursor;
  rootId: string;
  resolve: RefsToResolve<any>;
}): DecodedCoValueCursor => {
  const textDecoder = new TextDecoder();

  let maybeDecodedCursor: DecodedCoValueCursor;
  try {
    maybeDecodedCursor = cojsonInternals.parseJSON(
      textDecoder.decode(
        base58.decode(cursor.replace(/^cursor_z/, "")),
      ) as Stringified<DecodedCoValueCursor>,
    );
  } catch {
    throw new CursorError("Invalid cursor string");
  }

  const parseResult = cursorSchema.safeParse(maybeDecodedCursor);

  if (!parseResult.success) {
    throw new CursorError("Invalid cursor string");
  }

  const decodedCursor = parseResult.data;

  if (decodedCursor.rootId !== rootId) {
    throw new CursorError("Invalid cursor: root CoValue ID mismatch");
  }

  const normalizedResolveFingerprint = cojsonInternals.stableStringify(
    decodedCursor.resolveFingerprint,
  );
  const normalizedResolve = cojsonInternals.stableStringify(
    resolve === true ? {} : resolve,
  );

  if (normalizedResolveFingerprint !== normalizedResolve) {
    throw new CursorError(
      `Invalid cursor: resolve query mismatch. Expected ${normalizedResolve}, got cursor with ${normalizedResolveFingerprint}`,
    );
  }

  return decodedCursor;
};
