import { CoValueCore } from "../coValueCore.js";
import { JsonObject } from "../jsonValue.js";
import { DeletionOpPayload, OpID, RawCoList } from "./coList.js";

declare const navigator:
  | {
      language: string;
    }
  | undefined;

export type StringifiedOpID = string & { __stringifiedOpID: true };

export function stringifyOpID(opID: OpID): StringifiedOpID {
  return `${opID.sessionID}:${opID.txIndex}:${opID.changeIdx}` as StringifiedOpID;
}

type PlaintextIdxMapping = {
  opIDbeforeIdx: OpID[];
  opIDafterIdx: OpID[];
  idxAfterOpID: { [opID: StringifiedOpID]: number };
  idxBeforeOpID: { [opID: StringifiedOpID]: number };
};

/**
 * A collaborative plain text implementation that supports grapheme-accurate editing.
 *
 * Locale support:
 * - Locale can be specified in the meta field when creating the text: `{ meta: { locale: "ja-JP" } }`
 * - If no locale is specified, falls back to browser's locale (`navigator.language`)
 * - If browser locale is not available, defaults to 'en'
 *
 * @example
 * ```typescript
 * // With specific locale
 * const textJa = node.createCoValue({
 *   type: "coplaintext",
 *   ruleset: { type: "unsafeAllowAll" },
 *   meta: { locale: "ja-JP" },
 *   ...Crypto.createdNowUnique(),
 * });
 *
 * // Using browser locale
 * const text = node.createCoValue({
 *   type: "coplaintext",
 *   ruleset: { type: "unsafeAllowAll" },
 *   meta: null,
 *   ...Crypto.createdNowUnique(),
 * });
 * ```
 */
export class RawCoPlainText<
  Meta extends JsonObject | null = JsonObject | null,
> extends RawCoList<string, Meta> {
  /** @category 6. Meta */
  type = "coplaintext" as const;

  private _segmenter: Intl.Segmenter;

  _cachedMapping: WeakMap<
    NonNullable<typeof this._cachedEntries>,
    PlaintextIdxMapping
  >;

  constructor(core: CoValueCore) {
    super(core);
    this._cachedMapping = new WeakMap();
    if (!Intl.Segmenter) {
      throw new Error(
        "Intl.Segmenter is not supported. Use a polyfill to get coPlainText support in Jazz. (eg. https://formatjs.github.io/docs/polyfills/intl-segmenter/)",
      );
    }

    // Use locale from meta if provided, fallback to browser locale, or 'en' as last resort
    const effectiveLocale =
      (core.header.meta &&
      typeof core.header.meta === "object" &&
      "locale" in core.header.meta
        ? (core.header.meta.locale as string)
        : undefined) ||
      (typeof navigator !== "undefined" ? navigator.language : "en");

    this._segmenter = new Intl.Segmenter(effectiveLocale, {
      granularity: "grapheme",
    });
  }

  get mapping() {
    const entries = this.entries();
    let mapping = this._cachedMapping.get(entries);
    if (mapping) {
      return mapping;
    }

    mapping = {
      opIDbeforeIdx: [],
      opIDafterIdx: [],
      idxAfterOpID: {},
      idxBeforeOpID: {},
    };

    let idxBefore = 0;

    for (const entry of entries) {
      const idxAfter = idxBefore + entry.value.length;

      mapping.opIDafterIdx[idxBefore] = entry.opID;
      mapping.opIDbeforeIdx[idxAfter] = entry.opID;
      mapping.idxAfterOpID[stringifyOpID(entry.opID)] = idxAfter;
      mapping.idxBeforeOpID[stringifyOpID(entry.opID)] = idxBefore;

      idxBefore = idxAfter;
    }

    this._cachedMapping.set(entries, mapping);
    return mapping;
  }

  toString() {
    return this.entries()
      .map((entry) => entry.value)
      .join("");
  }

  /**
   * Inserts `text` before the character at index `idx`.
   * If idx is 0, inserts at the start of the text.
   *
   * @param idx - The index of the character to insert before
   * @param text - The text to insert
   * @param privacy - Whether the operation should be private or trusting
   * @category 2. Editing
   */
  insertBefore(
    idx: number,
    text: string,
    privacy: "private" | "trusting" = "private",
  ) {
    const graphemes = [...this._segmenter.segment(text)].map((g) => g.segment);

    if (idx === 0) {
      // For insertions at start, prepend each character in reverse
      for (const grapheme of graphemes.reverse()) {
        this.prepend(grapheme, 0, privacy);
      }
    } else {
      // For other insertions, append after the previous character
      this.appendItems(graphemes, idx - 1, privacy);
    }
  }

  /**
   * Inserts `text` after the character at index `idx`.
   *
   * @param idx - The index of the character to insert after
   * @param text - The text to insert
   * @param privacy - Whether the operation should be private or trusting
   * @category 2. Editing
   */
  insertAfter(
    idx: number,
    text: string,
    privacy: "private" | "trusting" = "private",
  ) {
    const graphemes = [...this._segmenter.segment(text)].map((g) => g.segment);
    this.appendItems(graphemes, idx, privacy);
  }

  deleteRange(
    { from, to }: { from: number; to: number },
    privacy: "private" | "trusting" = "private",
  ) {
    const ops: DeletionOpPayload[] = [];
    for (let idx = from; idx < to; ) {
      const insertion = this.mapping.opIDafterIdx[idx];
      if (!insertion) {
        throw new Error("Invalid idx to delete " + idx);
      }
      ops.push({
        op: "del",
        insertion,
      });
      let nextIdx = idx + 1;
      while (!this.mapping.opIDbeforeIdx[nextIdx] && nextIdx < to) {
        nextIdx++;
      }
      idx = nextIdx;
    }
    this.core.makeTransaction(ops, privacy);

    this.rebuildFromCore();
  }
}
