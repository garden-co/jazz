import { CoValueCore } from "../coValueCore.js";
import { JsonObject } from "../jsonValue.js";
import { DeletionOpPayload, OpID, RawCoList } from "./coList.js";

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
    this._segmenter = new Intl.Segmenter("en", {
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

  insertAfter(
    idx: number,
    text: string,
    privacy: "private" | "trusting" = "private",
  ) {
    const graphemes = [...this._segmenter.segment(text)].map((g) => g.segment);

    if (idx === 0) {
      // For insertions at start, just prepend each character, in reverse
      for (const grapheme of graphemes.reverse()) {
        this.prepend(grapheme, 0, privacy);
      }
    } else {
      // For other insertions, use append after the specified index
      // We append in forward order to maintain the text order
      let after = idx - 1;
      for (const grapheme of graphemes) {
        this.append(grapheme, after, privacy);
        after++; // Move the insertion point forward for each grapheme
      }
    }
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
