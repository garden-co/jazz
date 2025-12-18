import { RawCoValue } from "cojson";
import { CoreCoValueSchema, CoValue, Loaded } from "../internal.js";

const weakMap = new WeakMap<RawCoValue, CoValue>();

export const coValuesCache = {
  get: <S extends CoreCoValueSchema>(
    raw: RawCoValue,
    compute: () => Loaded<S, true>,
  ) => {
    const cached = weakMap.get(raw);
    if (cached) {
      return cached as Loaded<S, true>;
    }
    const computed = compute();
    weakMap.set(raw, computed);
    return computed;
  },
};
