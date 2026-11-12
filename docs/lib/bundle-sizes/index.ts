import fallback from "./fallback.json";

// Sizes measured by .github/workflows/bundle-sizes.yml after each release
// (see dev/benchmarks/bundle-size/README.md) and uploaded next to the benchmark timeline.
// The docs build reads them once; if they can't be fetched it uses the copy
// committed here, so a build never fails for want of a number.
export const bundleSizesUrl =
  "https://github.com/garden-co/jazz/releases/download/perf-timeline-data/bundle-sizes.json";

export type BundleSizes = {
  jazzToolsVersion: string;
  nextVersion: string;
  measuredAt: string;
  /** Gzipped JS a Next.js page loads before first render, with `<JazzProvider>` minus without. */
  thinClientAddedGzipBytes: number;
  wasmGzipBytes: number;
  wasmRawBytes: number;
};

export function parseBundleSizes(value: unknown): BundleSizes | null {
  const v = value as Partial<BundleSizes> | null;
  if (!v || typeof v !== "object") return null;
  const positive = (n: unknown) => typeof n === "number" && Number.isFinite(n) && n > 0;
  if (
    typeof v.jazzToolsVersion !== "string" ||
    typeof v.nextVersion !== "string" ||
    typeof v.measuredAt !== "string" ||
    !positive(v.thinClientAddedGzipBytes) ||
    !positive(v.wasmGzipBytes) ||
    !positive(v.wasmRawBytes)
  )
    return null;
  return v as BundleSizes;
}

/** "about 150 KB": to the nearest 10 KB (1 KB = 1000 bytes, as bundlers report). */
export function aboutKB(bytes: number) {
  return `about ${Math.max(10, Math.round(bytes / 10_000) * 10)} KB`;
}

/** "about 3 MB" / "about 8.8 MB": whole MB from 10 MB up, else one decimal, dropping ".0". */
export function aboutMB(bytes: number) {
  const mb = bytes / 1_000_000;
  const rounded = mb >= 10 ? Math.round(mb) : Math.round(mb * 10) / 10;
  return `about ${Number.isInteger(rounded) ? rounded : rounded.toFixed(1)} MB`;
}

export const bundleSizeTokens = {
  "%bundle.thin%": (s: BundleSizes) => aboutKB(s.thinClientAddedGzipBytes),
  "%bundle.wasm%": (s: BundleSizes) => aboutMB(s.wasmGzipBytes),
  "%bundle.wasmRaw%": (s: BundleSizes) => aboutMB(s.wasmRawBytes).replace(/^about /, ""),
} as const;

/** Fills every `%bundle.…%` token; an unknown one fails the build rather than reaching a page. */
export function replaceBundleSizeTokens(text: string, sizes: BundleSizes) {
  return text.replace(/%bundle\.[A-Za-z]*%?/g, (token) => {
    const format = bundleSizeTokens[token as keyof typeof bundleSizeTokens];
    if (!format)
      throw new Error(
        `Unknown bundle size token ${token}: use ${Object.keys(bundleSizeTokens).join(", ")}.`,
      );
    return format(sizes);
  });
}

let loaded: Promise<BundleSizes> | undefined;

export function loadBundleSizes(fetchImpl: typeof fetch = fetch): Promise<BundleSizes> {
  loaded ??= (async () => {
    try {
      const response = await fetchImpl(process.env.BUNDLE_SIZES_URL || bundleSizesUrl, {
        signal: AbortSignal.timeout(10_000),
      });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const sizes = parseBundleSizes(await response.json());
      if (!sizes) throw new Error("malformed bundle-sizes.json");
      return sizes;
    } catch (error) {
      const sizes = parseBundleSizes(fallback)!;
      console.warn(
        `Bundle sizes: using the committed fallback (jazz-tools ${sizes.jazzToolsVersion}, measured ${sizes.measuredAt.slice(0, 10)}); the published measurement couldn't be read: ${error instanceof Error ? error.message : String(error)}`,
      );
      return sizes;
    }
  })();
  return loaded;
}
