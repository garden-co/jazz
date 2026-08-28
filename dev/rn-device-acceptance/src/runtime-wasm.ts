import { Asset } from "expo-asset";

let source: Promise<ArrayBuffer> | undefined;

/** Load the Metro-packaged WASM asset; generated wasm-bindgen URL fetches do
 * not resolve inside a release APK. */
export function metroWasmSource(): Promise<ArrayBuffer> {
  source ??= (async () => {
    const asset = Asset.fromModule(require("jazz-wasm/pkg/jazz_wasm_bg.wasm"));
    await asset.downloadAsync();
    const uri = asset.localUri ?? asset.uri;
    const response = await fetch(uri);
    if (!response.ok) throw new Error(`Metro-packaged Jazz WASM asset failed to load: ${uri}`);
    return await response.arrayBuffer();
  })();
  return source;
}
