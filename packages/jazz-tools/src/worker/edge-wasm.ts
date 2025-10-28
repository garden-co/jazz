// InitWasm allow to load the wasm code in edge runtimes (ex. cloudflare worker and vercel edge functions)
import { init as InitWasm } from "cojson/crypto/WasmCrypto/edge";
import { WasmCrypto } from "cojson/crypto/WasmCrypto";

WasmCrypto.setInit(InitWasm);
