import {
  createOnMessage as __wasmCreateOnMessageForFsProxy,
  getDefaultContext as __emnapiGetDefaultContext,
  instantiateNapiModuleSync as __emnapiInstantiateNapiModuleSync,
  WASI as __WASI,
} from '@napi-rs/wasm-runtime'



const __wasi = new __WASI({
  version: 'preview1',
})

const __wasmUrl = new URL('./cojson-core-napi.wasm32-wasi.wasm', import.meta.url).href
const __emnapiContext = __emnapiGetDefaultContext()


const __sharedMemory = new WebAssembly.Memory({
  initial: 4000,
  maximum: 65536,
  shared: true,
})

const __wasmFile = await fetch(__wasmUrl).then((res) => res.arrayBuffer())

const {
  instance: __napiInstance,
  module: __wasiModule,
  napiModule: __napiModule,
} = __emnapiInstantiateNapiModuleSync(__wasmFile, {
  context: __emnapiContext,
  asyncWorkPoolSize: 4,
  wasi: __wasi,
  onCreateWorker() {
    const worker = new Worker(new URL('./wasi-worker-browser.mjs', import.meta.url), {
      type: 'module',
    })

    return worker
  },
  overwriteImports(importObject) {
    importObject.env = {
      ...importObject.env,
      ...importObject.napi,
      ...importObject.emnapi,
      memory: __sharedMemory,
    }
    return importObject
  },
  beforeInit({ instance }) {
    for (const name of Object.keys(instance.exports)) {
      if (name.startsWith('__napi_register__')) {
        instance.exports[name]()
      }
    }
  },
})
export default __napiModule.exports
export const Blake3Hasher = __napiModule.exports.Blake3Hasher
export const SessionLog = __napiModule.exports.SessionLog
export const blake3HashOnce = __napiModule.exports.blake3HashOnce
export const blake3HashOnceWithContext = __napiModule.exports.blake3HashOnceWithContext
export const decrypt = __napiModule.exports.decrypt
export const decryptXsalsa20 = __napiModule.exports.decryptXsalsa20
export const ed25519Sign = __napiModule.exports.ed25519Sign
export const ed25519SignatureFromBytes = __napiModule.exports.ed25519SignatureFromBytes
export const ed25519SigningKeyFromBytes = __napiModule.exports.ed25519SigningKeyFromBytes
export const ed25519SigningKeySign = __napiModule.exports.ed25519SigningKeySign
export const ed25519SigningKeyToPublic = __napiModule.exports.ed25519SigningKeyToPublic
export const ed25519Verify = __napiModule.exports.ed25519Verify
export const ed25519VerifyingKey = __napiModule.exports.ed25519VerifyingKey
export const ed25519VerifyingKeyFromBytes = __napiModule.exports.ed25519VerifyingKeyFromBytes
export const encrypt = __napiModule.exports.encrypt
export const encryptXsalsa20 = __napiModule.exports.encryptXsalsa20
export const generateNonce = __napiModule.exports.generateNonce
export const getSealerId = __napiModule.exports.getSealerId
export const getSignerId = __napiModule.exports.getSignerId
export const newEd25519SigningKey = __napiModule.exports.newEd25519SigningKey
export const newX25519PrivateKey = __napiModule.exports.newX25519PrivateKey
export const seal = __napiModule.exports.seal
export const sign = __napiModule.exports.sign
export const unseal = __napiModule.exports.unseal
export const verify = __napiModule.exports.verify
export const x25519DiffieHellman = __napiModule.exports.x25519DiffieHellman
export const x25519PublicKey = __napiModule.exports.x25519PublicKey
