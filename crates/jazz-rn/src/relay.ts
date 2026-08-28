import nativeRelay from './NativeJazzRelay';

export interface NativeRelayAbiRange {
  minimum: number;
  maximum: number;
}

export const NATIVE_RELAY_ABI: NativeRelayAbiRange = {
  minimum: 3,
  maximum: 3,
};

function requireNativeRelay() {
  if (nativeRelay == null) {
    throw new Error(
      'Jazz native relay is unavailable: install a matching native development or release build containing the Jazz relay artifact. Expo Go never includes it.'
    );
  }
  return nativeRelay;
}

/**
 * Execute one opaque base64-encoded native-relay command after checking the
 * embedded ABI.
 *
 * The command codec is intentionally not defined by this package yet: it will
 * be generated from the shared relay command contract once the native module
 * is implemented. This adapter establishes the only permitted JS/native shape
 * in advance—one version probe plus encoded-binary commands—not a row-object
 * API.
 */
export async function executeNativeRelayCommand(
  commandBase64: string
): Promise<string> {
  const relay = requireNativeRelay();
  const nativeAbi = relay.getAbiVersion();
  if (nativeAbi === 0) {
    throw new Error(
      'Jazz native relay is unavailable: this native build contains only the source fallback (ABI 0), not the Jazz relay artifact. Install a matching native development or release build.'
    );
  }
  if (
    nativeAbi < NATIVE_RELAY_ABI.minimum ||
    nativeAbi > NATIVE_RELAY_ABI.maximum
  ) {
    throw new Error(
      `Jazz native relay ABI ${nativeAbi} is incompatible with JavaScript ABI ${NATIVE_RELAY_ABI.minimum}..=${NATIVE_RELAY_ABI.maximum}; install a matching native development or release build.`
    );
  }
  return relay.execute(commandBase64);
}
