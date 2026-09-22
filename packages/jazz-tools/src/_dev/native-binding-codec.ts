/**
 * Repository acceptance support for native binding envelopes.
 *
 * This deliberately lives under `_dev`: it lets cross-package device receipts
 * reuse the production reader without making postcard primitives part of the
 * React Native application API.
 */
export {
  createRecord,
  decodeRecordBytes,
  fieldIndex,
  nativeRowDescriptorPublicName,
  PostcardReader,
  PostcardWriter,
  readNativeSubscriptionDelta,
  writeDescriptor,
} from "../runtime/native-runtime/native-codec.js";
