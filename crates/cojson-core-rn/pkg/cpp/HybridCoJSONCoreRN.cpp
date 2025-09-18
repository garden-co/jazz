#include "HybridCoJSONCoreRN.hpp"
#include "rust/lib.rs.h"
#include <cmath>
#include <stdexcept>

namespace margelo::nitro::cojson_core_rn {

// Helper function to convert Nitro SessionLogHandle to Rust FFI SessionLogHandle
// Creates a new handle on each call to avoid thread-local storage issues
static ::SessionLogHandle toRustHandle(const SessionLogHandle& nitroHandle) {
  // Convert double to uint64_t safely
  // Check for precision loss and invalid values
  if (std::isnan(nitroHandle.id) || std::isinf(nitroHandle.id) || nitroHandle.id < 0) {
    return ::SessionLogHandle{0}; // Invalid ID
  }
  
  // Check if the double can be represented as uint64_t without precision loss
  double rounded = std::round(nitroHandle.id);
  if (std::abs(nitroHandle.id - rounded) > 1e-9) {
    return ::SessionLogHandle{0}; // Precision loss detected
  }
  
  // Check if it's within uint64_t range
  if (rounded > static_cast<double>(UINT64_MAX)) {
    return ::SessionLogHandle{0}; // Out of range
  }
  
  return ::SessionLogHandle{static_cast<uint64_t>(rounded)};
}

// Helper function to convert Rust uint64_t ID to Nitro SessionLogHandle with precision validation
static SessionLogHandle fromRustHandle(const ::SessionLogHandle& rustHandle) {
  // Convert uint64_t to double - check for precision loss
  double id_as_double = static_cast<double>(rustHandle.id);
  
  // Check for precision loss (for IDs > 2^53)
  if (rustHandle.id > (1ULL << 53) && static_cast<uint64_t>(id_as_double) != rustHandle.id) {
    // CRITICAL: Precision loss detected - this could cause session log corruption
    // Log the error and throw an exception to fail fast rather than silently corrupt data
    
    // TODO: Consider implementing string-based IDs in the Nitro interface to eliminate this issue
    // For now, throw an exception to prevent silent data corruption
    throw std::runtime_error("SessionLog ID " + std::to_string(rustHandle.id) + 
                            " exceeds JavaScript number precision (2^53). This would cause data corruption.");
  }
  
  return SessionLogHandle(id_as_double);
}

SessionLogHandle HybridCoJSONCoreRN::createSessionLog(const std::string& coId, const std::string& sessionId, const std::string& signerId) {
  auto handle = create_session_log(coId, sessionId, signerId);
  return fromRustHandle(handle);
}

SessionLogHandle HybridCoJSONCoreRN::cloneSessionLog(const SessionLogHandle& handle) {
  auto rustHandle = toRustHandle(handle);
  auto clonedHandle = clone_session_log(rustHandle);
  return fromRustHandle(clonedHandle);
}

TransactionResult HybridCoJSONCoreRN::tryAddTransactions(const SessionLogHandle& handle, const std::vector<std::string>& transactionsJson,
                                                         const std::string& newSignature, bool skipVerify) {
  // Convert std::vector<std::string> to rust::Vec<rust::String>
  rust::Vec<rust::String> rustTransactions;
  for (const auto& tx : transactionsJson) {
    rustTransactions.push_back(rust::String(tx));
  }

  auto rustHandle = toRustHandle(handle);
  auto result = try_add_transactions(rustHandle, rustTransactions, rust::String(newSignature), skipVerify);
  return TransactionResult(result.success, std::string(result.result), std::string(result.error));
}

TransactionResult HybridCoJSONCoreRN::addNewPrivateTransaction(const SessionLogHandle& handle, const std::string& changesJson,
                                                               const std::string& signerSecret, const std::string& encryptionKey,
                                                               const std::string& keyId, double madeAt, const std::string& meta) {
  auto rustHandle = toRustHandle(handle);
  auto result = add_new_private_transaction(rustHandle, rust::String(changesJson), rust::String(signerSecret),
                                            rust::String(encryptionKey), rust::String(keyId), madeAt, rust::String(meta));
  return TransactionResult(result.success, std::string(result.result), std::string(result.error));
}

TransactionResult HybridCoJSONCoreRN::addNewTrustingTransaction(const SessionLogHandle& handle, const std::string& changesJson,
                                                                const std::string& signerSecret, double madeAt, const std::string& meta) {
  auto rustHandle = toRustHandle(handle);
  auto result = add_new_trusting_transaction(rustHandle, rust::String(changesJson), rust::String(signerSecret), madeAt, rust::String(meta));
  return TransactionResult(result.success, std::string(result.result), std::string(result.error));
}

TransactionResult HybridCoJSONCoreRN::testExpectedHashAfter(const SessionLogHandle& handle,
                                                            const std::vector<std::string>& transactionsJson) {
  // Convert std::vector<std::string> to rust::Vec<rust::String>
  rust::Vec<rust::String> rustTransactions;
  for (const auto& tx : transactionsJson) {
    rustTransactions.push_back(rust::String(tx));
  }

  auto rustHandle = toRustHandle(handle);
  auto result = test_expected_hash_after(rustHandle, rustTransactions);
  return TransactionResult(result.success, std::string(result.result), std::string(result.error));
}

TransactionResult HybridCoJSONCoreRN::decryptNextTransactionChangesJson(const SessionLogHandle& handle, double txIndex,
                                                                        const std::shared_ptr<margelo::nitro::ArrayBuffer>& keySecret) {
  // Convert ArrayBuffer to rust::Vec<uint8_t>
  rust::Vec<uint8_t> keySecretVec;
  if (keySecret) {
    const uint8_t* data = keySecret->data();
    size_t size = keySecret->size();
    for (size_t i = 0; i < size; ++i) {
      keySecretVec.push_back(data[i]);
    }
  }

  auto rustHandle = toRustHandle(handle);
  auto result = decrypt_next_transaction_changes_json(rustHandle, static_cast<uint32_t>(txIndex), keySecretVec);
  return TransactionResult(result.success, std::string(result.result), std::string(result.error));
}

void HybridCoJSONCoreRN::destroySessionLog(const SessionLogHandle& handle) {
  auto rustHandle = toRustHandle(handle);
  destroy_session_log(rustHandle);
}

U8VecResult HybridCoJSONCoreRN::sealMessage(const std::shared_ptr<ArrayBuffer>& message, const std::string& senderSecret, 
                                            const std::string& recipientId, const std::shared_ptr<ArrayBuffer>& nonceMaterial) {
  // Convert ArrayBuffer to rust::Vec<uint8_t>
  rust::Vec<uint8_t> messageVec;
  if (message) {
    const uint8_t* data = message->data();
    size_t size = message->size();
    for (size_t i = 0; i < size; ++i) {
      messageVec.push_back(data[i]);
    }
  }

  rust::Vec<uint8_t> nonceMaterialVec;
  if (nonceMaterial) {
    const uint8_t* data = nonceMaterial->data();
    size_t size = nonceMaterial->size();
    for (size_t i = 0; i < size; ++i) {
      nonceMaterialVec.push_back(data[i]);
    }
  }

  auto result = seal_message(messageVec, rust::String(senderSecret), rust::String(recipientId), nonceMaterialVec);
  
  // Convert rust::Vec<uint8_t> to ArrayBuffer
  std::shared_ptr<ArrayBuffer> dataArrayBuffer;
  if (result.success && !result.data.empty()) {
    dataArrayBuffer = ArrayBuffer::allocate(result.data.size());
    uint8_t* buffer = dataArrayBuffer->data();
    for (size_t i = 0; i < result.data.size(); ++i) {
      buffer[i] = result.data[i];
    }
  }

  return U8VecResult(result.success, dataArrayBuffer, std::string(result.error));
}

U8VecResult HybridCoJSONCoreRN::unsealMessage(const std::shared_ptr<ArrayBuffer>& sealedMessage, const std::string& recipientSecret, 
                                              const std::string& senderId, const std::shared_ptr<ArrayBuffer>& nonceMaterial) {
  // Convert ArrayBuffer to rust::Vec<uint8_t>
  rust::Vec<uint8_t> sealedMessageVec;
  if (sealedMessage) {
    const uint8_t* data = sealedMessage->data();
    size_t size = sealedMessage->size();
    for (size_t i = 0; i < size; ++i) {
      sealedMessageVec.push_back(data[i]);
    }
  }

  rust::Vec<uint8_t> nonceMaterialVec;
  if (nonceMaterial) {
    const uint8_t* data = nonceMaterial->data();
    size_t size = nonceMaterial->size();
    for (size_t i = 0; i < size; ++i) {
      nonceMaterialVec.push_back(data[i]);
    }
  }

  auto result = unseal_message(sealedMessageVec, rust::String(recipientSecret), rust::String(senderId), nonceMaterialVec);
  
  // Convert rust::Vec<uint8_t> to ArrayBuffer
  std::shared_ptr<ArrayBuffer> dataArrayBuffer;
  if (result.success && !result.data.empty()) {
    dataArrayBuffer = ArrayBuffer::allocate(result.data.size());
    uint8_t* buffer = dataArrayBuffer->data();
    for (size_t i = 0; i < result.data.size(); ++i) {
      buffer[i] = result.data[i];
    }
  }

  return U8VecResult(result.success, dataArrayBuffer, std::string(result.error));
}

} // namespace margelo::nitro::cojson_core_rn
