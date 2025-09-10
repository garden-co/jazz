#include "HybridCoJSONCoreRN.hpp"
#include "rust/lib.rs.h"

namespace margelo::nitro::cojson_core_rn {

// Helper function to convert Nitro SessionLogHandle to Rust FFI SessionLogHandle
// Use thread-local storage to avoid race conditions between threads
static thread_local ::SessionLogHandle rustHandleStorage;

static const ::SessionLogHandle& toRustHandle(const SessionLogHandle& nitroHandle) {
  // Create a Rust handle struct directly without calling create_session_log
  // The Rust side will look up the existing session log by ID
  // Using thread_local ensures each thread has its own storage
  rustHandleStorage.id = static_cast<uint64_t>(nitroHandle.id);
  return rustHandleStorage;
}

SessionLogHandle HybridCoJSONCoreRN::createSessionLog(const std::string& coId, const std::string& sessionId, const std::string& signerId) {
  auto handle = create_session_log(coId, sessionId, signerId);
  return SessionLogHandle(static_cast<double>(handle.id));
}

SessionLogHandle HybridCoJSONCoreRN::cloneSessionLog(const SessionLogHandle& handle) {
  auto clonedHandle = clone_session_log(toRustHandle(handle));
  return SessionLogHandle(static_cast<double>(clonedHandle.id));
}

TransactionResult HybridCoJSONCoreRN::tryAddTransactions(const SessionLogHandle& handle, const std::vector<std::string>& transactionsJson,
                                                         const std::string& newSignature, bool skipVerify) {
  // Convert std::vector<std::string> to rust::Vec<rust::String>
  rust::Vec<rust::String> rustTransactions;
  for (const auto& tx : transactionsJson) {
    rustTransactions.push_back(rust::String(tx));
  }

  auto result = try_add_transactions(toRustHandle(handle), rustTransactions, rust::String(newSignature), skipVerify);
  return TransactionResult(result.success, std::string(result.result), std::string(result.error));
}

TransactionResult HybridCoJSONCoreRN::addNewPrivateTransaction(const SessionLogHandle& handle, const std::string& changesJson,
                                                               const std::string& signerSecret, const std::string& encryptionKey,
                                                               const std::string& keyId, double madeAt, const std::string& meta) {
  auto result = add_new_private_transaction(toRustHandle(handle), rust::String(changesJson), rust::String(signerSecret),
                                            rust::String(encryptionKey), rust::String(keyId), madeAt, rust::String(meta));
  return TransactionResult(result.success, std::string(result.result), std::string(result.error));
}

TransactionResult HybridCoJSONCoreRN::addNewTrustingTransaction(const SessionLogHandle& handle, const std::string& changesJson,
                                                                const std::string& signerSecret, double madeAt, const std::string& meta) {
  auto result = add_new_trusting_transaction(toRustHandle(handle), rust::String(changesJson), rust::String(signerSecret), madeAt, rust::String(meta));
  return TransactionResult(result.success, std::string(result.result), std::string(result.error));
}

TransactionResult HybridCoJSONCoreRN::testExpectedHashAfter(const SessionLogHandle& handle,
                                                            const std::vector<std::string>& transactionsJson) {
  // Convert std::vector<std::string> to rust::Vec<rust::String>
  rust::Vec<rust::String> rustTransactions;
  for (const auto& tx : transactionsJson) {
    rustTransactions.push_back(rust::String(tx));
  }

  auto result = test_expected_hash_after(toRustHandle(handle), rustTransactions);
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

  auto result = decrypt_next_transaction_changes_json(toRustHandle(handle), static_cast<uint32_t>(txIndex), keySecretVec);
  return TransactionResult(result.success, std::string(result.result), std::string(result.error));
}

void HybridCoJSONCoreRN::destroySessionLog(const SessionLogHandle& handle) {
  destroy_session_log(toRustHandle(handle));
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
