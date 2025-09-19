#include <string>
#include <vector>

#include "HybridCoJSONCoreRNSpec.hpp"
#include "SessionLogHandle.hpp"
#include "TransactionResult.hpp"
#include "U8VecResult.hpp"

namespace margelo::nitro::cojson_core_rn {

using namespace margelo::nitro;

class HybridCoJSONCoreRN : public HybridCoJSONCoreRNSpec {

 public:
  HybridCoJSONCoreRN() : HybridObject(TAG) {}

 public:
  // Virtual function overrides from HybridCoJSONCoreRNSpec
  SessionLogHandle createSessionLog(const std::string& coId, const std::string& sessionId, const std::string& signerId) override;
  SessionLogHandle cloneSessionLog(const SessionLogHandle& handle) override;
  TransactionResult tryAddTransactions(const SessionLogHandle& handle, const std::vector<std::string>& transactionsJson,
                                       const std::string& newSignature, bool skipVerify) override;
  TransactionResult addNewPrivateTransaction(const SessionLogHandle& handle, const std::string& changesJson,
                                             const std::string& signerSecret, const std::string& encryptionKey, const std::string& keyId,
                                             double madeAt, const std::string& meta) override;
  TransactionResult addNewTrustingTransaction(const SessionLogHandle& handle, const std::string& changesJson,
                                              const std::string& signerSecret, double madeAt, const std::string& meta) override;
  TransactionResult testExpectedHashAfter(const SessionLogHandle& handle, const std::vector<std::string>& transactionsJson) override;
  TransactionResult decryptNextTransactionChangesJson(const SessionLogHandle& handle, double txIndex,
                                                      const std::string& keySecret) override;
  void destroySessionLog(const SessionLogHandle& handle) override;
  U8VecResult sealMessage(const std::shared_ptr<ArrayBuffer>& message, const std::string& senderSecret, 
                          const std::string& recipientId, const std::shared_ptr<ArrayBuffer>& nonceMaterial) override;
  U8VecResult unsealMessage(const std::shared_ptr<ArrayBuffer>& sealedMessage, const std::string& recipientSecret, 
                            const std::string& senderId, const std::shared_ptr<ArrayBuffer>& nonceMaterial) override;
};

} // namespace margelo::nitro::cojson_core_rn
