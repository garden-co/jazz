#include <string>

#include <NitroModules/ArrayBuffer.hpp>

#include "HybridJazzCryptoSpec.hpp"

namespace margelo {
namespace nitro {
namespace jazz_crypto {

using namespace margelo::nitro;
  
class HybridJazzCrypto: public HybridJazzCryptoSpec {

 public:
  HybridJazzCrypto(): HybridObject(TAG) {}

 public:
  std::string no_args_return_string() override;
  std::string args_return_string(const std::string& arg1) override;
  std::shared_ptr<ArrayBuffer> no_args_return_ab() override;
  std::shared_ptr<ArrayBuffer> args_return_ab(const std::shared_ptr<ArrayBuffer>& arg1) override;

};

} // namespace jazz_crypto
} // namespace nitro
} // namespace margelo
