#include <string>
#include <memory>

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

 protected:
  // copy a JSArrayBuffer that we do not own into a NativeArrayBuffer that we do own
  inline std::shared_ptr<margelo::nitro::NativeArrayBuffer> ToNativeArrayBuffer(const std::shared_ptr<margelo::nitro::ArrayBuffer>& buffer) {
    size_t bufferSize = buffer.get()->size();
    uint8_t* data = new uint8_t[bufferSize];
    memcpy(data, buffer.get()->data(), bufferSize);
    return std::make_shared<margelo::nitro::NativeArrayBuffer>(data, bufferSize, [=]() { delete[] data; });
  }
  
};

} // namespace jazz_crypto
} // namespace nitro
} // namespace margelo
