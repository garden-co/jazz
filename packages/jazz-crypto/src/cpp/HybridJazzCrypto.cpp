#include "HybridJazzCrypto.hpp"
#include "lib.rs.h"
#include "cxx.h"

namespace margelo {
namespace nitro {
namespace jazz_crypto {

std::string HybridJazzCrypto::no_args_return_string() {
  rust::String result = rust_no_args_return_string();
  return std::string(result.data());
}

std::string HybridJazzCrypto::args_return_string(const std::string& arg1) {
  rust::String result = rust_args_return_string(arg1);
  return std::string(result.data());
}

std::shared_ptr<ArrayBuffer> HybridJazzCrypto::no_args_return_ab() {
  rust::Vec<uint8_t> result = rust_no_args_return_ab();
  return std::make_shared<NativeArrayBuffer>(result.data(), result.size(), [=]() { delete[] result.data(); });
}

std::shared_ptr<ArrayBuffer> HybridJazzCrypto::args_return_ab(const std::shared_ptr<ArrayBuffer>& arg1) {
  auto ab = ToNativeArrayBuffer(arg1);
  rust::Vec<uint8_t> arg1_vec;
  arg1_vec.reserve(ab->size());
  for (size_t i = 0; i < ab->size(); ++i) {
    arg1_vec.push_back(ab->data()[i]);
  }
  rust::Vec<uint8_t> result = rust_args_return_ab(arg1_vec);
  return std::make_shared<NativeArrayBuffer>(result.data(), result.size(), [=]() { delete[] result.data(); });
}

} // namespace jazz_crypto
} // namespace nitro
} // namespace margelo
