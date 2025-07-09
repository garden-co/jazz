#include <string>
#include <memory>

#include <NitroModules/ArrayBuffer.hpp>

#include "HybridJazzCryptoSpec.hpp"

// Define the struct for FFI with Rust
struct ByteBuffer {
    uint8_t* ptr;
    size_t len;
    size_t cap;
};

// Declare the C-style functions from the Rust FFI
extern "C" {
    // String functions
    char* rust_no_args_return_string();
    char* rust_args_return_string(const char* arg1);
    void free_rust_string(char* s);

    // ArrayBuffer (byte buffer) functions
    ByteBuffer rust_no_args_return_ab();
    ByteBuffer rust_args_return_ab(const uint8_t* arg1_ptr, size_t arg1_len);
    void free_rust_byte_buffer(ByteBuffer buf);
}

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
