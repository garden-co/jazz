#include <vector>
#include "HybridJazzCrypto.hpp"

namespace margelo {
namespace nitro {
namespace jazz_crypto {

// Helper to convert Rust string to std::string and free memory
std::string rust_string_to_std_string(char* rust_str) {
    if (!rust_str) {
        return "";
    }
    std::string result(rust_str);
    free_rust_string(rust_str); // Free the memory allocated by Rust
    return result;
}

// Helper to convert Rust ByteBuffer to std::shared_ptr<ArrayBuffer> and free memory
std::shared_ptr<ArrayBuffer> rust_byte_buffer_to_array_buffer(ByteBuffer rust_buf) {
    if (!rust_buf.ptr) {
        return std::make_shared<ArrayBuffer>(0);
    }
    // Create a vector and copy the data
    std::vector<uint8_t> vec(rust_buf.ptr, rust_buf.ptr + rust_buf.len);
    // Free the memory allocated by Rust
    free_rust_byte_buffer(rust_buf);
    // Create and return the ArrayBuffer
    return std::make_shared<ArrayBuffer>(std::move(vec));
}

std::string HybridJazzCrypto::no_args_return_string() {
    char* rust_result = no_args_return_string();
    return rust_string_to_std_string(rust_result);
}

std::string HybridJazzCrypto::args_return_string(const std::string& arg1) {
    char* rust_result = args_return_string(arg1.c_str());
    return rust_string_to_std_string(rust_result);
}

std::shared_ptr<ArrayBuffer> HybridJazzCrypto::no_args_return_ab() {
    ByteBuffer rust_result = no_args_return_ab();
    return rust_byte_buffer_to_array_buffer(rust_result);
}

std::shared_ptr<ArrayBuffer> HybridJazzCrypto::args_return_ab(const std::shared_ptr<ArrayBuffer>& arg1) {
    ByteBuffer rust_result = args_return_ab(arg1->data(), arg1->size());
    return rust_byte_buffer_to_array_buffer(rust_result);
}

} // namespace jazz_crypto
} // namespace nitro
} // namespace margelo
