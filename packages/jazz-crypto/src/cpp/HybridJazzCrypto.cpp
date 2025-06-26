#include <stdexcept>
#include "HybridJazzCrypto.hpp"

namespace margelo {
namespace nitro {
namespace jazz_crypto {

std::string HybridJazzCrypto::no_args_return_string() {
    throw std::runtime_error("Not implemented");
}

std::string HybridJazzCrypto::args_return_string(const std::string& arg1) {
    throw std::runtime_error("Not implemented");
}

std::shared_ptr<ArrayBuffer> HybridJazzCrypto::no_args_return_ab() {
    throw std::runtime_error("Not implemented");
}

std::shared_ptr<ArrayBuffer> HybridJazzCrypto::args_return_ab(const std::shared_ptr<ArrayBuffer>& arg1) {
    throw std::runtime_error("Not implemented");
}

} // namespace jazz_crypto
} // namespace nitro
} // namespace margelo
