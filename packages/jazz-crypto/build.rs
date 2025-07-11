fn main() {
    cxx_build::bridge("src/rust/lib.rs")
        .file("src/cpp/HybridJazzCrypto.cpp")
        .include("src/cpp")
        .include("includes")
        .include("../../node_modules/react-native/ReactCommon/jsi")
        .include("nitrogen/generated/shared/c++")
        .include("target/cxxbridge/jazz-crypto/src/rust")
        .include("target/cxxbridge/rust")
        .std("c++20")
        .compile("jazz_crypto");

    println!("cargo:rerun-if-changed=src/rust/lib.rs");
    println!("cargo:rerun-if-changed=src/cpp/HybridJazzCrypto.cpp");
}
