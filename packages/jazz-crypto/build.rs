fn main() {
    println!("cargo:rerun-if-changed=src/rust/lib.rs");
    println!("cargo:rerun-if-changed=src/cpp/HybridJazzCrypto.cpp");
    
    let target = std::env::var("TARGET").unwrap();
    let is_android = target.contains("android");
    
    let mut build = cxx_build::bridge("src/rust/lib.rs");
    build.file("src/cpp/HybridJazzCrypto.cpp");
    build.include("src/cpp")
        .include("includes")
        .include("includes/rust")
        .include("includes/NitroModules")
        .include("nitrogen/generated/shared/c++");
    
    // Use C++20 for both platforms
    build.std("c++20");
    
    build.flag_if_supported("-fPIC");
    
    // Add platform-specific includes
    if is_android {
        // Android-specific JSI headers path
        if std::path::Path::new("../../node_modules/react-native/ReactAndroid/src/main/jni/react/jsi").exists() {
            build.include("../../node_modules/react-native/ReactAndroid/src/main/jni/react/jsi");
        } else {
            build.include("../../node_modules/react-native/ReactCommon/jsi");
        }
    } else {
        // iOS JSI headers path
        build.include("../../node_modules/react-native/ReactCommon/jsi");
    }
    
    build.compile("jazz_crypto");
}
