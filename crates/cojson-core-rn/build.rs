fn main() {
    println!("cargo:rerun-if-changed=src/lib.rs");

    let target = std::env::var("TARGET").unwrap();
    let is_android = target.contains("android");

    let mut build = cxx_build::bridge("src/lib.rs");
    build.file("pkg/cpp/HybridCoJSONCoreRN.cpp");

    build
        .include("pkg/cpp")
        .include("pkg/build/includes")
        .include("pkg/nitrogen/generated/shared/c++");

    // Use C++20 for both platforms
    build.std("c++20");

    build.flag_if_supported("-fPIC");

    // Add platform-specific includes
    if is_android {
        // Android-specific JSI headers path
        if std::path::Path::new(
            "../../node_modules/react-native/ReactAndroid/src/main/jni/react/jsi",
        )
        .exists()
        {
            build.include("../../node_modules/react-native/ReactAndroid/src/main/jni/react/jsi");
        } else {
            build.include("../../node_modules/react-native/ReactCommon/jsi");
        }
    } else {
        // iOS JSI headers path
        build.include("../../node_modules/react-native/ReactCommon/jsi");
    }

    build.compile("cojson_core_rn");
}
