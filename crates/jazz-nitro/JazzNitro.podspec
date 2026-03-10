require "json"

# __dir__ is crates/jazz-nitro/. All file patterns are relative to here.
# The absolute path is used only inside the shell build script.
CRATE_ABS = File.expand_path(__dir__)

Pod::Spec.new do |s|
  s.name         = "JazzNitro"
  s.version      = "0.1.0"
  s.summary      = "Fjall storage spike via Nitro Modules Rust bridge."
  s.homepage     = "https://github.com/gardencmp/jazz"
  s.license      = { :type => "MIT" }
  s.authors      = { "Jazz" => "jazz@garden.co" }
  s.platforms    = { :ios => "15.1" }
  s.source       = { :path => "." }

  s.source_files = [
    "nitrogen/generated/shared/**/*.{h,hpp,c,cpp}",
    "nitrogen/generated/ios/**/*.{h,hpp,c,cpp,mm}",
  ]

  s.public_header_files = [
    "nitrogen/generated/shared/**/*.{h,hpp}",
    "nitrogen/generated/ios/JazzNitro-Swift-Cxx-Bridge.hpp",
  ]

  s.private_header_files = [
    "nitrogen/generated/ios/c++/**/*.{h,hpp}",
  ]

  rust_src_dir = "#{CRATE_ABS}/nitrogen/generated/shared/rust"
  rust_lib_dir = "#{CRATE_ABS}/nitrogen/generated/shared/rust/target/apple"

  s.script_phases = [{
    :name => "Build Rust Library (jazz_nitro_rust)",
    :script => <<~SHELL,
      set -e
      RUST_SRC_DIR="#{rust_src_dir}"
      RUST_LIB_DIR="#{rust_lib_dir}"

      if [ "$PLATFORM_NAME" = "iphonesimulator" ]; then
        if [ "$ARCHS" = "x86_64" ]; then
          RUST_TARGET="x86_64-apple-ios"
        else
          RUST_TARGET="aarch64-apple-ios-sim"
        fi
      elif [ "$PLATFORM_NAME" = "macosx" ]; then
        if [ "$ARCHS" = "x86_64" ]; then
          RUST_TARGET="x86_64-apple-darwin"
        else
          RUST_TARGET="aarch64-apple-darwin"
        fi
      else
        RUST_TARGET="aarch64-apple-ios"
      fi

      if ! command -v cargo &> /dev/null; then
        export PATH="$HOME/.cargo/bin:$PATH"
      fi

      # Xcode sets CC/CXX/LD/SDKROOT to the iOS SDK, which breaks build scripts
      # (proc-macros, libc, etc.) that must compile and run on the host.
      # We must reset SDKROOT to the macOS SDK (not unset it) because the
      # Xcode-bundled cc requires SDKROOT to locate system libraries.
      unset CC CXX LD AR CFLAGS CXXFLAGS LDFLAGS LIBRARY_PATH
      export SDKROOT="$(xcrun --sdk macosx --show-sdk-path)"

      # Tell cargo which linker to use for the cross-compile target.
      export CARGO_TARGET_AARCH64_APPLE_IOS_SIM_LINKER="$(xcrun --sdk iphonesimulator --find clang)"
      export CARGO_TARGET_AARCH64_APPLE_IOS_LINKER="$(xcrun --sdk iphoneos --find clang)"
      export CARGO_TARGET_X86_64_APPLE_IOS_LINKER="$(xcrun --sdk iphonesimulator --find clang)"

      echo "Building jazz_nitro_rust for $RUST_TARGET..."
      cd "$RUST_SRC_DIR"
      cargo build --release --target "$RUST_TARGET"

      mkdir -p "$RUST_LIB_DIR"
      cp "target/$RUST_TARGET/release/libjazz_nitro_rust.a" "$RUST_LIB_DIR/"
    SHELL
    :execution_position => :before_compile,
    :shell_path => "/bin/sh",
  }]

  s.vendored_libraries = "nitrogen/generated/shared/rust/target/apple/libjazz_nitro_rust.a"

  s.pod_target_xcconfig = {
    "CLANG_CXX_LANGUAGE_STANDARD" => "c++20",
    "SWIFT_OBJC_INTEROP_MODE" => "objcxx",
    "DEFINES_MODULE" => "YES",
    "LIBRARY_SEARCH_PATHS" => "\"#{rust_lib_dir}\"",
    "OTHER_LDFLAGS" => "-ljazz_nitro_rust",
  }

  # Ensure the app target also links the Rust library
  s.user_target_xcconfig = {
    "LIBRARY_SEARCH_PATHS" => "\"#{rust_lib_dir}\"",
    "OTHER_LDFLAGS" => "-ljazz_nitro_rust",
  }

  s.dependency "NitroModules"
  s.dependency "React-jsi"
  s.dependency "React-callinvoker"
end
