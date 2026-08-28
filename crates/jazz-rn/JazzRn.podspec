require "json"

package = JSON.parse(File.read(File.join(__dir__, "package.json")))
folly_compiler_flags = '-DFOLLY_NO_CONFIG -DFOLLY_MOBILE=1 -DFOLLY_USE_LIBCPP=1 -Wno-comma -Wno-shorten-64-to-32'

# Expo records this in Podfile.properties.json rather than setting
# RCT_NEW_ARCH_ENABLED=1. Bare React Native projects conventionally set the
# environment variable. Accept either source of the same configuration.
podfile_properties_path = File.join(Pod::Config.instance.installation_root, "Podfile.properties.json")
podfile_properties = JSON.parse(File.read(podfile_properties_path)) if File.exist?(podfile_properties_path)
new_arch_enabled = ENV['RCT_NEW_ARCH_ENABLED'] == '1' || podfile_properties&.fetch('newArchEnabled', nil) == 'true'

if !new_arch_enabled then
  raise Pod::Informative, "jazz-rn requires the React Native New Architecture. Enable it before pod install (Expo: add the jazz-rn config plugin, then run expo prebuild)."
end

Pod::Spec.new do |s|
  s.name         = "JazzRn"
  s.version      = package["version"]
  s.summary      = package["description"]
  s.homepage     = package["homepage"]
  s.license      = package["license"]
  s.authors      = package["author"]

  s.platforms    = { :ios => min_ios_version_supported }
  s.source       = { :git => "https://github.com/garden-co/jazz.git", :tag => "#{s.version}" }

  s.source_files = "ios/**/*.{h,m,mm,swift}", "ios/generated/**/*.{h,m,mm}"
  # Consumer code may import only this stable host-facing surface. The
  # TurboModule protocol is generated into React-Codegen for this pod target;
  # it must remain an implementation detail instead of leaking through a
  # public header that an application compiles without that generated path.
  s.public_header_files = "ios/JazzRelay.h"
  s.private_header_files = "ios/JazzRelayModule.h"
  # The staged header is the source-of-truth ABI declaration shared with the
  # Android package. Keep this path when the legacy RN dependency branch below
  # adds its own headers: assigning pod_target_xcconfig twice would otherwise
  # compile this source with the unavailable-artifact fallback even though the
  # package contains a valid XCFramework.
  relay_header_search_path = "$(PODS_TARGET_SRCROOT)/native/include"
  relay_framework = File.join(__dir__, "JazzNativeRelay.xcframework")
  if File.exist?(relay_framework) then
    s.vendored_frameworks = "JazzNativeRelay.xcframework"
    s.pod_target_xcconfig = { "HEADER_SEARCH_PATHS" => relay_header_search_path }
  end
  # Use install_modules_dependencies helper to install the dependencies if React Native version >=0.71.0.
  # See https://github.com/facebook/react-native/blob/febf6b7f33fdb4904669f99d795eba4c0f95d7bf/scripts/cocoapods/new_architecture.rb#L79.
  if respond_to?(:install_modules_dependencies, true)
    install_modules_dependencies(s)
  else
    s.dependency "React-Core"

    if new_arch_enabled then
      s.compiler_flags = folly_compiler_flags + " -DRCT_NEW_ARCH_ENABLED=1"
      current_header_paths = s.pod_target_xcconfig&.fetch("HEADER_SEARCH_PATHS", "") || ""
      s.pod_target_xcconfig    = {
          "HEADER_SEARCH_PATHS" => "#{current_header_paths} \"$(PODS_ROOT)/boost\"",
          "OTHER_CPLUSPLUSFLAGS" => "-DFOLLY_NO_CONFIG -DFOLLY_MOBILE=1 -DFOLLY_USE_LIBCPP=1",
          "CLANG_CXX_LANGUAGE_STANDARD" => "c++17"
      }
      s.dependency "React-Codegen"
      s.dependency "RCT-Folly"
      s.dependency "RCTRequired"
      s.dependency "RCTTypeSafety"
      s.dependency "ReactCommon/turbomodule/core"
    end
  end
end
