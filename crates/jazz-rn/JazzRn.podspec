require "json"
require "fileutils"

package = JSON.parse(File.read(File.join(__dir__, "package.json")))
folly_compiler_flags = '-DFOLLY_NO_CONFIG -DFOLLY_MOBILE=1 -DFOLLY_USE_LIBCPP=1 -Wno-comma -Wno-shorten-64-to-32'

# Ask React Native itself whether this pod install uses the New Architecture.
# The Podfile requires react_native_pods.rb before autolinking evaluates this
# podspec, so NewArchitectureHelper is already loaded. It is the same answer
# use_react_native! acts on: always true from React Native 0.82 (the legacy
# architecture no longer exists, and Expo SDK 55+ stops writing newArchEnabled
# to Podfile.properties.json), and RCT_NEW_ARCH_ENABLED != "0" on 0.81 (which
# Expo SDK 54's Podfile sets from Podfile.properties.json). Do not read
# RCT_NEW_ARCH_ENABLED directly when the helper exists: use_react_native! only
# sets it after autolinking has already evaluated this file.
# Outside a React Native Podfile (pod spec lint, ad-hoc evaluation) fall back to
# the explicit signals a host can give.
new_arch_enabled =
  if defined?(NewArchitectureHelper) && NewArchitectureHelper.respond_to?(:new_arch_enabled)
    NewArchitectureHelper.new_arch_enabled == true
  elsif ENV['RCT_NEW_ARCH_ENABLED'] == '1'
    true
  else
    podfile_properties_path = File.join(Pod::Config.instance.installation_root, "Podfile.properties.json")
    File.exist?(podfile_properties_path) &&
      JSON.parse(File.read(podfile_properties_path)).fetch('newArchEnabled', nil) == 'true'
  end

if !new_arch_enabled then
  raise Pod::Informative, "jazz-rn requires the React Native New Architecture, and this pod install uses the legacy architecture. Expo SDK 54: remove `newArchEnabled: false` from your app config and run expo prebuild again. Bare React Native 0.81: do not set RCT_NEW_ARCH_ENABLED=0."
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

  s.source_files = "ios/**/*.{h,m,mm,swift}", "ios/generated/**/*.{h,m,mm}", "native/**/*.{h,cpp}"
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
  payload_script = File.join(__dir__, "scripts/resolve-payload.cjs")
  payload_root = Pod::Executable.execute_command("node", [payload_script, "ios"]).strip
  relay_framework = File.join(payload_root, "JazzNativeRelay.xcframework")
  unless File.directory?(relay_framework)
    raise Pod::Informative, "jazz-rn iOS payload is missing its XCFramework"
  end
  # CocoaPods only globs paths inside this pod's root. Expose the resolved
  # dependency through a package-local symlink without copying or modifying its
  # sealed bytes. Refuse an unmanaged file/directory at this generated path.
  payload_link_directory = File.join(__dir__, "ios-payload")
  if File.symlink?(payload_link_directory) || (File.exist?(payload_link_directory) && !File.directory?(payload_link_directory))
    raise Pod::Informative, "jazz-rn generated iOS payload directory is occupied: #{payload_link_directory}"
  end
  FileUtils.mkdir_p(payload_link_directory)
  payload_link = File.join(payload_link_directory, "JazzNativeRelay.xcframework")
  if File.symlink?(payload_link)
    File.unlink(payload_link) if File.readlink(payload_link) != relay_framework
  elsif File.exist?(payload_link)
    raise Pod::Informative, "jazz-rn generated iOS payload path is occupied: #{payload_link}"
  end
  File.symlink(relay_framework, payload_link) unless File.symlink?(payload_link)
  s.vendored_frameworks = "ios-payload/JazzNativeRelay.xcframework"
  # NWPathMonitor tells the relay when the device's network path changes.
  s.frameworks = "Network"
  s.pod_target_xcconfig = { "HEADER_SEARCH_PATHS" => relay_header_search_path }
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
