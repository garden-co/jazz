require "json"

package = JSON.parse(File.read(File.join(__dir__, "package.json")))

Pod::Spec.new do |s|
  s.name         = "CojsonCoreRn"
  s.version      = package["version"]
  s.summary      = package["description"]
  s.homepage     = package["homepage"]
  s.license      = package["license"]
  s.authors      = package["author"]

  s.platforms    = { :ios => min_ios_version_supported }
  s.source       = { :git => "https://github.com/garden-co/jazz.git", :tag => "#{s.version}" }

  s.source_files = ["ios/**/*.{m,mm,cc,cpp}", "cpp/**/*.cpp"]
  s.vendored_frameworks = "ios/framework/libcojsoncorern.xcframework"
  s.pod_target_xcconfig = {
    "HEADER_SEARCH_PATHS" => [
      '"${PODS_TARGET_SRCROOT}/cpp"',
      '"${PODS_TARGET_SRCROOT}/ios/include"',
    ].join(' '),
    "CLANG_CXX_LANGUAGE_STANDARD" => "c++20",
  }

  install_modules_dependencies(s)
end
