#import "JazzRelay.h"

#if __has_include("jazz_native_relay.h")
#import "jazz_native_relay.h"
#define JAZZ_RELAY_ARTIFACT_AVAILABLE 1
#else
#define JAZZ_RELAY_ARTIFACT_AVAILABLE 0
#endif

#ifdef RCT_NEW_ARCH_ENABLED
#import <React/RCTBridgeModule.h>
#import <ReactCommon/RCTTurboModule.h>
#endif

@implementation JazzRelay

#if JAZZ_RELAY_ARTIFACT_AVAILABLE
static jazz_native_relay_host *relayHost = NULL;
#endif

RCT_EXPORT_MODULE(JazzRelay)

- (NSNumber *)getAbiVersion {
  // ABI zero is the stable unavailable sentinel. The autolinked host exists,
  // but a matching Rust relay artifact has not been embedded yet.
#if JAZZ_RELAY_ARTIFACT_AVAILABLE
  return @(jazz_native_relay_abi_version());
#else
  return @0;
#endif
}

- (void)execute:(NSString *)commandBase64
         resolve:(RCTPromiseResolveBlock)resolve
          reject:(RCTPromiseRejectBlock)reject {
#if JAZZ_RELAY_ARTIFACT_AVAILABLE
  if (relayHost == NULL) relayHost = jazz_native_relay_host_new();
  NSData *request = [[NSData alloc] initWithBase64EncodedString:commandBase64 options:0];
  jazz_native_relay_bytes output = {0};
  jazz_native_relay_status status = jazz_native_relay_host_execute(
      relayHost, (const uint8_t *)request.bytes, request.length, &output);
  if (status != JAZZ_NATIVE_RELAY_OK) {
    reject(@"E_JAZZ_RELAY_COMMAND", @"Jazz native relay command failed", nil);
    return;
  }
  NSData *response = [NSData dataWithBytes:output.data length:output.len];
  jazz_native_relay_bytes_free(&output);
  resolve([response base64EncodedStringWithOptions:0]);
#else
  reject(@"E_JAZZ_RELAY_UNAVAILABLE", @"Jazz native relay commands require a development or release build containing the shared Rust relay artifact.", nil);
#endif
}

#ifdef RCT_NEW_ARCH_ENABLED
- (std::shared_ptr<facebook::react::TurboModule>)getTurboModule:
    (const facebook::react::ObjCTurboModule::InitParams &)params {
  return std::make_shared<facebook::react::NativeJazzRelaySpecJSI>(params);
}
#endif

@end
