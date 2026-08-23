#import "JazzRelay.h"

#ifdef RCT_NEW_ARCH_ENABLED
#import <React/RCTBridgeModule.h>
#import <ReactCommon/RCTTurboModule.h>
#endif

@implementation JazzRelay

RCT_EXPORT_MODULE(JazzRelay)

- (NSNumber *)getAbiVersion {
  // ABI zero is the stable unavailable sentinel. The autolinked host exists,
  // but a matching Rust relay artifact has not been embedded yet.
  return @0;
}

- (void)execute:(NSString *)commandBase64
         resolve:(RCTPromiseResolveBlock)resolve
          reject:(RCTPromiseRejectBlock)reject {
  reject(
      @"E_JAZZ_RELAY_UNAVAILABLE",
      @"Jazz native relay commands require a development or release build containing the shared Rust relay artifact.",
      nil);
}

#ifdef RCT_NEW_ARCH_ENABLED
- (std::shared_ptr<facebook::react::TurboModule>)getTurboModule:
    (const facebook::react::ObjCTurboModule::InitParams &)params {
  return std::make_shared<facebook::react::NativeJazzRelaySpecJSI>(params);
}
#endif

@end
