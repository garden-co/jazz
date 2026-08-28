#import <React/RCTBridgeModule.h>
#import <JazzNativeRelay/JazzRelay.h>

/** Development-build-only trusted fixture. Configuration is a compile-time
 * test fixture; JS is given only the opaque random admission capability. */
@interface JazzDeviceFixture : NSObject <RCTBridgeModule>
@property(nonatomic, nullable) NSData *capability;
@end

@implementation JazzDeviceFixture
RCT_EXPORT_MODULE();

RCT_REMAP_METHOD(admittedCapability, admittedCapabilityWithResolver:(RCTPromiseResolveBlock)resolve rejecter:(RCTPromiseRejectBlock)reject) {
  NSError *error = nil;
  if (self.capability == nil) {
    NSDictionary *scope = @{ @"app_namespace": @"jazz-device-acceptance",
                              @"storage_namespace": @"fixture-replace-at-build",
                              @"auth_scope": @"fixture-user-a" };
    NSDictionary *fixture = @{ @"scope": scope,
      @"sqlite_path": [NSTemporaryDirectory() stringByAppendingPathComponent:@"jazz-device.sqlite"],
      // CI replaces these test-only JSON values before the development build.
      @"schema_json": @"JAZZ_DEVICE_SCHEMA_JSON",
      @"identity": @"JAZZ_DEVICE_VERIFIED_IDENTITY_JSON",
      @"claims": @"JAZZ_DEVICE_VERIFIED_CLAIMS_JSON" };
    NSData *json = [NSJSONSerialization dataWithJSONObject:fixture options:0 error:&error];
    if (json != nil) self.capability = [JazzRelayTrustedAdmission admitScopeJSON:json error:&error];
  }
  if (self.capability == nil) { reject(@"E_JAZZ_DEVICE_FIXTURE", error.localizedDescription, error); return; }
  resolve([self.capability base64EncodedStringWithOptions:0]);
}

RCT_REMAP_METHOD(logout, logoutWithResolver:(RCTPromiseResolveBlock)resolve rejecter:(RCTPromiseRejectBlock)reject) {
  NSError *error = nil;
  if (self.capability != nil && ![JazzRelayTrustedAdmission revokeCapability:self.capability error:&error]) { reject(@"E_JAZZ_DEVICE_FIXTURE", error.localizedDescription, error); return; }
  self.capability = nil; resolve(nil);
}
@end
