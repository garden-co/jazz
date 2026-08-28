#import <React/RCTBridgeModule.h>
#import <JazzNativeRelay/JazzRelay.h>

/** Development-build-only trusted fixture. Configuration is a compile-time
 * test fixture; JS is given only the opaque random admission capability. */
@interface JazzDeviceFixture : NSObject <RCTBridgeModule>
@property(nonatomic, nullable) NSData *capability;
@end

@implementation JazzDeviceFixture
RCT_EXPORT_MODULE();

RCT_REMAP_METHOD(linkedAbi, linkedAbiWithResolver:(RCTPromiseResolveBlock)resolve rejecter:(RCTPromiseRejectBlock)reject) {
  resolve([[[JazzRelay alloc] init] getAbiVersion]);
}

RCT_REMAP_METHOD(admittedCapability, admittedCapabilityWithResolver:(RCTPromiseResolveBlock)resolve rejecter:(RCTPromiseRejectBlock)reject) {
  NSError *error = nil;
  if (self.capability == nil) {
    NSDictionary *scope = @{ @"app_namespace": @"jazz-device-acceptance",
                              @"storage_namespace": @"acceptance-fixture",
                              @"auth_scope": @"fixture-user-a" };
    NSDictionary *fixture = @{ @"scope": scope,
      @"sqlite_path": [NSTemporaryDirectory() stringByAppendingPathComponent:@"jazz-device.sqlite"],
      // Fixed non-secret fixture material. The capability minted from it is
      // opaque; no scope, identity, or claims cross this native boundary.
      @"schema_json": @"{\"tables\":{}}",
      @"identity": @{ @"node": @"11111111-1111-4111-8111-111111111111",
                       @"author": @"[\"https://jazz.device.test\",\"fixture-user-a\"]" },
      @"claims": @{} };
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

RCT_REMAP_METHOD(acceptanceRunMetadata, acceptanceRunMetadataWithResolver:(RCTPromiseResolveBlock)resolve rejecter:(RCTPromiseRejectBlock)reject) {
  NSArray<NSString *> *arguments = NSProcessInfo.processInfo.arguments;
  NSString *(^valueFor)(NSString *) = ^NSString *(NSString *key) {
    NSUInteger index = [arguments indexOfObject:key];
    return index != NSNotFound && index + 1 < arguments.count ? arguments[index + 1] : nil;
  };
  NSString *nonce = valueFor(@"-JazzDeviceRunNonce");
  NSString *fingerprint = valueFor(@"-JazzDeviceBuildFingerprint");
  NSString *device = valueFor(@"-JazzDeviceDeviceIdentifier");
  if (nonce.length == 0 || fingerprint.length == 0 || device.length == 0) {
    reject(@"E_JAZZ_DEVICE_METADATA", @"Missing simulator acceptance launch metadata", nil);
    return;
  }
  resolve(@{ @"runNonce": nonce, @"buildFingerprint": fingerprint, @"deviceIdentifier": device });
}
@end
