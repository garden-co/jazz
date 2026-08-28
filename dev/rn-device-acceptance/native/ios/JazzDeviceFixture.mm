#import <React/RCTBridgeModule.h>
#import <JazzRn/JazzRelay.h>

/** Development-build-only trusted fixture. Configuration is a compile-time
 * test fixture; JS is given only the opaque random admission capability. */
@interface JazzDeviceFixture : NSObject <RCTBridgeModule>
@property(nonatomic, nullable) NSData *capability;
@end

@implementation JazzDeviceFixture
RCT_EXPORT_MODULE();

static NSURL *JazzDeviceReceiptURL(void) {
  NSURL *caches = [[NSFileManager defaultManager] URLsForDirectory:NSCachesDirectory
                                                         inDomains:NSUserDomainMask].firstObject;
  return [caches URLByAppendingPathComponent:@"jazz-device-receipt.ndjson"];
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

RCT_REMAP_METHOD(receiptContext, receiptContextWithResolver:(RCTPromiseResolveBlock)resolve rejecter:(RCTPromiseRejectBlock)reject) {
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
  resolve(@{ @"platform": @"ios", @"runNonce": nonce, @"buildFingerprint": fingerprint, @"deviceIdentifier": device });
}

// This is intentionally a sink, not a native receipt generator: JavaScript
// supplies the complete protocol line after its relay proof, and the host
// validates it after reading the app-sandbox file.
RCT_REMAP_METHOD(recordReceipt, recordReceipt:(NSString *)receipt resolver:(RCTPromiseResolveBlock)resolve rejecter:(RCTPromiseRejectBlock)reject) {
  if (![receipt hasPrefix:@"JAZZ_DEVICE_RESULT "] || receipt.length > 16384) {
    reject(@"E_JAZZ_DEVICE_RECEIPT", @"Invalid device receipt", nil);
    return;
  }
  NSError *error = nil;
  NSData *data = [[receipt stringByAppendingString:@"\n"] dataUsingEncoding:NSUTF8StringEncoding];
  if (![data writeToURL:JazzDeviceReceiptURL() options:NSDataWritingAtomic error:&error]) {
    reject(@"E_JAZZ_DEVICE_RECEIPT", error.localizedDescription, error);
    return;
  }
  resolve(nil);
}
@end
