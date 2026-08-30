#import <React/RCTBridgeModule.h>
#import <JazzRn/JazzRelay.h>
#import <CommonCrypto/CommonDigest.h>
#include <limits.h>

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

static NSURL *JazzDeviceDiagnosticURL(void) {
  NSURL *caches = [[NSFileManager defaultManager] URLsForDirectory:NSCachesDirectory
                                                         inDomains:NSUserDomainMask].firstObject;
  return [caches URLByAppendingPathComponent:@"jazz-device-diagnostic.txt"];
}

static NSSet<NSString *> *JazzDeviceDiagnosticCodes(void) {
  return [NSSet setWithArray:@[
    @"fixture-metadata-failed",
    @"native-admission-failed",
    @"relay-command-abi-failed",
    @"relay-open-failed",
    @"relay-attach-failed",
    @"relay-probe-failed",
    @"relay-cleanup-failed",
    @"foreground-byte-abi-failed",
    @"foreground-install-failed",
    @"foreground-open-failed",
    @"foreground-probe-failed",
    @"foreground-tick-failed",
    @"foreground-close-failed",
    @"logout-revocation-failed",
    @"public-client-seed-failed",
    @"public-client-open-failed",
    @"public-client-subscribe-failed",
    @"public-client-write-failed",
    @"public-client-read-failed",
    @"public-client-publish-failed",
    @"public-client-shutdown-failed",
    @"scope-isolation-failed",
    @"scope-isolation-open-failed",
    @"scope-isolation-write-failed",
    @"scope-isolation-writer-read-failed",
    @"scope-isolation-read-failed",
    @"scope-isolation-assert-failed",
    @"auth-switch-failed",
    @"foreground-write-failed",
    @"same-runtime-subscription-failed",
    @"same-runtime-open-failed",
    @"same-runtime-subscribe-failed",
    @"same-runtime-initial-reset-failed",
    @"same-runtime-write-failed",
    @"same-runtime-transaction-open-failed",
    @"same-runtime-mutation-stage-failed",
    @"same-runtime-commit-failed",
    @"same-runtime-delta-failed",
    @"same-runtime-postcommit-wake-failed",
    @"same-runtime-delta-drain-failed",
    @"same-runtime-delta-decode-failed",
    @"same-runtime-delta-content-failed",
    @"same-runtime-delta-row-id-failed",
    @"same-runtime-unsubscribe-failed",
    @"scope-reopen-failed",
    @"public-client-restart-failed",
    @"receipt-write-failed",
  ]];
}

/** Hash the executable selected by the installed app bundle. The host driver
 * compares this to the artifact it installed; JavaScript and launch arguments
 * never get to choose the build identity placed in a receipt. */
static NSString *JazzDeviceExecutableSHA256(void) {
  NSString *path = NSBundle.mainBundle.executablePath;
  NSData *data = path == nil ? nil : [NSData dataWithContentsOfFile:path];
  if (data == nil || data.length > UINT_MAX) return nil;
  unsigned char digest[CC_SHA256_DIGEST_LENGTH];
  CC_SHA256(data.bytes, (CC_LONG)data.length, digest);
  NSMutableString *hex = [NSMutableString stringWithCapacity:CC_SHA256_DIGEST_LENGTH * 2];
  for (NSUInteger index = 0; index < CC_SHA256_DIGEST_LENGTH; index += 1)
    [hex appendFormat:@"%02x", digest[index]];
  return hex;
}

static NSDictionary *JazzDeviceScopeFixture(NSString *authScope) {
  BOOL userB = [authScope isEqualToString:@"fixture-user-b"];
  NSString *node = userB ? @"22222222-2222-4222-8222-222222222222"
                         : @"11111111-1111-4111-8111-111111111111";
  NSString *sqliteName = [NSString stringWithFormat:@"jazz-device-%@.sqlite", authScope];
  return @{ @"scope": @{ @"app_namespace": @"jazz-device-acceptance",
                            @"storage_namespace": @"acceptance-fixture",
                            @"auth_scope": authScope },
            // Auth scopes use distinct trusted storage paths. Neither path
            // nor either fixture identity is selected by JavaScript.
            @"sqlite_path": [NSTemporaryDirectory() stringByAppendingPathComponent:sqliteName],
            @"schema_json": @"{\"tables\":{\"todos\":{\"columns\":[{\"name\":\"title\",\"column_type\":{\"type\":\"Text\"},\"nullable\":false}]}}}",
            @"identity": @{ @"node": node,
                             @"author": [NSString stringWithFormat:@"[\"https://jazz.device.test\",\"%@\"]", authScope] },
            @"claims": @{} };
}

static NSData *JazzDeviceScopeJSON(NSString *authScope, NSError **error) {
  return [NSJSONSerialization dataWithJSONObject:JazzDeviceScopeFixture(authScope) options:0 error:error];
}

RCT_REMAP_METHOD(admittedCapability, admittedCapabilityWithResolver:(RCTPromiseResolveBlock)resolve rejecter:(RCTPromiseRejectBlock)reject) {
  NSError *error = nil;
  if (self.capability == nil) {
    NSData *json = JazzDeviceScopeJSON(@"fixture-user-a", &error);
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

RCT_REMAP_METHOD(switchAuthScope, switchAuthScopeWithResolver:(RCTPromiseResolveBlock)resolve rejecter:(RCTPromiseRejectBlock)reject) {
  NSError *error = nil;
  NSData *json = JazzDeviceScopeJSON(@"fixture-user-b", &error);
  NSData *replacement = json == nil ? nil : [JazzRelayTrustedAdmission replaceCapability:self.capability withScopeJSON:json error:&error];
  if (replacement == nil) { reject(@"E_JAZZ_DEVICE_FIXTURE", error.localizedDescription, error); return; }
  self.capability = replacement;
  resolve([replacement base64EncodedStringWithOptions:0]);
}

RCT_REMAP_METHOD(receiptContext, receiptContextWithResolver:(RCTPromiseResolveBlock)resolve rejecter:(RCTPromiseRejectBlock)reject) {
  NSArray<NSString *> *arguments = NSProcessInfo.processInfo.arguments;
  NSString *(^valueFor)(NSString *) = ^NSString *(NSString *key) {
    NSUInteger index = [arguments indexOfObject:key];
    return index != NSNotFound && index + 1 < arguments.count ? arguments[index + 1] : nil;
  };
  NSString *nonce = valueFor(@"-JazzDeviceRunNonce");
  NSString *fingerprint = JazzDeviceExecutableSHA256();
  NSString *device = valueFor(@"-JazzDeviceDeviceIdentifier");
  if (nonce.length == 0 || fingerprint.length == 0 || device.length == 0) {
    reject(@"E_JAZZ_DEVICE_METADATA", @"Missing simulator acceptance launch metadata", nil);
    return;
  }
  resolve(@{ @"platform": @"ios", @"runNonce": nonce, @"buildFingerprint": fingerprint, @"deviceIdentifier": device });
}

RCT_REMAP_METHOD(acceptancePhase, acceptancePhaseWithResolver:(RCTPromiseResolveBlock)resolve rejecter:(RCTPromiseRejectBlock)reject) {
  NSArray<NSString *> *arguments = NSProcessInfo.processInfo.arguments;
  NSUInteger index = [arguments indexOfObject:@"-JazzDeviceAcceptancePhase"];
  NSString *phase = index != NSNotFound && index + 1 < arguments.count ? arguments[index + 1] : @"seed";
  if (![phase isEqualToString:@"seed"] && ![phase isEqualToString:@"verify"]) {
    reject(@"E_JAZZ_DEVICE_FIXTURE", @"Invalid acceptance phase", nil); return;
  }
  resolve(phase);
}

// This is intentionally a sink, not a native receipt generator: JavaScript
// supplies the complete protocol transcript after its relay proof, and the
// host validates every line after reading the app-sandbox file.
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

RCT_REMAP_METHOD(recordDiagnostic, recordDiagnostic:(NSString *)detail resolver:(RCTPromiseResolveBlock)resolve rejecter:(RCTPromiseRejectBlock)reject) {
  if (![JazzDeviceDiagnosticCodes() containsObject:detail]) {
    reject(@"E_JAZZ_DEVICE_DIAGNOSTIC", @"Invalid device diagnostic", nil);
    return;
  }
  NSError *error = nil;
  NSData *data = [detail dataUsingEncoding:NSUTF8StringEncoding];
  if (![data writeToURL:JazzDeviceDiagnosticURL() options:NSDataWritingAtomic error:&error]) {
    reject(@"E_JAZZ_DEVICE_DIAGNOSTIC", error.localizedDescription, error);
    return;
  }
  resolve(nil);
}

RCT_REMAP_METHOD(clearDiagnostic, clearDiagnosticWithResolver:(RCTPromiseResolveBlock)resolve rejecter:(RCTPromiseRejectBlock)reject) {
  NSError *error = nil;
  NSURL *url = JazzDeviceDiagnosticURL();
  if ([[NSFileManager defaultManager] fileExistsAtPath:url.path] &&
      ![[NSFileManager defaultManager] removeItemAtURL:url error:&error]) {
    reject(@"E_JAZZ_DEVICE_DIAGNOSTIC", @"Failed to clear device diagnostic", nil);
    return;
  }
  resolve(nil);
}
@end
