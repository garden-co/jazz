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
static NSUInteger relayRuntimeReferences = 0;
static NSMutableSet<NSData *> *trustedCapabilities = nil;

static jazz_native_relay_host *EnsureRelayHost(void) {
  if (relayHost == NULL) relayHost = jazz_native_relay_host_new();
  return relayHost;
}

static void DestroyRelayHostIfUnused(void) {
  if (relayHost != NULL && relayRuntimeReferences == 0 && trustedCapabilities.count == 0) {
    jazz_native_relay_host_free(relayHost);
    relayHost = NULL;
  }
}

static NSError *RelayLifecycleError(NSString *message) {
  return [NSError errorWithDomain:@"JazzRelay" code:1
                          userInfo:@{NSLocalizedDescriptionKey: message}];
}
#endif

RCT_EXPORT_MODULE(JazzRelay)

- (instancetype)init {
  self = [super init];
#if JAZZ_RELAY_ARTIFACT_AVAILABLE
  if (self != nil) {
    @synchronized([JazzRelay class]) {
      EnsureRelayHost();
      relayRuntimeReferences += 1;
    }
  }
#endif
  return self;
}

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

- (void)invalidate {
#if JAZZ_RELAY_ARTIFACT_AVAILABLE
  @synchronized([JazzRelay class]) {
    if (relayRuntimeReferences > 0) relayRuntimeReferences -= 1;
    DestroyRelayHostIfUnused();
  }
#endif
}

#ifdef RCT_NEW_ARCH_ENABLED
- (std::shared_ptr<facebook::react::TurboModule>)getTurboModule:
    (const facebook::react::ObjCTurboModule::InitParams &)params {
  return std::make_shared<facebook::react::NativeJazzRelaySpecJSI>(params);
}
#endif

@end

@implementation JazzRelayTrustedAdmission

+ (NSData *)admitScopeJSON:(NSData *)configuration
                     error:(NSError **)error {
#if JAZZ_RELAY_ARTIFACT_AVAILABLE
  @synchronized([JazzRelay class]) {
    jazz_native_relay_bytes output = {0};
    jazz_native_relay_status status = jazz_native_relay_host_admit_scope_json(
        EnsureRelayHost(), (const uint8_t *)configuration.bytes, configuration.length, &output);
    if (status != JAZZ_NATIVE_RELAY_OK) {
      if (error != NULL) *error = RelayLifecycleError(@"Jazz trusted relay admission was rejected");
      return nil;
    }
    NSData *capability = [NSData dataWithBytes:output.data length:output.len];
    jazz_native_relay_bytes_free(&output);
    if (capability.length != 32) {
      if (error != NULL) *error = RelayLifecycleError(@"Jazz relay returned an invalid admission capability");
      return nil;
    }
    if (trustedCapabilities == nil) trustedCapabilities = [NSMutableSet set];
    [trustedCapabilities addObject:capability];
    return capability;
  }
#else
  if (error != NULL) *error = [NSError errorWithDomain:@"JazzRelay" code:2 userInfo:@{NSLocalizedDescriptionKey: @"Jazz native relay requires a matching development or release build."}];
  return nil;
#endif
}

+ (BOOL)revokeCapability:(NSData *)capability
                   error:(NSError **)error {
#if JAZZ_RELAY_ARTIFACT_AVAILABLE
  @synchronized([JazzRelay class]) {
    if (capability.length != 32) {
      if (error != NULL) *error = RelayLifecycleError(@"Jazz admission capabilities are exactly 32 bytes");
      return NO;
    }
    jazz_native_relay_status status = jazz_native_relay_host_revoke_scope_capability(
        EnsureRelayHost(), (const uint8_t *)capability.bytes, capability.length);
    if (status != JAZZ_NATIVE_RELAY_OK) {
      if (error != NULL) *error = RelayLifecycleError(@"Jazz trusted relay revocation failed");
      return NO;
    }
    [trustedCapabilities removeObject:capability];
    DestroyRelayHostIfUnused();
    return YES;
  }
#else
  if (error != NULL) *error = [NSError errorWithDomain:@"JazzRelay" code:2 userInfo:@{NSLocalizedDescriptionKey: @"Jazz native relay requires a matching development or release build."}];
  return NO;
#endif
}

+ (NSData *)replaceCapability:(NSData *)previous
                 withScopeJSON:(NSData *)configuration
                         error:(NSError **)error {
  if (previous != nil && ![self revokeCapability:previous error:error]) return nil;
  return [self admitScopeJSON:configuration error:error];
}

@end
