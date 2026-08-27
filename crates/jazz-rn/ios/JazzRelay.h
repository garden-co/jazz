#import <React/RCTBridgeModule.h>

#ifdef RCT_NEW_ARCH_ENABLED
#import "JazzRelaySpec.h"

@interface JazzRelay : NSObject <NativeJazzRelaySpec>
#else
@interface JazzRelay : NSObject <RCTBridgeModule>
#endif

@end

/**
 * Swift/Objective-C authentication lifecycle seam. This is intentionally not
 * a React Native module: trusted platform code supplies strict JSON to Rust
 * and receives only a random opaque capability for foreground JavaScript.
 */
@interface JazzRelayTrustedAdmission : NSObject
+ (nullable NSData *)admitScopeJSON:(NSData *)configuration
                              error:(NSError * _Nullable * _Nullable)error;
+ (BOOL)revokeCapability:(NSData *)capability
                   error:(NSError * _Nullable * _Nullable)error;
+ (nullable NSData *)replaceCapability:(nullable NSData *)previous
                    withScopeJSON:(NSData *)configuration
                            error:(NSError * _Nullable * _Nullable)error;
@end
