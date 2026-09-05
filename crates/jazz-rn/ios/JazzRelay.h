#import <Foundation/Foundation.h>

/**
 * Public Swift/Objective-C authentication lifecycle seam. This is
 * intentionally not a React Native module: trusted platform code supplies
 * strict JSON to Rust and receives only a random opaque capability for
 * foreground JavaScript. The TurboModule declaration lives in the private
 * JazzRelayModule.h because it depends on a pod-target-only generated header.
 */
@interface JazzRelayTrustedAdmission : NSObject
+ (nullable NSData *)beginPrivateSessionWithServerURL:(NSString *)serverURL
                                                 appID:(NSString *)appID
                                                   jwt:(NSString *)jwt
                                                 error:(NSError * _Nullable * _Nullable)error;
+ (nullable NSData *)attachCanonicalSchemaJSON:(NSData *)schema
                              sessionCapability:(NSData *)session
                                         error:(NSError * _Nullable * _Nullable)error;
+ (nullable NSData *)admitScopeJSON:(NSData *)configuration
                              error:(NSError * _Nullable * _Nullable)error;
+ (BOOL)revokeCapability:(NSData *)capability
                   error:(NSError * _Nullable * _Nullable)error;
+ (nullable NSData *)replaceCapability:(nullable NSData *)previous
                    withScopeJSON:(NSData *)configuration
                            error:(NSError * _Nullable * _Nullable)error;
@end
