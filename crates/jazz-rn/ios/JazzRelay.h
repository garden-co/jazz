#import <React/RCTBridgeModule.h>

#ifdef RCT_NEW_ARCH_ENABLED
#import "JazzRelaySpec.h"

@interface JazzRelay : NSObject <NativeJazzRelaySpec>
#else
@interface JazzRelay : NSObject <RCTBridgeModule>
#endif

@end
