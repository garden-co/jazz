#import "JazzRelaySpec.h"
#import <ReactCommon/RCTTurboModuleWithJSIBindings.h>

/**
 * Private TurboModule declaration. React-Codegen supplies JazzRelaySpec.h to
 * the JazzRn pod target, but consuming applications do not receive that
 * generated header as part of the public JazzRn module surface.
 */
@interface JazzRelay : NSObject <NativeJazzRelaySpec, RCTTurboModuleWithJSIBindings>
@end
