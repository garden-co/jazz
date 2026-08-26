package com.jazzrn

import com.facebook.react.TurboReactPackage
import com.facebook.react.bridge.NativeModule
import com.facebook.react.bridge.ReactApplicationContext
import com.facebook.react.module.model.ReactModuleInfo
import com.facebook.react.module.model.ReactModuleInfoProvider
import java.util.HashMap

/**
 * The sole Android entry point for the relay-only bridge.
 *
 * It deliberately owns no Rust runtime. Until a matching Rust artifact is
 * packaged, [JazzRelayModule] reports ABI 0 and rejects commands explicitly.
 */
class JazzRelayPackage : TurboReactPackage() {
  override fun getModule(name: String, reactContext: ReactApplicationContext): NativeModule? {
    return when (name) {
      JazzRelayModule.NAME -> JazzRelayModule(reactContext)
      else -> null
    }
  }

  override fun getReactModuleInfoProvider(): ReactModuleInfoProvider {
    return ReactModuleInfoProvider {
      val moduleInfos: MutableMap<String, ReactModuleInfo> = HashMap()
      moduleInfos[JazzRelayModule.NAME] = ReactModuleInfo(
        JazzRelayModule.NAME,
        JazzRelayModule.NAME,
        false,  // canOverrideExistingModule
        false,  // needsEagerInit
        false,  // isCxxModule
        true // isTurboModule
      )
      moduleInfos
    }
  }
}
