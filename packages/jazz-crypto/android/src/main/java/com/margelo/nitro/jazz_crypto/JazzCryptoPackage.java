
package com.margelo.nitro.jazz_crypto;

import androidx.annotation.NonNull;
import androidx.annotation.Nullable;

import com.facebook.react.bridge.NativeModule;
import com.facebook.react.bridge.ReactApplicationContext;
import com.facebook.react.module.model.ReactModuleInfoProvider;
import com.facebook.react.TurboReactPackage;
import com.facebook.react.uimanager.ViewManager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class JazzCryptoPackage extends TurboReactPackage {
  @Nullable
  @Override
  public NativeModule getModule(String name, ReactApplicationContext reactContext) {
    return null;
  }

  @Override
  public ReactModuleInfoProvider getReactModuleInfoProvider() {
    return () -> {
        return new HashMap<>();
    };
  }

  @Override
  public List<ViewManager> createViewManagers(@NonNull ReactApplicationContext reactContext) {
    return new ArrayList<>();
  }

  static {
    JazzCryptoOnLoad.initializeNative();
  }
}
