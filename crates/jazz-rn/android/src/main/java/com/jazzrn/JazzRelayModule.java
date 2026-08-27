package com.jazzrn;

import com.facebook.react.bridge.Promise;
import com.facebook.react.bridge.ReactApplicationContext;
import com.facebook.react.module.annotations.ReactModule;

/**
 * Android implementation of the generated JazzRelay TurboModule spec.
 *
 * This is Java deliberately: the React Native Gradle plugin generates
 * {@link NativeJazzRelaySpec} in this library's declared {@code com.jazzrn}
 * package and adds that output to the Android source set before javac runs.
 * Keeping the implementation alongside its generated base is the standard
 * TurboModule library boundary; the Kotlin bridge remains a thin owner of the
 * opaque native handle.
 */
@ReactModule(name = JazzRelayModule.NAME)
public class JazzRelayModule extends NativeJazzRelaySpec {
  public static final String NAME = "JazzRelay";

  private final JazzRelayBridge bridge;

  public JazzRelayModule(ReactApplicationContext reactContext) {
    super(reactContext);
    JazzRelayBridge resolvedBridge;
    try {
      resolvedBridge = JazzRelayBridge.INSTANCE;
    } catch (Throwable error) {
      resolvedBridge = null;
    }
    bridge = resolvedBridge;
  }

  @Override
  public double getAbiVersion() {
    return bridge == null ? 0.0 : bridge.abiVersion();
  }

  @Override
  public void execute(String encodedCommand, Promise promise) {
    if (bridge == null) {
      promise.reject(
          "E_JAZZ_RELAY_UNAVAILABLE",
          "Jazz native relay commands require an Android development or release build containing the shared Rust relay artifact.");
      return;
    }
    try {
      promise.resolve(bridge.execute(encodedCommand));
    } catch (Throwable error) {
      promise.reject("E_JAZZ_RELAY_COMMAND", error);
    }
  }
}
