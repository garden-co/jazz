package com.jazzrn;

import com.facebook.react.bridge.Promise;
import com.facebook.react.bridge.ReactApplicationContext;
import com.facebook.react.module.annotations.ReactModule;
import com.facebook.react.turbomodule.core.interfaces.BindingsInstallerHolder;
import com.facebook.react.turbomodule.core.interfaces.TurboModuleWithJSIBindings;

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
public class JazzRelayModule extends NativeJazzRelaySpec implements TurboModuleWithJSIBindings {
  public static final String NAME = "JazzRelay";

  private final JazzRelayBridge bridge;
  private long runtimeToken = 0;
  private boolean ownsRuntimeLease = false;

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
  public void initialize() {
    super.initialize();
    if (bridge != null) ensureRuntimeToken();
  }

  @Override
  public void invalidate() {
    try {
      if (bridge != null && ownsRuntimeLease) {
        final long releasedToken = runtimeToken;
        // Clear ownership before calling into native lifecycle code. If it
        // throws, React Native may invoke invalidate again; that retry must
        // never consume a sibling runtime's lease.
        ownsRuntimeLease = false;
        runtimeToken = 0;
        bridge.releaseRuntime(releasedToken);
      }
    } finally {
      super.invalidate();
    }
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

  @Override
  public void installForegroundRuntime() {
    if (bridge == null) {
      throw new IllegalStateException(
          "Jazz native foreground runtime requires an Android development or release build containing the shared Rust relay artifact.");
    }
    bridge.installForegroundRuntime(ensureRuntimeToken());
  }

  @Override
  public BindingsInstallerHolder getBindingsInstaller() {
    if (bridge == null) {
      throw new IllegalStateException(
          "Jazz native foreground runtime requires an Android development or release build containing the shared Rust relay artifact.");
    }
    return bridge.foregroundBindingsInstaller(ensureRuntimeToken());
  }

  /** A stable platform-issued token identifies this JS runtime's private JSI
   * factory. It is never a JavaScript-visible handle and is intentionally not
   * derived from a native pointer. */
  private long ensureRuntimeToken() {
    if (!ownsRuntimeLease) {
      runtimeToken = bridge.acquireRuntime();
      ownsRuntimeLease = true;
    }
    return runtimeToken;
  }
}
