import { JazzReactNativeProvider } from "jazz-tools/react-native";
import { JazzExpoProvider } from "jazz-tools/expo";

// #region Basic
export function MyApp({ children }: { children: React.ReactNode }) {
  // Get a free API Key at dashboard.jazz.tools, or use your email as a temporary key.
  const apiKey = "you@example.com";
  return (
    <JazzReactNativeProvider
      sync={{
        peer: `wss://cloud.jazz.tools/?key=${apiKey}`,
      }}
    >
      {children}
    </JazzReactNativeProvider>
  );
}
// #endregion

// #region Expo
export function MyExpoApp({ children }: { children: React.ReactNode }) {
  // Get a free API Key at dashboard.jazz.tools, or use your email as a temporary key.
  const apiKey = "you@example.com";
  return (
    <JazzExpoProvider
      sync={{
        peer: `wss://cloud.jazz.tools/?key=${apiKey}`,
        // ...
      }}
    >
      {children}
    </JazzExpoProvider>
  );
}
// #endregion
