# `jazz-expo-auth-clerk`

This package provides a [Clerk-based](https://clerk.com/) authentication strategy for Jazz.

## Usage

The `JazzProviderWithClerk` component is a JazzProvider that automatically handles Clerk authentication.

Once authenticated, authentication will persist across page reloads, even if the device is offline.

See the full [example app](https://github.com/garden-co/jazz/tree/main/examples/clerk) for a complete example.

```tsx
import { ClerkProvider, useClerk } from "@clerk/clerk-react";
import { JazzProviderWithClerk } from "jazz-expo-auth-clerk";

const PUBLISHABLE_KEY = import.meta.env.VITE_CLERK_PUBLISHABLE_KEY;

function JazzProvider({ children }: { children: React.ReactNode }) {
  const clerk = useClerk();

  return (
    <JazzProviderWithClerk
      clerk={clerk}
      storage="sqlite"
      sync={{
        peer: "wss://cloud.jazz.tools/?key=chat-rn-clerk-example-jazz@garden.co",
      }}
    >
      {children}
    </JazzProviderWithClerk>
  );
}


export default function App() {
  return (
    <ClerkProvider publishableKey={PUBLISHABLE_KEY} afterSignOutUrl="/">
      <JazzProvider>
        <Slot />
      </JazzProvider>
    </ClerkProvider>
  );
}
```
