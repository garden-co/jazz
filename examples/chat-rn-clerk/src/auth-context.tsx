import { useClerk, useUser } from "@clerk/clerk-expo";
import { JazzProvider, setupKvStore } from "jazz-react-native";
import { useJazzClerkAuth } from "jazz-react-native-auth-clerk";
import React, {
  createContext,
  PropsWithChildren,
  useContext,
  useEffect,
  useState,
} from "react";
import { Text, View } from "react-native";
const AuthContext = createContext<{
  isAuthenticated: boolean;
  isLoading: boolean;
}>({
  isAuthenticated: false,
  isLoading: true,
});

export function useAuth() {
  return useContext(AuthContext);
}

const kvStore = setupKvStore();

export function JazzAndAuth({ children }: PropsWithChildren) {
  const { isSignedIn, isLoaded: isClerkLoaded } = useUser();
  const clerk = useClerk();
  const [auth, state] = useJazzClerkAuth(clerk, kvStore);
  const [isAuthenticated, setIsAuthenticated] = useState(false);

  useEffect(() => {
    if (isSignedIn && isClerkLoaded && auth) {
      setIsAuthenticated(true);
    } else {
      setIsAuthenticated(false);
    }
  }, [isSignedIn, isClerkLoaded, auth]);

  return (
    <AuthContext.Provider
      value={{ isAuthenticated, isLoading: !isClerkLoaded || !auth }}
    >
      {state?.errors?.length > 0 &&
        state.errors.map((error) => (
          <View key={error}>
            <Text style={{ color: "red" }}>{error}</Text>
          </View>
        ))}
      {auth && clerk.user ? (
        <JazzProvider
          auth={auth}
          peer="wss://cloud.jazz.tools/?key=chat-rn-clerk-example-jazz@garden.co"
        >
          {children}
        </JazzProvider>
      ) : (
        children
      )}
    </AuthContext.Provider>
  );
}
