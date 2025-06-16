import { createSubscriber } from "svelte/reactivity";
import { getAuthSecretStorage } from "../jazz.svelte.js";

export function useIsAuthenticated() {
    const authSecretStorage = getAuthSecretStorage();

    const subscribe = createSubscriber((update) => {
        const off = authSecretStorage.onUpdate(update)

        return () => off()
    })

    function getCurrent() {
        subscribe();

        return authSecretStorage.isAuthenticated;
    }

    return {
        /** @deprecated Use `current` instead */
        get value() {
            return getCurrent();
        },
        get current() {
            return getCurrent();
        }
    };
}