import { type SyntheticUserProfile, type SyntheticUserStorageOptions } from '../synthetic-users.js';
interface Props extends SyntheticUserStorageOptions {
    appId: string;
    class?: string;
    reloadOnSwitch?: boolean;
    onProfileChange?: (profile: SyntheticUserProfile) => void;
}
declare const SyntheticUserSwitcher: import("svelte").Component<Props, {}, "">;
type SyntheticUserSwitcher = ReturnType<typeof SyntheticUserSwitcher>;
export default SyntheticUserSwitcher;
//# sourceMappingURL=SyntheticUserSwitcher.svelte.d.ts.map