// Node-based SDK and bridge tests exercise native runtime behavior through
// their explicit bridge fixtures. Only the UI host is substituted: Metro's
// React Native Flow transform is not part of these test runners.
export const View = "native-view";
export const Text = "native-text";
export const Pressable = "native-pressable";
