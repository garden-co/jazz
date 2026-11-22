// The fifth leg's rules: the phases of dev/scripts/test-kotlin-android-emulator.sh (jazz#6246).
export const ANDROID_RULES = [
  { id: "android.memory", phase: "memory", title: "memory storage and sync on an emulator" },
  { id: "android.write", phase: "write", title: "persistent storage opens, writes and syncs on an emulator" },
  { id: "android.restart", phase: "restart", title: "reads back after a SIGKILL and a restart on an emulator" },
];
