export const TARGETS = {
  "darwin-arm64": "jazz-tools-darwin-arm64",
  "darwin-x64": "jazz-tools-darwin-x64",
  "linux-arm64": "jazz-tools-linux-arm64",
  "linux-x64": "jazz-tools-linux-x64",
  "win32-x64": "jazz-tools-windows-x64.exe",
};

export function keyFor(platform, arch) {
  return `${platform}-${arch}`;
}
