import { type ClassValue, clsx } from "clsx";
import { twMerge } from "tailwind-merge";

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

// True when the app is embedded in a cross-origin iframe (e.g. the Jazz
// homepage). Same-origin iframes (e.g. vitest browser mode) are not treated
// as embedded so the full UI is available during tests.
export const inIframe = (() => {
  if (typeof window === "undefined") return false;
  if (window.self === window.top) return false;
  try {
    void window.parent.document;
    return false;
  } catch {
    return true;
  }
})();

const animals = [
  "elephant",
  "penguin",
  "giraffe",
  "octopus",
  "kangaroo",
  "dolphin",
  "cheetah",
  "koala",
  "platypus",
  "pangolin",
  "rhinoceros",
  "zebra",
  "lion",
  "tiger",
  "otter",
  "sloth",
  "capybara",
  "quokka",
  "lemur",
  "meerkat",
  "wombat",
  "hedgehog",
  "armadillo",
  "seal",
  "manatee",
  "narwhal",
  "beluga",
  "orca",
  "walrus",
  "fox",
  "alpaca",
  "llama",
  "tapir",
  "okapi",
];

export function getRandomUsername() {
  return `Anonymous ${animals[Math.floor(Math.random() * animals.length)]}`;
}

/** Decode a data URI (e.g. from FileReader.readAsDataURL) into a Blob. */
export function dataUriToBlob(dataUri: string, mimeType: string): Blob {
  const commaIdx = dataUri.indexOf(",");
  if (commaIdx === -1) {
    throw new Error("Invalid data URI: missing comma separator");
  }
  const byteString = atob(dataUri.slice(commaIdx + 1));
  const ab = new ArrayBuffer(byteString.length);
  const ia = new Uint8Array(ab);
  for (let i = 0; i < byteString.length; i++) {
    ia[i] = byteString.charCodeAt(i);
  }
  return new Blob([ab], { type: mimeType });
}

export function downloadBlob(blob: Blob, filename: string) {
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}

export function formatBytes(bytes: number): string {
  if (bytes === 0) return "0 B";
  const k = 1024;
  const sizes = ["B", "KB", "MB", "GB"];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return `${parseFloat((bytes / Math.pow(k, i)).toFixed(1))} ${sizes[i]}`;
}

/** Clear Jazz auth state and reload. Jazz 2 has no useLogOut hook yet. */
export function logOut() {
  for (let i = localStorage.length - 1; i >= 0; i--) {
    const key = localStorage.key(i);
    if (key?.startsWith("jazz")) localStorage.removeItem(key);
  }
  window.location.reload();
}

const MAX_FILE_SIZE = 10 * 1024 * 1024; // 10 MB

export function fileToBase64(file: File): Promise<string> {
  if (file.size > MAX_FILE_SIZE) {
    return Promise.reject(
      new Error(
        `File is too large (${formatBytes(file.size)}). Maximum size is ${formatBytes(MAX_FILE_SIZE)}.`,
      ),
    );
  }
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(reader.result as string);
    reader.onerror = reject;
    reader.readAsDataURL(file);
  });
}
