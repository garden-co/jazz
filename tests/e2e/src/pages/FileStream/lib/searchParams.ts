export function getValueId() {
  return (
    (new URLSearchParams(location.search).get("valueId") as
      | string
      | undefined) ?? undefined
  );
}

export function getIsAutoUpload() {
  return new URLSearchParams(location.search).has("auto");
}

export function getDefaultFileSize() {
  return parseInt(
    new URLSearchParams(location.search).get("fileSize") ?? (1e3).toString(),
  );
}
