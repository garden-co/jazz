# OPFS B-tree Viewer

Open a `.jazz-opfs-bundle` file to inspect raw `opfs-btree` key/value entries and physical pages.

## Export From A Browser App

Paste this snippet in the DevTools console for the origin that owns the OPFS data. It reads every persisted file currently available in OPFS for that origin and downloads a bundle the viewer can open.

Close other tabs for the same app first if the browser reports that an OPFS file is locked.

```js
await (async () => {
  const magic = "JAZZOPFSBUNDLE1";
  const version = 1;
  const encoder = new TextEncoder();

  const u32 = (value) => {
    if (!Number.isInteger(value) || value < 0 || value > 0xffffffff) {
      throw new Error(`Value does not fit in u32: ${value}`);
    }
    const bytes = new Uint8Array(4);
    new DataView(bytes.buffer).setUint32(0, value, true);
    return bytes;
  };

  const u64 = (value) => {
    if (!Number.isInteger(value) || value < 0 || value > Number.MAX_SAFE_INTEGER) {
      throw new Error(`Value does not fit in safe u64: ${value}`);
    }
    const bytes = new Uint8Array(8);
    new DataView(bytes.buffer).setBigUint64(0, BigInt(value), true);
    return bytes;
  };

  const concat = (chunks) => {
    const total = chunks.reduce((sum, chunk) => sum + chunk.byteLength, 0);
    const out = new Uint8Array(total);
    let offset = 0;
    for (const chunk of chunks) {
      out.set(chunk, offset);
      offset += chunk.byteLength;
    }
    return out;
  };

  const collectFiles = async (directory, prefix = "") => {
    const files = [];
    for await (const [name, handle] of directory.entries()) {
      const path = prefix ? `${prefix}/${name}` : name;
      if (handle.kind === "directory") {
        files.push(...(await collectFiles(handle, path)));
        continue;
      }
      if (handle.kind !== "file") continue;
      const file = await handle.getFile();
      files.push({
        path,
        bytes: new Uint8Array(await file.arrayBuffer()),
      });
    }
    return files;
  };

  if (!navigator.storage?.getDirectory) {
    throw new Error("OPFS is not available in this browser context.");
  }

  const root = await navigator.storage.getDirectory();
  const files = await collectFiles(root);
  const metadataBytes = encoder.encode(
    JSON.stringify({
      origin: location.origin,
      exportedAt: new Date().toISOString(),
      files: files.map((file) => ({
        path: file.path,
        size: file.bytes.byteLength,
      })),
    }),
  );
  const chunks = [
    encoder.encode(magic),
    u32(version),
    u32(metadataBytes.byteLength),
    metadataBytes,
    u32(files.length),
  ];

  for (const file of files) {
    const pathBytes = encoder.encode(file.path);
    chunks.push(u32(pathBytes.byteLength), pathBytes, u64(file.bytes.byteLength), file.bytes);
  }

  const bundle = concat(chunks);
  const originName = location.hostname.replace(/[^a-zA-Z0-9._-]+/g, "_") || "opfs";
  const url = URL.createObjectURL(
    new Blob([bundle], { type: "application/vnd.jazz.opfs-btree-bundle" }),
  );
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = `${originName}.jazz-opfs-bundle`;
  document.body.append(anchor);
  anchor.click();
  anchor.remove();
  setTimeout(() => URL.revokeObjectURL(url), 0);

  console.table(files.map((file) => ({ path: file.path, bytes: file.bytes.byteLength })));
})();
```
