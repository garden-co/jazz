export type StoreName = "coValueBlocks" | "chunks";

export function startIndexedDbTransaction(db: IDBDatabase) {
  return db.transaction(["coValueBlocks"], "readwrite");
}

export function queryIndexedDbStore<T>(
  db: IDBDatabase,
  storeName: StoreName,
  callback: (store: IDBObjectStore) => IDBRequest<T>,
) {
  return new Promise<T>((resolve, reject) => {
    const tx = db.transaction(storeName, "readonly");
    const request = callback(tx.objectStore(storeName));

    request.onerror = () => {
      reject(request.error);
    };

    request.onsuccess = () => {
      resolve(request.result as T);
      tx.commit();
    };
  });
}

export function queryLastValue<T>(db: IDBDatabase, storeName: StoreName) {
  return new Promise<T>((resolve, reject) => {
    const tx = db.transaction(storeName, "readonly");
    const request = tx.objectStore(storeName).openCursor(null, "prev");

    request.onerror = () => {
      reject(request.error);
    };

    request.onsuccess = () => {
      resolve(request.result?.value as T);
      tx.commit();
    };
  });
}

export function putIndexedDbStore<T, O extends IDBValidKey>(
  db: IDBDatabase,
  storeName: StoreName,
  value: T,
) {
  return new Promise<O>((resolve, reject) => {
    const tx = db.transaction(storeName, "readwrite");
    const request = tx.objectStore(storeName).put(value);

    request.onerror = () => {
      reject(request.error);
    };

    request.onsuccess = () => {
      resolve(request.result as O);
      tx.commit();
    };
  });
}
