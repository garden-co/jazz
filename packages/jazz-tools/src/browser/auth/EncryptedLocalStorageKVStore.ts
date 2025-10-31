import { KvStore } from "jazz-tools";

export class EncryptedLocalStorageKVStore implements KvStore {
  private cryptoKey: CryptoKey | null = null;
  private keyReady: Promise<void>;
  private readonly keyStoreName = "jazz-encrypted-kvstore";
  private readonly keyName = "encryption-key";
  private readonly prefix: string;

  constructor(prefix = "jazz-encrypted:") {
    this.prefix = prefix;
    this.keyReady = this.initializeKey();
  }

  private async initializeKey(): Promise<void> {
    try {
      const existingKey = await this.getKeyFromIndexedDB();

      if (existingKey) {
        this.cryptoKey = existingKey;
      } else {
        this.cryptoKey = await crypto.subtle.generateKey(
          {
            name: "AES-GCM",
            length: 256,
          },
          false,
          ["encrypt", "decrypt"],
        );

        await this.storeKeyInIndexedDB(this.cryptoKey);
      }
    } catch (error) {
      console.error("Failed to initialize encryption key:", error);
      throw new Error("Failed to initialize encrypted storage");
    }
  }

  private getKeyFromIndexedDB(): Promise<CryptoKey | null> {
    return new Promise((resolve, reject) => {
      const request = indexedDB.open(this.keyStoreName, 1);

      request.onerror = () => reject(request.error);

      request.onupgradeneeded = (event) => {
        const db = (event.target as IDBOpenDBRequest).result;
        if (!db.objectStoreNames.contains("keys")) {
          db.createObjectStore("keys");
        }
      };

      request.onsuccess = () => {
        const db = request.result;
        const transaction = db.transaction(["keys"], "readonly");
        const store = transaction.objectStore("keys");
        const getRequest = store.get(this.keyName);

        getRequest.onsuccess = () => {
          db.close();
          resolve(getRequest.result || null);
        };

        getRequest.onerror = () => {
          db.close();
          reject(getRequest.error);
        };
      };
    });
  }

  private storeKeyInIndexedDB(key: CryptoKey): Promise<void> {
    return new Promise((resolve, reject) => {
      const request = indexedDB.open(this.keyStoreName, 1);

      request.onerror = () => reject(request.error);

      request.onupgradeneeded = (event) => {
        const db = (event.target as IDBOpenDBRequest).result;
        if (!db.objectStoreNames.contains("keys")) {
          db.createObjectStore("keys");
        }
      };

      request.onsuccess = () => {
        const db = request.result;
        const transaction = db.transaction(["keys"], "readwrite");
        const store = transaction.objectStore("keys");
        const putRequest = store.put(key, this.keyName);

        putRequest.onsuccess = () => {
          db.close();
          resolve();
        };

        putRequest.onerror = () => {
          db.close();
          reject(putRequest.error);
        };
      };
    });
  }

  private async encrypt(value: string): Promise<string> {
    await this.keyReady;

    if (!this.cryptoKey) {
      throw new Error("Encryption key not initialized");
    }

    const iv = crypto.getRandomValues(new Uint8Array(12));

    const encoder = new TextEncoder();
    const data = encoder.encode(value);

    const encryptedData = await crypto.subtle.encrypt(
      {
        name: "AES-GCM",
        iv: iv,
      },
      this.cryptoKey,
      data,
    );

    const combined = new Uint8Array(iv.length + encryptedData.byteLength);
    combined.set(iv, 0);
    combined.set(new Uint8Array(encryptedData), iv.length);

    return this.arrayBufferToBase64(combined);
  }

  private async decrypt(encryptedValue: string): Promise<string> {
    await this.keyReady;

    if (!this.cryptoKey) {
      throw new Error("Encryption key not initialized");
    }

    const combined = this.base64ToArrayBuffer(encryptedValue);

    const iv = combined.slice(0, 12);
    const encryptedData = combined.slice(12);

    const decryptedData = await crypto.subtle.decrypt(
      {
        name: "AES-GCM",
        iv: iv,
      },
      this.cryptoKey,
      encryptedData,
    );

    const decoder = new TextDecoder();
    return decoder.decode(decryptedData);
  }

  private arrayBufferToBase64(buffer: Uint8Array): string {
    let binary = "";
    const len = buffer.byteLength;
    for (let i = 0; i < len; i++) {
      binary += String.fromCharCode(buffer[i]!);
    }
    return btoa(binary);
  }

  private base64ToArrayBuffer(base64: string): Uint8Array {
    const binary = atob(base64);
    const len = binary.length;
    const bytes = new Uint8Array(len);
    for (let i = 0; i < len; i++) {
      bytes[i] = binary.charCodeAt(i);
    }
    return bytes;
  }

  async get(key: string): Promise<string | null> {
    const encryptedValue = localStorage.getItem(this.prefix + key);

    if (encryptedValue) {
      try {
        return await this.decrypt(encryptedValue);
      } catch (error) {
        console.error("Failed to decrypt value:", error);
        return null;
      }
    }

    // Fallback to non-prefixed plaintext storage
    const plaintextValue = localStorage.getItem(key);

    if (plaintextValue) {
      // Migrate to encrypted storage
      try {
        await this.set(key, plaintextValue);
        localStorage.removeItem(key);
        return plaintextValue;
      } catch (error) {
        console.error("Failed to migrate plaintext value:", error);
        // Still return the plaintext value even if migration fails
        return plaintextValue;
      }
    }

    return null;
  }

  async set(key: string, value: string): Promise<void> {
    try {
      const encryptedValue = await this.encrypt(value);
      localStorage.setItem(this.prefix + key, encryptedValue);
    } catch (error) {
      console.error("Failed to encrypt value:", error);
      throw error;
    }
  }

  async delete(key: string): Promise<void> {
    localStorage.removeItem(this.prefix + key);
  }

  async clearAll(): Promise<void> {
    const keysToRemove: string[] = [];

    for (let i = 0; i < localStorage.length; i++) {
      const key = localStorage.key(i);
      if (key && key.startsWith(this.prefix)) {
        keysToRemove.push(key);
      }
    }

    keysToRemove.forEach((key) => localStorage.removeItem(key));
  }

  async deleteEncryptionKey(): Promise<void> {
    return new Promise((resolve, reject) => {
      const request = indexedDB.deleteDatabase(this.keyStoreName);

      request.onsuccess = () => {
        this.cryptoKey = null;
        resolve();
      };

      request.onerror = () => reject(request.error);
    });
  }
}
