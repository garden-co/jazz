// @ts-check

import fs from 'fs/promises';
import crypto from 'crypto';
import path from 'path';

const CACHE_DIR = '.next/highlight-cache';

/**
 * Get a cached result from the cache directory
 * @param {string} cacheKey - The cache key to get the result for
 * @returns {Promise<any>} The cached result
 */
export async function getCachedResult(cacheKey) {
  try {
    const cacheFile = path.join(CACHE_DIR, `${cacheKey}.json`);
    const cached = await fs.readFile(cacheFile, 'utf8');
    return JSON.parse(cached);
  } catch {
    return null;
  }
}

/**
 * Set a cached result in the cache directory
 * @param {string} cacheKey - The cache key to set the result for
 * @param {any} result - The result to cache
 */
export async function setCachedResult(cacheKey, result) {
  try {
    await fs.mkdir(CACHE_DIR, { recursive: true });
    const cacheFile = path.join(CACHE_DIR, `${cacheKey}.json`);
    await fs.writeFile(cacheFile, JSON.stringify(result));
  } catch (error) {
    console.warn(`Failed to cache result: ${error.message}`);
  }
}

/**
 * Create a cache key for a code block
 * @param {string} code - The code to create a cache key for
 * @param {string} lang - The language of the code
 * @param {string} meta - The meta data of the code
 * @returns {string} The cache key
 */
export function createCacheKey(code, lang, meta) {
  const content = `${code}|${lang || ''}|${meta || ''}`;
  return crypto.createHash('md5').update(content).digest('hex');
} 