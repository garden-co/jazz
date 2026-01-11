let wasm;

function addToExternrefTable0(obj) {
    const idx = wasm.__externref_table_alloc();
    wasm.__wbindgen_externrefs.set(idx, obj);
    return idx;
}

function _assertClass(instance, klass) {
    if (!(instance instanceof klass)) {
        throw new Error(`expected instance of ${klass.name}`);
    }
}

const CLOSURE_DTORS = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(state => state.dtor(state.a, state.b));

function debugString(val) {
    // primitive types
    const type = typeof val;
    if (type == 'number' || type == 'boolean' || val == null) {
        return  `${val}`;
    }
    if (type == 'string') {
        return `"${val}"`;
    }
    if (type == 'symbol') {
        const description = val.description;
        if (description == null) {
            return 'Symbol';
        } else {
            return `Symbol(${description})`;
        }
    }
    if (type == 'function') {
        const name = val.name;
        if (typeof name == 'string' && name.length > 0) {
            return `Function(${name})`;
        } else {
            return 'Function';
        }
    }
    // objects
    if (Array.isArray(val)) {
        const length = val.length;
        let debug = '[';
        if (length > 0) {
            debug += debugString(val[0]);
        }
        for(let i = 1; i < length; i++) {
            debug += ', ' + debugString(val[i]);
        }
        debug += ']';
        return debug;
    }
    // Test for built-in
    const builtInMatches = /\[object ([^\]]+)\]/.exec(toString.call(val));
    let className;
    if (builtInMatches && builtInMatches.length > 1) {
        className = builtInMatches[1];
    } else {
        // Failed to match the standard '[object ClassName]'
        return toString.call(val);
    }
    if (className == 'Object') {
        // we're a user defined class or Object
        // JSON.stringify avoids problems with cycles, and is generally much
        // easier than looping through ownProperties of `val`.
        try {
            return 'Object(' + JSON.stringify(val) + ')';
        } catch (_) {
            return 'Object';
        }
    }
    // errors
    if (val instanceof Error) {
        return `${val.name}: ${val.message}\n${val.stack}`;
    }
    // TODO we could test for more things here, like `Set`s and `Map`s.
    return className;
}

function getArrayU8FromWasm0(ptr, len) {
    ptr = ptr >>> 0;
    return getUint8ArrayMemory0().subarray(ptr / 1, ptr / 1 + len);
}

let cachedDataViewMemory0 = null;
function getDataViewMemory0() {
    if (cachedDataViewMemory0 === null || cachedDataViewMemory0.buffer.detached === true || (cachedDataViewMemory0.buffer.detached === undefined && cachedDataViewMemory0.buffer !== wasm.memory.buffer)) {
        cachedDataViewMemory0 = new DataView(wasm.memory.buffer);
    }
    return cachedDataViewMemory0;
}

function getStringFromWasm0(ptr, len) {
    ptr = ptr >>> 0;
    return decodeText(ptr, len);
}

let cachedUint8ArrayMemory0 = null;
function getUint8ArrayMemory0() {
    if (cachedUint8ArrayMemory0 === null || cachedUint8ArrayMemory0.byteLength === 0) {
        cachedUint8ArrayMemory0 = new Uint8Array(wasm.memory.buffer);
    }
    return cachedUint8ArrayMemory0;
}

function handleError(f, args) {
    try {
        return f.apply(this, args);
    } catch (e) {
        const idx = addToExternrefTable0(e);
        wasm.__wbindgen_exn_store(idx);
    }
}

function isLikeNone(x) {
    return x === undefined || x === null;
}

function makeMutClosure(arg0, arg1, dtor, f) {
    const state = { a: arg0, b: arg1, cnt: 1, dtor };
    const real = (...args) => {

        // First up with a closure we increment the internal reference
        // count. This ensures that the Rust closure environment won't
        // be deallocated while we're invoking it.
        state.cnt++;
        const a = state.a;
        state.a = 0;
        try {
            return f(a, state.b, ...args);
        } finally {
            state.a = a;
            real._wbg_cb_unref();
        }
    };
    real._wbg_cb_unref = () => {
        if (--state.cnt === 0) {
            state.dtor(state.a, state.b);
            state.a = 0;
            CLOSURE_DTORS.unregister(state);
        }
    };
    CLOSURE_DTORS.register(real, state, state);
    return real;
}

function passArray8ToWasm0(arg, malloc) {
    const ptr = malloc(arg.length * 1, 1) >>> 0;
    getUint8ArrayMemory0().set(arg, ptr / 1);
    WASM_VECTOR_LEN = arg.length;
    return ptr;
}

function passStringToWasm0(arg, malloc, realloc) {
    if (realloc === undefined) {
        const buf = cachedTextEncoder.encode(arg);
        const ptr = malloc(buf.length, 1) >>> 0;
        getUint8ArrayMemory0().subarray(ptr, ptr + buf.length).set(buf);
        WASM_VECTOR_LEN = buf.length;
        return ptr;
    }

    let len = arg.length;
    let ptr = malloc(len, 1) >>> 0;

    const mem = getUint8ArrayMemory0();

    let offset = 0;

    for (; offset < len; offset++) {
        const code = arg.charCodeAt(offset);
        if (code > 0x7F) break;
        mem[ptr + offset] = code;
    }
    if (offset !== len) {
        if (offset !== 0) {
            arg = arg.slice(offset);
        }
        ptr = realloc(ptr, len, len = offset + arg.length * 3, 1) >>> 0;
        const view = getUint8ArrayMemory0().subarray(ptr + offset, ptr + len);
        const ret = cachedTextEncoder.encodeInto(arg, view);

        offset += ret.written;
        ptr = realloc(ptr, len, offset, 1) >>> 0;
    }

    WASM_VECTOR_LEN = offset;
    return ptr;
}

function takeFromExternrefTable0(idx) {
    const value = wasm.__wbindgen_externrefs.get(idx);
    wasm.__externref_table_dealloc(idx);
    return value;
}

let cachedTextDecoder = new TextDecoder('utf-8', { ignoreBOM: true, fatal: true });
cachedTextDecoder.decode();
const MAX_SAFARI_DECODE_BYTES = 2146435072;
let numBytesDecoded = 0;
function decodeText(ptr, len) {
    numBytesDecoded += len;
    if (numBytesDecoded >= MAX_SAFARI_DECODE_BYTES) {
        cachedTextDecoder = new TextDecoder('utf-8', { ignoreBOM: true, fatal: true });
        cachedTextDecoder.decode();
        numBytesDecoded = len;
    }
    return cachedTextDecoder.decode(getUint8ArrayMemory0().subarray(ptr, ptr + len));
}

const cachedTextEncoder = new TextEncoder();

if (!('encodeInto' in cachedTextEncoder)) {
    cachedTextEncoder.encodeInto = function (arg, view) {
        const buf = cachedTextEncoder.encode(arg);
        view.set(buf);
        return {
            read: arg.length,
            written: buf.length
        };
    }
}

let WASM_VECTOR_LEN = 0;

function wasm_bindgen__convert__closures_____invoke__h76dfe62a3b69c085(arg0, arg1, arg2) {
    wasm.wasm_bindgen__convert__closures_____invoke__h76dfe62a3b69c085(arg0, arg1, arg2);
}

function wasm_bindgen__convert__closures_____invoke__h20bec3cca755663f(arg0, arg1, arg2) {
    wasm.wasm_bindgen__convert__closures_____invoke__h20bec3cca755663f(arg0, arg1, arg2);
}

function wasm_bindgen__convert__closures_____invoke__h3097c68d921a6b39(arg0, arg1, arg2, arg3) {
    wasm.wasm_bindgen__convert__closures_____invoke__h3097c68d921a6b39(arg0, arg1, arg2, arg3);
}

const __wbindgen_enum_IdbTransactionMode = ["readonly", "readwrite", "versionchange", "readwriteflush", "cleanup"];

const __wbindgen_enum_RequestMode = ["same-origin", "no-cors", "cors", "navigate"];

const SyncedQueryHandleFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_syncedqueryhandle_free(ptr >>> 0, 1));

const WasmBlobWriterFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_wasmblobwriter_free(ptr >>> 0, 1));

const WasmDatabaseFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_wasmdatabase_free(ptr >>> 0, 1));

const WasmQueryHandleFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_wasmqueryhandle_free(ptr >>> 0, 1));

const WasmQueryHandleBinaryFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_wasmqueryhandlebinary_free(ptr >>> 0, 1));

const WasmQueryHandleDeltaFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_wasmqueryhandledelta_free(ptr >>> 0, 1));

const WasmSyncClientFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_wasmsyncclient_free(ptr >>> 0, 1));

const WasmSyncedLocalNodeFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_wasmsyncedlocalnode_free(ptr >>> 0, 1));

/**
 * Connection state for the sync client.
 * @enum {0 | 1 | 2}
 */
export const ConnectionState = Object.freeze({
    Disconnected: 0, "0": "Disconnected",
    Connecting: 1, "1": "Connecting",
    Connected: 2, "2": "Connected",
});

/**
 * Connection state for the synced node.
 * @enum {0 | 1 | 2 | 3}
 */
export const SyncState = Object.freeze({
    Disconnected: 0, "0": "Disconnected",
    Connecting: 1, "1": "Connecting",
    Connected: 2, "2": "Connected",
    Reconnecting: 3, "3": "Reconnecting",
});

/**
 * Handle to an incremental query subscription.
 */
export class SyncedQueryHandle {
    static __wrap(ptr) {
        ptr = ptr >>> 0;
        const obj = Object.create(SyncedQueryHandle.prototype);
        obj.__wbg_ptr = ptr;
        SyncedQueryHandleFinalization.register(obj, obj.__wbg_ptr, obj);
        return obj;
    }
    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        SyncedQueryHandleFinalization.unregister(this);
        return ptr;
    }
    free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_syncedqueryhandle_free(ptr, 0);
    }
    /**
     * Unsubscribe from updates.
     */
    unsubscribe() {
        wasm.syncedqueryhandle_unsubscribe(this.__wbg_ptr);
    }
    /**
     * Get a text diagram of the query graph.
     * @returns {string}
     */
    diagram() {
        let deferred1_0;
        let deferred1_1;
        try {
            const ret = wasm.syncedqueryhandle_diagram(this.__wbg_ptr);
            deferred1_0 = ret[0];
            deferred1_1 = ret[1];
            return getStringFromWasm0(ret[0], ret[1]);
        } finally {
            wasm.__wbindgen_free(deferred1_0, deferred1_1, 1);
        }
    }
    /**
     * Free resources (no-op, but required by WasmQueryHandleLike interface).
     */
    free() {
        wasm.syncedqueryhandle_free(this.__wbg_ptr);
    }
}
if (Symbol.dispose) SyncedQueryHandle.prototype[Symbol.dispose] = SyncedQueryHandle.prototype.free;

/**
 * Handle for streaming blob creation.
 * Use write() to add chunks, then finish() to get a blob handle.
 */
export class WasmBlobWriter {
    static __wrap(ptr) {
        ptr = ptr >>> 0;
        const obj = Object.create(WasmBlobWriter.prototype);
        obj.__wbg_ptr = ptr;
        WasmBlobWriterFinalization.register(obj, obj.__wbg_ptr, obj);
        return obj;
    }
    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        WasmBlobWriterFinalization.unregister(this);
        return ptr;
    }
    free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_wasmblobwriter_free(ptr, 0);
    }
    /**
     * Write a chunk of data to the blob.
     * Can be called multiple times before finish().
     * @param {Uint8Array} data
     */
    write(data) {
        const ptr0 = passArray8ToWasm0(data, wasm.__wbindgen_malloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.wasmblobwriter_write(this.__wbg_ptr, ptr0, len0);
        if (ret[1]) {
            throw takeFromExternrefTable0(ret[0]);
        }
    }
    /**
     * Get the current total size of data written.
     * @returns {number}
     */
    size() {
        const ret = wasm.wasmblobwriter_size(this.__wbg_ptr);
        if (ret[2]) {
            throw takeFromExternrefTable0(ret[1]);
        }
        return ret[0] >>> 0;
    }
    /**
     * Finish writing and get a blob handle.
     * Stores chunks in the Environment's ChunkStore for persistence.
     * The writer cannot be used after this.
     * @returns {bigint}
     */
    finish() {
        const ret = wasm.wasmblobwriter_finish(this.__wbg_ptr);
        if (ret[2]) {
            throw takeFromExternrefTable0(ret[1]);
        }
        return BigInt.asUintN(64, ret[0]);
    }
    /**
     * Abort the blob creation, discarding all written data.
     */
    abort() {
        wasm.wasmblobwriter_abort(this.__wbg_ptr);
    }
}
if (Symbol.dispose) WasmBlobWriter.prototype[Symbol.dispose] = WasmBlobWriter.prototype.free;

/**
 * WASM-exposed database wrapper.
 */
export class WasmDatabase {
    static __wrap(ptr) {
        ptr = ptr >>> 0;
        const obj = Object.create(WasmDatabase.prototype);
        obj.__wbg_ptr = ptr;
        WasmDatabaseFinalization.register(obj, obj.__wbg_ptr, obj);
        return obj;
    }
    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        WasmDatabaseFinalization.unregister(this);
        return ptr;
    }
    free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_wasmdatabase_free(ptr, 0);
    }
    /**
     * Create a new in-memory database.
     */
    constructor() {
        const ret = wasm.wasmdatabase_new();
        this.__wbg_ptr = ret >>> 0;
        WasmDatabaseFinalization.register(this, this.__wbg_ptr, this);
        return this;
    }
    /**
     * Create or open a persistent IndexedDB-backed database.
     *
     * If a database already exists in IndexedDB, it will be loaded.
     * Otherwise, a new database will be created.
     *
     * @param db_name - Optional database name (defaults to "groove")
     * @returns Promise that resolves to WasmDatabase
     * @param {string | null} [db_name]
     * @returns {Promise<any>}
     */
    static withIndexedDb(db_name) {
        var ptr0 = isLikeNone(db_name) ? 0 : passStringToWasm0(db_name, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        var len0 = WASM_VECTOR_LEN;
        const ret = wasm.wasmdatabase_withIndexedDb(ptr0, len0);
        return ret;
    }
    /**
     * Check if a persistent database exists in IndexedDB.
     *
     * @param db_name - Optional database name (defaults to "groove")
     * @returns Promise that resolves to boolean
     * @param {string | null} [db_name]
     * @returns {Promise<any>}
     */
    static hasPersistedDatabase(db_name) {
        var ptr0 = isLikeNone(db_name) ? 0 : passStringToWasm0(db_name, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        var len0 = WASM_VECTOR_LEN;
        const ret = wasm.wasmdatabase_hasPersistedDatabase(ptr0, len0);
        return ret;
    }
    /**
     * Delete a persistent database from IndexedDB.
     *
     * @param db_name - Optional database name (defaults to "groove")
     * @returns Promise that resolves when deleted
     * @param {string | null} [db_name]
     * @returns {Promise<any>}
     */
    static deletePersistedDatabase(db_name) {
        var ptr0 = isLikeNone(db_name) ? 0 : passStringToWasm0(db_name, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        var len0 = WASM_VECTOR_LEN;
        const ret = wasm.wasmdatabase_deletePersistedDatabase(ptr0, len0);
        return ret;
    }
    /**
     * Execute a SQL statement (legacy string-based results).
     * @param {string} sql
     * @returns {any}
     */
    execute(sql) {
        const ptr0 = passStringToWasm0(sql, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.wasmdatabase_execute(this.__wbg_ptr, ptr0, len0);
        if (ret[2]) {
            throw takeFromExternrefTable0(ret[1]);
        }
        return takeFromExternrefTable0(ret[0]);
    }
    /**
     * Execute a SELECT query and return results as binary Uint8Array.
     *
     * Binary format:
     * - u32: row_count
     * - Per row:
     *   - 26 bytes: ObjectId (Base32 UTF-8)
     *   - Column values in schema order
     * @param {string} sql
     * @returns {Uint8Array}
     */
    select_binary(sql) {
        const ptr0 = passStringToWasm0(sql, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.wasmdatabase_select_binary(this.__wbg_ptr, ptr0, len0);
        if (ret[2]) {
            throw takeFromExternrefTable0(ret[1]);
        }
        return takeFromExternrefTable0(ret[0]);
    }
    /**
     * Update a specific row's column with a string value.
     * row_id should be a Base32 ObjectId string.
     * @param {string} table
     * @param {string} row_id
     * @param {string} column
     * @param {string} value
     * @returns {boolean}
     */
    updateRow(table, row_id, column, value) {
        const ptr0 = passStringToWasm0(table, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passStringToWasm0(row_id, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len1 = WASM_VECTOR_LEN;
        const ptr2 = passStringToWasm0(column, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len2 = WASM_VECTOR_LEN;
        const ptr3 = passStringToWasm0(value, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len3 = WASM_VECTOR_LEN;
        const ret = wasm.wasmdatabase_updateRow(this.__wbg_ptr, ptr0, len0, ptr1, len1, ptr2, len2, ptr3, len3);
        if (ret[2]) {
            throw takeFromExternrefTable0(ret[1]);
        }
        return ret[0] !== 0;
    }
    /**
     * Update a specific row's column with an i64 value.
     * row_id should be a Base32 ObjectId string.
     * @param {string} table
     * @param {string} row_id
     * @param {string} column
     * @param {bigint} value
     * @returns {boolean}
     */
    updateRowI64(table, row_id, column, value) {
        const ptr0 = passStringToWasm0(table, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passStringToWasm0(row_id, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len1 = WASM_VECTOR_LEN;
        const ptr2 = passStringToWasm0(column, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len2 = WASM_VECTOR_LEN;
        const ret = wasm.wasmdatabase_updateRowI64(this.__wbg_ptr, ptr0, len0, ptr1, len1, ptr2, len2, value);
        if (ret[2]) {
            throw takeFromExternrefTable0(ret[1]);
        }
        return ret[0] !== 0;
    }
    /**
     * Initialize the database schema from a SQL string containing CREATE TABLE statements.
     * Statements are separated by semicolons.
     * @param {string} schema
     */
    init_schema(schema) {
        const ptr0 = passStringToWasm0(schema, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.wasmdatabase_init_schema(this.__wbg_ptr, ptr0, len0);
        if (ret[1]) {
            throw takeFromExternrefTable0(ret[0]);
        }
    }
    /**
     * List all tables in the database.
     * Returns an array of table names.
     * @returns {any}
     */
    list_tables() {
        const ret = wasm.wasmdatabase_list_tables(this.__wbg_ptr);
        return ret;
    }
    /**
     * Create an incremental query that calls back on changes (legacy string-based).
     * Returns a handle that must be kept alive to maintain the subscription.
     * @param {string} sql
     * @param {Function} callback
     * @returns {WasmQueryHandle}
     */
    subscribe(sql, callback) {
        const ptr0 = passStringToWasm0(sql, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.wasmdatabase_subscribe(this.__wbg_ptr, ptr0, len0, callback);
        if (ret[2]) {
            throw takeFromExternrefTable0(ret[1]);
        }
        return WasmQueryHandle.__wrap(ret[0]);
    }
    /**
     * Create an incremental query that calls back with binary data on changes.
     * The callback receives a Uint8Array in the binary row format.
     * Returns a handle that must be kept alive to maintain the subscription.
     * @param {string} sql
     * @param {Function} callback
     * @returns {WasmQueryHandleBinary}
     */
    subscribe_binary(sql, callback) {
        const ptr0 = passStringToWasm0(sql, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.wasmdatabase_subscribe_binary(this.__wbg_ptr, ptr0, len0, callback);
        if (ret[2]) {
            throw takeFromExternrefTable0(ret[1]);
        }
        return WasmQueryHandleBinary.__wrap(ret[0]);
    }
    /**
     * Create an incremental query that calls back with individual delta buffers.
     * The callback receives an Array of Uint8Array, one per delta.
     * Each delta is: u8 type (1=add, 2=update, 3=remove) + row data (or just id for removes).
     * Returns a handle that must be kept alive to maintain the subscription.
     * @param {string} sql
     * @param {Function} callback
     * @returns {WasmQueryHandleDelta}
     */
    subscribeDelta(sql, callback) {
        const ptr0 = passStringToWasm0(sql, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.wasmdatabase_subscribeDelta(this.__wbg_ptr, ptr0, len0, callback);
        if (ret[2]) {
            throw takeFromExternrefTable0(ret[1]);
        }
        return WasmQueryHandleDelta.__wrap(ret[0]);
    }
    /**
     * Create a blob from raw bytes.
     * Returns a blob handle ID that can be used in insert/update operations.
     * Chunks are stored in the Environment's ChunkStore for persistence.
     * @param {Uint8Array} data
     * @returns {bigint}
     */
    create_blob(data) {
        const ptr0 = passArray8ToWasm0(data, wasm.__wbindgen_malloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.wasmdatabase_create_blob(this.__wbg_ptr, ptr0, len0);
        return BigInt.asUintN(64, ret);
    }
    /**
     * Create a blob writer for streaming blob creation.
     * Call write_blob_chunk() to add data, then finish_blob() to get the handle.
     * @returns {WasmBlobWriter}
     */
    create_blob_writer() {
        const ret = wasm.wasmdatabase_create_blob_writer(this.__wbg_ptr);
        return WasmBlobWriter.__wrap(ret);
    }
    /**
     * Read all bytes from a blob handle.
     * For small blobs this returns the inline data directly.
     * For large chunked blobs, this reads and concatenates all chunks from Environment.
     * Use read_blob_chunk() for streaming reads of large blobs.
     * @param {bigint} handle_id
     * @returns {Uint8Array}
     */
    read_blob(handle_id) {
        const ret = wasm.wasmdatabase_read_blob(this.__wbg_ptr, handle_id);
        if (ret[2]) {
            throw takeFromExternrefTable0(ret[1]);
        }
        return takeFromExternrefTable0(ret[0]);
    }
    /**
     * Get information about a blob.
     * Returns a JS object with: { isInline: bool, chunkCount: number, size?: number }
     * @param {bigint} handle_id
     * @returns {any}
     */
    get_blob_info(handle_id) {
        const ret = wasm.wasmdatabase_get_blob_info(this.__wbg_ptr, handle_id);
        if (ret[2]) {
            throw takeFromExternrefTable0(ret[1]);
        }
        return takeFromExternrefTable0(ret[0]);
    }
    /**
     * Read a specific chunk of a blob by index.
     * For inline blobs, index 0 returns all data.
     * For chunked blobs, returns the chunk at the given index from Environment.
     * @param {bigint} handle_id
     * @param {number} chunk_index
     * @returns {Uint8Array}
     */
    read_blob_chunk(handle_id, chunk_index) {
        const ret = wasm.wasmdatabase_read_blob_chunk(this.__wbg_ptr, handle_id, chunk_index);
        if (ret[2]) {
            throw takeFromExternrefTable0(ret[1]);
        }
        return takeFromExternrefTable0(ret[0]);
    }
    /**
     * Release a blob handle, freeing the associated memory.
     * Call this when you're done with a blob to prevent memory leaks.
     * @param {bigint} handle_id
     */
    release_blob(handle_id) {
        wasm.wasmdatabase_release_blob(this.__wbg_ptr, handle_id);
    }
    /**
     * Insert a row with blob values.
     * string_columns is an array of [column_name, value] pairs for string columns.
     * blob_columns is an array of [column_name, blob_handle_id] pairs.
     * @param {string} table
     * @param {any} string_columns
     * @param {any} blob_columns
     * @returns {string}
     */
    insert_with_blobs(table, string_columns, blob_columns) {
        let deferred3_0;
        let deferred3_1;
        try {
            const ptr0 = passStringToWasm0(table, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
            const len0 = WASM_VECTOR_LEN;
            const ret = wasm.wasmdatabase_insert_with_blobs(this.__wbg_ptr, ptr0, len0, string_columns, blob_columns);
            var ptr2 = ret[0];
            var len2 = ret[1];
            if (ret[3]) {
                ptr2 = 0; len2 = 0;
                throw takeFromExternrefTable0(ret[2]);
            }
            deferred3_0 = ptr2;
            deferred3_1 = len2;
            return getStringFromWasm0(ptr2, len2);
        } finally {
            wasm.__wbindgen_free(deferred3_0, deferred3_1, 1);
        }
    }
    /**
     * Update a row's blob column.
     * @param {string} table
     * @param {string} row_id
     * @param {string} column
     * @param {bigint} blob_handle_id
     * @returns {boolean}
     */
    update_row_blob(table, row_id, column, blob_handle_id) {
        const ptr0 = passStringToWasm0(table, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passStringToWasm0(row_id, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len1 = WASM_VECTOR_LEN;
        const ptr2 = passStringToWasm0(column, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len2 = WASM_VECTOR_LEN;
        const ret = wasm.wasmdatabase_update_row_blob(this.__wbg_ptr, ptr0, len0, ptr1, len1, ptr2, len2, blob_handle_id);
        if (ret[2]) {
            throw takeFromExternrefTable0(ret[1]);
        }
        return ret[0] !== 0;
    }
}
if (Symbol.dispose) WasmDatabase.prototype[Symbol.dispose] = WasmDatabase.prototype.free;

/**
 * Handle to an incremental query subscription.
 * The subscription stays active as long as this handle exists.
 */
export class WasmQueryHandle {
    static __wrap(ptr) {
        ptr = ptr >>> 0;
        const obj = Object.create(WasmQueryHandle.prototype);
        obj.__wbg_ptr = ptr;
        WasmQueryHandleFinalization.register(obj, obj.__wbg_ptr, obj);
        return obj;
    }
    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        WasmQueryHandleFinalization.unregister(this);
        return ptr;
    }
    free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_wasmqueryhandle_free(ptr, 0);
    }
    /**
     * Unsubscribe from updates.
     */
    unsubscribe() {
        wasm.wasmqueryhandle_unsubscribe(this.__wbg_ptr);
    }
}
if (Symbol.dispose) WasmQueryHandle.prototype[Symbol.dispose] = WasmQueryHandle.prototype.free;

/**
 * Handle to an incremental query subscription with binary encoding.
 * The subscription stays active as long as this handle exists.
 */
export class WasmQueryHandleBinary {
    static __wrap(ptr) {
        ptr = ptr >>> 0;
        const obj = Object.create(WasmQueryHandleBinary.prototype);
        obj.__wbg_ptr = ptr;
        WasmQueryHandleBinaryFinalization.register(obj, obj.__wbg_ptr, obj);
        return obj;
    }
    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        WasmQueryHandleBinaryFinalization.unregister(this);
        return ptr;
    }
    free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_wasmqueryhandlebinary_free(ptr, 0);
    }
    /**
     * Unsubscribe from updates.
     */
    unsubscribe() {
        wasm.wasmqueryhandlebinary_unsubscribe(this.__wbg_ptr);
    }
}
if (Symbol.dispose) WasmQueryHandleBinary.prototype[Symbol.dispose] = WasmQueryHandleBinary.prototype.free;

/**
 * Handle to an incremental query subscription with per-delta binary encoding.
 * Each delta is encoded individually for efficient incremental decoding on JS side.
 */
export class WasmQueryHandleDelta {
    static __wrap(ptr) {
        ptr = ptr >>> 0;
        const obj = Object.create(WasmQueryHandleDelta.prototype);
        obj.__wbg_ptr = ptr;
        WasmQueryHandleDeltaFinalization.register(obj, obj.__wbg_ptr, obj);
        return obj;
    }
    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        WasmQueryHandleDeltaFinalization.unregister(this);
        return ptr;
    }
    free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_wasmqueryhandledelta_free(ptr, 0);
    }
    /**
     * Unsubscribe from updates.
     */
    unsubscribe() {
        wasm.wasmqueryhandledelta_unsubscribe(this.__wbg_ptr);
    }
    /**
     * Get a text diagram of the query graph.
     *
     * Returns a human-readable representation of the computation DAG
     * showing node types, predicates, and current cache states.
     * @returns {string}
     */
    diagram() {
        let deferred1_0;
        let deferred1_1;
        try {
            const ret = wasm.wasmqueryhandledelta_diagram(this.__wbg_ptr);
            deferred1_0 = ret[0];
            deferred1_1 = ret[1];
            return getStringFromWasm0(ret[0], ret[1]);
        } finally {
            wasm.__wbindgen_free(deferred1_0, deferred1_1, 1);
        }
    }
}
if (Symbol.dispose) WasmQueryHandleDelta.prototype[Symbol.dispose] = WasmQueryHandleDelta.prototype.free;

/**
 * WASM sync client.
 *
 * Manages connection to a sync server from the browser.
 * This is a JavaScript-friendly wrapper around `SyncClient<WasmClientEnv>`.
 */
export class WasmSyncClient {
    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        WasmSyncClientFinalization.unregister(this);
        return ptr;
    }
    free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_wasmsyncclient_free(ptr, 0);
    }
    /**
     * Create a new sync client.
     *
     * # Arguments
     * * `base_url` - The sync server URL (e.g., "http://localhost:8080")
     * * `auth_token` - Bearer token for authentication
     * @param {string} base_url
     * @param {string} auth_token
     */
    constructor(base_url, auth_token) {
        const ptr0 = passStringToWasm0(base_url, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passStringToWasm0(auth_token, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len1 = WASM_VECTOR_LEN;
        const ret = wasm.wasmsyncclient_new(ptr0, len0, ptr1, len1);
        this.__wbg_ptr = ret >>> 0;
        WasmSyncClientFinalization.register(this, this.__wbg_ptr, this);
        return this;
    }
    /**
     * Set callback for commit events.
     *
     * Callback receives: (object_id: string, commits: Uint8Array[], frontier: string[])
     * @param {Function} callback
     */
    setOnCommits(callback) {
        wasm.wasmsyncclient_setOnCommits(this.__wbg_ptr, callback);
    }
    /**
     * Set callback for excluded events.
     *
     * Callback receives: (object_id: string)
     * @param {Function} callback
     */
    setOnExcluded(callback) {
        wasm.wasmsyncclient_setOnExcluded(this.__wbg_ptr, callback);
    }
    /**
     * Set callback for error events.
     *
     * Callback receives: (code: number, message: string)
     * @param {Function} callback
     */
    setOnError(callback) {
        wasm.wasmsyncclient_setOnError(this.__wbg_ptr, callback);
    }
    /**
     * Set callback for connection state changes.
     *
     * Callback receives: (state: string)
     * @param {Function} callback
     */
    setOnStateChange(callback) {
        wasm.wasmsyncclient_setOnStateChange(this.__wbg_ptr, callback);
    }
    /**
     * Get current connection state.
     * @returns {ConnectionState}
     */
    get connectionState() {
        const ret = wasm.wasmsyncclient_connectionState(this.__wbg_ptr);
        return ret;
    }
    /**
     * Subscribe to a query and start receiving updates.
     *
     * This opens an SSE connection to receive real-time updates.
     * @param {string} query
     * @returns {Promise<number>}
     */
    subscribe(query) {
        const ptr0 = passStringToWasm0(query, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.wasmsyncclient_subscribe(this.__wbg_ptr, ptr0, len0);
        return ret;
    }
    /**
     * Push commits for an object to the server.
     * @param {string} object_id
     * @param {Array<any>} commits_data
     * @returns {Promise<any>}
     */
    push(object_id, commits_data) {
        const ptr0 = passStringToWasm0(object_id, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.wasmsyncclient_push(this.__wbg_ptr, ptr0, len0, commits_data);
        return ret;
    }
    /**
     * Request reconciliation for an object.
     * @param {string} object_id
     * @param {Array<any>} local_frontier
     * @returns {Promise<any>}
     */
    reconcile(object_id, local_frontier) {
        const ptr0 = passStringToWasm0(object_id, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.wasmsyncclient_reconcile(this.__wbg_ptr, ptr0, len0, local_frontier);
        return ret;
    }
    /**
     * Disconnect from the server.
     */
    disconnect() {
        wasm.wasmsyncclient_disconnect(this.__wbg_ptr);
    }
}
if (Symbol.dispose) WasmSyncClient.prototype[Symbol.dispose] = WasmSyncClient.prototype.free;

/**
 * A synced local database for browser environments.
 *
 * Combines SQL database operations with real-time sync to a server.
 * All writes are automatically pushed to the server, and incoming
 * changes from other clients are automatically applied.
 */
export class WasmSyncedLocalNode {
    static __wrap(ptr) {
        ptr = ptr >>> 0;
        const obj = Object.create(WasmSyncedLocalNode.prototype);
        obj.__wbg_ptr = ptr;
        WasmSyncedLocalNodeFinalization.register(obj, obj.__wbg_ptr, obj);
        return obj;
    }
    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        WasmSyncedLocalNodeFinalization.unregister(this);
        return ptr;
    }
    free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_wasmsyncedlocalnode_free(ptr, 0);
    }
    /**
     * Create a new synced local node with in-memory storage.
     *
     * @param server_url - The sync server URL (e.g., "http://localhost:8080")
     * @param auth_token - Bearer token for authentication
     * @param catalog_id - Optional shared catalog ID (for sync between multiple clients)
     * @param {string} server_url
     * @param {string} auth_token
     * @param {string | null} [catalog_id]
     */
    constructor(server_url, auth_token, catalog_id) {
        const ptr0 = passStringToWasm0(server_url, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passStringToWasm0(auth_token, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len1 = WASM_VECTOR_LEN;
        var ptr2 = isLikeNone(catalog_id) ? 0 : passStringToWasm0(catalog_id, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        var len2 = WASM_VECTOR_LEN;
        const ret = wasm.wasmsyncedlocalnode_new(ptr0, len0, ptr1, len1, ptr2, len2);
        this.__wbg_ptr = ret >>> 0;
        WasmSyncedLocalNodeFinalization.register(this, this.__wbg_ptr, this);
        return this;
    }
    /**
     * Create a synced local node with IndexedDB persistence.
     *
     * @param server_url - The sync server URL
     * @param auth_token - Bearer token for authentication
     * @param db_name - Optional database name (defaults to "groove")
     * @param {string} server_url
     * @param {string} auth_token
     * @param {string | null} [db_name]
     * @returns {Promise<any>}
     */
    static withIndexedDb(server_url, auth_token, db_name) {
        const ptr0 = passStringToWasm0(server_url, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passStringToWasm0(auth_token, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len1 = WASM_VECTOR_LEN;
        var ptr2 = isLikeNone(db_name) ? 0 : passStringToWasm0(db_name, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        var len2 = WASM_VECTOR_LEN;
        const ret = wasm.wasmsyncedlocalnode_withIndexedDb(ptr0, len0, ptr1, len1, ptr2, len2);
        return ret;
    }
    /**
     * Set callback for sync state changes.
     *
     * Callback receives: (state: string)
     * @param {Function} callback
     */
    setOnStateChange(callback) {
        wasm.wasmsyncedlocalnode_setOnStateChange(this.__wbg_ptr, callback);
    }
    /**
     * Set callback for sync errors.
     *
     * Callback receives: (message: string)
     * @param {Function} callback
     */
    setOnError(callback) {
        wasm.wasmsyncedlocalnode_setOnError(this.__wbg_ptr, callback);
    }
    /**
     * Set callback for data changes (called when sync applies remote changes).
     *
     * Callback receives: no arguments
     * @param {Function} callback
     */
    setOnDataChange(callback) {
        wasm.wasmsyncedlocalnode_setOnDataChange(this.__wbg_ptr, callback);
    }
    /**
     * Get current sync state.
     * @returns {SyncState}
     */
    get syncState() {
        const ret = wasm.wasmsyncedlocalnode_syncState(this.__wbg_ptr);
        return ret;
    }
    /**
     * Connect to the sync server and start receiving updates.
     *
     * This subscribes to the given query and starts an SSE stream
     * to receive real-time updates from other clients.
     * @param {string} query
     * @returns {Promise<any>}
     */
    connect(query) {
        const ptr0 = passStringToWasm0(query, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.wasmsyncedlocalnode_connect(this.__wbg_ptr, ptr0, len0);
        return ret;
    }
    /**
     * Execute a SQL statement.
     * @param {string} sql
     * @returns {any}
     */
    execute(sql) {
        const ptr0 = passStringToWasm0(sql, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.wasmsyncedlocalnode_execute(this.__wbg_ptr, ptr0, len0);
        if (ret[2]) {
            throw takeFromExternrefTable0(ret[1]);
        }
        return takeFromExternrefTable0(ret[0]);
    }
    /**
     * Execute a SELECT query and return results as binary Uint8Array.
     * @param {string} sql
     * @returns {Uint8Array}
     */
    selectBinary(sql) {
        const ptr0 = passStringToWasm0(sql, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.wasmsyncedlocalnode_selectBinary(this.__wbg_ptr, ptr0, len0);
        if (ret[2]) {
            throw takeFromExternrefTable0(ret[1]);
        }
        return takeFromExternrefTable0(ret[0]);
    }
    /**
     * Initialize the database schema from a SQL string.
     * @param {string} schema
     */
    initSchema(schema) {
        const ptr0 = passStringToWasm0(schema, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.wasmsyncedlocalnode_initSchema(this.__wbg_ptr, ptr0, len0);
        if (ret[1]) {
            throw takeFromExternrefTable0(ret[0]);
        }
    }
    /**
     * Update a specific row's column with a string value.
     * @param {string} table
     * @param {string} row_id
     * @param {string} column
     * @param {string} value
     * @returns {boolean}
     */
    updateRow(table, row_id, column, value) {
        const ptr0 = passStringToWasm0(table, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passStringToWasm0(row_id, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len1 = WASM_VECTOR_LEN;
        const ptr2 = passStringToWasm0(column, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len2 = WASM_VECTOR_LEN;
        const ptr3 = passStringToWasm0(value, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len3 = WASM_VECTOR_LEN;
        const ret = wasm.wasmsyncedlocalnode_updateRow(this.__wbg_ptr, ptr0, len0, ptr1, len1, ptr2, len2, ptr3, len3);
        if (ret[2]) {
            throw takeFromExternrefTable0(ret[1]);
        }
        return ret[0] !== 0;
    }
    /**
     * Update a specific row's column with an i64 value.
     * @param {string} table
     * @param {string} row_id
     * @param {string} column
     * @param {bigint} value
     * @returns {boolean}
     */
    updateRowI64(table, row_id, column, value) {
        const ptr0 = passStringToWasm0(table, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passStringToWasm0(row_id, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len1 = WASM_VECTOR_LEN;
        const ptr2 = passStringToWasm0(column, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len2 = WASM_VECTOR_LEN;
        const ret = wasm.wasmsyncedlocalnode_updateRowI64(this.__wbg_ptr, ptr0, len0, ptr1, len1, ptr2, len2, value);
        if (ret[2]) {
            throw takeFromExternrefTable0(ret[1]);
        }
        return ret[0] !== 0;
    }
    /**
     * List all tables in the database.
     * @returns {any}
     */
    listTables() {
        const ret = wasm.wasmsyncedlocalnode_listTables(this.__wbg_ptr);
        return ret;
    }
    /**
     * Create an incremental query subscription (delta-based).
     * @param {string} sql
     * @param {Function} callback
     * @returns {SyncedQueryHandle}
     */
    subscribeDelta(sql, callback) {
        const ptr0 = passStringToWasm0(sql, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.wasmsyncedlocalnode_subscribeDelta(this.__wbg_ptr, ptr0, len0, callback);
        if (ret[2]) {
            throw takeFromExternrefTable0(ret[1]);
        }
        return SyncedQueryHandle.__wrap(ret[0]);
    }
}
if (Symbol.dispose) WasmSyncedLocalNode.prototype[Symbol.dispose] = WasmSyncedLocalNode.prototype.free;

/**
 * Create a ReadableStream that reads from a blob.
 * This is a convenience wrapper for JS interop.
 * @param {WasmDatabase} db
 * @param {bigint} handle_id
 * @returns {any}
 */
export function create_blob_readable_stream(db, handle_id) {
    _assertClass(db, WasmDatabase);
    const ret = wasm.create_blob_readable_stream(db.__wbg_ptr, handle_id);
    if (ret[2]) {
        throw takeFromExternrefTable0(ret[1]);
    }
    return takeFromExternrefTable0(ret[0]);
}

/**
 * Initialize panic hook for better error messages.
 */
export function init() {
    wasm.init();
}

/**
 * Convert a Base32 string ObjectId to 16-byte binary.
 *
 * Returns a Uint8Array containing the u128 little-endian bytes.
 * @param {string} s
 * @returns {Uint8Array}
 */
export function object_id_from_string(s) {
    const ptr0 = passStringToWasm0(s, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
    const len0 = WASM_VECTOR_LEN;
    const ret = wasm.object_id_from_string(ptr0, len0);
    if (ret[2]) {
        throw takeFromExternrefTable0(ret[1]);
    }
    return takeFromExternrefTable0(ret[0]);
}

/**
 * Convert a 16-byte binary ObjectId to a Base32 string.
 *
 * This is useful for displaying ObjectIds or using them as string keys.
 * The binary format is u128 little-endian.
 * @param {Uint8Array} bytes
 * @returns {string}
 */
export function object_id_to_string(bytes) {
    let deferred3_0;
    let deferred3_1;
    try {
        const ptr0 = passArray8ToWasm0(bytes, wasm.__wbindgen_malloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.object_id_to_string(ptr0, len0);
        var ptr2 = ret[0];
        var len2 = ret[1];
        if (ret[3]) {
            ptr2 = 0; len2 = 0;
            throw takeFromExternrefTable0(ret[2]);
        }
        deferred3_0 = ptr2;
        deferred3_1 = len2;
        return getStringFromWasm0(ptr2, len2);
    } finally {
        wasm.__wbindgen_free(deferred3_0, deferred3_1, 1);
    }
}

const EXPECTED_RESPONSE_TYPES = new Set(['basic', 'cors', 'default']);

async function __wbg_load(module, imports) {
    if (typeof Response === 'function' && module instanceof Response) {
        if (typeof WebAssembly.instantiateStreaming === 'function') {
            try {
                return await WebAssembly.instantiateStreaming(module, imports);
            } catch (e) {
                const validResponse = module.ok && EXPECTED_RESPONSE_TYPES.has(module.type);

                if (validResponse && module.headers.get('Content-Type') !== 'application/wasm') {
                    console.warn("`WebAssembly.instantiateStreaming` failed because your server does not serve Wasm with `application/wasm` MIME type. Falling back to `WebAssembly.instantiate` which is slower. Original error:\n", e);

                } else {
                    throw e;
                }
            }
        }

        const bytes = await module.arrayBuffer();
        return await WebAssembly.instantiate(bytes, imports);
    } else {
        const instance = await WebAssembly.instantiate(module, imports);

        if (instance instanceof WebAssembly.Instance) {
            return { instance, module };
        } else {
            return instance;
        }
    }
}

function __wbg_get_imports() {
    const imports = {};
    imports.wbg = {};
    imports.wbg.__wbg_Error_52673b7de5a0ca89 = function(arg0, arg1) {
        const ret = Error(getStringFromWasm0(arg0, arg1));
        return ret;
    };
    imports.wbg.__wbg_Number_2d1dcfcf4ec51736 = function(arg0) {
        const ret = Number(arg0);
        return ret;
    };
    imports.wbg.__wbg___wbindgen_bigint_get_as_i64_6e32f5e6aff02e1d = function(arg0, arg1) {
        const v = arg1;
        const ret = typeof(v) === 'bigint' ? v : undefined;
        getDataViewMemory0().setBigInt64(arg0 + 8 * 1, isLikeNone(ret) ? BigInt(0) : ret, true);
        getDataViewMemory0().setInt32(arg0 + 4 * 0, !isLikeNone(ret), true);
    };
    imports.wbg.__wbg___wbindgen_boolean_get_dea25b33882b895b = function(arg0) {
        const v = arg0;
        const ret = typeof(v) === 'boolean' ? v : undefined;
        return isLikeNone(ret) ? 0xFFFFFF : ret ? 1 : 0;
    };
    imports.wbg.__wbg___wbindgen_debug_string_adfb662ae34724b6 = function(arg0, arg1) {
        const ret = debugString(arg1);
        const ptr1 = passStringToWasm0(ret, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len1 = WASM_VECTOR_LEN;
        getDataViewMemory0().setInt32(arg0 + 4 * 1, len1, true);
        getDataViewMemory0().setInt32(arg0 + 4 * 0, ptr1, true);
    };
    imports.wbg.__wbg___wbindgen_is_bigint_0e1a2e3f55cfae27 = function(arg0) {
        const ret = typeof(arg0) === 'bigint';
        return ret;
    };
    imports.wbg.__wbg___wbindgen_is_function_8d400b8b1af978cd = function(arg0) {
        const ret = typeof(arg0) === 'function';
        return ret;
    };
    imports.wbg.__wbg___wbindgen_is_null_dfda7d66506c95b5 = function(arg0) {
        const ret = arg0 === null;
        return ret;
    };
    imports.wbg.__wbg___wbindgen_is_object_ce774f3490692386 = function(arg0) {
        const val = arg0;
        const ret = typeof(val) === 'object' && val !== null;
        return ret;
    };
    imports.wbg.__wbg___wbindgen_is_undefined_f6b95eab589e0269 = function(arg0) {
        const ret = arg0 === undefined;
        return ret;
    };
    imports.wbg.__wbg___wbindgen_jsval_eq_b6101cc9cef1fe36 = function(arg0, arg1) {
        const ret = arg0 === arg1;
        return ret;
    };
    imports.wbg.__wbg___wbindgen_jsval_loose_eq_766057600fdd1b0d = function(arg0, arg1) {
        const ret = arg0 == arg1;
        return ret;
    };
    imports.wbg.__wbg___wbindgen_number_get_9619185a74197f95 = function(arg0, arg1) {
        const obj = arg1;
        const ret = typeof(obj) === 'number' ? obj : undefined;
        getDataViewMemory0().setFloat64(arg0 + 8 * 1, isLikeNone(ret) ? 0 : ret, true);
        getDataViewMemory0().setInt32(arg0 + 4 * 0, !isLikeNone(ret), true);
    };
    imports.wbg.__wbg___wbindgen_string_get_a2a31e16edf96e42 = function(arg0, arg1) {
        const obj = arg1;
        const ret = typeof(obj) === 'string' ? obj : undefined;
        var ptr1 = isLikeNone(ret) ? 0 : passStringToWasm0(ret, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        var len1 = WASM_VECTOR_LEN;
        getDataViewMemory0().setInt32(arg0 + 4 * 1, len1, true);
        getDataViewMemory0().setInt32(arg0 + 4 * 0, ptr1, true);
    };
    imports.wbg.__wbg___wbindgen_throw_dd24417ed36fc46e = function(arg0, arg1) {
        throw new Error(getStringFromWasm0(arg0, arg1));
    };
    imports.wbg.__wbg__wbg_cb_unref_87dfb5aaa0cbcea7 = function(arg0) {
        arg0._wbg_cb_unref();
    };
    imports.wbg.__wbg_arrayBuffer_c04af4fce566092d = function() { return handleError(function (arg0) {
        const ret = arg0.arrayBuffer();
        return ret;
    }, arguments) };
    imports.wbg.__wbg_call_3020136f7a2d6e44 = function() { return handleError(function (arg0, arg1, arg2) {
        const ret = arg0.call(arg1, arg2);
        return ret;
    }, arguments) };
    imports.wbg.__wbg_call_abb4ff46ce38be40 = function() { return handleError(function (arg0, arg1) {
        const ret = arg0.call(arg1);
        return ret;
    }, arguments) };
    imports.wbg.__wbg_close_d8fdbb14189a985f = function(arg0) {
        arg0.close();
    };
    imports.wbg.__wbg_contains_de2a27de1ed31877 = function(arg0, arg1, arg2) {
        const ret = arg0.contains(getStringFromWasm0(arg1, arg2));
        return ret;
    };
    imports.wbg.__wbg_createObjectStore_dba64acfe84d4191 = function() { return handleError(function (arg0, arg1, arg2) {
        const ret = arg0.createObjectStore(getStringFromWasm0(arg1, arg2));
        return ret;
    }, arguments) };
    imports.wbg.__wbg_data_8bf4ae669a78a688 = function(arg0) {
        const ret = arg0.data;
        return ret;
    };
    imports.wbg.__wbg_deleteDatabase_19c91a8e3e6b92cf = function() { return handleError(function (arg0, arg1, arg2) {
        const ret = arg0.deleteDatabase(getStringFromWasm0(arg1, arg2));
        return ret;
    }, arguments) };
    imports.wbg.__wbg_delete_a8cf58aab29e18d2 = function() { return handleError(function (arg0, arg1) {
        const ret = arg0.delete(arg1);
        return ret;
    }, arguments) };
    imports.wbg.__wbg_done_62ea16af4ce34b24 = function(arg0) {
        const ret = arg0.done;
        return ret;
    };
    imports.wbg.__wbg_error_7534b8e9a36f1ab4 = function(arg0, arg1) {
        let deferred0_0;
        let deferred0_1;
        try {
            deferred0_0 = arg0;
            deferred0_1 = arg1;
            console.error(getStringFromWasm0(arg0, arg1));
        } finally {
            wasm.__wbindgen_free(deferred0_0, deferred0_1, 1);
        }
    };
    imports.wbg.__wbg_error_ad02a286da74488a = function() { return handleError(function (arg0) {
        const ret = arg0.error;
        return isLikeNone(ret) ? 0 : addToExternrefTable0(ret);
    }, arguments) };
    imports.wbg.__wbg_fetch_8119fbf8d0e4f4d1 = function(arg0, arg1) {
        const ret = arg0.fetch(arg1);
        return ret;
    };
    imports.wbg.__wbg_getAllKeys_925405ffbd671e86 = function() { return handleError(function (arg0) {
        const ret = arg0.getAllKeys();
        return ret;
    }, arguments) };
    imports.wbg.__wbg_getRandomValues_9b655bdd369112f2 = function() { return handleError(function (arg0, arg1) {
        globalThis.crypto.getRandomValues(getArrayU8FromWasm0(arg0, arg1));
    }, arguments) };
    imports.wbg.__wbg_get_6b7bd52aca3f9671 = function(arg0, arg1) {
        const ret = arg0[arg1 >>> 0];
        return ret;
    };
    imports.wbg.__wbg_get_7d8b665fa88606d5 = function() { return handleError(function (arg0, arg1) {
        const ret = arg0.get(arg1);
        return ret;
    }, arguments) };
    imports.wbg.__wbg_get_af9dab7e9603ea93 = function() { return handleError(function (arg0, arg1) {
        const ret = Reflect.get(arg0, arg1);
        return ret;
    }, arguments) };
    imports.wbg.__wbg_indexedDB_23c232e00a1e28ad = function() { return handleError(function (arg0) {
        const ret = arg0.indexedDB;
        return isLikeNone(ret) ? 0 : addToExternrefTable0(ret);
    }, arguments) };
    imports.wbg.__wbg_instanceof_ArrayBuffer_f3320d2419cd0355 = function(arg0) {
        let result;
        try {
            result = arg0 instanceof ArrayBuffer;
        } catch (_) {
            result = false;
        }
        const ret = result;
        return ret;
    };
    imports.wbg.__wbg_instanceof_IdbDatabase_f4e157055e32c479 = function(arg0) {
        let result;
        try {
            result = arg0 instanceof IDBDatabase;
        } catch (_) {
            result = false;
        }
        const ret = result;
        return ret;
    };
    imports.wbg.__wbg_instanceof_IdbOpenDbRequest_e4a587961e53201e = function(arg0) {
        let result;
        try {
            result = arg0 instanceof IDBOpenDBRequest;
        } catch (_) {
            result = false;
        }
        const ret = result;
        return ret;
    };
    imports.wbg.__wbg_instanceof_Response_cd74d1c2ac92cb0b = function(arg0) {
        let result;
        try {
            result = arg0 instanceof Response;
        } catch (_) {
            result = false;
        }
        const ret = result;
        return ret;
    };
    imports.wbg.__wbg_instanceof_Uint8Array_da54ccc9d3e09434 = function(arg0) {
        let result;
        try {
            result = arg0 instanceof Uint8Array;
        } catch (_) {
            result = false;
        }
        const ret = result;
        return ret;
    };
    imports.wbg.__wbg_instanceof_Window_b5cf7783caa68180 = function(arg0) {
        let result;
        try {
            result = arg0 instanceof Window;
        } catch (_) {
            result = false;
        }
        const ret = result;
        return ret;
    };
    imports.wbg.__wbg_isArray_51fd9e6422c0a395 = function(arg0) {
        const ret = Array.isArray(arg0);
        return ret;
    };
    imports.wbg.__wbg_isSafeInteger_ae7d3f054d55fa16 = function(arg0) {
        const ret = Number.isSafeInteger(arg0);
        return ret;
    };
    imports.wbg.__wbg_iterator_27b7c8b35ab3e86b = function() {
        const ret = Symbol.iterator;
        return ret;
    };
    imports.wbg.__wbg_length_22ac23eaec9d8053 = function(arg0) {
        const ret = arg0.length;
        return ret;
    };
    imports.wbg.__wbg_length_d45040a40c570362 = function(arg0) {
        const ret = arg0.length;
        return ret;
    };
    imports.wbg.__wbg_log_1d990106d99dacb7 = function(arg0) {
        console.log(arg0);
    };
    imports.wbg.__wbg_new_1ba21ce319a06297 = function() {
        const ret = new Object();
        return ret;
    };
    imports.wbg.__wbg_new_2434a2653f675856 = function() { return handleError(function (arg0, arg1) {
        const ret = new EventSource(getStringFromWasm0(arg0, arg1));
        return ret;
    }, arguments) };
    imports.wbg.__wbg_new_25f239778d6112b9 = function() {
        const ret = new Array();
        return ret;
    };
    imports.wbg.__wbg_new_3c79b3bb1b32b7d3 = function() { return handleError(function () {
        const ret = new Headers();
        return ret;
    }, arguments) };
    imports.wbg.__wbg_new_6421f6084cc5bc5a = function(arg0) {
        const ret = new Uint8Array(arg0);
        return ret;
    };
    imports.wbg.__wbg_new_8a6f238a6ece86ea = function() {
        const ret = new Error();
        return ret;
    };
    imports.wbg.__wbg_new_ff12d2b041fb48f1 = function(arg0, arg1) {
        try {
            var state0 = {a: arg0, b: arg1};
            var cb0 = (arg0, arg1) => {
                const a = state0.a;
                state0.a = 0;
                try {
                    return wasm_bindgen__convert__closures_____invoke__h3097c68d921a6b39(a, state0.b, arg0, arg1);
                } finally {
                    state0.a = a;
                }
            };
            const ret = new Promise(cb0);
            return ret;
        } finally {
            state0.a = state0.b = 0;
        }
    };
    imports.wbg.__wbg_new_from_slice_f9c22b9153b26992 = function(arg0, arg1) {
        const ret = new Uint8Array(getArrayU8FromWasm0(arg0, arg1));
        return ret;
    };
    imports.wbg.__wbg_new_no_args_cb138f77cf6151ee = function(arg0, arg1) {
        const ret = new Function(getStringFromWasm0(arg0, arg1));
        return ret;
    };
    imports.wbg.__wbg_new_with_length_aa5eaf41d35235e5 = function(arg0) {
        const ret = new Uint8Array(arg0 >>> 0);
        return ret;
    };
    imports.wbg.__wbg_new_with_str_and_init_c5748f76f5108934 = function() { return handleError(function (arg0, arg1, arg2) {
        const ret = new Request(getStringFromWasm0(arg0, arg1), arg2);
        return ret;
    }, arguments) };
    imports.wbg.__wbg_next_138a17bbf04e926c = function(arg0) {
        const ret = arg0.next;
        return ret;
    };
    imports.wbg.__wbg_next_3cfe5c0fe2a4cc53 = function() { return handleError(function (arg0) {
        const ret = arg0.next();
        return ret;
    }, arguments) };
    imports.wbg.__wbg_now_69d776cd24f5215b = function() {
        const ret = Date.now();
        return ret;
    };
    imports.wbg.__wbg_objectStoreNames_90900f9a531513ac = function(arg0) {
        const ret = arg0.objectStoreNames;
        return ret;
    };
    imports.wbg.__wbg_objectStore_da9a077b8849dbe9 = function() { return handleError(function (arg0, arg1, arg2) {
        const ret = arg0.objectStore(getStringFromWasm0(arg1, arg2));
        return ret;
    }, arguments) };
    imports.wbg.__wbg_ok_dd98ecb60d721e20 = function(arg0) {
        const ret = arg0.ok;
        return ret;
    };
    imports.wbg.__wbg_open_0d7b85f4c0a38ffe = function() { return handleError(function (arg0, arg1, arg2, arg3) {
        const ret = arg0.open(getStringFromWasm0(arg1, arg2), arg3 >>> 0);
        return ret;
    }, arguments) };
    imports.wbg.__wbg_prototypesetcall_dfe9b766cdc1f1fd = function(arg0, arg1, arg2) {
        Uint8Array.prototype.set.call(getArrayU8FromWasm0(arg0, arg1), arg2);
    };
    imports.wbg.__wbg_push_7d9be8f38fc13975 = function(arg0, arg1) {
        const ret = arg0.push(arg1);
        return ret;
    };
    imports.wbg.__wbg_put_d40a68e5a8902a46 = function() { return handleError(function (arg0, arg1, arg2) {
        const ret = arg0.put(arg1, arg2);
        return ret;
    }, arguments) };
    imports.wbg.__wbg_queueMicrotask_9b549dfce8865860 = function(arg0) {
        const ret = arg0.queueMicrotask;
        return ret;
    };
    imports.wbg.__wbg_queueMicrotask_fca69f5bfad613a5 = function(arg0) {
        queueMicrotask(arg0);
    };
    imports.wbg.__wbg_resolve_fd5bfbaa4ce36e1e = function(arg0) {
        const ret = Promise.resolve(arg0);
        return ret;
    };
    imports.wbg.__wbg_result_084f962aedb54250 = function() { return handleError(function (arg0) {
        const ret = arg0.result;
        return ret;
    }, arguments) };
    imports.wbg.__wbg_set_169e13b608078b7b = function(arg0, arg1, arg2) {
        arg0.set(getArrayU8FromWasm0(arg1, arg2));
    };
    imports.wbg.__wbg_set_3f1d0b984ed272ed = function(arg0, arg1, arg2) {
        arg0[arg1] = arg2;
    };
    imports.wbg.__wbg_set_425eb8b710d5beee = function() { return handleError(function (arg0, arg1, arg2, arg3, arg4) {
        arg0.set(getStringFromWasm0(arg1, arg2), getStringFromWasm0(arg3, arg4));
    }, arguments) };
    imports.wbg.__wbg_set_781438a03c0c3c81 = function() { return handleError(function (arg0, arg1, arg2) {
        const ret = Reflect.set(arg0, arg1, arg2);
        return ret;
    }, arguments) };
    imports.wbg.__wbg_set_7df433eea03a5c14 = function(arg0, arg1, arg2) {
        arg0[arg1 >>> 0] = arg2;
    };
    imports.wbg.__wbg_set_body_8e743242d6076a4f = function(arg0, arg1) {
        arg0.body = arg1;
    };
    imports.wbg.__wbg_set_headers_5671cf088e114d2b = function(arg0, arg1) {
        arg0.headers = arg1;
    };
    imports.wbg.__wbg_set_method_76c69e41b3570627 = function(arg0, arg1, arg2) {
        arg0.method = getStringFromWasm0(arg1, arg2);
    };
    imports.wbg.__wbg_set_mode_611016a6818fc690 = function(arg0, arg1) {
        arg0.mode = __wbindgen_enum_RequestMode[arg1];
    };
    imports.wbg.__wbg_set_onerror_08fecec3bdc9d24d = function(arg0, arg1) {
        arg0.onerror = arg1;
    };
    imports.wbg.__wbg_set_onerror_392d7cfd4a6b674a = function(arg0, arg1) {
        arg0.onerror = arg1;
    };
    imports.wbg.__wbg_set_onmessage_977ecf12c8015d49 = function(arg0, arg1) {
        arg0.onmessage = arg1;
    };
    imports.wbg.__wbg_set_onsuccess_94332a00452de699 = function(arg0, arg1) {
        arg0.onsuccess = arg1;
    };
    imports.wbg.__wbg_set_onupgradeneeded_3dc6e233a6d13fe2 = function(arg0, arg1) {
        arg0.onupgradeneeded = arg1;
    };
    imports.wbg.__wbg_stack_0ed75d68575b0f3c = function(arg0, arg1) {
        const ret = arg1.stack;
        const ptr1 = passStringToWasm0(ret, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len1 = WASM_VECTOR_LEN;
        getDataViewMemory0().setInt32(arg0 + 4 * 1, len1, true);
        getDataViewMemory0().setInt32(arg0 + 4 * 0, ptr1, true);
    };
    imports.wbg.__wbg_static_accessor_GLOBAL_769e6b65d6557335 = function() {
        const ret = typeof global === 'undefined' ? null : global;
        return isLikeNone(ret) ? 0 : addToExternrefTable0(ret);
    };
    imports.wbg.__wbg_static_accessor_GLOBAL_THIS_60cf02db4de8e1c1 = function() {
        const ret = typeof globalThis === 'undefined' ? null : globalThis;
        return isLikeNone(ret) ? 0 : addToExternrefTable0(ret);
    };
    imports.wbg.__wbg_static_accessor_SELF_08f5a74c69739274 = function() {
        const ret = typeof self === 'undefined' ? null : self;
        return isLikeNone(ret) ? 0 : addToExternrefTable0(ret);
    };
    imports.wbg.__wbg_static_accessor_WINDOW_a8924b26aa92d024 = function() {
        const ret = typeof window === 'undefined' ? null : window;
        return isLikeNone(ret) ? 0 : addToExternrefTable0(ret);
    };
    imports.wbg.__wbg_statusText_0eec2bbb2c8f22e2 = function(arg0, arg1) {
        const ret = arg1.statusText;
        const ptr1 = passStringToWasm0(ret, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len1 = WASM_VECTOR_LEN;
        getDataViewMemory0().setInt32(arg0 + 4 * 1, len1, true);
        getDataViewMemory0().setInt32(arg0 + 4 * 0, ptr1, true);
    };
    imports.wbg.__wbg_status_9bfc680efca4bdfd = function(arg0) {
        const ret = arg0.status;
        return ret;
    };
    imports.wbg.__wbg_target_0e3e05a6263c37a0 = function(arg0) {
        const ret = arg0.target;
        return isLikeNone(ret) ? 0 : addToExternrefTable0(ret);
    };
    imports.wbg.__wbg_then_429f7caf1026411d = function(arg0, arg1, arg2) {
        const ret = arg0.then(arg1, arg2);
        return ret;
    };
    imports.wbg.__wbg_then_4f95312d68691235 = function(arg0, arg1) {
        const ret = arg0.then(arg1);
        return ret;
    };
    imports.wbg.__wbg_transaction_257422def49a0094 = function() { return handleError(function (arg0, arg1, arg2) {
        const ret = arg0.transaction(arg1, __wbindgen_enum_IdbTransactionMode[arg2]);
        return ret;
    }, arguments) };
    imports.wbg.__wbg_value_57b7b035e117f7ee = function(arg0) {
        const ret = arg0.value;
        return ret;
    };
    imports.wbg.__wbg_wasmdatabase_new = function(arg0) {
        const ret = WasmDatabase.__wrap(arg0);
        return ret;
    };
    imports.wbg.__wbg_wasmsyncedlocalnode_new = function(arg0) {
        const ret = WasmSyncedLocalNode.__wrap(arg0);
        return ret;
    };
    imports.wbg.__wbindgen_cast_2241b6af4c4b2941 = function(arg0, arg1) {
        // Cast intrinsic for `Ref(String) -> Externref`.
        const ret = getStringFromWasm0(arg0, arg1);
        return ret;
    };
    imports.wbg.__wbindgen_cast_2eab6f6adaa15bfa = function(arg0, arg1) {
        // Cast intrinsic for `Closure(Closure { dtor_idx: 477, function: Function { arguments: [Externref], shim_idx: 478, ret: Unit, inner_ret: Some(Unit) }, mutable: true }) -> Externref`.
        const ret = makeMutClosure(arg0, arg1, wasm.wasm_bindgen__closure__destroy__h3d685ebc6ca20542, wasm_bindgen__convert__closures_____invoke__h76dfe62a3b69c085);
        return ret;
    };
    imports.wbg.__wbindgen_cast_4625c577ab2ec9ee = function(arg0) {
        // Cast intrinsic for `U64 -> Externref`.
        const ret = BigInt.asUintN(64, arg0);
        return ret;
    };
    imports.wbg.__wbindgen_cast_498bfe452d30ba84 = function(arg0, arg1) {
        // Cast intrinsic for `Closure(Closure { dtor_idx: 209, function: Function { arguments: [NamedExternref("IDBVersionChangeEvent")], shim_idx: 210, ret: Unit, inner_ret: Some(Unit) }, mutable: true }) -> Externref`.
        const ret = makeMutClosure(arg0, arg1, wasm.wasm_bindgen__closure__destroy__h4df8827c3765d533, wasm_bindgen__convert__closures_____invoke__h20bec3cca755663f);
        return ret;
    };
    imports.wbg.__wbindgen_cast_54c8ee795d00524d = function(arg0, arg1) {
        // Cast intrinsic for `Closure(Closure { dtor_idx: 209, function: Function { arguments: [NamedExternref("Event")], shim_idx: 210, ret: Unit, inner_ret: Some(Unit) }, mutable: true }) -> Externref`.
        const ret = makeMutClosure(arg0, arg1, wasm.wasm_bindgen__closure__destroy__h4df8827c3765d533, wasm_bindgen__convert__closures_____invoke__h20bec3cca755663f);
        return ret;
    };
    imports.wbg.__wbindgen_cast_94f296c274f18f45 = function(arg0, arg1) {
        // Cast intrinsic for `Closure(Closure { dtor_idx: 209, function: Function { arguments: [NamedExternref("MessageEvent")], shim_idx: 210, ret: Unit, inner_ret: Some(Unit) }, mutable: true }) -> Externref`.
        const ret = makeMutClosure(arg0, arg1, wasm.wasm_bindgen__closure__destroy__h4df8827c3765d533, wasm_bindgen__convert__closures_____invoke__h20bec3cca755663f);
        return ret;
    };
    imports.wbg.__wbindgen_cast_d6cd19b81560fd6e = function(arg0) {
        // Cast intrinsic for `F64 -> Externref`.
        const ret = arg0;
        return ret;
    };
    imports.wbg.__wbindgen_init_externref_table = function() {
        const table = wasm.__wbindgen_externrefs;
        const offset = table.grow(4);
        table.set(0, undefined);
        table.set(offset + 0, undefined);
        table.set(offset + 1, null);
        table.set(offset + 2, true);
        table.set(offset + 3, false);
    };

    return imports;
}

function __wbg_finalize_init(instance, module) {
    wasm = instance.exports;
    __wbg_init.__wbindgen_wasm_module = module;
    cachedDataViewMemory0 = null;
    cachedUint8ArrayMemory0 = null;


    wasm.__wbindgen_start();
    return wasm;
}

function initSync(module) {
    if (wasm !== undefined) return wasm;


    if (typeof module !== 'undefined') {
        if (Object.getPrototypeOf(module) === Object.prototype) {
            ({module} = module)
        } else {
            console.warn('using deprecated parameters for `initSync()`; pass a single object instead')
        }
    }

    const imports = __wbg_get_imports();
    if (!(module instanceof WebAssembly.Module)) {
        module = new WebAssembly.Module(module);
    }
    const instance = new WebAssembly.Instance(module, imports);
    return __wbg_finalize_init(instance, module);
}

async function __wbg_init(module_or_path) {
    if (wasm !== undefined) return wasm;


    if (typeof module_or_path !== 'undefined') {
        if (Object.getPrototypeOf(module_or_path) === Object.prototype) {
            ({module_or_path} = module_or_path)
        } else {
            console.warn('using deprecated parameters for the initialization function; pass a single object instead')
        }
    }

    if (typeof module_or_path === 'undefined') {
        module_or_path = new URL('groove_wasm_bg.wasm', import.meta.url);
    }
    const imports = __wbg_get_imports();

    if (typeof module_or_path === 'string' || (typeof Request === 'function' && module_or_path instanceof Request) || (typeof URL === 'function' && module_or_path instanceof URL)) {
        module_or_path = fetch(module_or_path);
    }

    const { instance, module } = await __wbg_load(await module_or_path, imports);

    return __wbg_finalize_init(instance, module);
}

export { initSync };
export default __wbg_init;
