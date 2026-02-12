// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

use crate::nodes::InnerNode;

/// A tagged union packed into a single `u64`. A PageID is either:
///
/// - A **heap pointer** to an `InnerNode` (the flag bit is 0), or
/// - An **integer index** into the `MappingTable` for leaf/mini pages (the flag
///   bit is 1).
///
/// Packing both into 8 bytes lets us store page IDs atomically (`AtomicU64`),
/// inline them in inner-node data arrays without indirection, and keep inner
/// nodes compact (higher fanout = shallower tree = fewer traversal steps).
///
/// ## Bit layout (AArch64)
///
/// On AArch64, a 64-bit pointer looks like this:
///
/// ```text
///  63       56 55     48 47                                0
/// ┌──────────┬─────────┬──────────────────────────────────────┐
/// │ TBI tag  │  zero   │         48-bit virtual address       │
/// └──────────┴─────────┴──────────────────────────────────────┘
///   bits 63-56  bits 55-48         bits 47-0
///   (ignored    (always 0 in       (actual address used
///    by MMU)     userspace)         by hardware)
/// ```
///
/// - **Bits 47-0:** The virtual address. All current OSes (Linux, Android,
///   macOS, iOS) use 48-bit virtual addressing, so userspace addresses fit in
///   these 48 bits.
/// - **Bits 55-48:** Always zero in userspace pointers — they're above the
///   48-bit VA range. This is where we place our flag bits (ID_MASK at bit 48,
///   ROOT_IS_LEAF_MASK at bit 49 in tree.rs).
/// - **Bits 63-56:** The TBI (Top Byte Ignore) tag byte. On Android, the
///   allocator stores MTE/HWASan memory tags here (e.g. `0x74`). The MMU
///   ignores these bits during address translation, but they must be preserved
///   — stripping them breaks MTE tag checks.
///
/// Our flag bits sit in the safe zone (48-55) between the address and the TBI
/// tag, so they never collide with either real addresses or allocator tags.
///
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(C)]
pub(crate) struct PageID {
    value: u64,
}

/// Discriminator flag: bit 48.
///
/// - **Set (1):** the PageID is an integer index into the MappingTable.
/// - **Clear (0):** the PageID is a heap pointer to an InnerNode.
///
/// Bit 48 is safe because:
/// - It's above the 48-bit virtual address range (userspace pointers never
///   have this bit set on any current OS/arch: Linux, Android, macOS, iOS,
///   x86-64, AArch64).
/// - It's below the TBI tag byte (bits 56-63), so it doesn't collide with
///   Android's MTE/HWASan pointer tags.
/// - On wasm32, pointers are 32-bit and can't reach bit 48 at all.
///
/// Historical note: this was originally bit 62 (`0x4000_0000_0000_0000`),
/// which falls inside the TBI tag byte. On Android, the allocator returns
/// tagged pointers like `0x7400_0075_99BF_2000` where the `0x74` tag has
/// bit 62 set — causing heap pointers to be misclassified as integer IDs
/// and leading to out-of-bounds access in the MappingTable.
///
/// Fallback plan: if the bit trick ever becomes untenable (e.g. future
/// architectures with >48-bit VA in userspace), we can replace PageID with
/// a proper enum on affected targets:
///
/// ```ignore
/// #[cfg(target_os = "android")]
/// enum PageID { Pointer(*const InnerNode), Id(u64) }
/// ```
///
/// This costs 16 bytes instead of 8 and prevents atomic load/store of the
/// root_page_id (would need a lock), so only use it as a last resort.
const ID_MASK: u64 = 0x0001_0000_0000_0000;

impl PageID {
    pub(crate) fn new(value: u64) -> Self {
        assert_eq!(std::mem::size_of::<Self>(), 8);
        Self { value }
    }

    /// Only used when you load a page ID from somewhere.
    pub(crate) unsafe fn from_raw(value: u64) -> Self {
        Self::new(value)
    }

    pub(crate) fn from_pointer(ptr: *const InnerNode) -> Self {
        Self::new(ptr as u64)
    }

    pub(crate) fn from_id(id: u64) -> Self {
        Self::new(id | ID_MASK)
    }

    pub(crate) fn is_id(&self) -> bool {
        (self.value & ID_MASK) != 0
    }

    pub(crate) fn as_id(&self) -> u64 {
        assert!(self.is_id());
        self.value & !ID_MASK
    }

    pub(crate) fn is_inner_node_pointer(&self) -> bool {
        (self.value & ID_MASK) == 0
    }

    pub(crate) fn raw(&self) -> u64 {
        self.value
    }

    pub(crate) fn as_inner_node(&self) -> *const InnerNode {
        assert!(self.is_inner_node_pointer());
        self.value as *const InnerNode
    }
}
