use std::cell::RefCell;
use std::hint::black_box;

pub const ARITHMETIC_ITERS: u32 = 200_000_000;
pub const DYN_DISPATCH_ITERS: u32 = 100_000_000;
pub const REFCELL_ITERS: u32 = 100_000_000;
pub const ALLOC_ITERS: u32 = 5_000_000;
pub const MEMORY_ITERS: u32 = 50_000_000;
pub const MEMORY_ENTRIES: u32 = 4_194_304;

pub fn arithmetic_hash(iterations: u32) -> u64 {
    let mut value = black_box(0x9e37_79b9_7f4a_7c15_u64);
    for i in 0..black_box(iterations) {
        value ^= u64::from(i).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        value = value.rotate_left(27).wrapping_mul(0x94d0_49bb_1331_11eb);
        value ^= value >> 31;
    }
    black_box(value)
}

trait ProbeOp {
    fn apply(&self, value: u64) -> u64;
}

struct MixOp {
    salt: u64,
}

impl ProbeOp for MixOp {
    #[inline(never)]
    fn apply(&self, value: u64) -> u64 {
        value
            .wrapping_add(self.salt)
            .rotate_left(13)
            .wrapping_mul(0x9e37_79b9_7f4a_7c15)
    }
}

#[inline(never)]
fn dyn_probe_op() -> Box<dyn ProbeOp> {
    Box::new(MixOp {
        salt: black_box(0xd6e8_feb8_6659_fd93),
    })
}

pub fn dyn_dispatch(iterations: u32) -> u64 {
    let op = dyn_probe_op();
    let mut value = black_box(0x243f_6a88_85a3_08d3_u64);
    for _ in 0..black_box(iterations) {
        value = op.apply(black_box(value));
    }
    black_box(value)
}

pub fn refcell_borrow(iterations: u32) -> u64 {
    let cell = RefCell::new(black_box(0x1319_8a2e_0370_7344_u64));
    let mut value = 0_u64;
    for i in 0..black_box(iterations) {
        {
            let mut borrowed = cell.borrow_mut();
            *borrowed = borrowed
                .wrapping_add(u64::from(i))
                .rotate_left(7)
                .wrapping_mul(0xa24b_aed4_963e_e407);
        }
        value ^= *cell.borrow();
    }
    black_box(value)
}

pub fn alloc_churn(iterations: u32) -> u64 {
    let mut value = black_box(0xcbf2_9ce4_8422_2325_u64);
    for i in 0..black_box(iterations) {
        let mixed = value ^ u64::from(i).wrapping_mul(0x100_0000_01b3);
        let mut bytes = Vec::with_capacity(40);
        bytes.extend_from_slice(&mixed.to_le_bytes());
        bytes.extend_from_slice(&mixed.rotate_left(17).to_le_bytes());
        let text = mixed.to_string();
        value = value
            .wrapping_mul(0x100_0000_01b3)
            .wrapping_add(bytes[usize::from((mixed & 7) as u8)] as u64)
            .wrapping_add(text.len() as u64);
        black_box(bytes);
        black_box(text);
    }
    black_box(value)
}

pub fn random_access_memory(iterations: u32, entries: u32) -> u64 {
    let entries = entries.max(1) as usize;
    let mut data = (0..entries)
        .map(|i| (i as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15))
        .collect::<Vec<_>>();
    let mut index = black_box(0x853c_49e6_748f_ea9b_u64);
    let mut value = 0_u64;
    for _ in 0..black_box(iterations) {
        index = index
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        let slot = (index as usize) % entries;
        let current = data[slot];
        value = value.wrapping_add(current.rotate_left((index & 31) as u32));
        data[slot] = value ^ index;
    }
    black_box(value ^ data[entries / 2])
}
