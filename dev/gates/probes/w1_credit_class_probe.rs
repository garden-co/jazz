//! Standalone optimized probe of the original credit-class selection expressions.
//! No Jazz, storage, FFI, async runtime, or serialization code participates.
use std::hint::black_box;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
enum ChannelClass {
    Control = 0,
    Requests = 1,
    Delivery = 2,
    Writes = 3,
    LargeValue = 4,
    Auxiliary = 5,
    Progress = 6,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WireCreditKind {
    Frames,
    Messages { count: u32, bulk: bool },
}

#[derive(Clone, Copy, Default)]
struct BufferCost {
    bytes: usize,
    count: usize,
}

fn bucket_class(index: usize) -> ChannelClass {
    [
        ChannelClass::Control,
        ChannelClass::Requests,
        ChannelClass::Delivery,
        ChannelClass::Writes,
        ChannelClass::Auxiliary,
        ChannelClass::Progress,
    ][index]
}

fn select_grant(
    buffer_grants: &[BufferCost; 6],
    pending_grants: &[usize; 6],
) -> Option<(usize, usize, ChannelClass, WireCreditKind)> {
    let (index, amount, class, kind) =
        if let Some(index) = buffer_grants.iter().position(|c| c.count != 0) {
            let cost = buffer_grants[index];
            (
                index,
                cost.bytes,
                [
                    ChannelClass::Control,
                    ChannelClass::Requests,
                    ChannelClass::Writes,
                    ChannelClass::Auxiliary,
                    ChannelClass::Control,
                    ChannelClass::Progress,
                ][index],
                WireCreditKind::Messages {
                    count: cost.count as u32,
                    bulk: index == 2 || index == 4,
                },
            )
        } else if let Some(index) = pending_grants.iter().position(|amount| *amount != 0) {
            (
                index,
                pending_grants[index],
                bucket_class(index),
                WireCreditKind::Frames,
            )
        } else {
            return None;
        };
    Some((index, amount, class, kind))
}

fn main() {
    let _ = black_box(ChannelClass::LargeValue);
    for index in 0..6 {
        let index = black_box(index);
        let expected = match index {
            0 => ChannelClass::Control,
            1 => ChannelClass::Requests,
            2 => ChannelClass::Delivery,
            3 => ChannelClass::Writes,
            4 => ChannelClass::Auxiliary,
            5 => ChannelClass::Progress,
            _ => unreachable!(),
        };
        let actual = bucket_class(index);
        println!(
            "direct bucket={index} class={actual:?} raw={}",
            actual as u8
        );
        assert_eq!(actual, expected);
        let mut pending = [0; 6];
        pending[index] = black_box(721_647);
        let selected = select_grant(black_box(&[BufferCost::default(); 6]), black_box(&pending));
        println!("physical selection={selected:?}");
        assert_eq!(
            selected,
            Some((index, 721_647, expected, WireCreditKind::Frames))
        );

        let mut buffers = [BufferCost::default(); 6];
        buffers[index] = BufferCost {
            bytes: 721_647,
            count: 1,
        };
        let selected = select_grant(black_box(&buffers), black_box(&[0; 6]));
        let expected = match index {
            0 | 4 => ChannelClass::Control,
            1 => ChannelClass::Requests,
            2 => ChannelClass::Writes,
            3 => ChannelClass::Auxiliary,
            5 => ChannelClass::Progress,
            _ => unreachable!(),
        };
        println!("buffer selection={selected:?}");
        assert_eq!(
            selected,
            Some((
                index,
                721_647,
                expected,
                WireCreditKind::Messages {
                    count: 1,
                    bulk: index == 2 || index == 4,
                }
            ))
        );
    }
}
