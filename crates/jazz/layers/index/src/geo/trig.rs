//! Sine and cosine computed from `+`, `-`, `*` and `/` only.
//!
//! `f64::sin` and `f64::cos` call the platform's libm (glibc, Apple's libm,
//! musl's port on wasm32, Bionic on Android), and those may differ in the
//! last bit. Stored unit vectors and chord thresholds must have the same bits
//! on every node (SPEC 20.6, `INV-IX-1`), so this module evaluates the fdlibm
//! kernels itself. IEEE-754 binary64 basic operations are correctly rounded
//! on every target, and Rust never contracts `a * b + c` into a fused
//! multiply-add, so these functions return identical bits everywhere.
//!
//! Inputs are degrees, which makes argument reduction exact: subtracting a
//! multiple of 90 from a value within a factor of two of it is exact
//! (Sterbenz), so the only rounding before the kernel is the one conversion
//! to radians.

/// π / 180, rounded to nearest.
const DEG_TO_RAD: f64 = core::f64::consts::PI / 180.0;

// fdlibm `__kernel_sin` coefficients, as exact bits (|x| ≤ π/4, error < 1 ulp).
const S1: f64 = f64::from_bits(0xBFC55555_55555549); // -1.66666666666666324348e-01
const S2: f64 = f64::from_bits(0x3F811111_1110F8A6); // 8.33333333332248946124e-03
const S3: f64 = f64::from_bits(0xBF2A01A0_19C161D5); // -1.98412698298579493134e-04
const S4: f64 = f64::from_bits(0x3EC71DE3_57B1FE7D); // 2.75573137070700676789e-06
const S5: f64 = f64::from_bits(0xBE5AE5E6_8A2B9CEB); // -2.50507602534068634195e-08
const S6: f64 = f64::from_bits(0x3DE5D93A_5ACFD57C); // 1.58969099521155010221e-10

// fdlibm `__kernel_cos` coefficients, as exact bits (|x| ≤ π/4, error < 1 ulp).
const C1: f64 = f64::from_bits(0x3FA55555_5555554C); // 4.16666666666666019037e-02
const C2: f64 = f64::from_bits(0xBF56C16C_16C15177); // -1.38888888888741095749e-03
const C3: f64 = f64::from_bits(0x3EFA01A0_19CB1590); // 2.48015872894767294178e-05
const C4: f64 = f64::from_bits(0xBE927E4F_809C52AD); // -2.75573143513906633035e-07
const C5: f64 = f64::from_bits(0x3E21EE9E_BDB4B1C4); // 2.08757232129817482790e-09
const C6: f64 = f64::from_bits(0xBDA8FAE9_BE8838D4); // -1.13596475577881948265e-11

/// `sin(x)` for `|x| ≤ π/4` radians.
pub(crate) fn kernel_sin(x: f64) -> f64 {
    let z = x * x;
    let v = z * x;
    let r = S2 + z * (S3 + z * (S4 + z * (S5 + z * S6)));
    x + v * (S1 + z * r)
}

/// `cos(x)` for `|x| ≤ π/4` radians.
pub(crate) fn kernel_cos(x: f64) -> f64 {
    let z = x * x;
    let w = z * z;
    let r = z * (C1 + z * (C2 + z * C3)) + w * w * (C4 + z * (C5 + z * C6));
    let hz = 0.5 * z;
    let w = 1.0 - hz;
    w + (((1.0 - w) - hz) + z * r)
}

/// `(sin d°, cos d°)` for `-180 ≤ d ≤ 180`.
pub(crate) fn sin_cos_deg(d: f64) -> (f64, f64) {
    debug_assert!((-180.0..=180.0).contains(&d));
    // Each subtraction is exact: the operands are within a factor of two.
    let (quadrant, r) = if d > 135.0 {
        (2, d - 180.0)
    } else if d > 45.0 {
        (1, d - 90.0)
    } else if d >= -45.0 {
        (0, d)
    } else if d >= -135.0 {
        (3, d + 90.0)
    } else {
        (2, d + 180.0)
    };
    let x = r * DEG_TO_RAD;
    let (s, c) = (kernel_sin(x), kernel_cos(x));
    match quadrant {
        0 => (s, c),
        1 => (c, -s),
        2 => (-s, -c),
        _ => (-c, s),
    }
}

#[cfg(test)]
mod tests {
    /// The bit patterns are fdlibm's published decimal coefficients.
    #[test]
    #[allow(clippy::excessive_precision)]
    fn coefficients_are_fdlibms() {
        assert_eq!(super::S1, -1.666_666_666_666_663_243_48e-01);
        assert_eq!(super::S2, 8.333_333_333_322_489_461_24e-03);
        assert_eq!(super::S3, -1.984_126_982_985_794_931_34e-04);
        assert_eq!(super::S4, 2.755_731_370_707_006_767_89e-06);
        assert_eq!(super::S5, -2.505_076_025_340_686_341_95e-08);
        assert_eq!(super::S6, 1.589_690_995_211_550_102_21e-10);
        assert_eq!(super::C1, 4.166_666_666_666_660_190_37e-02);
        assert_eq!(super::C2, -1.388_888_888_887_410_957_49e-03);
        assert_eq!(super::C3, 2.480_158_728_947_672_941_78e-05);
        assert_eq!(super::C4, -2.755_731_435_139_066_330_35e-07);
        assert_eq!(super::C5, 2.087_572_321_298_174_827_90e-09);
        assert_eq!(super::C6, -1.135_964_755_778_819_482_65e-11);
    }
}
