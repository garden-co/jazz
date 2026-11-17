//! Read amplification of a geo radius search (SPEC 20.10.3): cells read by
//! the cover ÷ cells inside the disc, for city-scale radii and latitudes.
//!
//! A cover reads at most 16 ranges of the index's `cell` column; every cell
//! it reads beyond the disc is wasted work (`INV-IX-4`). The cells inside a
//! disc are estimated from its area (`2πR²(1 − cos δ)`) and the area of one
//! cell at the center's latitude. Run with `cargo run --release -p
//! jazz-index --example geo_amplification`. Arguments: `[centers per row]`.

use jazz_index::geo::{EARTH_RADIUS_M, cover_radius};

/// A deterministic generator, so runs compare (xorshift64*).
struct Rng(u64);

impl Rng {
    fn unit(&mut self) -> f64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        (self.0.wrapping_mul(0x2545_F491_4F6C_DD1D) >> 11) as f64 / (1u64 << 53) as f64
    }
    fn range(&mut self, lo: f64, hi: f64) -> f64 {
        lo + (hi - lo) * self.unit()
    }
}

fn main() {
    let centers: usize = std::env::args()
        .nth(1)
        .map(|a| a.parse().unwrap())
        .unwrap_or(300);
    let steps = (1u64 << 31) as f64;
    let (cell_dlng, cell_dlat) = (
        2.0 * std::f64::consts::PI / steps,
        std::f64::consts::PI / steps,
    );
    println!(
        "{:>8} {:>5} | {:>6} {:>6} {:>6}",
        "radius", "lat", "mean", "p90", "max"
    );
    for meters in [100.0, 500.0, 1000.0, 2000.0, 5000.0, 10_000.0, 50_000.0] {
        for lat in [0.0, 37.8, 51.5, 60.0, 70.0] {
            let mut rng = Rng(7);
            let mut ratios: Vec<f64> = (0..centers)
                .map(|_| {
                    let (clat, clng) = (lat + rng.range(-0.5, 0.5), rng.range(-170.0, 170.0));
                    let read: f64 = cover_radius(clat, clng, meters)
                        .unwrap()
                        .iter()
                        .map(|&(lo, hi)| (hi - lo + 1) as f64)
                        .sum();
                    let delta = meters / EARTH_RADIUS_M;
                    let disc = 2.0 * std::f64::consts::PI * (1.0 - delta.cos())
                        / (cell_dlng * cell_dlat * clat.to_radians().cos());
                    read / disc
                })
                .collect();
            ratios.sort_by(f64::total_cmp);
            let mean = ratios.iter().sum::<f64>() / ratios.len() as f64;
            let p90 = ratios[(ratios.len() - 1) * 9 / 10];
            println!(
                "{:>7}m {:>5} | {:>6.2} {:>6.2} {:>6.2}",
                meters,
                lat,
                mean,
                p90,
                ratios[ratios.len() - 1]
            );
        }
    }
}
