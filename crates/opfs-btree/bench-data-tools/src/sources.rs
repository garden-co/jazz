mod objects;
mod wikipedia;

use anyhow::{Context, Result, bail};
use opfs_btree::bench_dataset::{ValueEncoding, encode_kv, encode_ops};
use std::fs;
use std::io::Read;
use std::path::Path;

use crate::normalize::encode_value;
use crate::ops::build_phases;

/// A benchmark dataset. Each maps to one committed `*.gz` source file under the
/// datasets directory and one adapter that parses it.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Profile {
    /// The Met Open Access museum objects (CC0) — medium structured records.
    Objects,
    /// Wikipedia article wikitext (CC BY-SA) — large text values.
    Wikipedia,
}

impl Profile {
    fn name(self) -> &'static str {
        match self {
            Profile::Objects => "objects",
            Profile::Wikipedia => "wikipedia",
        }
    }
    fn input_filename(self) -> &'static str {
        match self {
            Profile::Objects => "objects.csv.gz",
            Profile::Wikipedia => "wikipedia.jsonl.gz",
        }
    }
    fn default_count(self) -> usize {
        match self {
            Profile::Objects => 12_000,
            Profile::Wikipedia => 100,
        }
    }
    fn license(self) -> &'static str {
        match self {
            Profile::Objects => {
                "The Metropolitan Museum of Art Open Access — CC0 1.0 (public domain). \
                 https://github.com/metmuseum/openaccess"
            }
            Profile::Wikipedia => {
                "Wikipedia article text — CC BY-SA 4.0, © Wikipedia contributors, \
                 https://en.wikipedia.org/ (share-alike applies to this text)"
            }
        }
    }
}

pub fn selected_profiles(spec: &str) -> Result<Vec<Profile>> {
    let all = [Profile::Objects, Profile::Wikipedia];
    if spec.trim() == "all" {
        return Ok(all.to_vec());
    }
    let mut out = Vec::new();
    for token in spec.split(',') {
        out.push(match token.trim() {
            "objects" => Profile::Objects,
            "wikipedia" => Profile::Wikipedia,
            other => bail!("unknown profile: {other} (expected objects|wikipedia)"),
        });
    }
    Ok(out)
}

/// Read and gunzip a committed source dataset file.
fn read_input(profile: Profile, datasets: &Path) -> Result<String> {
    let path = datasets.join(profile.input_filename());
    let bytes = fs::read(&path)
        .with_context(|| format!("reading {} (committed dataset missing?)", path.display()))?;
    let mut text = String::new();
    flate2::read::GzDecoder::new(&bytes[..])
        .read_to_string(&mut text)
        .with_context(|| format!("gunzip {}", path.display()))?;
    Ok(text)
}

pub fn build_profile(
    profile: Profile,
    count: Option<usize>,
    datasets: &Path,
    out: &Path,
) -> Result<()> {
    let limit = count.unwrap_or_else(|| profile.default_count());
    let raw = read_input(profile, datasets)?;
    let records = match profile {
        Profile::Objects => objects::parse(&raw, limit)?,
        Profile::Wikipedia => wikipedia::parse(&raw, limit)?,
    };

    let kv: Vec<(Vec<u8>, Vec<u8>)> = records
        .iter()
        .map(|(k, rec)| Ok((k.clone(), encode_value(rec, ValueEncoding::Cbor)?)))
        .collect::<Result<_>>()?;
    let kv_bytes = encode_kv(profile.name(), profile.name(), ValueEncoding::Cbor, &kv);
    fs::write(out.join(format!("{}.kv", profile.name())), kv_bytes)?;

    let phases = build_phases(kv.len() as u32, (kv.len() as u32).min(50_000), 0xBEEF);
    fs::write(
        out.join(format!("{}.ops", profile.name())),
        encode_ops(&phases),
    )?;
    fs::write(
        out.join(format!("{}.license", profile.name())),
        profile.license(),
    )?;
    println!("built {} ({} records)", profile.name(), kv.len());
    Ok(())
}
