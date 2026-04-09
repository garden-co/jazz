use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StdCppLib {
    Cxx,
    StdCxx,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GhcrArtifact {
    pub repository: &'static str,
    pub manifest_digest: &'static str,
    pub archive_sha256: &'static str,
    pub blob_filename: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinkPlan {
    pub lib_dir: PathBuf,
    pub libs: Vec<&'static str>,
    pub stdcpp: StdCppLib,
    pub artifact: GhcrArtifact,
}

pub fn vendored_link_plan(
    cache_root: &Path,
    target: &str,
    _feature_lz4: bool,
    _feature_zstd: bool,
) -> Option<LinkPlan> {
    let (stdcpp, manifest_digest, archive_sha256) = match target {
        "aarch64-apple-darwin" => (
            StdCppLib::Cxx,
            "sha256:39977dc23d8b693d839c43652b1b45972e698bf3bf82ee7bd60ebb153d462463",
            "6091551009fe4d5bfd38c67ee379d981e7e3e32ac20f7f221bb8688e3948d858",
        ),
        "x86_64-apple-darwin" => (
            StdCppLib::Cxx,
            "sha256:c5b85c9c8286e45075ffe7bfe5ae09c6241b5b36fe60a81bdeded3ad2bea4259",
            "85ebe0302b55ed407685f0f67986dda83ebd6335028779a98cde710eb62bed67",
        ),
        "aarch64-unknown-linux-gnu" => (
            StdCppLib::StdCxx,
            "sha256:6dedfe2b2770e437085c4f9e5ea6cc663eedb07700b9a503175d46c0dc9966d3",
            "0ad965bfadc9e19ef04ac7d4f61040186bea3bdb1ca4787db530e79f8548b433",
        ),
        "x86_64-unknown-linux-gnu" => (
            StdCppLib::StdCxx,
            "sha256:1980c15f9a6c90bbb095749c3006050b30f7b1fe0e2f985a7130658edb52c3fc",
            "0cfd274a299a63b1d83467279d96e1fcc0662a81360d126ec48c7cc4be03891d",
        ),
        _ => return None,
    };

    Some(LinkPlan {
        lib_dir: cache_dir_for_manifest_digest(cache_root, target, manifest_digest),
        libs: vec!["rocksdb"],
        stdcpp,
        artifact: GhcrArtifact {
            repository: "ghcr.io/garden-co/jazz2-rocksdb-prebuilt",
            manifest_digest,
            archive_sha256,
            blob_filename: "librocksdb.a.gz",
        },
    })
}

pub fn cache_dir_for_manifest_digest(
    cache_root: &Path,
    target: &str,
    manifest_digest: &str,
) -> PathBuf {
    cache_root
        .join("rocksdb")
        .join(manifest_digest.replace(':', "-"))
        .join(target)
        .join("lib")
}

#[cfg(test)]
mod tests {
    use super::{StdCppLib, cache_dir_for_manifest_digest, vendored_link_plan};
    use std::path::Path;

    #[test]
    fn macos_arm64_links_expected_archives() {
        let plan = vendored_link_plan(Path::new("/tmp/jazz-cache"), "aarch64-apple-darwin", true, true)
            .expect("supported target should use vendored archives");

        assert_eq!(
            plan.lib_dir,
            Path::new("/tmp/jazz-cache")
                .join("rocksdb")
                .join("sha256-39977dc23d8b693d839c43652b1b45972e698bf3bf82ee7bd60ebb153d462463")
                .join("aarch64-apple-darwin")
                .join("lib")
        );
        assert_eq!(plan.libs, vec!["rocksdb"]);
        assert_eq!(plan.stdcpp, StdCppLib::Cxx);
        assert_eq!(plan.artifact.repository, "ghcr.io/garden-co/jazz2-rocksdb-prebuilt");
        assert_eq!(
            plan.artifact.manifest_digest,
            "sha256:39977dc23d8b693d839c43652b1b45972e698bf3bf82ee7bd60ebb153d462463"
        );
    }

    #[test]
    fn linux_x64_uses_stdcxx() {
        let plan = vendored_link_plan(Path::new("/tmp/jazz-cache"), "x86_64-unknown-linux-gnu", false, true)
            .expect("supported target should use vendored archives");

        assert_eq!(plan.libs, vec!["rocksdb"]);
        assert_eq!(plan.stdcpp, StdCppLib::StdCxx);
        assert_eq!(plan.artifact.archive_sha256.len(), 64);
    }

    #[test]
    fn unsupported_targets_do_not_use_vendored_archives() {
        assert_eq!(
            vendored_link_plan(
                Path::new("/tmp/jazz-cache"),
                "x86_64-pc-windows-msvc",
                true,
                true
            ),
            None
        );
    }

    #[test]
    fn cache_dir_uses_digest_without_colons() {
        assert_eq!(
            cache_dir_for_manifest_digest(
                Path::new("/tmp/jazz-cache"),
                "x86_64-unknown-linux-gnu",
                "sha256:abcd1234"
            ),
            Path::new("/tmp/jazz-cache")
                .join("rocksdb")
                .join("sha256-abcd1234")
                .join("x86_64-unknown-linux-gnu")
                .join("lib")
        );
    }
}
