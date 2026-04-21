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

fn compression_feature_profile(feature_lz4: bool, feature_zstd: bool) -> Option<&'static str> {
    match (feature_lz4, feature_zstd) {
        (true, true) => Some("all-compression-codecs"),
        _ => None,
    }
}

pub fn vendored_link_plan(
    cache_root: &Path,
    target: &str,
    feature_lz4: bool,
    feature_zstd: bool,
) -> Option<LinkPlan> {
    let feature_profile = compression_feature_profile(feature_lz4, feature_zstd)?;
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
            "sha256:7e00966e869532780fbbc2cfb5d881e89b3beca8573941864d37bc351aaf231a",
            "75aa0bec87eecd7c4a803ccf2ed337ae08e575ed79d74c9e1164bfcf7db31119",
        ),
        "x86_64-unknown-linux-gnu" => (
            StdCppLib::StdCxx,
            "sha256:66af20476b451d3a6745b22181d1cd29412371771826622308866578a2521a17",
            "ebd96a6946ce24f46714d22be84622f1a2f5b975976eb0a9376bd3f739df8fd9",
        ),
        _ => return None,
    };

    Some(LinkPlan {
        lib_dir: cache_dir_for_manifest_digest(cache_root, target, manifest_digest, feature_profile),
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
    feature_profile: &str,
) -> PathBuf {
    cache_root
        .join("rocksdb")
        .join(manifest_digest.replace(':', "-"))
        .join(feature_profile)
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
        let plan = vendored_link_plan(Path::new("/tmp/jazz-cache"), "x86_64-unknown-linux-gnu", true, true)
            .expect("supported target should use vendored archives");

        assert_eq!(plan.libs, vec!["rocksdb"]);
        assert_eq!(plan.stdcpp, StdCppLib::StdCxx);
        assert_eq!(plan.artifact.archive_sha256.len(), 64);
    }

    #[test]
    fn unsupported_feature_sets_do_not_reuse_vendored_archives() {
        assert_eq!(
            vendored_link_plan(
                Path::new("/tmp/jazz-cache"),
                "x86_64-unknown-linux-gnu",
                true,
                false
            ),
            None
        );
        assert_eq!(
            vendored_link_plan(
                Path::new("/tmp/jazz-cache"),
                "x86_64-unknown-linux-gnu",
                false,
                true
            ),
            None
        );
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
                "sha256:abcd1234",
                "all-compression-codecs"
            ),
            Path::new("/tmp/jazz-cache")
                .join("rocksdb")
                .join("sha256-abcd1234")
                .join("all-compression-codecs")
                .join("x86_64-unknown-linux-gnu")
                .join("lib")
        );
    }
}
