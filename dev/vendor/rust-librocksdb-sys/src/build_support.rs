use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StdCppLib {
    Cxx,
    StdCxx,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GhcrArtifact {
    pub repository: &'static str,
    pub reference: String,
    pub blob_filename: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinkPlan {
    pub lib_dir: PathBuf,
    pub libs: Vec<&'static str>,
    pub stdcpp: StdCppLib,
    pub rocksdb_version: &'static str,
    pub artifact: GhcrArtifact,
}

const ROCKSDB_VERSION: &str = "11.1.1";

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
    let stdcpp = match target {
        "aarch64-apple-darwin" | "x86_64-apple-darwin" => StdCppLib::Cxx,
        "aarch64-unknown-linux-gnu" | "x86_64-unknown-linux-gnu" => StdCppLib::StdCxx,
        _ => return None,
    };
    let reference = format!("rocksdb-{ROCKSDB_VERSION}-v1-{feature_profile}-{target}");

    Some(LinkPlan {
        lib_dir: cache_dir_for_artifact_reference(cache_root, target, &reference, feature_profile),
        libs: vec!["rocksdb"],
        stdcpp,
        rocksdb_version: ROCKSDB_VERSION,
        artifact: GhcrArtifact {
            repository: "ghcr.io/garden-co/jazz2-rocksdb-prebuilt",
            reference,
            blob_filename: "librocksdb.a.gz",
        },
    })
}

pub fn cache_dir_for_artifact_reference(
    cache_root: &Path,
    target: &str,
    reference: &str,
    feature_profile: &str,
) -> PathBuf {
    cache_root
        .join("rocksdb")
        .join(reference.replace([':', '/'], "-"))
        .join(feature_profile)
        .join(target)
        .join("lib")
}

#[cfg(test)]
mod tests {
    use super::{StdCppLib, cache_dir_for_artifact_reference, vendored_link_plan};
    use std::path::Path;

    #[test]
    fn macos_arm64_links_expected_archives() {
        let plan = vendored_link_plan(
            Path::new("/tmp/jazz-cache"),
            "aarch64-apple-darwin",
            true,
            true,
        )
        .expect("supported target should use vendored archives");

        assert_eq!(
            plan.lib_dir,
            Path::new("/tmp/jazz-cache")
                .join("rocksdb")
                .join("rocksdb-11.1.1-v1-all-compression-codecs-aarch64-apple-darwin")
                .join("all-compression-codecs")
                .join("aarch64-apple-darwin")
                .join("lib")
        );
        assert_eq!(plan.libs, vec!["rocksdb"]);
        assert_eq!(plan.stdcpp, StdCppLib::Cxx);
        assert_eq!(
            plan.artifact.repository,
            "ghcr.io/garden-co/jazz2-rocksdb-prebuilt"
        );
        assert_eq!(
            plan.artifact.reference,
            "rocksdb-11.1.1-v1-all-compression-codecs-aarch64-apple-darwin"
        );
    }

    #[test]
    fn linux_x64_uses_stdcxx() {
        let plan = vendored_link_plan(
            Path::new("/tmp/jazz-cache"),
            "x86_64-unknown-linux-gnu",
            true,
            true,
        )
        .expect("supported target should use vendored archives");

        assert_eq!(plan.libs, vec!["rocksdb"]);
        assert_eq!(plan.stdcpp, StdCppLib::StdCxx);
        assert_eq!(plan.rocksdb_version, "11.1.1");
        assert_eq!(
            plan.artifact.reference,
            "rocksdb-11.1.1-v1-all-compression-codecs-x86_64-unknown-linux-gnu"
        );
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
    fn cache_dir_uses_safe_artifact_reference() {
        assert_eq!(
            cache_dir_for_artifact_reference(
                Path::new("/tmp/jazz-cache"),
                "x86_64-unknown-linux-gnu",
                "rocksdb:11.1.1/test",
                "all-compression-codecs"
            ),
            Path::new("/tmp/jazz-cache")
                .join("rocksdb")
                .join("rocksdb-11.1.1-test")
                .join("all-compression-codecs")
                .join("x86_64-unknown-linux-gnu")
                .join("lib")
        );
    }
}
