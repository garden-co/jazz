use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StdCppLib {
    Cxx,
    StdCxx,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinkPlan {
    pub lib_dir: PathBuf,
    pub libs: Vec<&'static str>,
    pub stdcpp: StdCppLib,
}

pub fn vendored_link_plan(
    crate_dir: &Path,
    target: &str,
    _feature_lz4: bool,
    _feature_zstd: bool,
) -> Option<LinkPlan> {
    let stdcpp = match target {
        "aarch64-apple-darwin" | "x86_64-apple-darwin" => StdCppLib::Cxx,
        "aarch64-unknown-linux-gnu" | "x86_64-unknown-linux-gnu" => StdCppLib::StdCxx,
        _ => return None,
    };

    Some(LinkPlan {
        lib_dir: crate_dir.join("prebuilt").join(target).join("lib"),
        libs: vec!["rocksdb"],
        stdcpp,
    })
}

#[cfg(test)]
mod tests {
    use super::{StdCppLib, vendored_link_plan};
    use std::path::Path;

    #[test]
    fn macos_arm64_links_expected_archives() {
        let plan = vendored_link_plan(Path::new("/tmp/librocksdb-sys"), "aarch64-apple-darwin", true, true)
            .expect("supported target should use vendored archives");

        assert_eq!(
            plan.lib_dir,
            Path::new("/tmp/librocksdb-sys")
                .join("prebuilt")
                .join("aarch64-apple-darwin")
                .join("lib")
        );
        assert_eq!(plan.libs, vec!["rocksdb"]);
        assert_eq!(plan.stdcpp, StdCppLib::Cxx);
    }

    #[test]
    fn linux_x64_uses_stdcxx() {
        let plan = vendored_link_plan(Path::new("/tmp/librocksdb-sys"), "x86_64-unknown-linux-gnu", false, true)
            .expect("supported target should use vendored archives");

        assert_eq!(plan.libs, vec!["rocksdb"]);
        assert_eq!(plan.stdcpp, StdCppLib::StdCxx);
    }

    #[test]
    fn unsupported_targets_do_not_use_vendored_archives() {
        assert_eq!(
            vendored_link_plan(
                Path::new("/tmp/librocksdb-sys"),
                "x86_64-pc-windows-msvc",
                true,
                true
            ),
            None
        );
    }
}
