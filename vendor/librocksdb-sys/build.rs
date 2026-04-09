#[path = "src/build_support.rs"]
mod build_support;

use std::path::{Path, PathBuf};
use std::process::Command;
use std::{env, fs};
use std::io::{Read, Write};

use build_support::{LinkPlan, StdCppLib, vendored_link_plan};
use flate2::read::GzDecoder;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use sha2::{Digest, Sha256};

// On these platforms jemalloc-sys will use a prefixed jemalloc which cannot be linked together
// with RocksDB.
// See https://github.com/tikv/jemallocator/blob/tikv-jemalloc-sys-0.5.3/jemalloc-sys/src/env.rs#L25
const NO_JEMALLOC_TARGETS: &[&str] = &["android", "dragonfly", "musl", "darwin"];

fn link(name: &str, bundled: bool) {
    use std::env::var;
    let target = var("TARGET").unwrap();
    let target: Vec<_> = target.split('-').collect();
    if target.get(2) == Some(&"windows") {
        println!("cargo:rustc-link-lib=dylib={name}");
        if bundled && target.get(3) == Some(&"gnu") {
            let dir = var("CARGO_MANIFEST_DIR").unwrap();
            println!("cargo:rustc-link-search=native={}/{}", dir, target[0]);
        }
    }
}

fn fail_on_empty_directory(path: &Path) {
    if fs::read_dir(path).unwrap().count() == 0 {
        println!("The `{}` directory is empty.", path.display());
        println!(
            "Make sure the upstream rust-rocksdb checkout is available or set JAZZ_ROCKSDB_UPSTREAM_DIR."
        );
        panic!();
    }
}

#[derive(Debug, Deserialize)]
struct GhcrTokenResponse {
    token: String,
}

#[derive(Debug, Deserialize)]
struct GhcrManifest {
    #[serde(default)]
    layers: Vec<GhcrBlobDescriptor>,
    #[serde(default)]
    blobs: Vec<GhcrBlobDescriptor>,
}

#[derive(Debug, Deserialize)]
struct GhcrBlobDescriptor {
    digest: String,
}

fn link_vendored_librocksdb(target: &str) -> bool {
    println!("cargo:rerun-if-env-changed=JAZZ_ROCKSDB_CACHE_DIR");
    println!("cargo:rerun-if-env-changed=JAZZ_ROCKSDB_OFFLINE");
    println!("cargo:rerun-if-env-changed=CARGO_NET_OFFLINE");
    println!("cargo:rerun-if-env-changed=JAZZ_ROCKSDB_GHCR_USERNAME");
    println!("cargo:rerun-if-env-changed=JAZZ_ROCKSDB_GHCR_PASSWORD");
    println!("cargo:rerun-if-env-changed=GHCR_USERNAME");
    println!("cargo:rerun-if-env-changed=GHCR_PASSWORD");
    println!("cargo:rerun-if-env-changed=CR_PAT");
    println!("cargo:rerun-if-env-changed=GITHUB_ACTOR");
    println!("cargo:rerun-if-env-changed=GITHUB_TOKEN");

    let Some(cache_root) = rocksdb_cache_root() else {
        return false;
    };
    let Some(plan) = vendored_link_plan(
        &cache_root,
        target,
        cfg!(feature = "lz4"),
        cfg!(feature = "zstd"),
    ) else {
        return false;
    };

    if let Err(error) = ensure_vendored_librocksdb(&plan) {
        println!("cargo:warning={error}");
        return false;
    }

    println!("cargo:rustc-link-search=native={}", plan.lib_dir.display());
    for lib in plan.libs {
        println!("cargo:rustc-link-lib=static={lib}");
    }

    match plan.stdcpp {
        StdCppLib::Cxx => println!("cargo:rustc-link-lib=dylib=c++"),
        StdCppLib::StdCxx => println!("cargo:rustc-link-lib=dylib=stdc++"),
    }

    true
}

fn rocksdb_cache_root() -> Option<PathBuf> {
    if let Some(path) = env::var_os("JAZZ_ROCKSDB_CACHE_DIR") {
        return Some(PathBuf::from(path));
    }

    env::var_os("CARGO_HOME")
        .map(PathBuf::from)
        .or_else(|| env::var_os("HOME").map(|home| PathBuf::from(home).join(".cargo")))
        .map(|cargo_home| cargo_home.join("jazz-cache"))
}

fn ensure_vendored_librocksdb(plan: &LinkPlan) -> Result<(), String> {
    let archive_path = plan.lib_dir.join("librocksdb.a");
    if archive_path.exists() {
        return Ok(());
    }

    if rocksdb_fetch_disabled() {
        return Err(format!(
            "prebuilt RocksDB archive missing at {} and network fetch is disabled",
            archive_path.display()
        ));
    }

    fetch_vendored_librocksdb(plan, &archive_path)
}

fn rocksdb_fetch_disabled() -> bool {
    matches!(
        env::var("JAZZ_ROCKSDB_OFFLINE"),
        Ok(value) if is_truthy(&value)
    ) || matches!(
        env::var("CARGO_NET_OFFLINE"),
        Ok(value) if is_truthy(&value)
    )
}

fn is_truthy(value: &str) -> bool {
    matches!(value, "1" | "true" | "TRUE" | "yes" | "YES" | "on" | "ON")
}

fn fetch_vendored_librocksdb(plan: &LinkPlan, archive_path: &Path) -> Result<(), String> {
    println!(
        "cargo:warning=fetching prebuilt RocksDB archive for {} from {}@{}",
        env::var("TARGET").unwrap_or_else(|_| "unknown-target".to_owned()),
        plan.artifact.repository,
        plan.artifact.manifest_digest
    );

    fs::create_dir_all(&plan.lib_dir).map_err(|error| {
        format!(
            "failed to create RocksDB cache directory {}: {error}",
            plan.lib_dir.display()
        )
    })?;

    let compressed_path = plan.lib_dir.join(plan.artifact.blob_filename);
    let compressed_tmp_path = plan
        .lib_dir
        .join(format!("{}.download", plan.artifact.blob_filename));
    let archive_tmp_path = plan.lib_dir.join("librocksdb.a.download");

    let token = fetch_ghcr_token(plan)?;
    let blob = fetch_ghcr_blob_descriptor(plan, &token)?;
    let blob_url = format!(
        "https://ghcr.io/v2/{}/blobs/{}",
        plan.artifact.repository.trim_start_matches("ghcr.io/"),
        blob.digest
    );

    curl_download(&blob_url, &compressed_tmp_path, Some(&token), None)?;
    verify_sha256(&compressed_tmp_path, &blob.digest)?;
    unpack_vendored_archive(&compressed_tmp_path, &archive_tmp_path, plan.artifact.archive_sha256)?;

    fs::rename(&archive_tmp_path, archive_path).or_else(|error| {
        if archive_path.exists() {
            fs::remove_file(&archive_tmp_path).ok();
            Ok(())
        } else {
            Err(error)
        }
    }).map_err(|error| {
        format!(
            "failed to stage RocksDB archive {}: {error}",
            archive_path.display()
        )
    })?;

    fs::rename(&compressed_tmp_path, &compressed_path).or_else(|error| {
        if compressed_path.exists() {
            fs::remove_file(&compressed_tmp_path).ok();
            Ok(())
        } else {
            Err(error)
        }
    }).map_err(|error| {
        format!(
            "failed to stage compressed RocksDB archive {}: {error}",
            compressed_path.display()
        )
    })?;

    Ok(())
}

fn fetch_ghcr_token(plan: &LinkPlan) -> Result<String, String> {
    let token_url = format!(
        "https://ghcr.io/token?service=ghcr.io&scope=repository:{}:pull",
        plan.artifact.repository.trim_start_matches("ghcr.io/")
    );
    let basic_auth = ghcr_basic_auth().ok_or_else(|| {
        "missing GHCR credentials; set JAZZ_ROCKSDB_GHCR_USERNAME/JAZZ_ROCKSDB_GHCR_PASSWORD (or GHCR_USERNAME/CR_PAT) to use prebuilt RocksDB archives".to_owned()
    })?;
    let response: GhcrTokenResponse = curl_json(&token_url, None, None, Some(&basic_auth))?;
    Ok(response.token)
}

fn ghcr_basic_auth() -> Option<(String, String)> {
    let username = env::var("JAZZ_ROCKSDB_GHCR_USERNAME")
        .ok()
        .or_else(|| env::var("GHCR_USERNAME").ok())
        .or_else(|| env::var("GITHUB_ACTOR").ok())?;
    let password = env::var("JAZZ_ROCKSDB_GHCR_PASSWORD")
        .ok()
        .or_else(|| env::var("GHCR_PASSWORD").ok())
        .or_else(|| env::var("CR_PAT").ok())
        .or_else(|| env::var("GITHUB_TOKEN").ok())?;
    Some((username, password))
}

fn fetch_ghcr_blob_descriptor(
    plan: &LinkPlan,
    token: &str,
) -> Result<GhcrBlobDescriptor, String> {
    let manifest_url = format!(
        "https://ghcr.io/v2/{}/manifests/{}",
        plan.artifact.repository.trim_start_matches("ghcr.io/"),
        plan.artifact.manifest_digest
    );
    let manifest: GhcrManifest = curl_json(
        &manifest_url,
        Some(token),
        Some(
            "application/vnd.oci.artifact.manifest.v1+json, application/vnd.oci.image.manifest.v1+json",
        ),
        None,
    )?;

    manifest
        .blobs
        .into_iter()
        .chain(manifest.layers)
        .next()
        .ok_or_else(|| {
            format!(
                "GHCR manifest {} did not include any blobs or layers",
                plan.artifact.manifest_digest
            )
        })
}

fn curl_json<T: DeserializeOwned>(
    url: &str,
    bearer_token: Option<&str>,
    accept: Option<&str>,
    basic_auth: Option<&(String, String)>,
) -> Result<T, String> {
    let output = curl_command(url, bearer_token, accept, basic_auth)
        .output()
        .map_err(|error| format!("failed to run curl for {url}: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "curl failed for {url} with status {}",
            output.status
        ));
    }
    serde_json::from_slice(&output.stdout)
        .map_err(|error| format!("failed to parse JSON from {url}: {error}"))
}

fn curl_download(
    url: &str,
    destination: &Path,
    bearer_token: Option<&str>,
    accept: Option<&str>,
) -> Result<(), String> {
    let status = curl_command(url, bearer_token, accept, None)
        .arg("--output")
        .arg(destination)
        .status()
        .map_err(|error| format!("failed to run curl for {url}: {error}"))?;
    if !status.success() {
        return Err(format!(
            "curl download failed for {url} with status {status}"
        ));
    }
    Ok(())
}

fn curl_command(
    url: &str,
    bearer_token: Option<&str>,
    accept: Option<&str>,
    basic_auth: Option<&(String, String)>,
) -> Command {
    let mut command = Command::new("curl");
    command.args(["--fail", "--silent", "--show-error", "--location"]);
    if let Some((username, password)) = basic_auth {
        command.arg("--user").arg(format!("{username}:{password}"));
    }
    if let Some(token) = bearer_token {
        command.arg("--header").arg(format!("Authorization: Bearer {token}"));
    }
    if let Some(accept) = accept {
        command.arg("--header").arg(format!("Accept: {accept}"));
    }
    command.arg(url);
    command
}

fn verify_sha256(path: &Path, expected: &str) -> Result<(), String> {
    let Some(expected_hex) = expected.strip_prefix("sha256:") else {
        return Err(format!("unsupported digest format: {expected}"));
    };

    let actual = sha256_hex(path)?;
    if actual != expected_hex {
        return Err(format!(
            "sha256 mismatch for {}: expected {}, got {}",
            path.display(),
            expected_hex,
            actual
        ));
    }
    Ok(())
}

fn sha256_hex(path: &Path) -> Result<String, String> {
    let mut file = fs::File::open(path)
        .map_err(|error| format!("failed to open {}: {error}", path.display()))?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];

    loop {
        let bytes_read = file
            .read(&mut buffer)
            .map_err(|error| format!("failed to read {}: {error}", path.display()))?;
        if bytes_read == 0 {
            break;
        }
        hasher.update(&buffer[..bytes_read]);
    }

    Ok(format!("{:x}", hasher.finalize()))
}

fn unpack_vendored_archive(
    compressed_path: &Path,
    archive_path: &Path,
    expected_archive_sha256: &str,
) -> Result<(), String> {
    let mut source = GzDecoder::new(fs::File::open(compressed_path).map_err(|error| {
        format!(
            "failed to open compressed vendored archive {}: {error}",
            compressed_path.display()
        )
    })?);
    let mut destination = fs::File::create(archive_path).map_err(|error| {
        format!(
            "failed to create unpacked vendored archive {}: {error}",
            archive_path.display()
        )
    })?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];

    loop {
        let bytes_read = source.read(&mut buffer).map_err(|error| {
            format!(
                "failed to read compressed vendored archive {}: {error}",
                compressed_path.display()
            )
        })?;
        if bytes_read == 0 {
            break;
        }
        destination.write_all(&buffer[..bytes_read]).map_err(|error| {
            format!(
                "failed to write unpacked vendored archive {}: {error}",
                archive_path.display()
            )
        })?;
        hasher.update(&buffer[..bytes_read]);
    }

    let actual_sha256 = format!("{:x}", hasher.finalize());
    if actual_sha256 != expected_archive_sha256 {
        fs::remove_file(archive_path).ok();
        return Err(format!(
            "sha256 mismatch for unpacked archive {}: expected {}, got {}",
            archive_path.display(),
            expected_archive_sha256,
            actual_sha256
        ));
    }

    Ok(())
}

fn upstream_checkout_root() -> PathBuf {
    if let Some(path) = env::var_os("JAZZ_ROCKSDB_UPSTREAM_DIR") {
        let path = PathBuf::from(path);
        if path.join("Cargo.toml").exists() && path.join("rocksdb").exists() {
            return path;
        }
        let nested = path.join("librocksdb-sys");
        if nested.join("Cargo.toml").exists() && nested.join("rocksdb").exists() {
            return nested;
        }
        panic!(
            "JAZZ_ROCKSDB_UPSTREAM_DIR must point at librocksdb-sys or rust-rocksdb checkout, got {}",
            path.display()
        );
    }

    let cargo_home = env::var_os("CARGO_HOME")
        .map(PathBuf::from)
        .or_else(|| env::var_os("HOME").map(|home| PathBuf::from(home).join(".cargo")))
        .expect("CARGO_HOME or HOME must be set to locate the upstream rust-rocksdb checkout");
    let checkouts_dir = cargo_home.join("git").join("checkouts");

    let mut candidates = Vec::new();
    if let Ok(entries) = fs::read_dir(&checkouts_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            if !name.starts_with("rust-rocksdb-") {
                continue;
            }
            let Ok(revisions) = fs::read_dir(&path) else {
                continue;
            };
            for revision in revisions.flatten() {
                let candidate = revision.path().join("librocksdb-sys");
                if candidate.join("Cargo.toml").exists() && candidate.join("rocksdb").exists() {
                    candidates.push(candidate);
                }
            }
        }
    }

    candidates.sort();
    candidates.pop().unwrap_or_else(|| {
        panic!(
            "failed to locate the upstream rust-rocksdb checkout under {}; run `cargo fetch` or set JAZZ_ROCKSDB_UPSTREAM_DIR",
            checkouts_dir.display()
        )
    })
}

/// Splits `CARGO_ENCODED_RUSTFLAGS` into a Vec.
fn split_encoded_rustflags() -> Vec<String> {
    let flags = std::env::var("CARGO_ENCODED_RUSTFLAGS").unwrap_or_default();

    // extra flags that Cargo invokes rustc with, separated by a 0x1f character
    // https://doc.rust-lang.org/cargo/reference/environment-variables.html#environment-variables-cargo-sets-for-crates
    flags.split("\x1f").map(|flag| flag.to_string()).collect()
}

/// Returns the argument to `-Ctarget-cpu=` if it exists.
fn get_target_cpu_flag() -> Option<String> {
    const TARGET_CPU_FLAG: &str = "-Ctarget-cpu=";
    let flags = split_encoded_rustflags();
    let complete_flag = flags.iter().find(|flag| flag.starts_with(TARGET_CPU_FLAG));
    complete_flag.map(|flag| flag[TARGET_CPU_FLAG.len()..].to_string())
}

/// If the Rust `-Ctarget-cpu=` option is set, this attempts to pass it through to the C/C++
/// compiler. It should print a Cargo build warning if the compiler does not support the flag,
/// or if the architecture is not supported.
fn pass_through_target_cpu(cfg: &mut cc::Build) {
    let Some(target_cpu_flag) = get_target_cpu_flag() else {
        return;
    };

    let arch = std::env::var("CARGO_CFG_TARGET_ARCH").unwrap_or_default();
    match arch.as_str() {
        "x86_64" => {
            cfg.flag_if_supported(format!("-march={target_cpu_flag}"));
        }
        "aarch64" => {
            cfg.flag_if_supported(format!("-mcpu={target_cpu_flag}"));
        }
        // TODO: add more architectures/compilers
        _ => {
            println!(
                "cargo::warning=unknown target architecture: {arch}; C/C++ target flags not passed through"
            );
        }
    }
}

fn build_rocksdb(source_root: &Path) {
    // https://doc.rust-lang.org/cargo/reference/environment-variables.html
    let target = env::var("TARGET").unwrap();
    // https://doc.rust-lang.org/reference/conditional-compilation.html#target_arch
    let target_arch = env::var("CARGO_CFG_TARGET_ARCH").unwrap();
    let target_features_env = env::var("CARGO_CFG_TARGET_FEATURE").unwrap_or_default();
    let target_features: Vec<_> = target_features_env.split(',').collect();

    let mut config = cc::Build::new();
    config.include(source_root.join("rocksdb/include"));
    config.include(source_root.join("rocksdb"));
    config.include(source_root.join("rocksdb/third-party/gtest-1.8.1/fused-src"));

    if cfg!(feature = "snappy") {
        config.define("SNAPPY", Some("1"));
        config.include(source_root.join("snappy"));
    }

    if cfg!(feature = "lz4") {
        config.define("LZ4", Some("1"));
        if let Some(path) = env::var_os("DEP_LZ4_INCLUDE") {
            config.include(path);
        }
    }

    if cfg!(feature = "zstd") {
        config.define("ZSTD", Some("1"));
        if let Some(path) = env::var_os("DEP_ZSTD_INCLUDE") {
            config.include(path);
        }
    }

    if cfg!(feature = "zlib") {
        config.define("ZLIB", Some("1"));
        if let Some(path) = env::var_os("DEP_Z_INCLUDE") {
            config.include(path);
        }
    }

    if cfg!(feature = "bzip2") {
        config.define("BZIP2", Some("1"));
        if let Some(path) = env::var_os("DEP_BZIP2_INCLUDE") {
            config.include(path);
        }
    }

    if cfg!(feature = "rtti") {
        config.define("USE_RTTI", Some("1"));
    }

    // https://github.com/facebook/rocksdb/blob/be7703b27d9b3ac458641aaadf27042d86f6869c/Makefile#L195
    if cfg!(feature = "lto") {
        config.flag("-flto");
        if !config.get_compiler().is_like_clang() {
            panic!(
                "LTO is only supported with clang. Either disable the `lto` feature \
                or set `CC=/usr/bin/clang CXX=/usr/bin/clang++` environment variables."
            );
        }
    }

    config.include(source_root);
    config.define("NDEBUG", Some("1"));

    // true for C++ >= 17; we set -std=c++20 below
    config.define("HAVE_ALIGNED_NEW", None);

    // __uint128_t is supported by GCC and Clang; Don't use it for MSVC
    // TODO: implement a detection script?
    if !target.contains("msvc") {
        config.define("HAVE_UINT128_EXTENSION", None);
    }

    let mut lib_sources = fs::read_to_string(source_root.join("rocksdb_lib_sources.txt"))
        .expect("unable to read rocksdb_lib_sources.txt")
        .trim()
        .split('\n')
        .map(str::trim)
        // We have a pre-generated a version of build_version.cc in the local directory
        .filter(|file| !matches!(*file, "util/build_version.cc"))
        .map(ToOwned::to_owned)
        .collect::<Vec<String>>();

    // attempt to pass through the RUSTFLAGS -Ctarget-cpu to allow the same optimizations for C/C++
    pass_through_target_cpu(&mut config);

    // CPU-specific build configuration
    if target_arch == "x86_64" {
        // This is needed to enable hardware CRC32C. Technically, SSE 4.2 is
        // only available since Intel Nehalem (about 2010) and AMD Bulldozer
        // (about 2011).
        if target_features.contains(&"sse2") {
            config.flag_if_supported("-msse2");
        }
        if target_features.contains(&"sse4.1") {
            config.flag_if_supported("-msse4.1");
        }
        if target_features.contains(&"sse4.2") {
            config.flag_if_supported("-msse4.2");
        } else {
            println!(
                r#"cargo::warning=compiling without SSE4.2: CRC will be slow (set RUSTFLAGS="-Ctarget-cpu=..." to optimize RocksDB e.g. -Ctarget-cpu=broadwell)"#
            );
        }
        // Pass along additional target features as defined in
        // build_tools/build_detect_platform.
        if target_features.contains(&"avx2") {
            config.flag_if_supported("-mavx2");
        }
        if target_features.contains(&"bmi1") {
            config.flag_if_supported("-mbmi");
        }
        if target_features.contains(&"lzcnt") {
            config.flag_if_supported("-mlzcnt");
        }

        if !target.contains("android") && target_features.contains(&"pclmulqdq") {
            config.flag_if_supported("-mpclmul");
        }

        if target_features.contains(&"avx") && !target_features.contains(&"pclmulqdq") {
            // RocksDB BUG (<= 10.11.0/2026-01-23): assumes AVX implies -mpclmul
            // x86-64-v3/-v4 does not include PCLMUL
            println!(
                r#"cargo:warning=RocksDB BUG: target arch missing -mpclmul; compile may fail: pass named architecture e.g. -Ctarget-cpu=broadwell"#
            );
        }
    } else if target_arch == "aarch64" {
        if target_features.contains(&"crc") && target_features.contains(&"aes") {
            // the target supports the instructions RocksDB needs: if we don't have a target-cpu,
            // use -march=armv8-a+crc+aes+crypto, like the RocksDB Makefile.
            // If we DO have a target-cpu, assume pass_through_target_cpu() has set it above
            if get_target_cpu_flag().is_none() {
                // TODO: Should just be +crc+aes but RocksDB checks for __ARM_FEATURE_CRYPTO
                // https://github.com/facebook/rocksdb/pull/14217
                config.flag_if_supported("-march=armv8-a+crc+aes+crypto");
            }
        } else {
            println!(
                r#"cargo:warning=building for aarch64 WITHOUT CRC instruction: build with RUSTFLAGS="-Ctarget-cpu=..." to optimize RocksDB e.g. -Ctarget-cpu=neoverse-n1"#
            );
        }
    }

    if target.contains("apple-ios") {
        config.define("OS_MACOSX", None);

        config.define("IOS_CROSS_COMPILE", None);
        config.define("PLATFORM", "IOS");
        config.define("NIOSTATS_CONTEXT", None);
        config.define("NPERF_CONTEXT", None);
        config.define("ROCKSDB_PLATFORM_POSIX", None);
        config.define("ROCKSDB_LIB_IO_POSIX", None);

        env::set_var("IPHONEOS_DEPLOYMENT_TARGET", "12.0");
    } else if target.contains("darwin") {
        config.define("OS_MACOSX", None);
        config.define("ROCKSDB_PLATFORM_POSIX", None);
        config.define("ROCKSDB_LIB_IO_POSIX", None);
    } else if target.contains("android") {
        config.define("OS_ANDROID", None);
        config.define("ROCKSDB_PLATFORM_POSIX", None);
        config.define("ROCKSDB_LIB_IO_POSIX", None);

        if &target == "armv7-linux-androideabi" {
            config.define("_FILE_OFFSET_BITS", Some("32"));
        }
    } else if target.contains("aix") {
        config.define("OS_AIX", None);
        config.define("ROCKSDB_PLATFORM_POSIX", None);
        config.define("ROCKSDB_LIB_IO_POSIX", None);
    } else if target.contains("linux") {
        config.define("OS_LINUX", None);
        config.define("ROCKSDB_PLATFORM_POSIX", None);
        config.define("ROCKSDB_LIB_IO_POSIX", None);
        config.define("ROCKSDB_SCHED_GETCPU_PRESENT", None);
        config.define("ROCKSDB_AUXV_GETAUXVAL_PRESENT", None);
        config.define("ROCKSDB_FALLOCATE_PRESENT", None);
        config.define("ROCKSDB_RANGESYNC_PRESENT", None);
    } else if target.contains("dragonfly") {
        config.define("OS_DRAGONFLYBSD", None);
        config.define("ROCKSDB_PLATFORM_POSIX", None);
        config.define("ROCKSDB_LIB_IO_POSIX", None);
    } else if target.contains("freebsd") {
        config.define("OS_FREEBSD", None);
        config.define("ROCKSDB_PLATFORM_POSIX", None);
        config.define("ROCKSDB_LIB_IO_POSIX", None);
    } else if target.contains("netbsd") {
        config.define("OS_NETBSD", None);
        config.define("ROCKSDB_PLATFORM_POSIX", None);
        config.define("ROCKSDB_LIB_IO_POSIX", None);
    } else if target.contains("openbsd") {
        config.define("OS_OPENBSD", None);
        config.define("ROCKSDB_PLATFORM_POSIX", None);
        config.define("ROCKSDB_LIB_IO_POSIX", None);
    } else if target.contains("windows") {
        link("rpcrt4", false);
        link("shlwapi", false);
        config.define("DWIN32", None);
        config.define("OS_WIN", None);
        config.define("_MBCS", None);
        config.define("WIN64", None);
        config.define("NOMINMAX", None);
        config.define("ROCKSDB_WINDOWS_UTF8_FILENAMES", None);

        if &target == "x86_64-pc-windows-gnu" {
            // Tell MinGW to create localtime_r wrapper of localtime_s function.
            config.define("_POSIX_C_SOURCE", Some("1"));
            // Tell MinGW to use at least Windows Vista headers instead of the ones of Windows XP.
            // (This is minimum supported version of rocksdb)
            config.define("_WIN32_WINNT", Some("_WIN32_WINNT_VISTA"));
        }

        // Remove POSIX-specific sources
        lib_sources = lib_sources
            .iter()
            .cloned()
            .filter(|file| {
                !matches!(
                    file.as_str(),
                    "port/port_posix.cc"
                        | "env/env_posix.cc"
                        | "env/fs_posix.cc"
                        | "env/io_posix.cc"
                )
            })
            .collect::<Vec<String>>();

        // Add Windows-specific sources
        lib_sources.extend(
            [
                "port/win/env_default.cc",
                "port/win/env_win.cc",
                "port/win/io_win.cc",
                "port/win/port_win.cc",
                "port/win/win_logger.cc",
                "port/win/win_thread.cc",
            ]
            .into_iter()
            .map(ToOwned::to_owned),
        );

        if cfg!(feature = "jemalloc") {
            lib_sources.push("port/win/win_jemalloc.cc".to_string());
        }
    }

    if cfg!(feature = "jemalloc") && NO_JEMALLOC_TARGETS.iter().all(|i| !target.contains(i)) {
        config.define("ROCKSDB_JEMALLOC", Some("1"));
        config.define("JEMALLOC_NO_DEMANGLE", Some("1"));
        if let Some(jemalloc_root) = env::var_os("DEP_JEMALLOC_ROOT") {
            config.include(Path::new(&jemalloc_root).join("include"));
        }
    }

    #[cfg(feature = "io-uring")]
    if target.contains("linux") {
        pkg_config::probe_library("liburing")
            .expect("The io-uring feature was requested but the library is not available");
        config.define("ROCKSDB_IOURING_PRESENT", Some("1"));
    }

    if &target != "armv7-linux-androideabi"
        && env::var("CARGO_CFG_TARGET_POINTER_WIDTH").unwrap() != "64"
    {
        config.define("_FILE_OFFSET_BITS", Some("64"));
        config.define("_LARGEFILE64_SOURCE", Some("1"));
    }

    if target.contains("msvc") {
        if cfg!(feature = "mt_static") {
            config.static_crt(true);
        }
        config.flag("-EHsc");
        // Don't use cxx_standard: Uses : instead of =
        config.flag("-std:c++20");
    } else {
        config.flag(cxx_standard());
        // matches the flags in CMakeLists.txt from rocksdb
        config.flag("-Wsign-compare");
        config.flag("-Wshadow");
        config.flag("-Wno-unused-parameter");
        config.flag("-Wno-unused-variable");
        config.flag("-Woverloaded-virtual");
        config.flag("-Wnon-virtual-dtor");
        config.flag("-Wno-missing-field-initializers");
        config.flag("-Wno-strict-aliasing");
        config.flag("-Wno-invalid-offsetof");
    }
    if target.contains("riscv64gc") {
        // link libatomic required to build for riscv64gc
        println!("cargo:rustc-link-lib=atomic");
    }
    for file in lib_sources {
        config.file(source_root.join("rocksdb").join(file));
    }

    config.file(source_root.join("build_version.cc"));

    config.cpp(true);

    if !target.contains("windows") {
        config.flag("-include").flag("cstdint");
    }

    // By default `cc` will link C++ standard library automatically,
    // see https://docs.rs/cc/latest/cc/index.html#c-support.
    // There is no need to manually set `cpp_link_stdlib`.

    config.compile("librocksdb.a");
}

fn build_snappy(source_root: &Path) {
    let target = env::var("TARGET").unwrap();
    let endianness = env::var("CARGO_CFG_TARGET_ENDIAN").unwrap();
    let mut config = cc::Build::new();

    config.include(source_root.join("snappy"));
    config.include(source_root);
    config.define("NDEBUG", Some("1"));
    config.extra_warnings(false);

    if target.contains("msvc") {
        config.flag("-EHsc");
        if cfg!(feature = "mt_static") {
            config.static_crt(true);
        }
    } else {
        // Snappy requires C++11.
        // See: https://github.com/google/snappy/blob/master/CMakeLists.txt#L32-L38
        config.flag("-std=c++11");
    }

    if endianness == "big" {
        config.define("SNAPPY_IS_BIG_ENDIAN", Some("1"));
    }

    config.file(source_root.join("snappy/snappy.cc"));
    config.file(source_root.join("snappy/snappy-sinksource.cc"));
    config.file(source_root.join("snappy/snappy-c.cc"));
    config.cpp(true);
    config.compile("libsnappy.a");
}

fn try_to_find_and_link_lib(lib_name: &str) -> bool {
    println!("cargo:rerun-if-env-changed={lib_name}_COMPILE");
    if let Ok(v) = env::var(format!("{lib_name}_COMPILE")) {
        if v.to_lowercase() == "true" || v == "1" {
            return false;
        }
    }

    println!("cargo:rerun-if-env-changed={lib_name}_LIB_DIR");
    println!("cargo:rerun-if-env-changed={lib_name}_STATIC");

    if let Ok(lib_dir) = env::var(format!("{lib_name}_LIB_DIR")) {
        println!("cargo:rustc-link-search=native={lib_dir}");
        let mode = match env::var_os(format!("{lib_name}_STATIC")) {
            Some(_) => "static",
            None => "dylib",
        };
        println!("cargo:rustc-link-lib={}={}", mode, lib_name.to_lowercase());
        return true;
    }
    false
}

/// Returns the value of the `ROCKSDB_CXX_STD` env var, or the default `-std=c++{version}` flag for
/// building RocksDB.
fn cxx_standard() -> String {
    env::var("ROCKSDB_CXX_STD").map_or("-std=c++20".to_owned(), |cxx_std| {
        if !cxx_std.starts_with("-std=") {
            format!("-std={cxx_std}")
        } else {
            cxx_std
        }
    })
}

fn cpp_link_stdlib(target: &str) {
    // according to https://github.com/alexcrichton/cc-rs/blob/master/src/lib.rs#L2189
    if let Ok(stdlib) = env::var("CXXSTDLIB") {
        println!("cargo:rustc-link-lib=dylib={stdlib}");
    } else if target.contains("apple") || target.contains("freebsd") || target.contains("openbsd") {
        println!("cargo:rustc-link-lib=dylib=c++");
    } else if target.contains("linux") {
        println!("cargo:rustc-link-lib=dylib=stdc++");
    } else if target.contains("aix") {
        println!("cargo:rustc-link-lib=dylib=c++");
        println!("cargo:rustc-link-lib=dylib=c++abi");
    }
}

fn main() {
    let target = env::var("TARGET").unwrap();
    let mut upstream_source_root = None;

    if !try_to_find_and_link_lib("ROCKSDB") {
        if !link_vendored_librocksdb(&target) {
            // rocksdb only works with the prebuilt rocksdb system lib on freebsd.
            // we don't need to rebuild rocksdb
            if target.contains("freebsd") {
                println!("cargo:rustc-link-search=native=/usr/local/lib");
                let mode = match env::var_os("ROCKSDB_STATIC") {
                    Some(_) => "static",
                    None => "dylib",
                };
                println!("cargo:rustc-link-lib={mode}=rocksdb");

                return;
            }

            let source_root = upstream_checkout_root();
            println!("cargo:rerun-if-changed={}", source_root.join("rocksdb").display());
            fail_on_empty_directory(&source_root.join("rocksdb"));
            build_rocksdb(&source_root);
            upstream_source_root = Some(source_root);
        }
    } else {
        cpp_link_stdlib(&target);
    }
    if cfg!(feature = "snappy") && !try_to_find_and_link_lib("SNAPPY") {
        let source_root = upstream_source_root.get_or_insert_with(upstream_checkout_root);
        println!("cargo:rerun-if-changed={}", source_root.join("snappy").display());
        fail_on_empty_directory(&source_root.join("snappy"));
        build_snappy(source_root);
    }

    // Allow dependent crates to locate the sources and output directory of
    // this crate. Notably, this allows a dependent crate to locate the RocksDB
    // sources and built archive artifacts provided by this crate.
    println!(
        "cargo:cargo_manifest_dir={}",
        env::var("CARGO_MANIFEST_DIR").unwrap()
    );
    println!("cargo:out_dir={}", env::var("OUT_DIR").unwrap());
}
