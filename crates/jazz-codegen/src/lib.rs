//! Typed bindings and migration stubs generated from an app's
//! `jazz.app.json` (SPEC §10.10), in every language Jazz binds.
//!
//! - [`model`]: the language-neutral [`App`] every back end reads.
//! - [`ts`]: the TypeScript back end (`jazz.generated.ts`).
//! - [`migrations`]: `migrations create`, the schema diff and stub writer.
//! - [`bindings`]: what committed bindings record, for staleness checks.
//!
//! A back end is a module with `generate(&App) -> Result<String, CodegenError>`
//! and a [`Lang`] variant. Plain string writers, no templates.

pub mod bindings;
mod json_schema;
pub mod migrations;
pub mod model;
pub mod names;
pub mod rust;
pub mod ts;

use jazz::tools::deployment::artifact::{ArtifactError, json};

pub use model::App;

/// Why generating was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodegenError {
    message: String,
    unsupported: bool,
}

impl std::error::Error for CodegenError {}

impl std::fmt::Display for CodegenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl CodegenError {
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            unsupported: false,
        }
    }

    /// What isn't supported (yet), as opposed to wrong input.
    pub(crate) fn unsupported(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            unsupported: true,
        }
    }

    pub(crate) fn from_artifact(error: ArtifactError) -> Self {
        Self {
            unsupported: error.is_unsupported(),
            message: error.to_string(),
        }
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    /// Whether the input is fine but can't be handled yet: bindings give it
    /// `error.code` `JAZZ_APP_FILES_UNSUPPORTED`, as the loaders do.
    pub fn is_unsupported(&self) -> bool {
        self.unsupported
    }
}

/// A language bindings are generated in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lang {
    TypeScript,
    Rust,
}

impl Lang {
    /// From its CLI/napi name: `ts`.
    pub fn parse(name: &str) -> Result<Lang, CodegenError> {
        match name {
            "ts" | "typescript" => Ok(Lang::TypeScript),
            "rust" | "rs" => Ok(Lang::Rust),
            other => Err(CodegenError::unsupported(format!(
                "generating bindings in `{other}` is not supported yet; supported: ts, rust"
            ))),
        }
    }

    /// Every language, in the order `jazz-tools generate` handles them.
    pub const ALL: &'static [Lang] = &[Lang::TypeScript, Lang::Rust];

    /// The file whose presence beside `jazz.app.json` says the app is
    /// written in this language, so `validate` writes its bindings.
    pub fn marker(self) -> &'static str {
        match self {
            Lang::TypeScript => "package.json",
            Lang::Rust => "Cargo.toml",
        }
    }

    /// Whether the app in `dir` (the directory of its `jazz.app.json`) is
    /// written in this language: a TS app has a `package.json`; a Rust app
    /// a `Cargo.toml` with a `[package]` that depends on `jazz-app` (a
    /// workspace root or another crate beside a TS app's files is not
    /// one). Committed bindings say so too; the caller checks those.
    pub fn detected_in(self, dir: &std::path::Path) -> bool {
        let marker = dir.join(self.marker());
        match self {
            Lang::TypeScript => marker.exists(),
            Lang::Rust => std::fs::read_to_string(marker)
                .is_ok_and(|manifest| rust::is_app_manifest(&manifest)),
        }
    }

    /// The version of the bindings' shape this generator writes.
    pub fn format(self) -> u32 {
        match self {
            Lang::TypeScript => ts::FORMAT,
            Lang::Rust => rust::FORMAT,
        }
    }

    /// The file the bindings go in, relative to `jazz.app.json`'s directory.
    pub fn file(self) -> &'static str {
        match self {
            Lang::TypeScript => ts::FILE,
            Lang::Rust => rust::FILE,
        }
    }
}

/// The bindings for `app_json` (the file's bytes) in `lang`.
pub fn generate(lang: Lang, app_json: &[u8]) -> Result<String, CodegenError> {
    let app = App::load(app_json).map_err(CodegenError::from_artifact)?;
    generate_app(lang, &app)
}

/// The bindings for a loaded `app` in `lang`.
pub fn generate_app(lang: Lang, app: &App) -> Result<String, CodegenError> {
    refuse_stranded(app)?;
    match lang {
        Lang::TypeScript => ts::generate(app),
        Lang::Rust => rust::generate(app),
    }
}

/// The options `bindApp` takes, as JSON text, without the callbacks: what
/// the TS bindings embed, for tests that define an app inline.
pub fn compile_app(app_json: &[u8]) -> Result<String, CodegenError> {
    let app = App::load(app_json).map_err(CodegenError::from_artifact)?;
    refuse_stranded(&app)?;
    Ok(json::print(&ts::bind_options(&app)))
}

/// Bindings must not ship a version clients couldn't open.
fn refuse_stranded(app: &App) -> Result<(), CodegenError> {
    match app.stranded.as_slice() {
        [] => Ok(()),
        stranded => Err(CodegenError::new(stranded.join("\n"))),
    }
}
