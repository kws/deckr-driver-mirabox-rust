#![allow(dead_code)]

use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use serde::Deserialize;

#[path = "src/policy.rs"]
mod policy;

#[derive(Debug, Deserialize)]
struct LayoutLint {
    candidate: String,
    #[serde(rename = "match")]
    match_expr: String,
}

fn main() -> Result<()> {
    println!("cargo:rustc-check-cfg=cfg(libusb)");

    let layout_dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR")?).join("layouts/built-in");

    println!("cargo:rerun-if-changed={}", layout_dir.display());

    for path in layout_files(&layout_dir)? {
        println!("cargo:rerun-if-changed={}", path.display());
        lint_layout(&path)?;
    }

    Ok(())
}

fn layout_files(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    for entry in fs::read_dir(dir).with_context(|| format!("reading {}", dir.display()))? {
        let entry = entry?;
        let path = entry.path();
        if matches!(path.extension().and_then(|ext| ext.to_str()), Some("yml" | "yaml")) {
            files.push(path);
        }
    }
    files.sort();
    Ok(files)
}

fn lint_layout(path: &Path) -> Result<()> {
    let data = fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    let layout: LayoutLint =
        serde_yaml::from_str(&data).with_context(|| format!("parsing {}", path.display()))?;

    if !(layout.candidate.contains("vendor_id") && layout.candidate.contains("product_id")) {
        bail!(
            "{} candidate must include a non-usage_page fallback",
            path.display()
        );
    }

    policy::parse_expression(&layout.candidate)
        .with_context(|| format!("parsing candidate expression in {}", path.display()))?;
    policy::parse_expression(&layout.match_expr)
        .with_context(|| format!("parsing match expression in {}", path.display()))?;
    Ok(())
}
