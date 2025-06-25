//! Infrastructure to test the mdbook documentation.
//!
//! Generates a module for each Markdown file in the `src/` directory, and includes
//! the contents of each file as a doc comment for that module.

use std::env;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

/// Recursively finds all Markdown files in the given path and collects their paths into `mds`.
fn find_mds(dir: impl AsRef<Path>, mds: &mut Vec<PathBuf>) -> io::Result<()> {
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        if path.is_dir() {
            find_mds(path, mds)?;
        } else if path.extension().and_then(|s| s.to_str()) == Some("md") {
            mds.push(path);
        }
    }
    Ok(())
}

fn main() -> io::Result<()> {
    let mut mds = Vec::new();
    find_mds("src", &mut mds)?;

    let mut lib = String::new();

    for md in mds {
        let md_path = md.to_str().unwrap();
        println!("cargo::rerun-if-changed={md_path}");
        let mod_name = md_path.replace(['/', '\\', '-', '.'], "_");
        use std::fmt::Write;
        writeln!(
            &mut lib,
            "#[allow(non_snake_case)] #[doc = include_str!(concat!(env!(\"CARGO_MANIFEST_DIR\"), r\"{}{md_path}\"))] mod {mod_name} {{}}",
            std::path::MAIN_SEPARATOR,
        ).unwrap();
    }

    let dest_path = Path::new(&env::var("OUT_DIR").unwrap()).join("mdbook.rs");
    fs::write(&dest_path, lib)?;
    println!("cargo::rerun-if-changed=build.rs");
    Ok(())
}
