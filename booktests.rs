extern crate skeptic;

use skeptic::*;

fn main() {
    // skeptic::generate_doc_tests(&["mdbook/src/chapter_1_1.md"]);
    let mut mdbook_files = markdown_files_of_directory("mdbook/src");
    // mdbook_files.push("SUMMARY.md".to_owned());
    generate_doc_tests(&mdbook_files);
}