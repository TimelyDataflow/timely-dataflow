extern crate skeptic;

use skeptic::*;

fn main() {
    println!("building tests");
    let mdbook_files = markdown_files_of_directory("mdbook/src");
    generate_doc_tests(&mdbook_files);
}