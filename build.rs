use std::{path::PathBuf, env};

fn main() {
    let _out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());

    println!("cargo:rerun-if-changed=c/debug.c");

    /*let output = Command::new("env")
        .output()
        .unwrap();
    println!("cargo:warning={}", String::from_utf8_lossy(&output.stdout));*/

    cc::Build::new()
        .file("c/debug.c")
        .include(env::var("DEP_FREETDS_INCLUDE").unwrap())
        .flag_if_supported("-Wno-unused-parameter")
        .compile("debug");

}
