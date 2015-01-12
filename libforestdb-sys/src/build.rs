#![allow(unstable)]

use std::io::{self, fs};
use std::io::process::{Command, InheritFd};
use std::os;

fn run(cmd: &mut Command) {
    println!("running: {}", cmd);
    assert!(cmd.stdout(InheritFd(1))
            .stderr(InheritFd(2))
            .status()
            .unwrap()
            .success());
}

fn main() {
    let root_dir = Path::new(os::getenv("CARGO_MANIFEST_DIR").unwrap());
    let out_dir = Path::new(os::getenv("OUT_DIR").expect("out dir must be specified"));

    let build_dir = out_dir.join("build");
    assert!(fs::mkdir_recursive(&build_dir, io::USER_DIR).is_ok());
    assert!(os::change_dir(&build_dir).is_ok());

    let lib_dir = out_dir.join("lib");
    assert!(fs::mkdir_recursive(&lib_dir, io::USER_DIR).is_ok());

    let profile = match os::getenv("PROFILE").unwrap().as_slice() {
        "bench" | "release" => "Release",
        // FIXME: it should be Debug, but might fail as debug requires gcov/lcov to be installed
        _ => "RelWithDebugInfo",
    };

    run(Command::new("cmake")
        .arg(root_dir.join("forestdb"))
        .arg(format!("-DCMAKE_BUILD_TYPE={}", profile))
        .arg(format!("-DCMAKE_LIBRARY_OUTPUT_DIRECTORY={}", lib_dir.display()))
        .arg("-DSNAPPY_OPTION=Disable")
        .cwd(&build_dir));

    run(Command::new("cmake")
        .arg("--build").arg(".")
        .arg("--target").arg("forestdb")
        .cwd(&build_dir));

    println!("cargo:rustc-flags=-l forestdb -L {}", lib_dir.display());
}
