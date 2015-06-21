use std::env;
use std::fs;
use std::path::{PathBuf};
use std::process::{Command, Stdio};

fn run(cmd: &mut Command) {
    println!("running: {:?}", cmd);
    assert!(cmd.stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .status()
            .unwrap()
            .success());
}

fn main() {
    let root_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    let out_dir = PathBuf::from(env::var("OUT_DIR").ok().expect("out dir must be specified"));

    let build_dir = out_dir.join("build");
    assert!(fs::create_dir_all(&build_dir).is_ok());
    assert!(env::set_current_dir(&build_dir).is_ok());

    let lib_dir = out_dir.join("lib");
    assert!(fs::create_dir_all(&lib_dir).is_ok());

    let profile = match &env::var("PROFILE").unwrap()[..] {
        "bench" | "release" => "Release",
        // FIXME: it should be Debug, but might fail as debug requires gcov/lcov to be installed
        _ => "RelWithDebugInfo",
    };

    run(Command::new("cmake")
        .arg(root_dir.join("forestdb"))
        .arg(format!("-DCMAKE_BUILD_TYPE={}", profile))
        .arg(format!("-DCMAKE_LIBRARY_OUTPUT_DIRECTORY={}", lib_dir.display()))
        .arg("-DSNAPPY_OPTION=Disable")
        .current_dir(&build_dir));

    run(Command::new("cmake")
        .arg("--build").arg(".")
        .arg("--target").arg("forestdb")
        .current_dir(&build_dir));

    println!("cargo:rustc-flags=-l forestdb -L {}", lib_dir.display());
}
