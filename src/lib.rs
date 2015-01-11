#![allow(unstable)]

extern crate "libforestdb-sys" as ffi;

#[macro_use] extern crate log;

use std::default::Default;
use std::error;
use std::ffi::{CString, c_str_to_bytes};
use std::mem;

#[derive(Copy)]
pub struct Error {code: i32}

impl Error {
    pub fn from_code(code: i32) -> Error {
        Error {code: code}
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        "ForestDB error"
    }

    fn detail(&self) -> Option<String> {
        unsafe { String::from_utf8(c_str_to_bytes(&ffi::fdb_error_msg(self.code)).to_vec()).ok() }
    }
}

impl std::fmt::Show for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str(&format!("fdb error: {:?}", (self as &error::Error).detail())[])
    }
}

macro_rules! lift_error {
    ($e: expr) => (lift_error!($e, ()));
    ($e: expr, $r: expr) => ({
        let t = $e;
        match t {
            ffi::FDB_RESULT_SUCCESS => Ok($r),
            _ => return Err(Error::from_code(t))
        }
    })
}

macro_rules! try_fdb {
    ($e: expr) => ({
        let t = $e;
        match t {
            ffi::FDB_RESULT_SUCCESS => (),
            _ => return Err(Error::from_code(t))
        }
    })
}

#[derive(Copy)]
pub struct Config {
    raw: ffi::fdb_config
}

impl Config {
}

impl Default for Config {
    fn default() -> Config {
        Config {
            raw: unsafe { ffi::fdb_get_default_config() }
        }
    }
}

pub fn init(config: Config) -> Result<(), Error> {
    lift_error!(unsafe {ffi::fdb_init(mem::transmute(&config.raw))})
}

pub fn shutdown() -> Result<(), Error> {
    lift_error!(unsafe {ffi::fdb_shutdown()})
}

pub struct FileHandle {
    path: Path,
    config: Config,
    raw: *mut ffi::fdb_file_handle,
}

impl FileHandle {
    fn from_raw(handle: *mut ffi::fdb_file_handle, path: &Path, config: Config) -> FileHandle {
        FileHandle {
            raw: handle,
            path: path.clone(),
            config: config
        }
    }

    pub fn open(path: &Path, config: Config) -> Result<FileHandle, Error> {
        let mut handle: *mut ffi::fdb_file_handle = unsafe { mem::zeroed() };
        let c_path = CString::from_slice(path.as_vec());

        try_fdb!(unsafe { ffi::fdb_open(mem::transmute(&mut handle),
                                        c_path.as_ptr(),
                                        mem::transmute(&config.raw)) });
        let res = FileHandle::from_raw(handle, path, config);
        Ok(res)
    }
}

unsafe impl Send for FileHandle {}


// FIXME: do not use trait, implement clone as method
// which returns result? Otherwise it is impossible
// to handle cloning right
impl Clone for FileHandle {
    fn clone(&self) -> FileHandle {
        match FileHandle::open(&self.path, self.config) {
            Ok(res) => res,
            Err(_) => panic!("failed to clone")
        }
    }
}

impl Drop for FileHandle {
    fn drop(&mut self) {
        debug!("Dropping");
        unsafe { ffi::fdb_close(self.raw); }
    }
}

#[cfg(test)]
mod tests {
    use super::{FileHandle, Error};

    use std::default::Default;
    use std::error;
    use std::thread::Thread;

    use ffi;

    #[test]
    fn test_error_msg(){
        let err = Error::from_code(ffi::FDB_RESULT_OPEN_FAIL);
        assert_eq!("error opening file", (&err as &error::Error).detail().unwrap().as_slice());
    }

    #[test]
    fn test_init() {
        assert!(super::init(Default::default()).is_ok());
    }

    #[test]
    fn test_open_file() {
        assert!(super::init(Default::default()).is_ok());
        assert!(FileHandle::open(&Path::new("test1"), Default::default()).is_ok());
    }

    #[test]
    fn test_clone() {
        let fh1 = FileHandle::open(&Path::new("test2"), Default::default()).unwrap();
        let fh2 = fh1.clone();

        Thread::scoped(move || {
            let fh_cloned = fh2;
            // FIXME: test something useful
        }).join();
    }
}
