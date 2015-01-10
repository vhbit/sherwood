#![allow(unstable)]

extern crate "libforestdb-sys" as ffi;

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

pub struct FileHandle {
    raw: *mut ffi::fdb_file_handle
}

impl FileHandle {
    fn from_raw(handle: *mut ffi::fdb_file_handle) -> FileHandle {
        FileHandle { raw: handle }
    }

    pub fn open(path: &Path, config: Config) -> Result<FileHandle, Error> {
        let mut handle: *mut ffi::fdb_file_handle = unsafe { mem::zeroed() };
        let c_path = CString::from_slice(path.as_vec());

        try_fdb!(unsafe { ffi::fdb_open(mem::transmute(&mut handle),
                                        c_path.as_ptr(),
                                        mem::transmute(&config.raw)) });
        Ok(FileHandle::from_raw(handle))
    }
}

impl Drop for FileHandle {
    fn drop(&mut self) {
        println!("Dropping");
        unsafe { ffi::fdb_close(self.raw); }
    }
}

#[cfg(test)]
mod tests {
    use super::{FileHandle, Error};

    use std::default::Default;
    use std::error;

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
}
