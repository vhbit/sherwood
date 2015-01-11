#![allow(unstable)]

extern crate libc;
extern crate "libforestdb-sys" as ffi;

#[macro_use] extern crate log;

use std::default::Default;
use std::error;
use std::ffi::{CString, c_str_to_bytes};
use std::mem;
use std::ptr;

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

pub type FdbResult<T> = Result<T, Error>;

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

/// Initializes ForestDB. Usually is called automatically
pub fn init(config: Config) -> Result<(), Error> {
    lift_error!(unsafe {ffi::fdb_init(mem::transmute(&config.raw))})
}

/// Forces ForestDB shutdown: closing everything and terminating
/// compactor thread
pub fn shutdown() -> Result<(), Error> {
    lift_error!(unsafe {ffi::fdb_shutdown()})
}

/// Represents ForestDB file handle
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

    /// Opens database with specified config
    pub fn open(path: &Path, config: Config) -> FdbResult<FileHandle> {
        let mut handle: *mut ffi::fdb_file_handle = ptr::null_mut();
        let c_path = CString::from_slice(path.as_vec());

        try_fdb!(unsafe { ffi::fdb_open(mem::transmute(&mut handle),
                                        c_path.as_ptr(),
                                        mem::transmute(&config.raw)) });
        let res = FileHandle::from_raw(handle, path, config);
        Ok(res)
    }

    /// Retrieves default store
    pub fn get_default_store(&self, config: StoreConfig) -> FdbResult<Store> {
        self._get_store(None, config)
    }

    /// Retrieves store by name
    pub fn get_store(&self, name: &str, config: StoreConfig) -> FdbResult<Store> {
        let c_name = CString::from_slice(name.as_bytes());
        self._get_store(Some(c_name), config)
    }

    fn _get_store(&self, name: Option<CString>, config: StoreConfig) -> FdbResult<Store> {
        let mut handle: *mut ffi::fdb_kvs_handle = ptr::null_mut();
        try_fdb!(unsafe { ffi::fdb_kvs_open(self.raw,
                                            mem::transmute(&mut handle),
                                            if name.is_some() {name.unwrap().as_ptr()} else {ptr::null()},
                                            mem::transmute(&config.raw)
                                            )});
        Ok(Store::from_raw(handle, config))
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

impl std::fmt::Show for FileHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str(&format!("FdbFileHandle {{path: {:?}}}", self.path)[])
    }
}

#[derive(Copy)]
pub struct StoreConfig {
    raw: ffi::fdb_kvs_config
}

impl Default for StoreConfig {
    fn default() -> StoreConfig {
        StoreConfig {
            raw: unsafe {ffi::fdb_get_default_kvs_config()}
        }
    }
}


/// Represents ForestDB key value store
pub struct Store {
    raw: *mut ffi::fdb_kvs_handle,
    config: StoreConfig
}

impl Store {
    fn from_raw(raw: *mut ffi::fdb_kvs_handle, config: StoreConfig) -> Store {
        Store {
            raw: raw,
            config: config
        }
    }

    /// Retrieves a value by key (plain KV mode)
    pub fn set_value(&self, key: &[u8], value: &[u8]) -> FdbResult<()> {
        lift_error!(unsafe {
            let mut doc = UnsafeDoc::with_key_value(key, value);
            ffi::fdb_set(self.raw, &mut doc.raw)
        })
    }

    /// Sets a value for key (plain KV mode)
    pub fn get_value(&self, key: &[u8]) -> FdbResult<Vec<u8>> {
        let mut doc = UnsafeDoc::with_key(key);
        try_fdb!(unsafe {
            ffi::fdb_get(self.raw, &mut doc.raw)
        });
        Ok(unsafe {
            Vec::from_raw_buf(mem::transmute(doc.raw.body), doc.raw.bodylen as usize)
        })
    }
}

impl Drop for Store {
    fn drop(&mut self) {
        unsafe {ffi::fdb_kvs_close(self.raw); }
    }
}

pub struct UnsafeDoc<'a> {
    raw: ffi::fdb_doc,
}

impl<'a> UnsafeDoc<'a> {
    fn with_key(key: &'a [u8]) -> UnsafeDoc<'a> {
        UnsafeDoc {
            raw: ffi::fdb_doc {
                key: unsafe { mem::transmute(key.as_ptr()) },
                keylen: key.len() as libc::size_t,
                body: ptr::null_mut(),
                bodylen: 0,
                meta: ptr::null_mut(),
                metalen: 0,
                size_ondisk: 0,
                seqnum: 0,
                offset: 0,
                deleted: 0
            }
        }
    }

    fn with_key_value(key: &'a [u8], value: &'a [u8]) -> UnsafeDoc<'a> {
        UnsafeDoc {
            raw: ffi::fdb_doc {
                key: unsafe { mem::transmute(key.as_ptr()) },
                keylen: key.len() as libc::size_t,
                body: unsafe { mem::transmute(value.as_ptr()) },
                bodylen: value.len() as libc::size_t,
                meta: ptr::null_mut(),
                metalen: 0,
                size_ondisk: 0,
                seqnum: 0,
                offset: 0,
                deleted: 0

            }
        }
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

    #[test]
    fn test_open_store() {
        let db = FileHandle::open(&Path::new("test3"), Default::default()).unwrap();
        assert!(db.get_default_store(Default::default()).is_ok());
        assert!(db.get_store("hello", Default::default()).is_ok())
    }

    #[test]
    fn test_simple_keys() {
        let db = FileHandle::open(&Path::new("test4"), Default::default()).unwrap();
        let store = db.get_default_store(Default::default()).unwrap();
        assert!(store.get_value("hello".as_bytes()).is_err());
        assert!(store.set_value("hello".as_bytes(), "world".as_bytes()).is_ok());
        let value = store.get_value("hello".as_bytes()).unwrap();
        assert_eq!(value.as_slice(), "world".as_bytes());
    }
}
