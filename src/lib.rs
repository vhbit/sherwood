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

#[repr(u8)]
#[derive(Copy)]
pub enum CommitOptions {
    Normal = ffi::FDB_COMMIT_NORMAL as u8,
    ManualWalFlush = ffi::FDB_COMMIT_MANUAL_WAL_FLUSH as u8
}

#[repr(u8)]
#[derive(Copy)]
/// Transaction isolation level
pub enum IsolationLevel {
    // Serializable = 0, // unsupported yet
    // RepeatableRead = 1, // unsupported yet
    /// Prevent a transaction from reading uncommitted data from other
    /// transactions.
    ReadCommitted = 2,
    ///  Allow a transaction to see uncommitted data from other transaction.
    ReadUncommited = 3,
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

    /// Commit all pending doc changes
    pub fn commit(&self, options: CommitOptions) -> FdbResult<()> {
        lift_error!(unsafe {ffi::fdb_commit(self.raw, options as u8)})
    }

    /// Writes compacted database to new_path. If it is set to None - it'll be in-place
    /// compaction
    pub fn compact(&self, new_path: Option<Path>) -> FdbResult<()> {
        lift_error!(unsafe {ffi::fdb_compact(self.raw,
                                             if new_path.is_none() {
                                                 ptr::null_mut()
                                             } else {
                                                 CString::from_slice(new_path.unwrap().as_vec()).as_ptr()
                                             })})
    }

    pub fn estimate_size(&self) -> u64 {
        unsafe {ffi::fdb_estimate_space_used(self.raw) as u64}
    }

    pub fn begin_transaction(&self, isolation: IsolationLevel) -> FdbResult<()> {
        lift_error!(unsafe {ffi::fdb_begin_transaction(self.raw, isolation as u8)})
    }

    pub fn end_transaction(&self, options: CommitOptions) -> FdbResult<()> {
        lift_error!(unsafe {ffi::fdb_end_transaction(self.raw, options as u8)})
    }

    pub fn abort_transaction(&self) -> FdbResult<()> {
        lift_error!(unsafe {ffi::fdb_abort_transaction(self.raw)})
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
    pub fn set_value<K, V>(&self, key: &K, value: &V) -> FdbResult<()>
        where K: AsSlice<u8>,
              V: AsSlice<u8>
    {
        let mut doc = try!(Doc::new(key));
        try!(doc.set_body(value));

        lift_error!(unsafe {
            ffi::fdb_set(self.raw, doc.raw)
        })
    }

    /// Sets a value for key (plain KV mode)
    pub fn get_value<K>(&self, key: &K) -> FdbResult<Vec<u8>> where K: AsSlice<u8> {
        let mut doc = try!(Doc::new(key));
        try_fdb!(unsafe {
            ffi::fdb_get(self.raw, doc.raw)
        });
        Ok(unsafe {
            Vec::from_raw_buf(mem::transmute((*doc.raw).body),
                              (*doc.raw).bodylen as usize)
        })
    }

}

impl Drop for Store {
    fn drop(&mut self) {
        unsafe {ffi::fdb_kvs_close(self.raw); }
    }
}

pub struct Doc {
    raw: *mut ffi::fdb_doc
}

impl Doc {
    pub fn new<K>(key: &K) -> FdbResult<Doc> where K: AsSlice<u8>{
        let mut handle: *mut ffi::fdb_doc = ptr::null_mut();
        let key = key.as_slice();
        try_fdb!(unsafe {ffi::fdb_doc_create(&mut handle,
                                             mem::transmute(key.as_ptr()), key.len() as u64,
                                             ptr::null(), 0,
                                             ptr::null(), 0)});
        Ok(Doc {raw: handle})
    }

    pub fn set_body<B>(&mut self, body: &B) -> FdbResult<()> where B: AsSlice<u8> {
        let body = body.as_slice();
        lift_error!(unsafe {ffi::fdb_doc_update(&mut self.raw,
                                                ptr::null(), 0,
                                                mem::transmute(body.as_ptr()), body.len() as u64)
        })
    }

    pub fn set_meta<M>(&mut self, meta: &M) -> FdbResult<()> where M: AsSlice<u8> {
        let meta = meta.as_slice();
        lift_error!(unsafe {ffi::fdb_doc_update(&mut self.raw,
                                                mem::transmute(meta.as_ptr()), meta.len() as u64,
                                                ptr::null(), 0)
        })
    }
}

impl Drop for Doc {
    fn drop(&mut self) {
        unsafe {ffi::fdb_doc_free(self.raw)};
    }
}

pub struct UnsafeDoc<'a> {
    raw: ffi::fdb_doc,
}

fn empty_doc() -> ffi::fdb_doc {
    ffi::fdb_doc {
        key: ptr::null_mut(),
        keylen: 0,
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

impl<'a> UnsafeDoc<'a> {
    fn with_key(key: &'a [u8]) -> UnsafeDoc<'a> {
        UnsafeDoc {
            raw: ffi::fdb_doc {
                key: unsafe { mem::transmute(key.as_ptr()) },
                keylen: key.len() as libc::size_t,
                .. empty_doc()
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
                .. empty_doc()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{FileHandle, Error};

    use std::sync::atomic::{AtomicUint, ATOMIC_UINT_INIT, Ordering};
    use std::default::Default;
    use std::error;
    use std::io::{self, USER_DIR};
    use std::io::fs::PathExtensions;
    use std::os;
    use std::sync::{Once, ONCE_INIT};
    use std::thread::Thread;

    use ffi;

    fn next_db_path() -> Path {
        static NEXT_TEST: AtomicUint = ATOMIC_UINT_INIT;
        static CLEAR_DIR_ONCE: Once = ONCE_INIT;
        let cur_test = NEXT_TEST.fetch_add(1, Ordering::SeqCst);
        let db_dir = os::self_exe_path().unwrap().join("db_tests");

        CLEAR_DIR_ONCE.call_once(|| {
            if db_dir.exists() {
                assert!(io::fs::rmdir_recursive(&db_dir).is_ok());
            }

            assert!(io::fs::mkdir(&db_dir, USER_DIR).is_ok());
        });

        db_dir.join(format!("db-{}", cur_test))
    }

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
        assert!(FileHandle::open(&next_db_path(), Default::default()).is_ok());
    }

    #[test]
    fn test_clone() {
        let fh1 = FileHandle::open(&next_db_path(), Default::default()).unwrap();
        let fh2 = fh1.clone();

        Thread::scoped(move || {
            let fh_cloned = fh2;
            // FIXME: test something useful
        }).join();
    }

    #[test]
    fn test_open_store() {
        let db = FileHandle::open(&next_db_path(), Default::default()).unwrap();
        assert!(db.get_default_store(Default::default()).is_ok());
        assert!(db.get_store("hello", Default::default()).is_ok())
    }

    #[test]
    fn test_simple_keys() {
        let db = FileHandle::open(&next_db_path(), Default::default()).unwrap();
        let store = db.get_default_store(Default::default()).unwrap();
        assert!(store.get_value(&"hello".as_bytes()).is_err());
        assert!(store.set_value(&"hello".as_bytes(), &"world".as_bytes()).is_ok());
        let value = store.get_value(&"hello".as_bytes()).unwrap();
        assert_eq!(value.as_slice(), "world".as_bytes());
    }
}
