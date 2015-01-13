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

pub trait KeyRange {
    fn min_key<'a>(&'a self) -> Option<&'a [u8]>;
    fn max_key<'a>(&'a self) -> Option<&'a [u8]>;
    fn options(&self) -> IteratorOptions;
}

pub trait SeqRange {
    fn min_seq(&self) -> u64;
    fn max_seq(&self) -> u64;
    fn options(&self) -> IteratorOptions;
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

bitflags!{
    flags IteratorOptions: u16 {
        const NONE = ffi::FDB_ITR_NONE,
        #[doc="Skip deleted documents"]
        const NO_DELETES = ffi::FDB_ITR_NO_DELETES,
        #[doc="Exclude range minimum value"]
        const SKIP_MIN_KEY = ffi::FDB_ITR_SKIP_MIN_KEY,
        #[doc="Exclude range maximum value"]
        const SKIP_MAX_KEY = ffi::FDB_ITR_SKIP_MAX_KEY,
    }
}

#[repr(u8)]
#[derive(Copy)]
pub enum SeekOptions {
    /// If seek key does not exist return the next sorted
    /// key higher than it
    Higher = ffi::FDB_ITR_SEEK_HIGHER,
    /// If seek key does not exist return the previous sorted
    /// key lower than it
    Lower = ffi::FDB_ITR_SEEK_LOWER
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

    /// Estimates space used by file
    pub fn estimate_size(&self) -> u64 {
        unsafe {ffi::fdb_estimate_space_used(self.raw) as u64}
    }

    /// Starts a transaction with specified isolation level
    pub fn begin_transaction(&self, isolation: IsolationLevel) -> FdbResult<()> {
        lift_error!(unsafe {ffi::fdb_begin_transaction(self.raw, isolation as u8)})
    }

    /// Ends current transaction with specified commit options
    pub fn end_transaction(&self, options: CommitOptions) -> FdbResult<()> {
        lift_error!(unsafe {ffi::fdb_end_transaction(self.raw, options as u8)})
    }

    /// Aborts current transaction
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
    #[allow(dead_code)]
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
        let doc = try!(Doc::new(key));
        try_fdb!(unsafe {
            ffi::fdb_get(self.raw, doc.raw)
        });
        Ok(unsafe {
            Vec::from_raw_buf(mem::transmute((*doc.raw).body),
                              (*doc.raw).bodylen as usize)
        })
    }

    /// Creates a new iterator
    pub fn key_iter<T>(&self, range: T, skip_deleted: bool) -> FdbResult<Iterator> where T: KeyRange {
        let mut handle: *mut ffi::fdb_iterator = ptr::null_mut();
        let mut min_key = ptr::null();
        let mut min_key_len = 0;
        let mut max_key = ptr::null();
        let mut max_key_len = 0;

        if let Some(key) = range.min_key() {
            min_key = unsafe {mem::transmute(key.as_ptr())};
            min_key_len = key.len() as u64;
        }

        if let Some(key) = range.max_key() {
            max_key = unsafe {mem::transmute(key.as_ptr())};
            max_key_len = key.len() as u64;
        }

        let options = if skip_deleted {NO_DELETES} else {IteratorOptions::empty()};

        try_fdb!(unsafe {
            ffi::fdb_iterator_init(self.raw, &mut handle,
                                   min_key, min_key_len,
                                   max_key, max_key_len,
                                   (range.options() | options).bits())
        });
        Ok(Iterator::from_raw(handle))
    }

    pub fn seq_iter<T>(&self, range: T, skip_deleted: bool) -> FdbResult<Iterator> where T: SeqRange {
        let mut handle: *mut ffi::fdb_iterator = ptr::null_mut();
        let min_seq = range.min_seq();
        let max_seq = range.max_seq();

        let options = if skip_deleted {NO_DELETES} else {NONE};

        try_fdb!(unsafe {
            ffi::fdb_iterator_sequence_init(self.raw, &mut handle,
                                   min_seq,
                                   max_seq,
                                   (range.options() | options).bits())
        });
        Ok(Iterator::from_raw(handle))
    }
}

impl Drop for Store {
    fn drop(&mut self) {
        unsafe {ffi::fdb_kvs_close(self.raw); }
    }
}

pub struct Iterator {
    raw: *mut ffi::fdb_iterator,
}

impl Iterator {
    fn from_raw(raw: *mut ffi::fdb_iterator) -> Iterator {
        Iterator { raw: raw }
    }

    pub fn to_next(&self) -> FdbResult<()> {
        lift_error!(unsafe {ffi::fdb_iterator_next(self.raw)})
    }

    pub fn to_prev(&self) -> FdbResult<()> {
        lift_error!(unsafe {ffi::fdb_iterator_prev(self.raw)})
    }

    pub fn get_doc(&self) -> FdbResult<Doc> {
        let mut handle: *mut ffi::fdb_doc = ptr::null_mut();
        try_fdb!(unsafe {ffi::fdb_iterator_get(self.raw, &mut handle)});
        Ok(Doc::from_raw(handle))
    }

    pub fn get_meta_only(&self) -> FdbResult<Doc> {
        let mut handle: *mut ffi::fdb_doc = ptr::null_mut();
        try_fdb!(unsafe {ffi::fdb_iterator_get_metaonly(self.raw, &mut handle)});
        Ok(Doc::from_raw(handle))
    }

    pub fn to_min_key(&self) -> FdbResult<()> {
        lift_error!(unsafe {ffi::fdb_iterator_seek_to_min(self.raw)})
    }

    pub fn to_max_key(&self) -> FdbResult<()> {
        lift_error!(unsafe {ffi::fdb_iterator_seek_to_max(self.raw)})
    }

    pub fn to_key<K>(&self, key: &K, options: SeekOptions) -> FdbResult<()> where K: AsSlice<u8> {
        let key = key.as_slice();
        lift_error!(unsafe {ffi::fdb_iterator_seek(self.raw,
                                                   mem::transmute(key.as_ptr()), key.len() as u64,
                                                   options as ffi::fdb_iterator_seek_opt_t)})
    }
}

impl Drop for Iterator {
    fn drop(&mut self) {
        unsafe {ffi::fdb_iterator_close(self.raw)};
    }
}

pub struct Doc {
    raw: *mut ffi::fdb_doc
}

impl Doc {
    fn from_raw(handle: *mut ffi::fdb_doc) -> Doc {
        Doc {raw: handle}
    }

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

#[allow(dead_code)]
pub struct UnsafeDoc<'a> {
    raw: ffi::fdb_doc,
}

impl std::iter::Iterator for Iterator {
    type Item = Doc;

    fn next(&mut self) -> Option<Doc> {
        match self.get_doc() {
            Err(_) => return None,
            Ok(doc) => {
                let _ = self.to_next();
                Some(doc)
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, None)
    }
}

impl KeyRange for FullRange {
    fn min_key(&self) -> Option<&[u8]> {
        None
    }

    fn max_key(&self) -> Option<&[u8]> {
        None
    }

    fn options(&self) -> IteratorOptions {
        NONE
    }
}

impl<T: AsSlice<u8>> KeyRange for std::ops::Range<T> {
    fn min_key(&self) -> Option<&[u8]> {
        Some(self.start.as_slice())
    }

    fn max_key(&self) -> Option<&[u8]> {
        Some(self.end.as_slice())
    }

    fn options(&self) -> IteratorOptions {
        SKIP_MAX_KEY
    }
}

impl<T: AsSlice<u8>> KeyRange for std::ops::RangeFrom<T> {
    fn min_key(&self) -> Option<&[u8]> {
        Some(self.start.as_slice())
    }

    fn max_key(&self) -> Option<&[u8]> {
        None
    }

    fn options(&self) -> IteratorOptions {
        NONE
    }
}


impl<T: AsSlice<u8>> KeyRange for std::ops::RangeTo<T> {
    fn min_key(&self) -> Option<&[u8]> {
        None
    }

    fn max_key(&self) -> Option<&[u8]> {
        Some(self.end.as_slice())
    }

    fn options(&self) -> IteratorOptions {
        NONE
    }
}

impl SeqRange for FullRange {
    fn min_seq(&self) -> u64 {
        0
    }

    fn max_seq(&self) -> u64 {
        0
    }

    fn options(&self) -> IteratorOptions {
        NONE
    }
}

macro_rules! uint_seq_iter_impl {
    ($t:ty) => (
        impl SeqRange for std::ops::Range<$t> {
            fn min_seq(&self) -> u64 {
                self.start as u64
            }

            fn max_seq(&self) -> u64 {
                self.end as u64
            }

            fn options(&self) -> IteratorOptions {
                SKIP_MAX_KEY
            }
        }

        impl SeqRange for std::ops::RangeFrom<$t> {
            fn min_seq(&self) -> u64 {
                self.start as u64
            }

            fn max_seq(&self) -> u64 {
                0
            }

            fn options(&self) -> IteratorOptions {
                NONE
            }
        }

        impl SeqRange for std::ops::RangeTo<$t> {
            fn min_seq(&self) -> u64 {
                0
            }

            fn max_seq(&self) -> u64 {
                self.end as u64
            }

            fn options(&self) -> IteratorOptions {
                NONE
            }
        }

        )
}

uint_seq_iter_impl!(u8);
uint_seq_iter_impl!(u16);
uint_seq_iter_impl!(u32);
uint_seq_iter_impl!(u64);
uint_seq_iter_impl!(usize);

#[allow(dead_code)]
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

#[allow(dead_code)]
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

        println!("current test is {}", cur_test);
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

    #[test]
    fn test_key_iterator() {
        let db = FileHandle::open(&next_db_path(), Default::default()).unwrap();
        let store = db.get_default_store(Default::default()).unwrap();

        assert!(store.set_value(&"a".as_bytes(), &"a".as_bytes()).is_ok());
        assert!(store.set_value(&"b".as_bytes(), &"b".as_bytes()).is_ok());
        assert!(store.set_value(&"c".as_bytes(), &"c".as_bytes()).is_ok());
        assert!(store.set_value(&"d".as_bytes(), &"d".as_bytes()).is_ok());

        // FIXME: check contents too
        let iter = store.key_iter(FullRange, false).unwrap();
        assert_eq!(iter.count(), 4);

        let sub_iter = store.key_iter("b".as_bytes().."d".as_bytes(), false).unwrap();
        assert_eq!(sub_iter.count(), 2);
    }


    #[test]
    fn test_seq_iterator() {
        let db = FileHandle::open(&next_db_path(), Default::default()).unwrap();
        let store = db.get_default_store(Default::default()).unwrap();

        assert!(store.set_value(&"a".as_bytes(), &"a".as_bytes()).is_ok());
        assert!(store.set_value(&"b".as_bytes(), &"b".as_bytes()).is_ok());
        assert!(store.set_value(&"c".as_bytes(), &"c".as_bytes()).is_ok());
        assert!(store.set_value(&"d".as_bytes(), &"d".as_bytes()).is_ok());

        // FIXME: check contents too
        let iter = store.seq_iter(FullRange, false).unwrap();
        assert_eq!(iter.count(), 4);

        let sub_iter = store.seq_iter(1us..4, false).unwrap();
        // FIXME: find out failure reason
        assert_eq!(sub_iter.count(), 3);
    }
}
