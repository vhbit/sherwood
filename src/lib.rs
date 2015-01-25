#![allow(unstable)]
#![feature(unsafe_destructor)]

extern crate libc;
extern crate "libforestdb-sys" as ffi;

#[macro_use] extern crate bitflags;
#[macro_use] extern crate log;

use std::default::Default;
use std::error;
use std::ffi::{CString, c_str_to_bytes};
use std::mem;
use std::ptr;
use std::rc::Rc;
use std::slice::from_raw_buf;

#[derive(Copy)]
pub struct Error {code: i32}

impl Error {
    pub fn from_code(code: i32) -> Error {
        Error {code: code}
    }

    pub fn is_not_found(self) -> bool {
        self.code == ffi::FDB_RESULT_KEY_NOT_FOUND
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
    fn options(&self) -> IteratorOptions {
        NONE
    }
}

pub trait SeqRange {
    fn min_seq(&self) -> u64;
    fn max_seq(&self) -> u64;
    fn options(&self) -> IteratorOptions {
        NONE
    }
}

pub trait DocKey {
    fn get_raw_key<'a>(&'a self) -> Option<&'a [u8]>;

    fn get_key<T: FromBytes>(&self) -> Option<T> {
        self.get_raw_key().and_then(|x| FromBytes::from_bytes(x))
    }
}

pub trait DocBody {
    fn set_body<B>(&mut self, body: &B) -> FdbResult<()> where B: AsSlice<u8>;
    fn set_meta<M>(&mut self, meta: &M) -> FdbResult<()> where M: AsSlice<u8>;
    fn get_raw_body<'a>(&'a self) -> Option<&'a [u8]>;

    fn get_body<T:FromBytes>(&self) -> Option<T> {
        self.get_raw_body().and_then(|x| FromBytes::from_bytes(x))
    }
}

pub trait DocMeta {
    fn get_raw_meta<'a>(&'a self) -> Option<&'a [u8]>;

    fn get_meta<T:FromBytes>(&self) -> Option<T> {
        self.get_raw_meta().and_then(|x| FromBytes::from_bytes(x))
    }

    fn seq_num(&self) -> u64;
    fn is_deleted(&self) -> bool;
    fn offset(&self) -> u64;
}

pub trait FromBytes {
    fn from_bytes(bytes: &[u8]) -> Option<Self>;
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
    fn from_raw(raw: ffi::fdb_config) -> Config {
        Config {
            raw: raw
        }
    }

    pub fn new() -> Config {
        Default::default()
    }

    pub fn custom() -> ConfigBuilder {
        ConfigBuilder::new()
    }
}

impl Default for Config {
    fn default() -> Config {
        Config {
            raw: unsafe { ffi::fdb_get_default_config() }
        }
    }
}

#[allow(missing_copy_implementations)]
pub struct ConfigBuilder {
    raw: ffi::fdb_config
}

impl ConfigBuilder {
    pub fn new() -> ConfigBuilder {
        ConfigBuilder {
            raw: unsafe { ffi::fdb_get_default_config() }
        }
    }

    pub fn set_cache_size(mut self, size: u64) -> ConfigBuilder {
        self.raw.buffercache_size = size;
        self
    }

    pub fn build(self) -> Config {
        let tmp = self;
        Config::from_raw(tmp.raw)
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
    pub fn get_default_store(&self, config: StoreConfig) -> FdbResult<KvHandle<ReadWrite>> {
        self._get_store(None, config)
    }

    /// Retrieves store by name
    pub fn get_store(&self, name: &str, config: StoreConfig) -> FdbResult<KvHandle<ReadWrite>> {
        let c_name = CString::from_slice(name.as_bytes());
        self._get_store(Some(c_name), config)
    }

    fn _get_store(&self, name: Option<CString>, config: StoreConfig) -> FdbResult<KvHandle<ReadWrite>> {
        let mut handle: *mut ffi::fdb_kvs_handle = ptr::null_mut();
        try_fdb!(unsafe { ffi::fdb_kvs_open(self.raw,
                                            mem::transmute(&mut handle),
                                            if name.is_some() {name.unwrap().as_ptr()} else {ptr::null()},
                                            mem::transmute(&config.raw)
                                            )});
        Ok(KvHandle::from_raw(handle))
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

    /// Rollbacks file handle to specified seq num
    pub fn rollback(&self, seq_num: u64) -> FdbResult<()> {
        lift_error!(unsafe {ffi::fdb_rollback(mem::transmute(&self.raw),
                                              seq_num)})
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
        f.write_str(&format!("Database {{path: {}}}", self.path.display())[])
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

type GetDocFunc = unsafe extern fn(*mut ffi::fdb_kvs_handle, *mut ffi::fdb_doc) -> ffi::fdb_status;

#[derive(Clone)]
#[allow(raw_pointer_derive)]
struct InnerKvHandle {
    raw: *mut ffi::fdb_kvs_handle,
}

impl InnerKvHandle {
    fn from_raw(raw: *mut ffi::fdb_kvs_handle) -> InnerKvHandle {
        InnerKvHandle {raw: raw}
    }
}

impl Drop for InnerKvHandle {
    fn drop(&mut self) {
        unsafe {ffi::fdb_kvs_close(self.raw); }
    }
}


#[allow(missing_copy_implementations)]
pub enum ReadOnly {}
#[allow(missing_copy_implementations)]
pub enum ReadWrite {}

/// Represents ForestDB key value store
pub struct KvHandle<T> {
    inner: Rc<InnerKvHandle>
}

impl<Access> KvHandle<Access> {
    fn from_raw(raw: *mut ffi::fdb_kvs_handle) -> KvHandle<Access> {
        KvHandle {
            inner: Rc::new(InnerKvHandle::from_raw(raw)),
        }
    }

    /// Sets a value for key (plain KV mode)
    pub fn get_value<K>(&self, key: &K) -> FdbResult<Vec<u8>> where K: AsSlice<u8> {
        let doc = try!(InnerDoc::with_key(key));

        try_fdb!(unsafe {
            ffi::fdb_get(self.inner.raw, doc.raw)
        });
        Ok(unsafe {
            Vec::from_raw_buf(mem::transmute((*doc.raw).body),
                              (*doc.raw).bodylen as usize)
        })
    }

    /// Creates a new iterator
    pub fn key_iter<T, U>(&self, range: T, skip_deleted: bool) -> FdbResult<Iterator<U>> where T: KeyRange {
        let mut handle: *mut ffi::fdb_iterator = ptr::null_mut();
        let mut min_key = ptr::null();
        let mut min_key_len = 0;
        let mut max_key = ptr::null();
        let mut max_key_len = 0;

        if let Some(key) = range.min_key() {
            min_key = unsafe {mem::transmute(key.as_ptr())};
            min_key_len = key.len() as libc::size_t;
        }

        if let Some(key) = range.max_key() {
            max_key = unsafe {mem::transmute(key.as_ptr())};
            max_key_len = key.len() as libc::size_t;
        }

        let options = if skip_deleted {NO_DELETES} else {IteratorOptions::empty()};

        try_fdb!(unsafe {
            ffi::fdb_iterator_init(self.inner.raw, &mut handle,
                                   min_key, min_key_len,
                                   max_key, max_key_len,
                                   (range.options() | options).bits())
        });
        Ok(Iterator::from_raw(handle))
    }

    pub fn seq_iter<T, U>(&self, range: T, skip_deleted: bool) -> FdbResult<Iterator<U>> where T: SeqRange {
        let mut handle: *mut ffi::fdb_iterator = ptr::null_mut();
        let min_seq = range.min_seq();
        let max_seq = range.max_seq();

        let options = if skip_deleted {NO_DELETES} else {NONE};

        try_fdb!(unsafe {
            ffi::fdb_iterator_sequence_init(self.inner.raw, &mut handle,
                                   min_seq,
                                   max_seq,
                                   (range.options() | options).bits())
        });
        Ok(Iterator::from_raw(handle))
    }

    fn get_inner_doc<'l>(&self, loc: Location<'l>, f: GetDocFunc) -> FdbResult<InnerDoc> {
        use Location::*;

        let mut handle: *mut ffi::fdb_doc = ptr::null_mut();
        let mut key_ptr = ptr::null();
        let mut key_len = 0;

        if let Key(key) = loc {
            key_ptr = key.as_ptr();
            key_len = key.len();
        }

        try_fdb!(unsafe {ffi::fdb_doc_create(&mut handle,
                                             mem::transmute(key_ptr), key_len as libc::size_t,
                                             ptr::null(), 0,
                                             ptr::null(), 0)});

        unsafe {
            match loc {
                Offset(offset) => {
                    (*handle).offset = offset;
                },
                SeqNum(seq_num) => {
                    (*handle).seqnum = seq_num;
                },
                _ => ()
            }
        }

        let doc = InnerDoc::from_raw(handle);

        try_fdb!(unsafe{f(self.inner.raw, handle)});

        Ok(doc)
    }

    pub fn get_doc<'l>(&self, loc: Location<'l>) -> FdbResult<Doc> {
        use Location::*;

        let f: GetDocFunc = match loc {
            Key(_) => ffi::fdb_get,
            Offset(_) => ffi::fdb_get_byoffset,
            SeqNum(_) => ffi::fdb_get_byseq
        };
        let inner = try!(self.get_inner_doc(loc, f));
        Ok(Doc::with_inner(inner))
    }

    pub fn get_meta<'l>(&self, loc: Location<'l>) -> FdbResult<Meta> {
        use Location::*;

        let f: GetDocFunc = match loc {
            Key(_) => ffi::fdb_get_metaonly,
            Offset(_) => panic!("Can't get meta by offset"),
            SeqNum(_) => ffi::fdb_get_metaonly_byseq
        };
        let inner = try!(self.get_inner_doc(loc, f));
        Ok(Meta::with_inner(inner))
    }

    /// Returns max seq number of current store
    pub fn seq_num(&self) -> FdbResult<u64> {
        let mut result: u64 = 0;
        try_fdb!(unsafe {ffi::fdb_get_kvs_seqnum(self.inner.raw, &mut result)});
        Ok(result)
    }

    /// Creates a snapshot on specified seq_num
    pub fn snapshot(&self, seq_num: u64) -> FdbResult<KvHandle<ReadOnly>> {
        let mut handle: *mut ffi::fdb_kvs_handle = ptr::null_mut();
        try_fdb!(unsafe {ffi::fdb_snapshot_open(self.inner.raw, &mut handle, seq_num)});
        Ok(KvHandle::from_raw(handle))
    }
}

impl KvHandle<ReadWrite> {
    /// Retrieves a value by key (plain KV mode)
    pub fn set_value<K, V>(&self, key: &K, value: &V) -> FdbResult<()>
        where K: AsSlice<u8>,
              V: AsSlice<u8>
    {
        let mut doc = try!(InnerDoc::with_key(key));
        try!(doc.set_body(value));
        self.set_inner_doc(&doc)
    }

    pub fn del_value<K>(&self, key: &K) -> FdbResult<()>
        where K: AsSlice<u8>
    {
        let doc = try!(InnerDoc::with_key(key));
        self.del_inner_doc(&doc)
    }

    fn set_inner_doc(&self, doc: &InnerDoc) -> FdbResult<()> {
        lift_error!(unsafe {ffi::fdb_set(self.inner.raw, doc.raw)})
    }

    /// Sets the document
    pub fn set_doc(&self, doc: &Doc) -> FdbResult<()> {
        self.set_inner_doc(&doc.inner)
    }

    fn del_inner_doc(&self, doc: &InnerDoc) -> FdbResult<()> {
        lift_error!(unsafe {ffi::fdb_del(self.inner.raw, doc.raw)})
    }

    /// Deletes the document
    ///
    /// It's equivalent to
    ///
    /// ``` ignore
    /// doc.deleted = 1;
    /// store.set(doc)
    /// ```

    pub fn del_doc(&self, doc: &Doc) -> FdbResult<()>  {
        self.del_inner_doc(&doc.inner)
    }

    /// Deletes the document specified by available meta
    pub fn del_meta(&self, meta: &Meta) -> FdbResult<()>  {
        self.del_inner_doc(&meta.inner)
    }
}

impl<T> Clone for KvHandle<T> {
    fn clone(&self) -> KvHandle<T> {
        KvHandle {inner: self.inner.clone()}
    }
}

pub type Store = KvHandle<ReadWrite>;
pub type Snapshot = KvHandle<ReadOnly>;

pub struct Iterator<T> {
    raw: *mut ffi::fdb_iterator,
}

impl<T> Iterator<T> {
    fn from_raw(raw: *mut ffi::fdb_iterator) -> Iterator<T> {
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
        Ok(Doc::with_inner(InnerDoc::from_raw(handle)))
    }

    pub fn get_meta_only(&self) -> FdbResult<Meta> {
        let mut handle: *mut ffi::fdb_doc = ptr::null_mut();
        try_fdb!(unsafe {ffi::fdb_iterator_get_metaonly(self.raw, &mut handle)});
        Ok(Meta::with_inner(InnerDoc::from_raw(handle)))
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
                                                   mem::transmute(key.as_ptr()), key.len() as libc::size_t,
                                                   options as ffi::fdb_iterator_seek_opt_t)})
    }
}

#[unsafe_destructor]
impl<T> Drop for Iterator<T> {
    fn drop(&mut self) {
        unsafe {ffi::fdb_iterator_close(self.raw)};
    }
}

pub enum Location<'a> {
    Key(&'a [u8]),
    Offset(u64),
    SeqNum(u64)
}

impl<'a> Location<'a> {
    pub fn with_key<'k, K>(key: &'k K) -> Location<'k> where K: AsSlice<u8> {
        Location::Key(key.as_slice())
    }
}

struct InnerDoc {
    raw: *mut ffi::fdb_doc
}

impl InnerDoc {
    fn from_raw(handle: *mut ffi::fdb_doc) -> InnerDoc {
        InnerDoc {raw: handle}
    }

    fn with_key<K>(key: &K) -> FdbResult<InnerDoc> where K: AsSlice<u8> {
        let mut handle: *mut ffi::fdb_doc = ptr::null_mut();
        let key = key.as_slice();
        try_fdb!(unsafe {ffi::fdb_doc_create(&mut handle,
                                             mem::transmute(key.as_ptr()), key.len() as libc::size_t,
                                             ptr::null(), 0,
                                             ptr::null(), 0)});
        Ok(InnerDoc::from_raw(handle))
    }

    fn set_body<B>(&mut self, body: &B) -> FdbResult<()> where B: AsSlice<u8> {
        let body = body.as_slice();
        lift_error!(unsafe {ffi::fdb_doc_update(&mut self.raw,
                                                ptr::null(), 0,
                                                mem::transmute(body.as_ptr()), body.len() as libc::size_t)
        })
    }

    fn set_meta<M>(&mut self, meta: &M) -> FdbResult<()> where M: AsSlice<u8> {
        let meta = meta.as_slice();
        lift_error!(unsafe {ffi::fdb_doc_update(&mut self.raw,
                                                mem::transmute(meta.as_ptr()), meta.len() as libc::size_t,
                                                ptr::null(), 0)
        })
    }

    unsafe fn get_raw_part<'a>(&'a self, part: *mut libc::c_void, len: libc::size_t) -> Option<&'a [u8]> {
        if part == ptr::null_mut() {
            None
        } else {
            Some(from_raw_buf(mem::transmute(&part), len as usize))
        }
    }

    fn get_raw_meta<'a>(&'a self) -> Option<&'a [u8]> {
        unsafe { self.get_raw_part((*self.raw).meta, (*self.raw).metalen) }
    }

    /*
    fn get_meta<T:FromBytes>(&self) -> Option<T> {
        self.get_raw_meta().and_then(|x| FromBytes::from_bytes(x))
    }
    */

    fn get_raw_key<'a>(&'a self) -> Option<&'a [u8]> {
        unsafe { self.get_raw_part((*self.raw).key, (*self.raw).keylen) }
    }

    fn get_raw_body<'a>(&'a self) -> Option<&'a [u8]> {
        unsafe { self.get_raw_part((*self.raw).body, (*self.raw).bodylen) }
    }

    /*
    fn get_body<T:FromBytes>(&self) -> Option<T> {
        self.get_raw_body().and_then(|x| FromBytes::from_bytes(x))
    }
    */

    /// Sequence number assigned to the doc
    #[inline(always)]
    fn seq_num(&self) -> u64 {
        unsafe { (*self.raw).seqnum }
    }

    /// Is doc deleted?
    #[inline(always)]
    fn is_deleted(&self) -> bool {
        unsafe { (*self.raw).deleted != 0 }
    }

    /// Offset to the doc on disk
    #[inline(always)]
    fn offset(&self) -> u64 {
        unsafe { (*self.raw).offset }
    }
}

impl Drop for InnerDoc {
    fn drop(&mut self) {
        unsafe {ffi::fdb_doc_free(self.raw)};
    }
}

pub struct Doc {
    inner: InnerDoc
}

impl Doc {
    fn with_inner(inner: InnerDoc) -> Doc {
        Doc {inner: inner}
    }

    pub fn with_key<K>(key: &K) -> FdbResult<Doc> where K: AsSlice<u8> {
        let inner = try!(InnerDoc::with_key(key));
        Ok(Doc::with_inner(inner))
    }
}

impl DocKey for Doc {
    fn get_raw_key<'a>(&'a self) -> Option<&'a [u8]> {
        self.inner.get_raw_key()
    }
}

impl DocBody for Doc {
    fn set_body<B>(&mut self, body: &B) -> FdbResult<()> where B: AsSlice<u8> {
        self.inner.set_body(body)
    }

    fn set_meta<M>(&mut self, meta: &M) -> FdbResult<()> where M: AsSlice<u8> {
        self.inner.set_meta(meta)
    }

    fn get_raw_body<'a>(&'a self) -> Option<&'a [u8]> {
        self.inner.get_raw_body()
    }
}

impl DocMeta for Doc {
    fn get_raw_meta<'a>(&'a self) -> Option<&'a [u8]> {
        self.inner.get_raw_meta()
    }

    /// Sequence number assigned to the doc
    #[inline(always)]
    fn seq_num(&self) -> u64 {
        self.inner.seq_num()
    }

    /// Is doc deleted?
    #[inline(always)]
    fn is_deleted(&self) -> bool {
        self.inner.is_deleted()
    }

    /// Offset to the doc on disk
    #[inline(always)]
    fn offset(&self) -> u64 {
        self.inner.offset()
    }
}

pub struct Meta {
    inner: InnerDoc
}

impl Meta {
    fn with_inner(inner: InnerDoc) -> Meta {
        Meta {inner: inner}
    }

    pub fn into_doc(self) -> Doc {
        let tmp = self;
        Doc::with_inner(tmp.inner)
    }
}

impl DocKey for Meta {
    fn get_raw_key<'a>(&'a self) -> Option<&'a [u8]> {
        self.inner.get_raw_key()
    }
}

impl DocMeta for Meta {
    fn get_raw_meta<'a>(&'a self) -> Option<&'a [u8]> {
        self.inner.get_raw_meta()
    }

    /// Sequence number assigned to the doc
    #[inline(always)]
    fn seq_num(&self) -> u64 {
        self.inner.seq_num()
    }

    /// Is doc deleted?
    #[inline(always)]
    fn is_deleted(&self) -> bool {
        self.inner.is_deleted()
    }

    /// Offset to the doc on disk
    #[inline(always)]
    fn offset(&self) -> u64 {
        self.inner.offset()
    }
}

#[allow(dead_code)]
pub struct UnsafeDoc<'a> {
    raw: ffi::fdb_doc,
}

impl std::iter::Iterator for Iterator<Doc> {
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

impl std::iter::Iterator for Iterator<Meta> {
    type Item = Meta;

    fn next(&mut self) -> Option<Meta> {
        match self.get_meta_only() {
            Err(_) => return None,
            Ok(meta) => {
                let _ = self.to_next();
                Some(meta)
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
}


impl<T: AsSlice<u8>> KeyRange for std::ops::RangeTo<T> {
    fn min_key(&self) -> Option<&[u8]> {
        None
    }

    fn max_key(&self) -> Option<&[u8]> {
        Some(self.end.as_slice())
    }
}

impl SeqRange for FullRange {
    fn min_seq(&self) -> u64 {
        0
    }

    fn max_seq(&self) -> u64 {
        0
    }
}

macro_rules! uint_seq_iter_impl {
    ($t:ty) => (
        impl SeqRange for std::ops::Range<$t> {
            fn min_seq(&self) -> u64 {
                self.start as u64
            }

            fn max_seq(&self) -> u64 {
                // Range in Rust excludes high end
                // so to use the same semantics we have to
                // decrease end as SKIP_MAX_KEY skips key
                // and has no effect on seq number
                (self.end - 1) as u64
            }
        }

        impl SeqRange for std::ops::RangeFrom<$t> {
            fn min_seq(&self) -> u64 {
                self.start as u64
            }

            fn max_seq(&self) -> u64 {
                0
            }
        }

        impl SeqRange for std::ops::RangeTo<$t> {
            fn min_seq(&self) -> u64 {
                0
            }

            fn max_seq(&self) -> u64 {
                self.end as u64
            }
        }

        )
}

uint_seq_iter_impl!(u8);
uint_seq_iter_impl!(u16);
uint_seq_iter_impl!(u32);
uint_seq_iter_impl!(u64);
uint_seq_iter_impl!(usize);

impl FromBytes for String {
    fn from_bytes(bytes: &[u8]) -> Option<String> {
        std::str::from_utf8(bytes).ok().map(|s| s.to_string())
    }
}

impl FromBytes for Vec<u8> {
    fn from_bytes(bytes: &[u8]) -> Option<Vec<u8>> {
        Some(bytes.to_vec())
    }
}

/// Beware - not endiannes safe
impl FromBytes for u64 {
    fn from_bytes(bytes: &[u8]) -> Option<u64> {
        unsafe {
            let data_ptr: *const u64 = mem::transmute(bytes.as_ptr());
            Some(*data_ptr)
        }
    }
}

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
    use super::{FileHandle, Error, Location, Iterator, Doc, CommitOptions, DocBody, DocMeta, DocKey};

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

        println!("db is {}", cur_test);
        db_dir.join(format!("db-{}", cur_test))
    }

    #[test]
    fn test_error_msg(){
        let err = Error::from_code(ffi::FDB_RESULT_OPEN_FAIL);
        assert_eq!("error opening file", (&err as &error::Error).detail().unwrap().as_slice());
    }

    #[test]
    fn test_open_file() {
        assert!(super::init(Default::default()).is_ok());
        assert!(FileHandle::open(&next_db_path(), Default::default()).is_ok());
    }

    #[test]
    fn test_file_handle_clone() {
        let db1 = FileHandle::open(&next_db_path(), Default::default()).unwrap();
        let db2 = db1.clone();

        let store = db1.get_default_store(Default::default()).unwrap();
        assert!(store.set_value(&"hello".as_bytes(), &"world".as_bytes()).is_ok());
        assert!(db1.commit(CommitOptions::Normal).is_ok());

        let _ = Thread::scoped(move || {
            let store = db2.get_default_store(Default::default()).unwrap();
            let value = store.get_value(&"hello".as_bytes()).unwrap();
            assert_eq!("world".as_bytes(), value);
        }).join();
    }

    #[test]
    fn test_open_store() {
        let db = FileHandle::open(&next_db_path(), Default::default()).unwrap();
        assert!(db.get_default_store(Default::default()).is_ok());
        assert!(db.get_store("hello", Default::default()).is_ok())
    }

    #[test]
    fn test_clone_store() {
        let db = FileHandle::open(&next_db_path(), Default::default()).unwrap();
        let store = db.get_store("hello", Default::default()).unwrap();
        let store2 = store.clone();
        store2.set_value(&"hello".as_bytes(), &"world".as_bytes()).unwrap();
        // It should not fail after dropping both stores
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

        let keys: Vec<_> = vec!["a", "b", "c", "d", "e"].iter().map(|s| s.to_string()).collect();
        for k in keys.iter() {
            assert!(store.set_value(&k.as_bytes(), &k.as_bytes()).is_ok());
        }

        assert!(db.commit(CommitOptions::Normal).is_ok());

        let iter: Iterator<Doc> = store.key_iter(FullRange, false).unwrap();
        let values: Vec<_> = iter.map(|doc| doc.get_body::<String>().unwrap()).collect();
        assert_eq!(values, keys);

        let sub_iter: Iterator<Doc> = store.key_iter("b".as_bytes().."d".as_bytes(), false).unwrap();
        let values: Vec<_> = sub_iter.map(|doc| doc.get_body::<String>().unwrap()).collect();
        assert_eq!(values.as_slice(), &keys[1..3]);
    }

    #[test]
    fn test_seq_iterator() {
        let db = FileHandle::open(&next_db_path(), Default::default()).unwrap();
        let store = db.get_default_store(Default::default()).unwrap();

        let keys: Vec<_> = vec!["a", "b", "c", "d", "e"];
        for k in keys.iter() {
            assert!(store.set_value(&k.as_bytes(), &k.as_bytes()).is_ok());
        }
        assert!(db.commit(CommitOptions::Normal).is_ok());

        let iter: Iterator<Doc> = store.seq_iter(FullRange, false).unwrap();
        let seq_nums: Vec<_> = iter.map(|doc| doc.seq_num()).collect();
        assert_eq!(seq_nums, (1..keys.len() + 1).map(|x| x as u64).collect::<Vec<_>>());

        let start = 2us;
        let end = 4;
        let sub_iter: Iterator<Doc> = store.seq_iter(start..end, false).unwrap();

        let seq_nums: Vec<_> = sub_iter.map(|doc| doc.seq_num()).collect();
        assert_eq!(seq_nums, (start..end).map(|x| x as u64).collect::<Vec<_>>());
    }

    #[test]
    fn test_doc_locator() {
        let db = FileHandle::open(&next_db_path(), Default::default()).unwrap();
        let store = db.get_default_store(Default::default()).unwrap();

        let keys: Vec<_> = vec!["a", "b", "c", "d", "e"];
        for k in keys.iter() {
            assert!(store.set_value(&k.as_bytes(), &k.as_bytes()).is_ok());
        }
        assert!(db.commit(CommitOptions::Normal).is_ok());

        let doc1 = store.get_doc(Location::with_key(&"a".as_bytes())).unwrap();
        let doc2 = store.get_doc(Location::SeqNum(doc1.seq_num())).unwrap();
        let doc3 = store.get_doc(Location::Offset(doc1.offset())).unwrap();
        let v1: String = doc1.get_body().unwrap();
        let v2: String = doc2.get_body().unwrap();
        let v3: String = doc3.get_body().unwrap();

        assert_eq!(v1, v2);
        assert_eq!(v2, v3);

        assert_eq!(doc1.seq_num(), doc2.seq_num());
        assert_eq!(doc2.seq_num(), doc3.seq_num());

        assert_eq!(doc1.offset(), doc2.offset());
        assert_eq!(doc2.offset(), doc3.offset());
    }

    #[test]
    fn test_access_through_meta() {
        let db = FileHandle::open(&next_db_path(), Default::default()).unwrap();
        let store = db.get_default_store(Default::default()).unwrap();

        let key = "key";
        let value = "value";
        assert!(store.set_value(&key.as_bytes(), &value.as_bytes()).is_ok());

        let meta1 = store.get_meta(Location::with_key(&key.as_bytes())).unwrap();
        let meta2 = store.get_meta(Location::SeqNum(meta1.seq_num())).unwrap();

        assert_eq!(meta1.get_raw_key(), meta2.get_raw_key());

        let mut doc = meta1.into_doc();
        let new_value = "new_value";
        assert!(doc.set_body(&new_value.as_bytes()).is_ok());
        assert!(store.set_doc(&doc).is_ok());

        let doc2 = store.get_doc(Location::SeqNum(doc.seq_num())).unwrap();
        let v2: String = doc2.get_body().unwrap();

        assert_eq!(v2.as_slice(), new_value);
    }
}
