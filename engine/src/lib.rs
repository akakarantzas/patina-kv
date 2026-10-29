pub mod sstable;
pub mod wal;

use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Path, PathBuf};
use std::ptr;
use std::sync::RwLock;

use sstable::SsTable;
use wal::{Record, Wal};

pub struct Engine {
    dir: PathBuf,
    options: Options,
    state: RwLock<State>,
}

/// Engine settings; the C ABI always uses the defaults.
pub struct Options {
    /// A write that takes the bytes written since the last flush past this flushes the memtable to an SSTable.
    pub memtable_limit_bytes: usize,
}

impl Default for Options {
    fn default() -> Options {
        Options { memtable_limit_bytes: 4 * 1024 * 1024 }
    }
}

/// A deleted key stays in the memtable as a Tombstone so older SSTables can't show through it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    Value(Vec<u8>),
    Tombstone,
}

// The WAL shares the memtable's lock, so WAL order always matches memtable order.
struct State {
    memtable: BTreeMap<Vec<u8>, Value>,
    /// Key and value bytes written since the last flush. Overwrites count again, so this also bounds the WAL size.
    memtable_bytes: usize,
    wal: Wal,
    /// Newest seq first.
    sstables: Vec<SsTable>,
    next_seq: u64,
}

/// Applies `record` to the memtable and returns the key and value bytes it wrote.
fn apply(memtable: &mut BTreeMap<Vec<u8>, Value>, record: Record) -> usize {
    let (key, value) = match record {
        Record::Put { key, value } => (key, Value::Value(value)),
        Record::Delete { key } => (key, Value::Tombstone),
    };
    let len = key.len() + if let Value::Value(v) = &value { v.len() } else { 0 };
    memtable.insert(key, value);
    len
}

fn sst_path(dir: &Path, seq: u64) -> PathBuf {
    dir.join(format!("{seq:06}.sst"))
}

impl Engine {
    /// Creates `dir` if needed, deletes leftover `*.tmp` files, opens its `<seq>.sst` files
    /// and replays `<dir>/wal.log` into the memtable.
    /// shortcut: no lock file, so two processes opening the same dir corrupt each other's WAL; add one before running more than one instance.
    pub fn open(dir: &Path, options: Options) -> io::Result<Engine> {
        fs::create_dir_all(dir)?;
        // Same Unix-only rule as wal.rs: make a newly created dir's own entry durable before any write is acked.
        #[cfg(unix)]
        {
            let parent = dir.parent().filter(|d| !d.as_os_str().is_empty()).unwrap_or(Path::new("."));
            fs::File::open(parent)?.sync_all()?;
        }
        let mut tables = BTreeMap::new();
        for entry in fs::read_dir(dir)? {
            let path = entry?.path();
            match path.extension().and_then(|e| e.to_str()) {
                // A flush that never reached its rename; the WAL is reset only after the rename, so it still holds the data.
                Some("tmp") => fs::remove_file(&path)?,
                Some("sst") => {
                    let seq = path.file_stem().and_then(|s| s.to_str()?.parse::<u64>().ok()).ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidData, format!("unexpected SSTable name {}", path.display()))
                    })?;
                    tables.insert(seq, path);
                }
                _ => {}
            }
        }
        let next_seq = tables.keys().next_back().map_or(1, |seq| seq + 1);
        let sstables = tables.values().rev().map(|path| SsTable::open(path)).collect::<io::Result<_>>()?;

        let (wal, records) = Wal::open(&dir.join("wal.log"))?;
        let mut memtable = BTreeMap::new();
        let mut memtable_bytes = 0;
        for record in records {
            memtable_bytes += apply(&mut memtable, record);
        }
        let state = State { memtable, memtable_bytes, wal, sstables, next_seq };
        Ok(Engine { dir: dir.to_path_buf(), options, state: RwLock::new(state) })
    }

    /// Returns Ok only once the write is on disk; on error the memtable is unchanged.
    pub fn put(&self, key: &[u8], value: &[u8]) -> io::Result<()> {
        self.write(Record::Put { key: key.to_vec(), value: value.to_vec() })
    }

    /// Same durability as `put`. Deleting a missing key succeeds and still records a tombstone.
    pub fn delete(&self, key: &[u8]) -> io::Result<()> {
        self.write(Record::Delete { key: key.to_vec() })
    }

    fn write(&self, record: Record) -> io::Result<()> {
        let mut state = self.state.write().unwrap();
        state.wal.append(&record)?;
        state.memtable_bytes += apply(&mut state.memtable, record);
        if state.memtable_bytes > self.options.memtable_limit_bytes {
            // The write is already durable in the WAL, so a failed flush must not fail it; the next write retries.
            if let Err(e) = self.flush(&mut state) {
                eprintln!("patina: flush to an SSTable failed, retrying on the next write: {e}");
            }
        }
        Ok(())
    }

    /// Writes the memtable to the next `<seq>.sst` and then empties the WAL. On error nothing in `state` changes.
    fn flush(&self, state: &mut State) -> io::Result<()> {
        let path = sst_path(&self.dir, state.next_seq);
        let tmp = path.with_extension("sst.tmp");
        sstable::write(&tmp, &state.memtable, 0)?;
        fs::rename(&tmp, &path)?;
        // Same Unix-only rule as wal.rs: the rename must be durable before the WAL stops holding the data.
        #[cfg(unix)]
        fs::File::open(&self.dir)?.sync_all()?;
        let table = SsTable::open(&path)?;
        // A crash before this reset leaves the data in both places: replay rebuilds exactly this table's entries, which shadow it harmlessly.
        state.wal.reset()?;
        state.sstables.insert(0, table);
        state.next_seq += 1;
        state.memtable.clear();
        state.memtable_bytes = 0;
        Ok(())
    }

    /// Looks in the memtable, then the SSTables newest first. The first entry found wins, so a tombstone hides older values.
    pub fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        let state = self.state.read().unwrap();
        let found = match state.memtable.get(key) {
            Some(entry) => Some(entry.clone()),
            None => state.sstables.iter().find_map(|table| table.get(key).transpose()).transpose()?,
        };
        Ok(match found {
            Some(Value::Value(v)) => Some(v),
            Some(Value::Tombstone) | None => None,
        })
    }
}

// ---- C ABI used by service/src/main/java/dev/patina/engine/PatinaEngine.java ----
// Every entry point catches panics: unwinding into the JVM is undefined behaviour.

pub const OK: i32 = 0;
pub const NOT_FOUND: i32 = 1;
pub const ERR: i32 = -1;

unsafe fn bytes<'a>(p: *const u8, len: usize) -> &'a [u8] {
    if len == 0 { &[] } else { unsafe { std::slice::from_raw_parts(p, len) } }
}

/// `path` is a UTF-8 data directory path. Returns null on failure, with the reason on stderr. Release with `patina_close`.
///
/// # Safety
/// `path` must point to `path_len` readable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn patina_open(path: *const u8, path_len: usize) -> *mut Engine {
    catch_unwind(AssertUnwindSafe(|| {
        let dir = match std::str::from_utf8(unsafe { bytes(path, path_len) }) {
            Ok(dir) => dir,
            Err(e) => {
                eprintln!("patina_open: data dir path is not UTF-8: {e}");
                return ptr::null_mut();
            }
        };
        match Engine::open(Path::new(dir), Options::default()) {
            Ok(engine) => Box::into_raw(Box::new(engine)),
            Err(e) => {
                eprintln!("patina_open: cannot open data dir {dir}: {e}");
                ptr::null_mut()
            }
        }
    }))
    .unwrap_or(ptr::null_mut())
}

/// # Safety
/// `engine` must be null or a handle from `patina_open`; no other call on it may be running or start after this one.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn patina_close(engine: *mut Engine) {
    if !engine.is_null() {
        let _ = catch_unwind(AssertUnwindSafe(|| drop(unsafe { Box::from_raw(engine) })));
    }
}

/// # Safety
/// `engine` must be null or an open handle; `key` and `value` must point to `key_len` and `value_len` readable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn patina_put(
    engine: *const Engine,
    key: *const u8,
    key_len: usize,
    value: *const u8,
    value_len: usize,
) -> i32 {
    if engine.is_null() {
        return ERR;
    }
    catch_unwind(AssertUnwindSafe(|| unsafe {
        match (*engine).put(bytes(key, key_len), bytes(value, value_len)) {
            Ok(()) => OK,
            Err(e) => {
                eprintln!("patina_put: {e}");
                ERR
            }
        }
    }))
    .unwrap_or(ERR)
}

/// OK also when the key does not exist.
///
/// # Safety
/// `engine` must be null or an open handle; `key` must point to `key_len` readable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn patina_delete(engine: *const Engine, key: *const u8, key_len: usize) -> i32 {
    if engine.is_null() {
        return ERR;
    }
    catch_unwind(AssertUnwindSafe(|| unsafe {
        match (*engine).delete(bytes(key, key_len)) {
            Ok(()) => OK,
            Err(e) => {
                eprintln!("patina_delete: {e}");
                ERR
            }
        }
    }))
    .unwrap_or(ERR)
}

/// On OK, `*out`/`*out_len` hold a Rust-owned buffer the caller must release with `patina_free`.
///
/// # Safety
/// `engine` must be null or an open handle; `key` must point to `key_len` readable bytes;
/// `out` and `out_len` must be null or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn patina_get(
    engine: *const Engine,
    key: *const u8,
    key_len: usize,
    out: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    if engine.is_null() || out.is_null() || out_len.is_null() {
        return ERR;
    }
    catch_unwind(AssertUnwindSafe(|| unsafe {
        match (*engine).get(bytes(key, key_len)) {
            Ok(None) => NOT_FOUND,
            Ok(Some(v)) => {
                let buf = v.into_boxed_slice();
                *out_len = buf.len();
                *out = Box::into_raw(buf) as *mut u8;
                OK
            }
            Err(e) => {
                eprintln!("patina_get: {e}");
                ERR
            }
        }
    }))
    .unwrap_or(ERR)
}

/// # Safety
/// `buf` and `len` must be null or exactly what one `patina_get` call returned, and each buffer is freed once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn patina_free(buf: *mut u8, len: usize) {
    if !buf.is_null() {
        drop(unsafe { Box::from_raw(ptr::slice_from_raw_parts_mut(buf, len)) });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wal::MAX_RECORD;

    #[test]
    fn ffi_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        unsafe {
            let e = patina_open(path.as_ptr(), path.len());
            assert!(!e.is_null());
            let (k, v) = (b"user#1", b"alice");
            assert_eq!(patina_put(e, k.as_ptr(), k.len(), v.as_ptr(), v.len()), OK);
            let huge = vec![0; MAX_RECORD];
            assert_eq!(patina_put(e, k.as_ptr(), k.len(), huge.as_ptr(), huge.len()), ERR);

            let (mut out, mut out_len) = (ptr::null_mut(), 0usize);
            assert_eq!(patina_get(e, k.as_ptr(), k.len(), &mut out, &mut out_len), OK);
            assert_eq!(std::slice::from_raw_parts(out, out_len), v);
            patina_free(out, out_len);

            let missing = b"nope";
            assert_eq!(patina_get(e, missing.as_ptr(), missing.len(), &mut out, &mut out_len), NOT_FOUND);

            assert_eq!(patina_delete(e, k.as_ptr(), k.len()), OK);
            assert_eq!(patina_get(e, k.as_ptr(), k.len(), &mut out, &mut out_len), NOT_FOUND);
            assert_eq!(patina_delete(e, missing.as_ptr(), missing.len()), OK);
            assert_eq!(patina_delete(ptr::null(), k.as_ptr(), k.len()), ERR);
            patina_close(e);
        }
    }

    #[test]
    fn open_rejects_non_utf8_path() {
        assert!(unsafe { patina_open([0xFF].as_ptr(), 1) }.is_null());
    }

    #[test]
    fn writes_survive_reopen() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("not/yet/created");
        let e = Engine::open(&dir, Options::default()).unwrap();
        e.put(b"k", b"v1").unwrap();
        e.put(b"other", b"x").unwrap();
        e.put(b"k", b"v2").unwrap();
        drop(e);

        let e = Engine::open(&dir, Options::default()).unwrap();
        assert_eq!(e.get(b"k").unwrap(), Some(b"v2".to_vec()));
        assert_eq!(e.get(b"other").unwrap(), Some(b"x".to_vec()));
    }

    #[test]
    fn delete_hides_key_and_keeps_a_tombstone() {
        let dir = tempfile::tempdir().unwrap();
        let e = Engine::open(dir.path(), Options::default()).unwrap();
        e.put(b"k", b"v").unwrap();
        e.delete(b"k").unwrap();
        assert_eq!(e.get(b"k").unwrap(), None);
        assert_eq!(e.state.read().unwrap().memtable.get(&b"k"[..]), Some(&Value::Tombstone));

        e.delete(b"never-existed").unwrap();
        assert_eq!(e.get(b"never-existed").unwrap(), None);
    }

    #[test]
    fn delete_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let e = Engine::open(dir.path(), Options::default()).unwrap();
        e.put(b"k", b"v").unwrap();
        e.put(b"other", b"x").unwrap();
        e.delete(b"k").unwrap();
        e.delete(b"never-existed").unwrap();
        drop(e);

        let e = Engine::open(dir.path(), Options::default()).unwrap();
        assert_eq!(e.get(b"k").unwrap(), None);
        assert_eq!(e.get(b"other").unwrap(), Some(b"x".to_vec()));
    }

    #[test]
    fn put_after_delete_brings_key_back_and_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let e = Engine::open(dir.path(), Options::default()).unwrap();
        e.put(b"k", b"old").unwrap();
        e.delete(b"k").unwrap();
        e.put(b"k", b"new").unwrap();
        assert_eq!(e.get(b"k").unwrap(), Some(b"new".to_vec()));
        drop(e);

        let e = Engine::open(dir.path(), Options::default()).unwrap();
        assert_eq!(e.get(b"k").unwrap(), Some(b"new".to_vec()));
    }

    #[test]
    fn failed_put_leaves_memtable_unchanged() {
        let dir = tempfile::tempdir().unwrap();
        let e = Engine::open(dir.path(), Options::default()).unwrap();
        e.put(b"k", b"old").unwrap();
        assert!(e.put(b"k", &vec![0; MAX_RECORD]).is_err());
        assert_eq!(e.get(b"k").unwrap(), Some(b"old".to_vec()));
    }

    #[test]
    fn empty_value_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let e = Engine::open(dir.path(), Options::default()).unwrap();
        e.put(b"k", b"").unwrap();
        assert_eq!(e.get(b"k").unwrap(), Some(vec![]));
    }

    fn sst_count(dir: &Path) -> usize {
        fs::read_dir(dir).unwrap().filter(|entry| entry.as_ref().unwrap().path().extension().is_some_and(|ext| ext == "sst")).count()
    }

    #[test]
    fn reads_span_many_sstables_and_survive_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let tiny = || Options { memtable_limit_bytes: 64 };
        let key = |i: u32| i.to_be_bytes();
        let e = Engine::open(dir.path(), tiny()).unwrap();
        for i in 0..100 {
            e.put(&key(i), format!("old{i}").as_bytes()).unwrap();
        }
        drop(e);
        // Reopened between the passes, so these flushes must pick seqs after the existing files.
        let e = Engine::open(dir.path(), tiny()).unwrap();
        for i in (0..100).step_by(3) {
            e.put(&key(i), format!("new{i}").as_bytes()).unwrap();
        }
        for i in (0..100).step_by(5) {
            e.delete(&key(i)).unwrap();
        }
        assert!(sst_count(dir.path()) >= 3, "only {} SSTables", sst_count(dir.path()));

        let check = |e: &Engine| {
            for i in 0..100 {
                let expected = if i % 5 == 0 { None } else if i % 3 == 0 { Some(format!("new{i}")) } else { Some(format!("old{i}")) };
                assert_eq!(e.get(&key(i)).unwrap(), expected.map(String::into_bytes), "key {i}");
            }
        };
        check(&e);
        drop(e);
        check(&Engine::open(dir.path(), tiny()).unwrap());
    }

    #[test]
    fn stray_tmp_file_is_deleted_on_open_and_never_read() {
        let dir = tempfile::tempdir().unwrap();
        // A complete, valid table, so only its name keeps it from being read.
        let tmp = dir.path().join("000001.sst.tmp");
        sstable::write(&tmp, [(b"k", Value::Value(b"v".to_vec()))], 0).unwrap();
        let e = Engine::open(dir.path(), Options::default()).unwrap();
        assert!(!tmp.exists());
        assert_eq!(e.get(b"k").unwrap(), None);
    }

    #[test]
    fn crash_between_rename_and_wal_reset_replays_harmlessly() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("wal.log");
        let e = Engine::open(dir.path(), Options::default()).unwrap();
        e.put(b"gone", b"x").unwrap();
        e.flush(&mut e.state.write().unwrap()).unwrap(); // an older table holds "gone"
        e.put(b"k", b"old").unwrap();
        e.put(b"k", b"new").unwrap();
        e.delete(b"gone").unwrap();
        let wal_before = fs::read(&wal_path).unwrap();
        e.flush(&mut e.state.write().unwrap()).unwrap();
        assert_eq!(fs::metadata(&wal_path).unwrap().len(), 0);
        drop(e);

        fs::write(&wal_path, wal_before).unwrap(); // as if the process died just before the WAL reset
        let e = Engine::open(dir.path(), Options::default()).unwrap();
        assert_eq!(e.get(b"k").unwrap(), Some(b"new".to_vec()));
        assert_eq!(e.get(b"gone").unwrap(), None);
        assert_eq!(sst_count(dir.path()), 2);
    }

    #[test]
    fn failed_flush_keeps_the_write_and_retries_on_the_next_one() {
        let dir = tempfile::tempdir().unwrap();
        let e = Engine::open(dir.path(), Options { memtable_limit_bytes: 10 }).unwrap();
        // A directory where the flush wants its temp file, so creating that file fails.
        let blocker = dir.path().join("000001.sst.tmp");
        fs::create_dir(&blocker).unwrap();
        e.put(b"a", b"1").unwrap();
        e.put(b"k", b"0123456789").unwrap(); // over the limit: this write's flush fails
        assert_eq!(e.get(b"k").unwrap(), Some(b"0123456789".to_vec()));
        assert_eq!(sst_count(dir.path()), 0);

        fs::remove_dir(&blocker).unwrap();
        e.put(b"b", b"2").unwrap(); // the retry succeeds
        assert_eq!(sst_count(dir.path()), 1);
        assert!(e.state.read().unwrap().memtable.is_empty());
        let check = |e: &Engine| {
            for (k, v) in [("a", "1"), ("k", "0123456789"), ("b", "2")] {
                assert_eq!(e.get(k.as_bytes()).unwrap(), Some(v.as_bytes().to_vec()), "{k}");
            }
        };
        check(&e);
        drop(e);
        check(&Engine::open(dir.path(), Options::default()).unwrap());
    }
}
