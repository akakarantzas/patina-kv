pub mod wal;

use std::collections::BTreeMap;
use std::io;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::Path;
use std::ptr;
use std::sync::RwLock;

use wal::{Record, Wal};

pub struct Engine {
    state: RwLock<State>,
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
    wal: Wal,
}

fn apply(memtable: &mut BTreeMap<Vec<u8>, Value>, record: Record) {
    match record {
        Record::Put { key, value } => memtable.insert(key, Value::Value(value)),
        Record::Delete { key } => memtable.insert(key, Value::Tombstone),
    };
}

impl Engine {
    /// Creates `dir` if needed and replays `<dir>/wal.log` into the memtable.
    /// shortcut: no lock file, so two processes opening the same dir corrupt each other's WAL; add one before running more than one instance.
    pub fn open(dir: &Path) -> io::Result<Engine> {
        std::fs::create_dir_all(dir)?;
        // Same Unix-only rule as wal.rs: make a newly created dir's own entry durable before any write is acked.
        #[cfg(unix)]
        {
            let parent = dir.parent().filter(|d| !d.as_os_str().is_empty()).unwrap_or(Path::new("."));
            std::fs::File::open(parent)?.sync_all()?;
        }
        let (wal, records) = Wal::open(&dir.join("wal.log"))?;
        let mut memtable = BTreeMap::new();
        for record in records {
            apply(&mut memtable, record);
        }
        Ok(Engine { state: RwLock::new(State { memtable, wal }) })
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
        apply(&mut state.memtable, record);
        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        match self.state.read().unwrap().memtable.get(key) {
            Some(Value::Value(v)) => Some(v.clone()),
            Some(Value::Tombstone) | None => None,
        }
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
        match Engine::open(Path::new(dir)) {
            Ok(engine) => Box::into_raw(Box::new(engine)),
            Err(e) => {
                eprintln!("patina_open: cannot open data dir {dir}: {e}");
                ptr::null_mut()
            }
        }
    }))
    .unwrap_or(ptr::null_mut())
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn patina_close(engine: *mut Engine) {
    if !engine.is_null() {
        let _ = catch_unwind(AssertUnwindSafe(|| drop(unsafe { Box::from_raw(engine) })));
    }
}

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
            None => NOT_FOUND,
            Some(v) => {
                let buf = v.into_boxed_slice();
                *out_len = buf.len();
                *out = Box::into_raw(buf) as *mut u8;
                OK
            }
        }
    }))
    .unwrap_or(ERR)
}

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
        let e = Engine::open(&dir).unwrap();
        e.put(b"k", b"v1").unwrap();
        e.put(b"other", b"x").unwrap();
        e.put(b"k", b"v2").unwrap();
        drop(e);

        let e = Engine::open(&dir).unwrap();
        assert_eq!(e.get(b"k"), Some(b"v2".to_vec()));
        assert_eq!(e.get(b"other"), Some(b"x".to_vec()));
    }

    #[test]
    fn delete_hides_key_and_keeps_a_tombstone() {
        let dir = tempfile::tempdir().unwrap();
        let e = Engine::open(dir.path()).unwrap();
        e.put(b"k", b"v").unwrap();
        e.delete(b"k").unwrap();
        assert_eq!(e.get(b"k"), None);
        assert_eq!(e.state.read().unwrap().memtable.get(&b"k"[..]), Some(&Value::Tombstone));

        e.delete(b"never-existed").unwrap();
        assert_eq!(e.get(b"never-existed"), None);
    }

    #[test]
    fn delete_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let e = Engine::open(dir.path()).unwrap();
        e.put(b"k", b"v").unwrap();
        e.put(b"other", b"x").unwrap();
        e.delete(b"k").unwrap();
        e.delete(b"never-existed").unwrap();
        drop(e);

        let e = Engine::open(dir.path()).unwrap();
        assert_eq!(e.get(b"k"), None);
        assert_eq!(e.get(b"other"), Some(b"x".to_vec()));
    }

    #[test]
    fn put_after_delete_brings_key_back_and_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let e = Engine::open(dir.path()).unwrap();
        e.put(b"k", b"old").unwrap();
        e.delete(b"k").unwrap();
        e.put(b"k", b"new").unwrap();
        assert_eq!(e.get(b"k"), Some(b"new".to_vec()));
        drop(e);

        let e = Engine::open(dir.path()).unwrap();
        assert_eq!(e.get(b"k"), Some(b"new".to_vec()));
    }

    #[test]
    fn failed_put_leaves_memtable_unchanged() {
        let dir = tempfile::tempdir().unwrap();
        let e = Engine::open(dir.path()).unwrap();
        e.put(b"k", b"old").unwrap();
        assert!(e.put(b"k", &vec![0; MAX_RECORD]).is_err());
        assert_eq!(e.get(b"k"), Some(b"old".to_vec()));
    }

    #[test]
    fn empty_value_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let e = Engine::open(dir.path()).unwrap();
        e.put(b"k", b"").unwrap();
        assert_eq!(e.get(b"k"), Some(vec![]));
    }
}
