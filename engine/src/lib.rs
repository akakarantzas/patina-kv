pub mod sstable;
pub mod wal;

use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::ops::Bound;
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

/// Engine settings; the C ABI sets only `sync_writes` and keeps the other defaults.
pub struct Options {
    /// A write that takes the bytes written since the last flush past this flushes the memtable to an SSTable.
    pub memtable_limit_bytes: usize,
    /// A flush that leaves more SSTables than this merges them all into one.
    pub max_sstables: usize,
    /// Sync the WAL before a write returns (the default). Off, the guarantee drops to process-crash safety:
    /// an acked write has reached the OS but not the disk, so power loss or an OS crash can lose it.
    pub sync_writes: bool,
}

impl Default for Options {
    fn default() -> Options {
        Options { memtable_limit_bytes: 4 * 1024 * 1024, max_sstables: 4, sync_writes: true }
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
    /// (seq, table), newest seq first.
    sstables: Vec<(u64, SsTable)>,
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

/// Closes `tables`, then deletes their files: Windows can't fully delete a file that is still open.
fn remove_tables(dir: &Path, tables: Vec<(u64, SsTable)>) -> io::Result<()> {
    let seqs: Vec<u64> = tables.iter().map(|&(seq, _)| seq).collect();
    drop(tables);
    seqs.into_iter().try_for_each(|seq| fs::remove_file(sst_path(dir, seq)))
}

/// Streams the live entries at or after `start` in key order from `memtable` and then `tables` (newest first),
/// reading one table value at a time. Per key the newest source wins; a key whose newest entry is a tombstone is skipped.
// shortcut: finds the smallest key by checking every source, O(sources) per key; use a BinaryHeap if max_sstables grows large.
fn merge<'a>(
    memtable: &'a BTreeMap<Vec<u8>, Value>,
    tables: &'a [(u64, SsTable)],
    start: &'a [u8],
) -> impl Iterator<Item = io::Result<(&'a [u8], Value)>> {
    let memtable_keys: Box<dyn Iterator<Item = &'a [u8]> + 'a> =
        Box::new(memtable.range::<[u8], _>((Bound::Included(start), Bound::Unbounded)).map(|(key, _)| key.as_slice()));
    let table_keys = tables.iter().map(|(_, table)| Box::new(table.keys(start)) as Box<dyn Iterator<Item = &'a [u8]> + 'a>);
    let mut cursors: Vec<_> = std::iter::once(memtable_keys).chain(table_keys).map(Iterator::peekable).collect();
    std::iter::from_fn(move || {
        loop {
            // min_by_key returns the first of equal keys, and the sources are newest first.
            let (newest, key) =
                cursors.iter_mut().enumerate().filter_map(|(i, keys)| Some((i, *keys.peek()?))).min_by_key(|&(_, key)| key)?;
            for keys in &mut cursors {
                keys.next_if_eq(&key);
            }
            let entry = match newest {
                0 => Ok(memtable[key].clone()),
                i => tables[i - 1].1.get(key).transpose()?,
            };
            match entry {
                Ok(Value::Tombstone) => {}
                entry => return Some(entry.map(|value| (key, value))),
            }
        }
    })
}

impl Engine {
    /// Creates `dir` if needed, deletes leftover `*.tmp` files, opens its `<seq>.sst` files, deletes the ones
    /// a compaction already merged, and replays `<dir>/wal.log` into the memtable.
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
        let mut sstables =
            tables.into_iter().rev().map(|(seq, path)| Ok((seq, SsTable::open(&path)?))).collect::<io::Result<Vec<_>>>()?;
        // A crash between a compaction's rename and its deletes leaves inputs behind that may still hold values
        // whose tombstones the merge dropped. Every seq up to the newest compacted_through was an input.
        let through = sstables.iter().map(|(_, table)| table.compacted_through()).max().unwrap_or(0);
        let merged = sstables.split_off(sstables.partition_point(|&(seq, _)| seq > through));
        remove_tables(dir, merged)?;

        let (mut wal, records) = Wal::open(&dir.join("wal.log"))?;
        wal.sync = options.sync_writes;
        let mut memtable = BTreeMap::new();
        let mut memtable_bytes = 0;
        for record in records {
            memtable_bytes += apply(&mut memtable, record);
        }
        let state = State { memtable, memtable_bytes, wal, sstables, next_seq };
        Ok(Engine { dir: dir.to_path_buf(), options, state: RwLock::new(state) })
    }

    /// Returns Ok only once the write is on disk (with `sync_writes` off: once the OS has it); on error the memtable is unchanged.
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
            // The write is already durable in the WAL, so a failed flush or compaction must not fail it; the next flush retries.
            if let Err(e) = self.flush(&mut state) {
                eprintln!("patina: flush to an SSTable failed, retrying on the next write: {e}");
            } else if state.sstables.len() > self.options.max_sstables {
                if let Err(e) = self.compact(&mut state) {
                    eprintln!("patina: compaction failed: {e}");
                }
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
        state.sstables.insert(0, (state.next_seq, table));
        state.next_seq += 1;
        state.memtable.clear();
        state.memtable_bytes = 0;
        Ok(())
    }

    /// Merges every SSTable into one at the next seq without tombstones, then deletes the inputs.
    /// Dropping tombstones is safe: every table is an input, so no older value is left for them to hide.
    // shortcut: rewrites every live entry (one seek + read per value) under the write lock, so reads and writes stall meanwhile; move it to a background thread if stalls matter.
    fn compact(&self, state: &mut State) -> io::Result<()> {
        let path = sst_path(&self.dir, state.next_seq);
        let tmp = path.with_extension("sst.tmp");
        let through = state.sstables[0].0;
        let mut failed = None;
        let no_memtable = BTreeMap::new(); // the memtable is still in the WAL, so it stays out of the merged table
        // A read error ends the merge early; checking `failed` keeps that short file from being renamed into place.
        let entries = merge(&no_memtable, &state.sstables, b"").map_while(|entry| entry.map_err(|e| failed = Some(e)).ok());
        sstable::write(&tmp, entries, through)?;
        if let Some(e) = failed {
            return Err(e);
        }
        fs::rename(&tmp, &path)?;
        // Same Unix-only rule as wal.rs: the rename must be durable before the inputs are deleted.
        #[cfg(unix)]
        fs::File::open(&self.dir)?.sync_all()?;
        let table = SsTable::open(&path)?;
        let inputs = std::mem::replace(&mut state.sstables, vec![(state.next_seq, table)]);
        state.next_seq += 1;
        remove_tables(&self.dir, inputs)
    }

    /// Looks in the memtable, then the SSTables newest first. The first entry found wins, so a tombstone hides older values.
    pub fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        let state = self.state.read().unwrap();
        let found = match state.memtable.get(key) {
            Some(entry) => Some(entry.clone()),
            None => state.sstables.iter().find_map(|(_, table)| table.get(key).transpose()).transpose()?,
        };
        Ok(match found {
            Some(Value::Value(v)) => Some(v),
            Some(Value::Tombstone) | None => None,
        })
    }

    /// Live entries with keys >= `start` in byte order, newest value per key, all read under one read lock.
    /// Stops after `limit` entries, or before an entry that would take the key and value bytes past `max_bytes`,
    /// but always returns the first entry. `more` is true only if another live key follows the last one returned.
    #[allow(clippy::type_complexity)]
    pub fn scan(&self, start: &[u8], limit: usize, max_bytes: usize) -> io::Result<(Vec<(Vec<u8>, Vec<u8>)>, bool)> {
        let state = self.state.read().unwrap();
        let (mut items, mut bytes) = (Vec::new(), 0);
        for entry in merge(&state.memtable, &state.sstables, start) {
            let (key, Value::Value(value)) = entry? else { unreachable!("merge skips tombstones") };
            bytes += key.len() + value.len();
            if items.len() == limit || (bytes > max_bytes && !items.is_empty()) {
                return Ok((items, true));
            }
            items.push((key.to_vec(), value));
        }
        Ok((items, false))
    }
}

// ---- C ABI used by service/src/main/java/dev/patina/engine/PatinaEngine.java ----
// Every entry point except patina_free (it only drops a byte buffer, which can't panic) catches panics: unwinding into the JVM is undefined behaviour.

pub const OK: i32 = 0;
pub const NOT_FOUND: i32 = 1;
pub const ERR: i32 = -1;

unsafe fn bytes<'a>(p: *const u8, len: usize) -> &'a [u8] {
    if len == 0 { &[] } else { unsafe { std::slice::from_raw_parts(p, len) } }
}

/// `path` is a UTF-8 data directory path; `sync_writes` is `Options::sync_writes`.
/// Returns null on failure, with the reason on stderr. Release with `patina_close`.
///
/// # Safety
/// `path` must point to `path_len` readable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn patina_open(path: *const u8, path_len: usize, sync_writes: bool) -> *mut Engine {
    catch_unwind(AssertUnwindSafe(|| {
        let dir = match std::str::from_utf8(unsafe { bytes(path, path_len) }) {
            Ok(dir) => dir,
            Err(e) => {
                eprintln!("patina_open: data dir path is not UTF-8: {e}");
                return ptr::null_mut();
            }
        };
        match Engine::open(Path::new(dir), Options { sync_writes, ..Options::default() }) {
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

/// Scans live keys >= `start` (see `Engine::scan`): at most `limit` entries, clamped to 1..=1000, and about 4 MiB
/// of keys and values. On OK, `*out`/`*out_len` hold a Rust-owned buffer the caller must release with `patina_free`,
/// little-endian: [count u32][more u8: 0 or 1] then count x [key_len u32][key][value_len u32][value].
///
/// # Safety
/// `engine` must be null or an open handle; `start` must point to `start_len` readable bytes;
/// `out` and `out_len` must be null or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn patina_scan(
    engine: *const Engine,
    start: *const u8,
    start_len: usize,
    limit: u32,
    out: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    if engine.is_null() || out.is_null() || out_len.is_null() {
        return ERR;
    }
    catch_unwind(AssertUnwindSafe(|| unsafe {
        match (*engine).scan(bytes(start, start_len), limit.clamp(1, 1000) as usize, 4 * 1024 * 1024) {
            Ok((items, more)) => {
                let mut buf = Vec::with_capacity(5 + items.iter().map(|(k, v)| 8 + k.len() + v.len()).sum::<usize>());
                buf.extend_from_slice(&(items.len() as u32).to_le_bytes());
                buf.push(more as u8);
                // Keys and values fit in u32: the WAL rejects records over MAX_RECORD (4 MiB).
                for (key, value) in &items {
                    buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
                    buf.extend_from_slice(key);
                    buf.extend_from_slice(&(value.len() as u32).to_le_bytes());
                    buf.extend_from_slice(value);
                }
                let buf = buf.into_boxed_slice();
                *out_len = buf.len();
                *out = Box::into_raw(buf) as *mut u8;
                OK
            }
            Err(e) => {
                eprintln!("patina_scan: {e}");
                ERR
            }
        }
    }))
    .unwrap_or(ERR)
}

/// # Safety
/// `buf` and `len` must be null or exactly what one `patina_get` or `patina_scan` call returned, and each buffer is freed once.
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
            let e = patina_open(path.as_ptr(), path.len(), true);
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
        assert!(unsafe { patina_open([0xFF].as_ptr(), 1, true) }.is_null());
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
        let tiny = || Options { memtable_limit_bytes: 64, max_sstables: usize::MAX, ..Options::default() }; // never compacts, so reads must span tables
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
        let e = Engine::open(dir.path(), Options { memtable_limit_bytes: 10, ..Options::default() }).unwrap();
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

    /// Flushes three tables, oldest first: {a: a1, b: b1, gone: x}, {a: a2, c: c2, gone: tombstone}, {b: b3}.
    fn three_tables(dir: &Path) -> Engine {
        let e = Engine::open(dir, Options::default()).unwrap();
        let flush = |e: &Engine| e.flush(&mut e.state.write().unwrap()).unwrap();
        e.put(b"a", b"a1").unwrap();
        e.put(b"b", b"b1").unwrap();
        e.put(b"gone", b"x").unwrap();
        flush(&e);
        e.put(b"a", b"a2").unwrap();
        e.put(b"c", b"c2").unwrap();
        e.delete(b"gone").unwrap();
        flush(&e);
        e.put(b"b", b"b3").unwrap();
        flush(&e);
        e
    }

    fn check_compacted(e: &Engine) {
        for (k, v) in [("a", Some("a2")), ("b", Some("b3")), ("c", Some("c2")), ("gone", None)] {
            assert_eq!(e.get(k.as_bytes()).unwrap(), v.map(|v| v.as_bytes().to_vec()), "{k}");
        }
    }

    #[test]
    fn compaction_keeps_newest_values_and_drops_tombstones() {
        let dir = tempfile::tempdir().unwrap();
        let e = three_tables(dir.path());
        e.compact(&mut e.state.write().unwrap()).unwrap();
        {
            let state = e.state.read().unwrap();
            let [(seq, table)] = &state.sstables[..] else { panic!("{} tables after compaction", state.sstables.len()) };
            assert_eq!((*seq, table.compacted_through()), (4, 3));
            assert!(table.keys(b"").eq([&b"a"[..], b"b", b"c"]), "the tombstone for \"gone\" is still there");
        }
        assert_eq!(sst_count(dir.path()), 1);
        check_compacted(&e);
        drop(e);
        check_compacted(&Engine::open(dir.path(), Options::default()).unwrap());
    }

    #[test]
    fn crash_before_compaction_deletes_never_resurrects_a_deleted_key() {
        // All inputs left behind, or only the oldest one that still holds "gone" = x: a crash partway
        // through the deletes, or power loss undoing some of them, can leave any subset.
        for left_behind in [3, 1] {
            let dir = tempfile::tempdir().unwrap();
            let e = three_tables(dir.path());
            let old: Vec<_> = (1..=left_behind).map(|seq| (sst_path(dir.path(), seq), fs::read(sst_path(dir.path(), seq)).unwrap())).collect();
            e.compact(&mut e.state.write().unwrap()).unwrap();
            drop(e);
            for (path, bytes) in &old {
                fs::write(path, bytes).unwrap();
            }

            let e = Engine::open(dir.path(), Options::default()).unwrap();
            check_compacted(&e);
            assert!(old.iter().all(|(path, _)| !path.exists()), "{left_behind} left behind: old files survived open");
            assert_eq!(sst_count(dir.path()), 1);
        }
    }

    #[test]
    fn read_error_mid_merge_renames_nothing_and_deletes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let e = three_tables(dir.path());
        // Table 2 holds the newest "a" and "c", so the merge has to read values from it.
        fs::OpenOptions::new().write(true).open(sst_path(dir.path(), 2)).unwrap().set_len(0).unwrap();
        assert!(e.compact(&mut e.state.write().unwrap()).is_err());
        assert!(!sst_path(dir.path(), 4).exists());
        assert_eq!(sst_count(dir.path()), 3);
    }

    #[test]
    fn a_flush_past_max_sstables_compacts() {
        let dir = tempfile::tempdir().unwrap();
        // Limit 0: every write flushes.
        let e = Engine::open(dir.path(), Options { memtable_limit_bytes: 0, max_sstables: 2, ..Options::default() }).unwrap();
        e.put(b"a", b"1").unwrap();
        e.put(b"b", b"2").unwrap();
        assert_eq!(sst_count(dir.path()), 2);
        e.delete(b"a").unwrap(); // a third table
        assert_eq!(sst_count(dir.path()), 1);
        assert_eq!(e.get(b"a").unwrap(), None);
        assert_eq!(e.get(b"b").unwrap(), Some(b"2".to_vec()));
    }

    /// `three_tables` plus a memtable that overwrites "c", deletes "b" and adds "d". Live: a=a2, c=c4, d=d4.
    fn tables_and_memtable(dir: &Path) -> Engine {
        let e = three_tables(dir);
        e.put(b"c", b"c4").unwrap();
        e.delete(b"b").unwrap();
        e.put(b"d", b"d4").unwrap();
        e
    }

    /// `e.scan` without a byte cap, as space-separated "key=value" pairs.
    fn scan(e: &Engine, start: &str, limit: usize) -> (String, bool) {
        let (items, more) = e.scan(start.as_bytes(), limit, usize::MAX).unwrap();
        let items: Vec<_> =
            items.iter().map(|(k, v)| format!("{}={}", String::from_utf8_lossy(k), String::from_utf8_lossy(v))).collect();
        (items.join(" "), more)
    }

    #[test]
    fn scan_empty_engine() {
        let dir = tempfile::tempdir().unwrap();
        let e = Engine::open(dir.path(), Options::default()).unwrap();
        assert_eq!(scan(&e, "", 10), ("".into(), false));
    }

    #[test]
    fn scan_merges_memtable_and_sstables_from_any_start() {
        let dir = tempfile::tempdir().unwrap();
        let e = tables_and_memtable(dir.path());
        for start in ["", "0", "a"] {
            assert_eq!(scan(&e, start, 10), ("a=a2 c=c4 d=d4".into(), false), "start {start:?}");
        }
        assert_eq!(scan(&e, "a\0", 10), ("c=c4 d=d4".into(), false)); // just after "a"
        assert_eq!(scan(&e, "bb", 10), ("c=c4 d=d4".into(), false));
        assert_eq!(scan(&e, "d", 10), ("d=d4".into(), false));
        for start in ["e", "z"] {
            assert_eq!(scan(&e, start, 10), ("".into(), false), "start {start:?}");
        }
    }

    #[test]
    fn scan_skips_tombstones_over_older_sstable_values() {
        let dir = tempfile::tempdir().unwrap();
        let e = tables_and_memtable(dir.path());
        // "b": memtable tombstone over b3 and b1 in tables; "gone": table tombstone over x in an older table.
        assert_eq!(scan(&e, "b", 10), ("c=c4 d=d4".into(), false));
        assert_eq!(scan(&e, "gone", 10), ("".into(), false));
    }

    #[test]
    fn scan_limit_sets_more_only_if_a_live_key_follows() {
        let dir = tempfile::tempdir().unwrap();
        let e = tables_and_memtable(dir.path());
        assert_eq!(scan(&e, "", 2), ("a=a2 c=c4".into(), true));
        assert_eq!(scan(&e, "", 1), ("a=a2".into(), true)); // the next key "b" is deleted; "c" is what makes more true
        assert_eq!(scan(&e, "", 3), ("a=a2 c=c4 d=d4".into(), false)); // only the deleted "gone" follows
        assert_eq!(scan(&e, "c", 2), ("c=c4 d=d4".into(), false));
    }

    #[test]
    fn scan_byte_cap_stops_early_but_returns_at_least_one_entry() {
        let dir = tempfile::tempdir().unwrap();
        let e = tables_and_memtable(dir.path());
        // Every live entry is 3 key and value bytes.
        let page = |max_bytes| {
            let (items, more) = e.scan(b"", 10, max_bytes).unwrap();
            (items.len(), more)
        };
        assert_eq!(page(0), (1, true));
        assert_eq!(page(5), (1, true));
        assert_eq!(page(6), (2, true));
        assert_eq!(page(9), (3, false));
    }

    /// Calls `patina_scan`, frees the buffer, and decodes it into "key=value" strings and `more`.
    unsafe fn ffi_scan(e: *const Engine, start: &[u8], limit: u32) -> (Vec<String>, bool) {
        let (mut out, mut out_len) = (ptr::null_mut(), 0usize);
        assert_eq!(unsafe { patina_scan(e, start.as_ptr(), start.len(), limit, &mut out, &mut out_len) }, OK);
        let buf = unsafe { std::slice::from_raw_parts(out, out_len) }.to_vec();
        unsafe { patina_free(out, out_len) };
        let mut rest = &buf[..];
        let mut take = |n: usize| {
            let (head, tail) = rest.split_at(n);
            rest = tail;
            head
        };
        let count = u32::from_le_bytes(take(4).try_into().unwrap());
        let more = take(1)[0];
        let items = (0..count)
            .map(|_| {
                let mut field = || {
                    let len = u32::from_le_bytes(take(4).try_into().unwrap()) as usize;
                    String::from_utf8(take(len).to_vec()).unwrap()
                };
                format!("{}={}", field(), field())
            })
            .collect();
        assert!(rest.is_empty(), "{} bytes after the last entry", rest.len());
        assert!(more <= 1, "more = {more}");
        (items, more == 1)
    }

    #[test]
    fn ffi_scan_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        // 1002 keys written straight to an SSTable, so the test doesn't pay one WAL sync per key.
        let key = |i: u32| format!("k{i:04}");
        let entries = (0..1002).map(|i| (key(i), Value::Value(i.to_string().into_bytes())));
        sstable::write(&sst_path(dir.path(), 1), entries, 0).unwrap();
        let path = dir.path().to_str().unwrap();
        unsafe {
            let e = patina_open(path.as_ptr(), path.len(), true);
            assert!(!e.is_null());
            let (k, v) = (b"k0001", b"new");
            assert_eq!(patina_put(e, k.as_ptr(), k.len(), v.as_ptr(), v.len()), OK);
            assert_eq!(patina_delete(e, b"k0002".as_ptr(), 5), OK);

            assert_eq!(ffi_scan(e, b"", 3), (vec!["k0000=0".into(), "k0001=new".into(), "k0003=3".into()], true));
            assert_eq!(ffi_scan(e, b"k1001", 3), (vec!["k1001=1001".into()], false));
            assert_eq!(ffi_scan(e, b"l", 3), (vec![], false));
            assert_eq!(ffi_scan(e, b"", 0), (vec!["k0000=0".into()], true)); // clamped up to 1
            // 1001 live keys, clamped down to 1000.
            let (items, more) = ffi_scan(e, b"", u32::MAX);
            assert_eq!((items.len(), items.last().map(String::as_str), more), (1000, Some("k1000=1000"), true));

            let (mut out, mut out_len) = (ptr::null_mut(), 0usize);
            assert_eq!(patina_scan(ptr::null(), ptr::null(), 0, 10, &mut out, &mut out_len), ERR);
            assert_eq!(patina_scan(e, ptr::null(), 0, 10, ptr::null_mut(), &mut out_len), ERR);
            patina_close(e);
        }
    }
}
