//! Sorted string table: an immutable file of sorted, unique keys, each holding a value or a tombstone.
//! Little-endian. Each entry: [key_len u32][key][kind u8: 0 value, 1 tombstone][value_len u32][value],
//! then the footer: [entry_count u64][compacted_through u64][crc32 u32 of everything before it][magic u32].

use std::borrow::Borrow;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Mutex;

use crate::Value;

const KIND_VALUE: u8 = 0;
const KIND_TOMBSTONE: u8 = 1;
const FOOTER_LEN: u64 = 8 + 8 + 4 + 4;
const MAGIC: u32 = u32::from_le_bytes(*b"PSST");

/// Writes `entries`, sorted by key with no duplicates, to `path` and syncs the file.
/// The caller owns renaming it into place and syncing the directory. On error the file is incomplete.
pub fn write<K: AsRef<[u8]>, V: Borrow<Value>>(
    path: &Path,
    entries: impl IntoIterator<Item = (K, V)>,
    compacted_through: u64,
) -> io::Result<()> {
    let mut out = BufWriter::new(File::create(path)?);
    let mut hasher = crc32fast::Hasher::new();
    let mut emit = |bytes: &[u8]| {
        hasher.update(bytes);
        out.write_all(bytes)
    };
    let (mut prev, mut count) = (Vec::new(), 0u64);
    for (key, entry) in entries {
        let key = key.as_ref();
        if count > 0 && prev.as_slice() >= key {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "SSTable entries must be sorted by key with no duplicates"));
        }
        let (kind, value) = match entry.borrow() {
            Value::Value(v) => (KIND_VALUE, v.as_slice()),
            Value::Tombstone => (KIND_TOMBSTONE, &[][..]),
        };
        emit(&len_le(key)?)?;
        emit(key)?;
        emit(&[kind])?;
        emit(&len_le(value)?)?;
        emit(value)?;
        prev.clear();
        prev.extend_from_slice(key);
        count += 1;
    }
    emit(&count.to_le_bytes())?;
    emit(&compacted_through.to_le_bytes())?;
    out.write_all(&hasher.finalize().to_le_bytes())?;
    out.write_all(&MAGIC.to_le_bytes())?;
    out.into_inner().map_err(io::IntoInnerError::into_error)?.sync_all()
}

pub struct SsTable {
    // shortcut: full key index in memory; switch to a sparse index if key counts grow.
    index: BTreeMap<Vec<u8>, Slot>,
    // shortcut: reads of one file are serialized by the mutex; switch to positional reads (read_at / seek_read) if gets on one table contend.
    file: Mutex<File>,
    compacted_through: u64,
}

/// Where an entry's value sits in the file.
struct Slot {
    offset: u64,
    len: u32,
    tombstone: bool,
}

impl SsTable {
    /// Reads the file once, checks magic, checksum and entry count, and indexes every key. Errors name the file.
    pub fn open(path: &Path) -> io::Result<SsTable> {
        Self::load(path).map_err(|e| io::Error::new(e.kind(), format!("SSTable {}: {e}", path.display())))
    }

    fn load(path: &Path) -> io::Result<SsTable> {
        let file = File::open(path)?;
        let body_len = file.metadata()?.len().checked_sub(FOOTER_LEN).ok_or_else(|| corrupt("shorter than the footer"))?;
        let mut r = Hashing { inner: BufReader::new(&file), hasher: crc32fast::Hasher::new() };
        let (mut index, mut pos) = (BTreeMap::new(), 0);
        while pos < body_len {
            let key_len = u64::from(u32::from_le_bytes(read_array(&mut r)?));
            // Checked before allocating, so a corrupt length can't size a buffer.
            let value_at = pos + 4 + key_len + 1 + 4;
            if value_at > body_len {
                return Err(corrupt("entry runs into the footer"));
            }
            let mut key = vec![0; key_len as usize];
            r.read_exact(&mut key)?;
            let tombstone = match read_array::<1>(&mut r)?[0] {
                KIND_VALUE => false,
                KIND_TOMBSTONE => true,
                kind => return Err(corrupt(&format!("unknown entry kind {kind}"))),
            };
            let len = u32::from_le_bytes(read_array(&mut r)?);
            pos = value_at + u64::from(len);
            if pos > body_len {
                return Err(corrupt("entry runs into the footer"));
            }
            io::copy(&mut (&mut r).take(len.into()), &mut io::sink())?; // hashed, not kept: get() reads values on demand
            index.insert(key, Slot { offset: value_at, len, tombstone });
        }
        let count = u64::from_le_bytes(read_array(&mut r)?);
        let compacted_through = u64::from_le_bytes(read_array(&mut r)?);
        let crc = u32::from_le_bytes(read_array(&mut r.inner)?);
        let magic = u32::from_le_bytes(read_array(&mut r.inner)?);
        if magic != MAGIC {
            return Err(corrupt("bad magic number"));
        }
        if crc != r.hasher.finalize() {
            return Err(corrupt("checksum mismatch"));
        }
        if count != index.len() as u64 {
            return Err(corrupt(&format!("footer says {count} entries, found {}", index.len())));
        }
        Ok(SsTable { index, file: Mutex::new(file), compacted_through })
    }

    /// This table's entry for `key`, reading a value from disk; None if the table has no entry for it.
    pub fn get(&self, key: &[u8]) -> io::Result<Option<Value>> {
        let Some(slot) = self.index.get(key) else { return Ok(None) };
        if slot.tombstone {
            return Ok(Some(Value::Tombstone));
        }
        let mut value = vec![0; slot.len as usize];
        let mut file = self.file.lock().unwrap();
        file.seek(SeekFrom::Start(slot.offset))?;
        file.read_exact(&mut value)?;
        Ok(Some(Value::Value(value)))
    }

    /// Every key, tombstones included, in ascending byte order.
    pub fn keys(&self) -> impl Iterator<Item = &[u8]> {
        self.index.keys().map(Vec::as_slice)
    }

    pub fn compacted_through(&self) -> u64 {
        self.compacted_through
    }
}

/// `field.len()` as a little-endian u32 length prefix.
fn len_le(field: &[u8]) -> io::Result<[u8; 4]> {
    u32::try_from(field.len())
        .map(u32::to_le_bytes)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "SSTable key or value longer than u32::MAX bytes"))
}

/// A reader that feeds every byte it returns into a crc.
struct Hashing<R> {
    inner: R,
    hasher: crc32fast::Hasher,
}

impl<R: Read> Read for Hashing<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.hasher.update(&buf[..n]);
        Ok(n)
    }
}

fn read_array<const N: usize>(r: &mut impl Read) -> io::Result<[u8; N]> {
    let mut buf = [0; N];
    r.read_exact(&mut buf)?;
    Ok(buf)
}

fn corrupt(msg: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;

    fn val(v: &[u8]) -> Value {
        Value::Value(v.to_vec())
    }

    /// Writes `entries` with compacted_through = 7 to a new file; the TempDir keeps it alive.
    fn write_file(entries: &[(&[u8], Value)]) -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.sst");
        write(&path, entries.iter().map(|(k, v)| (k, v)), 7).unwrap();
        (dir, path)
    }

    #[test]
    fn round_trip_values_and_tombstones() {
        let entries: &[(&[u8], Value)] =
            &[(b"", val(b"empty key")), (b"a", val(b"")), (b"b", Value::Tombstone), (b"c", val(b"3")), (b"d", Value::Tombstone)];
        let (_dir, path) = write_file(entries);
        let table = SsTable::open(&path).unwrap();
        // Reverse order, so gets seek backwards too.
        for (key, value) in entries.iter().rev() {
            assert_eq!(table.get(key).unwrap().as_ref(), Some(value));
        }
        assert!(table.keys().eq(entries.iter().map(|(k, _)| *k)));
        assert_eq!(table.compacted_through(), 7);
    }

    #[test]
    fn empty_table() {
        let (_dir, path) = write_file(&[]);
        let table = SsTable::open(&path).unwrap();
        assert_eq!(table.keys().count(), 0);
        assert_eq!(table.get(b"").unwrap(), None);
        assert_eq!(table.compacted_through(), 7);
        assert_eq!(fs::metadata(&path).unwrap().len(), FOOTER_LEN);
    }

    #[test]
    fn one_mib_value() {
        let big = vec![7; 1 << 20];
        let (_dir, path) = write_file(&[(b"big", val(&big)), (b"next", val(b"x"))]);
        let table = SsTable::open(&path).unwrap();
        assert_eq!(table.get(b"big").unwrap(), Some(val(&big)));
        assert_eq!(table.get(b"next").unwrap(), Some(val(b"x")));
    }

    #[test]
    fn missing_key_is_none() {
        let (_dir, path) = write_file(&[(b"b", val(b"1")), (b"d", Value::Tombstone)]);
        let table = SsTable::open(&path).unwrap();
        let missing: [&[u8]; 5] = [b"", b"a", b"bb", b"c", b"e"];
        for key in missing {
            assert_eq!(table.get(key).unwrap(), None, "{key:?}");
        }
    }

    #[test]
    fn flipped_byte_anywhere_is_rejected() {
        let (dir, path) = write_file(&[(b"a", val(b"1")), (b"b", Value::Tombstone)]);
        let bytes = fs::read(&path).unwrap();
        let bad = dir.path().join("bad.sst");
        for i in 0..bytes.len() {
            let mut flipped = bytes.clone();
            flipped[i] ^= 0xFF;
            fs::write(&bad, &flipped).unwrap();
            let err = SsTable::open(&bad).err().unwrap_or_else(|| panic!("flip at byte {i} was accepted"));
            assert!(err.to_string().contains("bad.sst"), "{err}");
        }
    }

    #[test]
    fn truncated_file_is_rejected() {
        let (dir, path) = write_file(&[(b"a", val(b"1")), (b"b", Value::Tombstone)]);
        let bytes = fs::read(&path).unwrap();
        let cut_path = dir.path().join("cut.sst");
        for cut in 0..bytes.len() {
            fs::write(&cut_path, &bytes[..cut]).unwrap();
            assert!(SsTable::open(&cut_path).is_err(), "cut at {cut} was accepted");
        }
    }

    #[test]
    fn bad_count_or_kind_is_rejected_even_with_a_valid_crc() {
        let (_dir, path) = write_file(&[(b"a", val(b"1"))]);
        let bytes = fs::read(&path).unwrap();
        // Rewrites the file from `bytes` with one byte changed and a recomputed crc.
        let reseal = |at: usize, byte: u8| {
            let mut b = bytes.clone();
            b[at] = byte;
            let crc_at = b.len() - 8;
            let crc = crc32fast::hash(&b[..crc_at]);
            b[crc_at..crc_at + 4].copy_from_slice(&crc.to_le_bytes());
            fs::write(&path, &b).unwrap();
        };
        let count_at = bytes.len() - FOOTER_LEN as usize;
        reseal(count_at, 1); // the real count, so resealing alone keeps the file valid
        assert!(SsTable::open(&path).is_ok());
        reseal(count_at, 2);
        assert!(SsTable::open(&path).is_err());
        reseal(5, 2); // the kind byte, after [key_len u32][b'a']
        assert!(SsTable::open(&path).is_err());
    }

    #[test]
    fn write_rejects_unsorted_or_duplicate_keys() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.sst");
        for entries in [[(b"b", val(b"1")), (b"a", val(b"2"))], [(b"a", val(b"1")), (b"a", Value::Tombstone)]] {
            let err = write(&path, entries, 0).unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        }
    }
}
