//! Write-ahead log. Each record, little-endian: [payload_len u32][crc32 u32 of payload][payload],
//! where payload = [op u8][key_len u32][key][value_len u32][value] for a Put (op 0),
//! and [op u8][key_len u32][key] for a Delete (op 1).

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;

/// Largest payload `append` accepts; recovery treats a bigger length header as corruption.
pub const MAX_RECORD: usize = 4 * 1024 * 1024;
const HEADER: usize = 8;
const OP_PUT: u8 = 0;
const OP_DELETE: u8 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Record {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

pub struct Wal {
    file: File,
    failed: bool,
    /// Whether `append` syncs before returning (the default). Off, an acked record has only reached the OS:
    /// it survives a process crash but not power loss or an OS crash.
    pub sync: bool,
}

impl Wal {
    /// Creates the file if missing, returns every intact record, and truncates whatever follows them.
    pub fn open(path: &Path) -> io::Result<(Wal, Vec<Record>)> {
        let mut file = OpenOptions::new().read(true).write(true).create(true).truncate(false).open(path)?;
        // shortcut: reads the whole log into memory; fine once flushes (phase 2) keep it near the memtable size.
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;

        // Windows std can't open a directory to sync it, so only Unix makes a new file's entry durable here.
        // Checking for an empty file also covers a create whose directory sync was cut off by a crash.
        #[cfg(unix)]
        if buf.is_empty() {
            let dir = path.parent().filter(|d| !d.as_os_str().is_empty()).unwrap_or(Path::new("."));
            File::open(dir)?.sync_all()?;
        }

        // Corruption in the middle also cuts off later records; acceptable because the only expected damage is a torn tail.
        let (mut records, mut end) = (Vec::new(), 0);
        while let Some((record, len)) = parse(&buf[end..]) {
            records.push(record);
            end += len;
        }
        if end < buf.len() {
            file.set_len(end as u64)?;
            file.sync_all()?;
        }
        file.seek(SeekFrom::Start(end as u64))?;
        Ok((Wal { file, failed: false, sync: true }, records))
    }

    /// Returns Ok only once the record is on disk, or with `sync` off, once the OS has it.
    pub fn append(&mut self, record: &Record) -> io::Result<()> {
        // A failed write can leave a partial record, and recovery stops there: anything appended after it would be lost.
        if self.failed {
            return Err(io::Error::other("an earlier WAL append failed; reopen the log to recover"));
        }
        let (op, key, value) = match record {
            Record::Put { key, value } => (OP_PUT, key, Some(value)),
            Record::Delete { key } => (OP_DELETE, key, None),
        };
        let len = 1 + 4 + key.len() + value.map_or(0, |v| 4 + v.len());
        if len > MAX_RECORD {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("WAL record of {len} bytes exceeds the {MAX_RECORD} byte limit"),
            ));
        }
        let mut buf = Vec::with_capacity(HEADER + len);
        buf.extend_from_slice(&[0; HEADER]); // filled in once the payload's crc is known
        buf.push(op);
        for field in std::iter::once(key).chain(value) {
            buf.extend_from_slice(&(field.len() as u32).to_le_bytes());
            buf.extend_from_slice(field);
        }
        let crc = crc32fast::hash(&buf[HEADER..]);
        buf[..4].copy_from_slice(&(len as u32).to_le_bytes());
        buf[4..HEADER].copy_from_slice(&crc.to_le_bytes());

        let result = self.file.write_all(&buf).and_then(|()| if self.sync { self.file.sync_data() } else { Ok(()) });
        self.failed = result.is_err();
        result
    }
}

/// The record at the start of `buf` and its encoded length; None if it is incomplete or corrupt.
fn parse(buf: &[u8]) -> Option<(Record, usize)> {
    let (len, rest) = take_u32(buf)?;
    let (crc, rest) = take_u32(rest)?;
    let len = len as usize;
    if len > MAX_RECORD {
        return None;
    }
    let payload = rest.get(..len)?;
    if crc32fast::hash(payload) != crc {
        return None;
    }
    let (&op, rest) = payload.split_first()?;
    let (key, rest) = take_bytes(rest)?;
    let (record, rest) = match op {
        OP_PUT => {
            let (value, rest) = take_bytes(rest)?;
            (Record::Put { key: key.to_vec(), value: value.to_vec() }, rest)
        }
        OP_DELETE => (Record::Delete { key: key.to_vec() }, rest),
        _ => return None,
    };
    rest.is_empty().then_some((record, HEADER + len))
}

fn take_u32(buf: &[u8]) -> Option<(u32, &[u8])> {
    let (n, rest) = buf.split_first_chunk()?;
    Some((u32::from_le_bytes(*n), rest))
}

fn take_bytes(buf: &[u8]) -> Option<(&[u8], &[u8])> {
    let (len, rest) = take_u32(buf)?;
    rest.split_at_checked(len as usize)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn put(key: &[u8], value: &[u8]) -> Record {
        Record::Put { key: key.to_vec(), value: value.to_vec() }
    }

    /// Appends `records` to the log at `path`; returns the file length after each one.
    fn write_log(path: &Path, records: &[Record]) -> Vec<usize> {
        let (mut wal, _) = Wal::open(path).unwrap();
        records
            .iter()
            .map(|r| {
                wal.append(r).unwrap();
                fs::metadata(path).unwrap().len() as usize
            })
            .collect()
    }

    /// A hand-built record around `payload`, with a valid crc.
    fn frame(payload: &[u8]) -> Vec<u8> {
        [&(payload.len() as u32).to_le_bytes()[..], &crc32fast::hash(payload).to_le_bytes(), payload].concat()
    }

    /// Opens a log holding one good record followed by the raw bytes `tail`.
    fn open_with_tail(tail: &[u8]) -> Vec<Record> {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.log");
        write_log(&path, &[put(b"a", b"1")]);
        OpenOptions::new().append(true).open(&path).unwrap().write_all(tail).unwrap();
        Wal::open(&path).unwrap().1
    }

    #[test]
    fn round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.log");
        let records = vec![put(b"", b""), put(b"k", b""), put(b"", b"v"), put(b"big", &vec![7; 1 << 20]), put(b"k", b"v2"),
            Record::Delete { key: b"k".to_vec() }, Record::Delete { key: vec![] }];
        write_log(&path, &records);
        assert_eq!(Wal::open(&path).unwrap().1, records);
    }

    #[test]
    fn torn_tail_at_every_length() {
        let dir = tempfile::tempdir().unwrap();
        let (full, cut_path) = (dir.path().join("full.log"), dir.path().join("cut.log"));
        let records: Vec<Record> = (0..8u8)
            .map(|i| if i % 3 == 2 { Record::Delete { key: vec![i; 3] } } else { put(&[i; 3], &vec![i; 10 * i as usize]) })
            .collect();
        let ends = write_log(&full, &records);
        let bytes = fs::read(&full).unwrap();
        let extra = put(b"new", b"x");

        for cut in 0..=bytes.len() {
            fs::write(&cut_path, &bytes[..cut]).unwrap();
            let complete = ends.iter().take_while(|&&end| end <= cut).count();
            let good_end = if complete == 0 { 0 } else { ends[complete - 1] };

            let (mut wal, got) = Wal::open(&cut_path).unwrap();
            assert_eq!(got, records[..complete], "cut at {cut}");
            assert_eq!(fs::metadata(&cut_path).unwrap().len() as usize, good_end, "cut at {cut}");

            // The next append must land right after the last good record, not after the torn bytes.
            wal.append(&extra).unwrap();
            drop(wal);
            let mut expected = records[..complete].to_vec();
            expected.push(extra.clone());
            assert_eq!(Wal::open(&cut_path).unwrap().1, expected, "append after cut at {cut}");
        }
    }

    #[test]
    fn corrupt_record_cuts_log_there() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.log");
        let records = [put(b"a", b"1"), put(b"b", b"2"), put(b"c", b"3")];
        let ends = write_log(&path, &records);
        let mut bytes = fs::read(&path).unwrap();
        bytes[ends[0] + HEADER + 2] ^= 0xFF; // inside the 2nd record's payload
        fs::write(&path, &bytes).unwrap();
        assert_eq!(Wal::open(&path).unwrap().1, records[..1]);
    }

    #[test]
    fn oversized_length_header_is_corrupt() {
        let huge = [&u32::MAX.to_le_bytes()[..], &[0; 20]].concat();
        assert_eq!(open_with_tail(&huge), [put(b"a", b"1")]);

        // One byte over the limit is rejected even when the crc is valid and every byte is there.
        let value = vec![1; MAX_RECORD - 8];
        let over = frame(&[&[OP_PUT, 0, 0, 0, 0][..], &(value.len() as u32).to_le_bytes(), &value].concat());
        assert_eq!(open_with_tail(&over), [put(b"a", b"1")]);
    }

    #[test]
    fn unknown_op_is_corrupt() {
        let op_2 = frame(&[2, 0, 0, 0, 0]); // empty key
        assert_eq!(open_with_tail(&op_2), [put(b"a", b"1")]);
    }

    #[test]
    fn put_record_in_the_original_byte_layout_still_opens() {
        // Built by hand, not by append(), so a change to the Put layout can't slip through unnoticed.
        let old_put = frame(&[0, 1, 0, 0, 0, b'k', 1, 0, 0, 0, b'v']);
        assert_eq!(open_with_tail(&old_put), [put(b"a", b"1"), put(b"k", b"v")]);
    }

    #[test]
    fn delete_with_trailing_value_is_corrupt() {
        let delete_with_value = frame(&[1, 0, 0, 0, 0, 0, 0, 0, 0]); // empty key, then a value field
        assert_eq!(open_with_tail(&delete_with_value), [put(b"a", b"1")]);
        let good_delete = frame(&[1, 1, 0, 0, 0, b'k']);
        assert_eq!(open_with_tail(&good_delete), [put(b"a", b"1"), Record::Delete { key: b"k".to_vec() }]);
    }

    #[test]
    fn append_accepts_exactly_max_record() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.log");
        let at_limit = put(b"", &vec![1; MAX_RECORD - 9]);
        let (mut wal, _) = Wal::open(&path).unwrap();
        wal.append(&at_limit).unwrap();
        let err = wal.append(&put(b"", &vec![1; MAX_RECORD - 8])).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        drop(wal);
        assert_eq!(Wal::open(&path).unwrap().1, [at_limit]);
    }

    #[test]
    fn failed_append_blocks_later_appends() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.log");
        let (mut wal, _) = Wal::open(&path).unwrap();
        wal.file = File::open(&path).unwrap(); // read-only handle, so the write fails
        assert!(wal.append(&put(b"a", b"1")).is_err());
        wal.file = OpenOptions::new().write(true).open(&path).unwrap();
        assert!(wal.append(&put(b"b", b"2")).is_err());
    }
}
