//! Kill-the-process test (PLAN.md phases 1 and 2): no acknowledged write is lost when the process dies,
//! including mid-flush and mid-compaction: the tiny limits make the child flush every few writes and compact every other flush.
// A process kill doesn't test power loss; the torn-write test in wal.rs and fsync-before-ack cover that.

use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use patina_engine::sstable::SsTable;
use patina_engine::{Engine, Options};

/// Caps the child, so one orphaned by a dead parent can't fill the disk.
const WRITES: u32 = 100_000;

fn tiny() -> Options {
    Options { memtable_limit_bytes: 100, max_sstables: 2 }
}

fn key(i: u32) -> [u8; 4] {
    i.to_be_bytes()
}

fn value(i: u32) -> Vec<u8> {
    format!("value-{i}").into_bytes()
}

/// Every 5th key is put and then deleted before its ack.
fn is_deleted(i: u32) -> bool {
    i % 5 == 0
}

/// The writer process the test below spawns and kills; a no-op unless PATINA_CRASH_DIR is set.
#[test]
#[ignore]
fn child_writer() {
    let Some(dir) = std::env::var_os("PATINA_CRASH_DIR") else { return };
    let engine = Engine::open(Path::new(&dir), tiny()).unwrap();
    for i in 0..WRITES {
        engine.put(&key(i), &value(i)).unwrap();
        if is_deleted(i) {
            engine.delete(&key(i)).unwrap();
        }
        println!("ack {i}");
        std::io::stdout().flush().unwrap();
    }
}

#[test]
fn no_acked_write_lost_on_kill() {
    let mut iterations_compacted = 0;
    let mut iterations_mid_flush = 0;
    for iteration in 0..100 {
        let tmp = tempfile::tempdir().unwrap();
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "child_writer", "--ignored", "--nocapture"])
            .env("PATINA_CRASH_DIR", tmp.path())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();

        // Drain stdout on its own thread: a full pipe would block the child and the test would measure nothing.
        let stdout = child.stdout.take().unwrap();
        let (tx, acks) = mpsc::channel();
        let reader = std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                if let Some(i) = line.strip_prefix("ack ") {
                    let _ = tx.send(i.parse::<u32>().unwrap());
                }
            }
        });

        // Wait for a random number of acks, not a random time: compaction starts after a fixed number of writes
        // (about 20), and on a slow disk (the Windows CI runner) a 20-200 ms window never got that far.
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().subsec_nanos();
        let target = nanos % 61;
        let seen = loop {
            let Ok(ack) = acks.recv_timeout(Duration::from_secs(10)) else {
                child.kill().unwrap();
                panic!("iteration {iteration}: no ack within 10 s, dir {}", tmp.keep().display());
            };
            if ack >= target {
                break ack;
            }
        };
        // Then a random delay, or the kill always lands at the start of the next write, before any flush begins.
        std::thread::sleep(Duration::from_millis((nanos / 61 % 51).into()));
        assert!(child.try_wait().unwrap().is_none(), "iteration {iteration}: child exited before the kill");
        child.kill().unwrap();
        child.wait().unwrap();
        reader.join().unwrap();
        let last_ack = acks.try_iter().last().unwrap_or(seen);

        // Only a compaction writes a nonzero compacted_through.
        let compacted = std::fs::read_dir(tmp.path()).unwrap().map(|e| e.unwrap().path()).any(|path| {
            path.extension().is_some_and(|x| x == "sst") && SsTable::open(&path).unwrap().compacted_through() > 0
        });
        iterations_compacted += usize::from(compacted);
        // A flush or compaction writes <seq>.sst.tmp and renames it, so a leftover .tmp means the kill landed mid-way.
        let mid_flush = std::fs::read_dir(tmp.path()).unwrap().any(|e| e.unwrap().path().extension().is_some_and(|x| x == "tmp"));
        iterations_mid_flush += usize::from(mid_flush);

        // The child writes only keys 0..WRITES, so probing that range finds any key it could have left behind.
        let engine = Engine::open(tmp.path(), tiny()).unwrap();
        let bad = (0..WRITES).find(|&i| {
            let got = engine.get(&key(i)).unwrap();
            if i > last_ack + 1 {
                got.is_some()
            } else if i == last_ack + 1 {
                got.is_some_and(|v| v != value(i)) // in flight: absent, put or deleted, but never garbled
            } else if is_deleted(i) {
                got.is_some()
            } else {
                got != Some(value(i))
            }
        });
        if let Some(i) = bad {
            let got = engine.get(&key(i)).unwrap().map(|v| String::from_utf8_lossy(&v).into_owned());
            panic!(
                "iteration {iteration}: first bad key is {i} (got {got:?}, last ack {last_ack}), dir {}",
                tmp.keep().display()
            );
        }
    }
    // Otherwise the kills never met a flush or a compaction and the test proved nothing about them.
    assert!(iterations_compacted > 0, "no iteration compacted its SSTables");
    // Measured locally: 17-20 of 100 with the delay, 0-1 without it, so 5 catches losing it without flaking.
    assert!(iterations_mid_flush >= 5, "only {iterations_mid_flush} kills landed mid-flush or mid-compaction");
}
