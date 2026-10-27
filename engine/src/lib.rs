use std::collections::BTreeMap;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::ptr;
use std::sync::RwLock;

/// shortcut: in-memory BTreeMap with no durability; replace with the LSM tree (WAL + memtable + SSTables) in phase 1.
pub struct Engine {
    map: RwLock<BTreeMap<Vec<u8>, Vec<u8>>>,
}

impl Engine {
    pub fn new() -> Self {
        Engine { map: RwLock::new(BTreeMap::new()) }
    }

    pub fn put(&self, key: &[u8], value: &[u8]) {
        self.map.write().unwrap().insert(key.to_vec(), value.to_vec());
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.map.read().unwrap().get(key).cloned()
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

/// Returns null on failure. Release with `patina_close`.
#[unsafe(no_mangle)]
pub extern "C" fn patina_open() -> *mut Engine {
    catch_unwind(|| Box::into_raw(Box::new(Engine::new()))).unwrap_or(ptr::null_mut())
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
        (*engine).put(bytes(key, key_len), bytes(value, value_len));
        OK
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

    #[test]
    fn ffi_round_trip() {
        unsafe {
            let e = patina_open();
            let (k, v) = (b"user#1", b"alice");
            assert_eq!(patina_put(e, k.as_ptr(), k.len(), v.as_ptr(), v.len()), OK);

            let (mut out, mut out_len) = (ptr::null_mut(), 0usize);
            assert_eq!(patina_get(e, k.as_ptr(), k.len(), &mut out, &mut out_len), OK);
            assert_eq!(std::slice::from_raw_parts(out, out_len), v);
            patina_free(out, out_len);

            let missing = b"nope";
            assert_eq!(patina_get(e, missing.as_ptr(), missing.len(), &mut out, &mut out_len), NOT_FOUND);
            patina_close(e);
        }
    }

    #[test]
    fn empty_value_round_trips() {
        let e = Engine::new();
        e.put(b"k", b"");
        assert_eq!(e.get(b"k"), Some(vec![]));
    }
}
