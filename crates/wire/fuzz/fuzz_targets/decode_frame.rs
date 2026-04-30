#![no_main]

use bytes::Bytes;
use inferadb_ledger_wire::Frame;
use libfuzzer_sys::fuzz_target;

// Fuzz target: feed arbitrary bytes to Frame::decode and assert no panic.
//
// The decode function must reject malformed frames with a CodecError, never
// panic. This target catches any panic path (out-of-bounds access, division
// by zero, integer overflow, etc.) on hostile input.
fuzz_target!(|data: &[u8]| {
    let mut bytes = Bytes::copy_from_slice(data);
    let _ = Frame::decode(&mut bytes);
});
