#![no_main]
use libfuzzer_sys::fuzz_target;
fuzz_target!(|data: &[u8]| {
    let _ = bkxpro_codec::parse_record_header(data);
    let _ = bkxpro_codec::parse_record(data);
});
