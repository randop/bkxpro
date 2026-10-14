#![no_main]
use bytes::Bytes;
use libfuzzer_sys::fuzz_target;
fuzz_target!(|data: &[u8]| {
    let _ = bkxpro_codec::FooterView::parse(Bytes::copy_from_slice(data));
});
