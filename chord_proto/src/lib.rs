pub mod chord {
    tonic::include_proto!("chord");
}

pub fn hash_addr(addr: &str) -> u64 {
    use sha1::{Digest, Sha1};
    let mut hasher = Sha1::new();
    hasher.update(addr.as_bytes());
    let result = hasher.finalize();
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&result[0..8]);
    u64::from_be_bytes(bytes)
}
