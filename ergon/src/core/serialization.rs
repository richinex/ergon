use super::error::{Error, Result};
use serde::{Deserialize, Serialize};

/// Compute a stable hash of serialized bytes for parameter comparison.
///
/// Uses SeaHash which provides:
/// - Stable hashing across Rust compiler versions
/// - Stable across process restarts and machines
/// - Fast hashing for serialized parameters
/// - Well-tested, battle-proven algorithm
///
/// This is critical for parameter comparison in flow replay - we need the same
/// parameters to produce the same hash every time, regardless of Rust version.
///
/// Note: This is NOT cryptographically secure, but that's not needed here.
/// We just need stable, fast hashing for equality checks.
pub fn hash_params(bytes: &[u8]) -> u64 {
    seahash::hash(bytes)
}

/// Serializes a value to bytes using bincode.
///
/// # Errors
/// Returns `Error::Serialization` if the value cannot be serialized.
pub fn serialize_value<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    bincode::serde::encode_to_vec(value, bincode::config::standard()).map_err(Error::Serialization)
}

/// Deserializes bytes to a value using bincode.
///
/// # Errors
/// Returns `Error::Deserialization` if the bytes cannot be deserialized.
pub fn deserialize_value<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<T> {
    bincode::serde::decode_from_slice(bytes, bincode::config::standard())
        .map(|(value, _len)| value)
        .map_err(Error::Deserialization)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_params() {
        let some_val: Option<String> = Some("SAVE20".to_string());
        let none_val: Option<String> = None;

        let some_bytes = serialize_value(&some_val).unwrap();
        let none_bytes = serialize_value(&none_val).unwrap();

        let some_hash = hash_params(&some_bytes);
        let none_hash = hash_params(&none_bytes);

        // Different values should have different hashes
        assert_ne!(some_hash, none_hash);

        // Same value should have same hash
        let some_bytes2 = serialize_value(&some_val).unwrap();
        let some_hash2 = hash_params(&some_bytes2);
        assert_eq!(some_hash, some_hash2);
    }

    #[test]
    fn test_serialization() {
        let test_value = vec!["hello".to_string(), "world".to_string()];
        let serialized = serialize_value(&test_value).unwrap();
        let deserialized: Vec<String> = deserialize_value(&serialized).unwrap();
        assert_eq!(test_value, deserialized);
    }
}
