use super::error::{Error, Result};
use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Compute a hash of serialized bytes for parameter comparison.
/// Uses DefaultHasher which is fast and sufficient for non-cryptographic purposes.
pub fn hash_params(bytes: &[u8]) -> u64 {
    let mut hasher = DefaultHasher::new();
    bytes.hash(&mut hasher);
    hasher.finish()
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
