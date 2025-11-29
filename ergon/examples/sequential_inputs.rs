//! Sequential Execution with Input Data Wiring
//!
//! This example demonstrates how to use the `inputs` attribute to wire data
//! between steps. The `inputs` attribute automatically adds the referenced
//! steps to the dependencies list.
//!
//! Pattern: fetch -> transform -> validate -> save
//!
//! Key Points:
//! - `inputs` attribute automatically creates dependencies (no depends_on needed!)
//! - Type-safe data flow: inputs are deserialized and type-checked
//! - Can reference any earlier step, not just the previous one
//! - Multiple inputs create parallel fan-in (merge pattern)
//!
//! Example:
//!   #[step(inputs(value = "step1"))]  // Auto-depends on step1
//!   async fn step2(value: i32) -> i32 { value * 2 }

use ergon::Ergon;
use ergon::{flow, step};
use ergon::{ExecutionLog, InMemoryExecutionLog};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DataPipeline {
    source: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RawData {
    id: String,
    value: i32,
    timestamp: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TransformedData {
    id: String,
    normalized_value: f64,
    timestamp: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ValidationResult {
    data: TransformedData,
    is_valid: bool,
    validation_message: String,
}

impl DataPipeline {
    fn new(source: String) -> Self {
        Self { source }
    }

    // Step 1: Fetch raw data from source
    // No depends_on - this is the first step
    #[step]
    async fn fetch_raw_data(self: Arc<Self>) -> Result<RawData, String> {
        println!("\n[1/4] Fetching raw data from source: {}", self.source);
        tokio::time::sleep(Duration::from_millis(100)).await;

        let data = RawData {
            id: "DATA-001".to_string(),
            value: 150,
            timestamp: "2025-01-01T10:00:00Z".to_string(),
        };

        println!("      Fetched: id={}, value={}", data.id, data.value);
        Ok(data)
    }

    // Step 2: Transform the raw data
    // inputs automatically adds fetch_raw_data to dependencies (no need for depends_on!)
    #[step(inputs(raw = "fetch_raw_data"))]
    async fn transform_data(self: Arc<Self>, raw: RawData) -> Result<TransformedData, String> {
        println!("\n[2/4] Transforming data: {}", raw.id);
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Normalize value to 0-1 range (assuming max is 200)
        let normalized = raw.value as f64 / 200.0;

        let transformed = TransformedData {
            id: raw.id,
            normalized_value: normalized,
            timestamp: raw.timestamp,
        };

        println!(
            "      Transformed: {} -> normalized={}",
            raw.value, transformed.normalized_value
        );
        Ok(transformed)
    }

    // Step 3: Validate the transformed data
    // inputs automatically adds transform_data to dependencies
    #[step(inputs(data = "transform_data"))]
    async fn validate_data(
        self: Arc<Self>,
        data: TransformedData,
    ) -> Result<ValidationResult, String> {
        println!("\n[3/4] Validating transformed data: {}", data.id);
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Validation rule: normalized value must be between 0.3 and 0.9
        let is_valid = data.normalized_value >= 0.3 && data.normalized_value <= 0.9;
        let message = if is_valid {
            "Data is within acceptable range".to_string()
        } else {
            format!(
                "Data out of range: {} (expected 0.3-0.9)",
                data.normalized_value
            )
        };

        println!(
            "      Validation: {} - {}",
            if is_valid { "PASS" } else { "FAIL" },
            message
        );

        Ok(ValidationResult {
            data,
            is_valid,
            validation_message: message,
        })
    }

    // Step 4: Save the validated result
    // inputs automatically adds validate_data to dependencies
    #[step(inputs(result = "validate_data"))]
    async fn save_result(self: Arc<Self>, result: ValidationResult) -> Result<String, String> {
        println!("\n[4/4] Saving result for: {}", result.data.id);
        tokio::time::sleep(Duration::from_millis(100)).await;

        if !result.is_valid {
            println!("      Skipping save - data failed validation");
            return Err(format!("Validation failed: {}", result.validation_message));
        }

        let save_path = format!("/data/{}.json", result.data.id);
        println!("      Saved to: {}", save_path);
        println!(
            "      Data: normalized_value={}",
            result.data.normalized_value
        );

        Ok(save_path)
    }

    // Flow orchestrator: register all steps
    #[flow]
    async fn process_pipeline(self: Arc<Self>) -> Result<String, String> {
        // Steps execute sequentially in registration order
        // Data flows through inputs automatically
        self.register_fetch_raw_data();
        self.register_transform_data();
        self.register_validate_data();
        self.register_save_result()
    }
}

// Example 2: Multi-branch pipeline with inputs
#[derive(Debug, Clone, Serialize, Deserialize)]
struct EnrichmentPipeline {
    user_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UserProfile {
    id: String,
    name: String,
    age: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Preferences {
    theme: String,
    language: String,
}

impl EnrichmentPipeline {
    fn new(user_id: String) -> Self {
        Self { user_id }
    }

    // Root: Fetch user profile
    #[step]
    async fn fetch_profile(self: Arc<Self>) -> Result<UserProfile, String> {
        println!("\n[Branch] Fetching profile for user: {}", self.user_id);
        tokio::time::sleep(Duration::from_millis(100)).await;

        Ok(UserProfile {
            id: self.user_id.clone(),
            name: "Alice".to_string(),
            age: 30,
        })
    }

    // Branch 1: Fetch preferences (depends on profile)
    #[step(depends_on = "fetch_profile")]
    async fn fetch_preferences(self: Arc<Self>) -> Result<Preferences, String> {
        println!("[Branch] Fetching preferences (parallel with analytics)");
        tokio::time::sleep(Duration::from_millis(100)).await;

        Ok(Preferences {
            theme: "dark".to_string(),
            language: "en".to_string(),
        })
    }

    // Branch 2: Fetch analytics (also depends on profile - runs in PARALLEL)
    #[step(depends_on = "fetch_profile")]
    async fn fetch_analytics(self: Arc<Self>) -> Result<i32, String> {
        println!("[Branch] Fetching analytics (parallel with preferences)");
        tokio::time::sleep(Duration::from_millis(100)).await;

        Ok(42) // Activity score
    }

    // Merge: Combine all data using inputs
    // inputs automatically adds all three steps to dependencies (no depends_on needed!)
    #[step(inputs(
        profile = "fetch_profile",
        prefs = "fetch_preferences",
        score = "fetch_analytics"
    ))]
    async fn enrich_profile(
        self: Arc<Self>,
        profile: UserProfile,
        prefs: Preferences,
        score: i32,
    ) -> Result<String, String> {
        println!("\n[Merge] Enriching profile with all data");
        tokio::time::sleep(Duration::from_millis(50)).await;

        println!("  Profile: {} (age {})", profile.name, profile.age);
        println!(
            "  Preferences: theme={}, lang={}",
            prefs.theme, prefs.language
        );
        println!("  Activity Score: {}", score);

        Ok(format!(
            "Enriched profile for {} with score {}",
            profile.name, score
        ))
    }

    #[flow]
    async fn process(self: Arc<Self>) -> Result<String, String> {
        self.register_fetch_profile();
        self.register_fetch_preferences(); // Parallel with analytics
        self.register_fetch_analytics(); // Parallel with preferences
        self.register_enrich_profile() // Merges all three
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n=== EXAMPLE 1: Linear Sequential Pipeline with Inputs ===");
    println!("Pattern: Fetch -> Transform -> Validate -> Save");
    println!("Each step receives data from previous step via inputs attribute\n");

    let storage1 = Arc::new(InMemoryExecutionLog::new());
    storage1.reset().await?;

    let pipeline = Arc::new(DataPipeline::new("database://users".to_string()));
    let flow_id1 = Uuid::new_v4();

    let instance1 = Ergon::new_flow(Arc::clone(&pipeline), flow_id1, Arc::clone(&storage1));
    let result1 = instance1.execute(|f| f.process_pipeline()).await;

    println!("\n=== Result ===");
    println!("{:?}", result1);

    println!("\n\n=== EXAMPLE 2: Parallel Branches with Input Merging ===");
    println!("Pattern: Root -> (Branch1 || Branch2) -> Merge");
    println!("Merge step receives data from all branches via inputs\n");

    let storage2 = Arc::new(InMemoryExecutionLog::new());
    storage2.reset().await?;

    let enrichment = Arc::new(EnrichmentPipeline::new("USER-123".to_string()));
    let flow_id2 = Uuid::new_v4();

    let instance2 = Ergon::new_flow(Arc::clone(&enrichment), flow_id2, Arc::clone(&storage2));
    let result2 = instance2.execute(|f| f.process()).await;

    println!("\n=== Result ===");
    println!("{:?}", result2);

    println!("\n\n=== Key Takeaways ===");
    println!("1. inputs automatically adds referenced steps to dependencies");
    println!("2. No need to duplicate step names in depends_on and inputs");
    println!("3. Type-safe data wiring with automatic serialization/deserialization");
    println!("4. Can reference any earlier step (not just the previous one)");
    println!("5. Multiple inputs create fan-in merge patterns naturally");

    Ok(())
}
