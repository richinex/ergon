//! Comprehensive ETL Pipeline Example - Sequential Version
//!
//! This example demonstrates a real-world ETL (Extract, Transform, Load) pipeline
//! for retail sales analytics using SEQUENTIAL execution (for comparison with parallel version).
//!
//! ## Pipeline Architecture (Medallion-inspired)
//!
//! EXTRACT (Bronze Layer - Raw Data) - SEQUENTIAL:
//!   - extract_customer_data: Simulates reading from CSV files
//!   - extract_sales_data: Simulates API data ingestion
//!   - extract_product_data: Simulates database query results
//!
//! TRANSFORM (Silver Layer - Cleaned & Validated) - SEQUENTIAL:
//!   - validate_customers: Data validation and quality checks
//!   - cleanse_sales: Data cleansing, deduplication, normalization
//!   - enrich_product_data: Data enrichment with external attributes
//!
//! TRANSFORM (Gold Layer - Business Logic & Aggregation) - SEQUENTIAL:
//!   - join_sales_with_products: Combine sales and product information
//!   - aggregate_by_region: Regional sales aggregation
//!   - calculate_customer_metrics: Customer lifetime value and segments
//!
//! LOAD (Analytics Layer) - SEQUENTIAL:
//!   - generate_analytics_report: Final consolidated report
//!   - persist_to_warehouse: Simulate loading to data warehouse
//!
//! ## Scenario
//! - Process retail sales data from 3 sources: customers (CSV), sales (API), products (DB)
//! - 4 customers including 1 invalid record (empty name, bad email)
//! - 5 sales records including 1 duplicate transaction
//! - 3 products across Electronics and Accessories categories
//! - ALL STEPS RUN SEQUENTIALLY - one after another via natural flow control
//! - Extract phase: 3 steps run ONE AT A TIME
//! - Transform-Silver: 3 validation/cleansing steps run ONE AT A TIME
//! - Transform-Gold: Join, aggregate, calculate run ONE AT A TIME
//! - Load phase: Generate report, then persist to warehouse
//! - Final report includes revenue, regional breakdown, top customers, and quality score
//!
//! ## Key Takeaways
//! - Sequential flow using natural async/await control flow (no DAG)
//! - No parallelism - each step waits for previous step to complete
//! - Type-safe data flow between steps using inputs attribute
//! - Automatic deduplication detects duplicate transactions
//! - Data quality scoring based on validation results
//! - JSON report exported to workflow_sequential.json
//! - Execution time comparison shows impact of sequential execution
//! - Medallion architecture (Bronze->Silver->Gold) implemented sequentially
//!
//! ## Run with
//! ```bash
//! cargo run --example comparison_etl_sequential
//! ```

use ergon::deserialize_value;
use ergon::executor::{Executor, FlowOutcome};
use ergon::prelude::*;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::Duration;

// ==================== Data Models ====================

#[derive(Debug, Clone, Serialize, Deserialize, FlowType)]
struct RawCustomer {
    customer_id: String,
    name: String,
    email: String,
    region: String,
    signup_date: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, FlowType)]
struct ValidatedCustomer {
    customer_id: String,
    name: String,
    email: String,
    region: String,
    signup_date: String,
    validation_status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, FlowType)]
struct RawSalesRecord {
    transaction_id: String,
    customer_id: String,
    product_id: String,
    amount: f64,
    quantity: i32,
    timestamp: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, FlowType)]
struct CleansedSalesRecord {
    transaction_id: String,
    customer_id: String,
    product_id: String,
    amount: f64,
    quantity: i32,
    timestamp: String,
    is_duplicate: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, FlowType)]
struct RawProduct {
    product_id: String,
    name: String,
    category: String,
    base_price: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, FlowType)]
struct EnrichedProduct {
    product_id: String,
    name: String,
    category: String,
    base_price: f64,
    margin_percentage: f64,
    supplier: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, FlowType)]
struct SalesWithProduct {
    transaction_id: String,
    customer_id: String,
    product_name: String,
    category: String,
    amount: f64,
    quantity: i32,
    margin: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, FlowType)]
struct RegionalAggregation {
    region: String,
    total_sales: f64,
    transaction_count: usize,
    average_order_value: f64,
    top_category: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, FlowType)]
struct CustomerMetrics {
    customer_id: String,
    total_spend: f64,
    transaction_count: usize,
    average_order_value: f64,
    customer_segment: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, FlowType)]
struct AnalyticsReport {
    report_id: String,
    generated_at: String,
    total_revenue: f64,
    total_transactions: usize,
    unique_customers: usize,
    regional_breakdown: Vec<RegionalAggregation>,
    top_customers: Vec<CustomerMetrics>,
    data_quality_score: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, FlowType)]
struct WarehouseLoadResult {
    success: bool,
    records_loaded: usize,
    load_duration_ms: u64,
    warehouse_table: String,
}

// ==================== ETL Pipeline Implementation ====================

#[derive(Serialize, Deserialize, FlowType)]
struct RetailETLPipeline {
    pipeline_id: String,
    run_date: String,
    generation_timestamp: String,
}

impl RetailETLPipeline {
    fn new(pipeline_id: String, run_date: String, generation_timestamp: String) -> Self {
        Self {
            pipeline_id,
            run_date,
            generation_timestamp,
        }
    }

    // ==================== EXTRACT PHASE ====================

    #[step]
    async fn extract_customer_data(self: Arc<Self>) -> Result<Vec<RawCustomer>, String> {
        println!("  [EXTRACT] Reading customer data from CSV...");
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Simulate CSV extraction
        Ok(vec![
            RawCustomer {
                customer_id: "CUST001".to_string(),
                name: "Alice Johnson".to_string(),
                email: "alice@example.com".to_string(),
                region: "North".to_string(),
                signup_date: "2024-01-15".to_string(),
            },
            RawCustomer {
                customer_id: "CUST002".to_string(),
                name: "Bob Smith".to_string(),
                email: "bob@example.com".to_string(),
                region: "South".to_string(),
                signup_date: "2024-02-20".to_string(),
            },
            RawCustomer {
                customer_id: "CUST003".to_string(),
                name: "Carol Williams".to_string(),
                email: "carol@example.com".to_string(),
                region: "East".to_string(),
                signup_date: "2024-03-10".to_string(),
            },
            RawCustomer {
                customer_id: "CUST004".to_string(),
                name: "".to_string(),               // Invalid: empty name
                email: "invalid-email".to_string(), // Invalid email format
                region: "West".to_string(),
                signup_date: "2024-04-05".to_string(),
            },
        ])
    }

    #[step]
    async fn extract_sales_data(self: Arc<Self>) -> Result<Vec<RawSalesRecord>, String> {
        println!("  [EXTRACT] Fetching sales data from API...");
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Simulate API extraction with some duplicates
        Ok(vec![
            RawSalesRecord {
                transaction_id: "TXN001".to_string(),
                customer_id: "CUST001".to_string(),
                product_id: "PROD001".to_string(),
                amount: 1299.99,
                quantity: 1,
                timestamp: "2024-05-01T10:30:00Z".to_string(),
            },
            RawSalesRecord {
                transaction_id: "TXN002".to_string(),
                customer_id: "CUST001".to_string(),
                product_id: "PROD002".to_string(),
                amount: 49.99,
                quantity: 2,
                timestamp: "2024-05-02T14:20:00Z".to_string(),
            },
            RawSalesRecord {
                transaction_id: "TXN002".to_string(), // Duplicate transaction
                customer_id: "CUST001".to_string(),
                product_id: "PROD002".to_string(),
                amount: 49.99,
                quantity: 2,
                timestamp: "2024-05-02T14:20:00Z".to_string(),
            },
            RawSalesRecord {
                transaction_id: "TXN003".to_string(),
                customer_id: "CUST002".to_string(),
                product_id: "PROD003".to_string(),
                amount: 799.99,
                quantity: 1,
                timestamp: "2024-05-03T09:15:00Z".to_string(),
            },
            RawSalesRecord {
                transaction_id: "TXN004".to_string(),
                customer_id: "CUST003".to_string(),
                product_id: "PROD001".to_string(),
                amount: 1299.99,
                quantity: 1,
                timestamp: "2024-05-04T16:45:00Z".to_string(),
            },
        ])
    }

    #[step]
    async fn extract_product_data(self: Arc<Self>) -> Result<Vec<RawProduct>, String> {
        println!("  [EXTRACT] Querying product data from database...");
        tokio::time::sleep(Duration::from_millis(120)).await;

        // Simulate database query
        Ok(vec![
            RawProduct {
                product_id: "PROD001".to_string(),
                name: "Laptop Pro 15".to_string(),
                category: "Electronics".to_string(),
                base_price: 1299.99,
            },
            RawProduct {
                product_id: "PROD002".to_string(),
                name: "Wireless Mouse".to_string(),
                category: "Accessories".to_string(),
                base_price: 24.99,
            },
            RawProduct {
                product_id: "PROD003".to_string(),
                name: "Mechanical Keyboard".to_string(),
                category: "Accessories".to_string(),
                base_price: 799.99,
            },
        ])
    }

    // ==================== TRANSFORM PHASE - SILVER LAYER ====================

    #[step(inputs(customers = "extract_customer_data"))]
    async fn validate_customers(
        self: Arc<Self>,
        customers: Vec<RawCustomer>,
    ) -> Result<Vec<ValidatedCustomer>, String> {
        println!("  [TRANSFORM-SILVER] Validating customer data...");
        tokio::time::sleep(Duration::from_millis(80)).await;

        let validated: Vec<ValidatedCustomer> = customers
            .into_iter()
            .map(|c| {
                let mut status = Vec::new();

                if c.name.is_empty() {
                    status.push("invalid_name");
                }
                if !c.email.contains('@') {
                    status.push("invalid_email");
                }

                ValidatedCustomer {
                    customer_id: c.customer_id,
                    name: c.name,
                    email: c.email,
                    region: c.region,
                    signup_date: c.signup_date,
                    validation_status: if status.is_empty() {
                        "valid".to_string()
                    } else {
                        format!("invalid: {}", status.join(", "))
                    },
                }
            })
            .collect();

        let valid_count = validated
            .iter()
            .filter(|c| c.validation_status == "valid")
            .count();
        println!(
            "    Validated {}/{} customers",
            valid_count,
            validated.len()
        );

        Ok(validated)
    }

    #[step(inputs(sales = "extract_sales_data"))]
    async fn cleanse_sales(
        self: Arc<Self>,
        sales: Vec<RawSalesRecord>,
    ) -> Result<Vec<CleansedSalesRecord>, String> {
        println!("  [TRANSFORM-SILVER] Cleansing and deduplicating sales data...");
        tokio::time::sleep(Duration::from_millis(90)).await;

        let mut seen_transactions = HashSet::new();
        let mut cleansed = Vec::new();

        for record in sales {
            let is_duplicate = !seen_transactions.insert(record.transaction_id.clone());

            cleansed.push(CleansedSalesRecord {
                transaction_id: record.transaction_id,
                customer_id: record.customer_id,
                product_id: record.product_id,
                amount: record.amount,
                quantity: record.quantity,
                timestamp: record.timestamp,
                is_duplicate,
            });
        }

        let duplicate_count = cleansed.iter().filter(|s| s.is_duplicate).count();
        println!(
            "    Found {} duplicates out of {} records",
            duplicate_count,
            cleansed.len()
        );

        Ok(cleansed)
    }

    #[step(inputs(products = "extract_product_data"))]
    async fn enrich_product_data(
        self: Arc<Self>,
        products: Vec<RawProduct>,
    ) -> Result<Vec<EnrichedProduct>, String> {
        println!("  [TRANSFORM-SILVER] Enriching product data...");
        tokio::time::sleep(Duration::from_millis(100)).await;

        let enriched: Vec<EnrichedProduct> = products
            .into_iter()
            .map(|p| {
                let margin_percentage = if p.category == "Electronics" {
                    15.0
                } else {
                    25.0
                };

                let supplier = if p.category == "Electronics" {
                    "TechSupply Co."
                } else {
                    "Accessory Imports Ltd."
                };

                EnrichedProduct {
                    product_id: p.product_id,
                    name: p.name,
                    category: p.category,
                    base_price: p.base_price,
                    margin_percentage,
                    supplier: supplier.to_string(),
                }
            })
            .collect();

        println!("    Enriched {} products", enriched.len());
        Ok(enriched)
    }

    // ==================== TRANSFORM PHASE - GOLD LAYER ====================

    #[step(inputs(sales = "cleanse_sales", products = "enrich_product_data"))]
    async fn join_sales_with_products(
        self: Arc<Self>,
        sales: Vec<CleansedSalesRecord>,
        products: Vec<EnrichedProduct>,
    ) -> Result<Vec<SalesWithProduct>, String> {
        println!("  [TRANSFORM-GOLD] Joining sales with product data...");
        tokio::time::sleep(Duration::from_millis(70)).await;

        let product_map: HashMap<String, EnrichedProduct> = products
            .into_iter()
            .map(|p| (p.product_id.clone(), p))
            .collect();

        let joined: Vec<SalesWithProduct> = sales
            .into_iter()
            .filter(|s| !s.is_duplicate) // Filter out duplicates
            .filter_map(|s| {
                product_map.get(&s.product_id).map(|p| SalesWithProduct {
                    transaction_id: s.transaction_id,
                    customer_id: s.customer_id,
                    product_name: p.name.clone(),
                    category: p.category.clone(),
                    amount: s.amount,
                    quantity: s.quantity,
                    margin: s.amount * (p.margin_percentage / 100.0),
                })
            })
            .collect();

        println!("    Joined {} sales records with products", joined.len());
        Ok(joined)
    }

    #[step(inputs(sales = "join_sales_with_products", customers = "validate_customers"))]
    async fn aggregate_by_region(
        self: Arc<Self>,
        sales: Vec<SalesWithProduct>,
        customers: Vec<ValidatedCustomer>,
    ) -> Result<Vec<RegionalAggregation>, String> {
        println!("  [TRANSFORM-GOLD] Aggregating sales by region...");
        tokio::time::sleep(Duration::from_millis(60)).await;

        let customer_regions: HashMap<String, String> = customers
            .into_iter()
            .filter(|c| c.validation_status == "valid")
            .map(|c| (c.customer_id, c.region))
            .collect();

        let mut region_data: HashMap<String, (f64, Vec<String>)> = HashMap::new();

        for sale in &sales {
            if let Some(region) = customer_regions.get(&sale.customer_id) {
                let entry = region_data
                    .entry(region.clone())
                    .or_insert((0.0, Vec::new()));
                entry.0 += sale.amount;
                entry.1.push(sale.category.clone());
            }
        }

        let aggregations: Vec<RegionalAggregation> = region_data
            .into_iter()
            .map(|(region, (total_sales, categories))| {
                let transaction_count = categories.len();
                let average_order_value = total_sales / transaction_count as f64;

                let mut category_counts: BTreeMap<String, usize> = BTreeMap::new();
                for cat in &categories {
                    *category_counts.entry(cat.clone()).or_insert(0) += 1;
                }
                let top_category = category_counts
                    .into_iter()
                    .max_by_key(|(_, count)| *count)
                    .map(|(cat, _)| cat)
                    .unwrap_or_else(|| "Unknown".to_string());

                RegionalAggregation {
                    region,
                    total_sales,
                    transaction_count,
                    average_order_value,
                    top_category,
                }
            })
            .collect();

        println!("    Aggregated data for {} regions", aggregations.len());
        Ok(aggregations)
    }

    #[step(inputs(sales = "join_sales_with_products"))]
    async fn calculate_customer_metrics(
        self: Arc<Self>,
        sales: Vec<SalesWithProduct>,
    ) -> Result<Vec<CustomerMetrics>, String> {
        println!("  [TRANSFORM-GOLD] Calculating customer lifetime value and segments...");
        tokio::time::sleep(Duration::from_millis(50)).await;

        let mut customer_data: HashMap<String, (f64, usize)> = HashMap::new();

        for sale in &sales {
            let entry = customer_data
                .entry(sale.customer_id.clone())
                .or_insert((0.0, 0));
            entry.0 += sale.amount;
            entry.1 += 1;
        }

        let mut metrics: Vec<CustomerMetrics> = customer_data
            .into_iter()
            .map(|(customer_id, (total_spend, transaction_count))| {
                let average_order_value = total_spend / transaction_count as f64;

                let customer_segment = if total_spend > 2000.0 {
                    "VIP"
                } else if total_spend > 1000.0 {
                    "Premium"
                } else {
                    "Standard"
                };

                CustomerMetrics {
                    customer_id,
                    total_spend,
                    transaction_count,
                    average_order_value,
                    customer_segment: customer_segment.to_string(),
                }
            })
            .collect();

        metrics.sort_by(|a, b| b.total_spend.partial_cmp(&a.total_spend).unwrap());

        println!("    Calculated metrics for {} customers", metrics.len());
        Ok(metrics)
    }

    // ==================== LOAD PHASE ====================

    #[step(inputs(
        regional_agg = "aggregate_by_region",
        customer_metrics = "calculate_customer_metrics",
        customers = "validate_customers"
    ))]
    async fn generate_analytics_report(
        self: Arc<Self>,
        regional_agg: Vec<RegionalAggregation>,
        customer_metrics: Vec<CustomerMetrics>,
        customers: Vec<ValidatedCustomer>,
    ) -> Result<AnalyticsReport, String> {
        println!("  [LOAD] Generating final analytics report...");
        tokio::time::sleep(Duration::from_millis(100)).await;

        let total_revenue: f64 = regional_agg.iter().map(|r| r.total_sales).sum();
        let total_transactions: usize = regional_agg.iter().map(|r| r.transaction_count).sum();
        let unique_customers = customer_metrics.len();

        let valid_customers = customers
            .iter()
            .filter(|c| c.validation_status == "valid")
            .count();
        let data_quality_score = (valid_customers as f64 / customers.len() as f64) * 100.0;

        let top_customers = customer_metrics.into_iter().take(3).collect();

        Ok(AnalyticsReport {
            report_id: self.pipeline_id.clone(),
            generated_at: self.generation_timestamp.clone(),
            total_revenue,
            total_transactions,
            unique_customers,
            regional_breakdown: regional_agg,
            top_customers,
            data_quality_score,
        })
    }

    #[step(inputs(report = "generate_analytics_report"))]
    async fn persist_to_warehouse(
        self: Arc<Self>,
        report: AnalyticsReport,
    ) -> Result<WarehouseLoadResult, String> {
        println!("  [LOAD] Persisting data to warehouse...");
        let start = std::time::Instant::now();
        tokio::time::sleep(Duration::from_millis(200)).await;

        let records_loaded = report.regional_breakdown.len() + report.top_customers.len();

        Ok(WarehouseLoadResult {
            success: true,
            records_loaded,
            load_duration_ms: start.elapsed().as_millis() as u64,
            warehouse_table: "analytics.daily_sales_report".to_string(),
        })
    }

    // ==================== SEQUENTIAL FLOW ====================

    #[flow]
    async fn run_etl_pipeline(self: Arc<Self>) -> Result<WarehouseLoadResult, String> {
        // Extract phase - runs sequentially
        let customers = self.clone().extract_customer_data().await?;
        let sales = self.clone().extract_sales_data().await?;
        let products = self.clone().extract_product_data().await?;

        // Transform phase - Silver layer - runs sequentially
        let validated_customers = self.clone().validate_customers(customers).await?;
        let cleansed_sales = self.clone().cleanse_sales(sales).await?;
        let enriched_products = self.clone().enrich_product_data(products).await?;

        // Transform phase - Gold layer - runs sequentially
        let sales_with_products = self
            .clone()
            .join_sales_with_products(cleansed_sales, enriched_products)
            .await?;
        let regional_agg = self
            .clone()
            .aggregate_by_region(sales_with_products.clone(), validated_customers.clone())
            .await?;
        let customer_metrics = self
            .clone()
            .calculate_customer_metrics(sales_with_products)
            .await?;

        // Load phase - runs sequentially
        let report = self
            .clone()
            .generate_analytics_report(regional_agg, customer_metrics, validated_customers)
            .await?;
        self.persist_to_warehouse(report).await
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let storage = Arc::new(InMemoryExecutionLog::new());
    storage.reset().await?;

    let pipeline_id = format!("ETL-{}", uuid::Uuid::new_v4());
    let run_date = chrono::Utc::now().format("%Y-%m-%d").to_string();
    let generation_timestamp = chrono::Utc::now().to_rfc3339();

    let pipeline = Arc::new(RetailETLPipeline::new(
        pipeline_id.clone(),
        run_date,
        generation_timestamp,
    ));
    let flow_id = uuid::Uuid::new_v4();

    let instance = Executor::new(flow_id, pipeline, Arc::clone(&storage));

    match instance
        .execute(|p| Box::pin(p.clone().run_etl_pipeline()))
        .await
    {
        FlowOutcome::Completed(Ok(_)) => {}
        FlowOutcome::Completed(Err(e)) => return Err(e.into()),
        FlowOutcome::Suspended(reason) => {
            return Err(format!("Flow suspended unexpectedly: {:?}", reason).into())
        }
    };

    // Retrieve the analytics report from storage for JSON export
    let report_invocation = storage
        .get_invocations_for_flow(flow_id)
        .await?
        .into_iter()
        .find(|inv| inv.method_name().starts_with("generate_analytics_report"))
        .ok_or("Analytics report not found")?;

    let report_bytes = report_invocation
        .return_value()
        .ok_or("Report output not found")?;

    let report: Result<AnalyticsReport, String> = deserialize_value(report_bytes)
        .map_err(|e| format!("Failed to deserialize report: {}", e))?;

    let report = report?;

    // Save report to JSON file
    let json_output = serde_json::to_string_pretty(&report)?;
    let json_filename = "workflow_sequential.json";
    std::fs::write(json_filename, json_output)?;

    Ok(())
}
