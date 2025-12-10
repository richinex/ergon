//! Comprehensive ETL Pipeline Example
//!
//! This example demonstrates a real-world ETL (Extract, Transform, Load) pipeline
//! for retail sales analytics using the DAG flow system.
//!
//! ## Pipeline Architecture (Medallion-inspired)
//!
//! EXTRACT (Bronze Layer - Raw Data):
//!   - extract_customer_data: Simulates reading from CSV files
//!   - extract_sales_data: Simulates API data ingestion
//!   - extract_product_data: Simulates database query results
//!
//! TRANSFORM (Silver Layer - Cleaned & Validated):
//!   - validate_customers: Data validation and quality checks
//!   - cleanse_sales: Data cleansing, deduplication, normalization
//!   - enrich_product_data: Data enrichment with external attributes
//!
//! TRANSFORM (Gold Layer - Business Logic & Aggregation):
//!   - join_sales_with_products: Combine sales and product information
//!   - aggregate_by_region: Regional sales aggregation
//!   - calculate_customer_metrics: Customer lifetime value and segments
//!
//! LOAD (Analytics Layer):
//!   - generate_analytics_report: Final consolidated report
//!   - persist_to_warehouse: Simulate loading to data warehouse
//!
//! ## Scenario
//! - Process retail sales data from 3 sources: customers (CSV), sales (API), products (DB)
//! - 4 customers including 1 invalid record (empty name, bad email)
//! - 5 sales records including 1 duplicate transaction
//! - 3 products across Electronics and Accessories categories
//! - Extract phase: 3 steps run in parallel (Bronze layer)
//! - Transform-Silver: 3 validation/cleansing steps run in parallel
//! - Transform-Gold: Join sales+products, aggregate by region, calculate customer metrics
//! - Load phase: Generate analytics report and persist to warehouse
//! - Final report includes revenue, regional breakdown, top customers, and quality score
//!
//! ## Key Takeaways
//! - DAG flow enables complex multi-stage pipelines with clear dependencies
//! - Parallel execution at each layer (Extract, Silver, Gold) via depends_on = []
//! - Type-safe data flow between steps using inputs attribute
//! - Automatic deduplication detects duplicate transactions
//! - Data quality scoring based on validation results
//! - JSON report exported to workflow_parallel.json
//! - Parallelism analysis shows which steps ran simultaneously
//! - Medallion architecture (Bronze->Silver->Gold) implemented natively
//!
//! ## Run with
//! ```bash
//! cargo run --example comparison_etl_parallel
//! ```

use ergon::deserialize_value;
use ergon::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
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
}

impl RetailETLPipeline {
    fn new(pipeline_id: String, run_date: String) -> Self {
        Self {
            pipeline_id,
            run_date,
        }
    }

    // ==================== EXTRACT PHASE ====================

    #[step(depends_on = [])] // Explicit: no auto-chain, run in parallel
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

    #[step(depends_on = [])] // Explicit: no auto-chain, run in parallel
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

    #[step(depends_on = [])] // Explicit: no auto-chain, run in parallel
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

    #[step(
        depends_on = "extract_customer_data",
        inputs(customers = "extract_customer_data")
    )]
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

    #[step(
        depends_on = "extract_sales_data",
        inputs(sales = "extract_sales_data")
    )]
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

    #[step(
        depends_on = "extract_product_data",
        inputs(products = "extract_product_data")
    )]
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

    #[step(
        depends_on = ["cleanse_sales", "enrich_product_data"],
        inputs(sales = "cleanse_sales", products = "enrich_product_data")
    )]
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

    #[step(
        depends_on = ["join_sales_with_products", "validate_customers"],
        inputs(sales = "join_sales_with_products", customers = "validate_customers")
    )]
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

    #[step(
        depends_on = "join_sales_with_products",
        inputs(sales = "join_sales_with_products")
    )]
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

    #[step(
        depends_on = ["aggregate_by_region", "calculate_customer_metrics", "validate_customers"],
        inputs(
            regional_agg = "aggregate_by_region",
            customer_metrics = "calculate_customer_metrics",
            customers = "validate_customers"
        )
    )]
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
            generated_at: chrono::Utc::now().to_rfc3339(),
            total_revenue,
            total_transactions,
            unique_customers,
            regional_breakdown: regional_agg,
            top_customers,
            data_quality_score,
        })
    }

    #[step(
        depends_on = "generate_analytics_report",
        inputs(report = "generate_analytics_report")
    )]
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

    // ==================== DAG FLOW ====================

    #[flow]
    async fn run_etl_pipeline(self: Arc<Self>) -> Result<WarehouseLoadResult, String> {
        dag! {
            // Extract phase
            self.register_extract_customer_data();
            self.register_extract_sales_data();
            self.register_extract_product_data();

            // Transform phase - Silver layer
            self.register_validate_customers();
            self.register_cleanse_sales();
            self.register_enrich_product_data();

            // Transform phase - Gold layer
            self.register_join_sales_with_products();
            self.register_aggregate_by_region();
            self.register_calculate_customer_metrics();

            // Load phase
            self.register_generate_analytics_report();
            self.register_persist_to_warehouse()
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("==========================================================");
    println!("    COMPREHENSIVE ETL PIPELINE - Retail Sales Analytics");
    println!("==========================================================\n");

    let storage = Arc::new(InMemoryExecutionLog::new());
    storage.reset().await?;

    let pipeline_id = format!("ETL-{}", uuid::Uuid::new_v4());
    let run_date = chrono::Utc::now().format("%Y-%m-%d").to_string();

    let pipeline = Arc::new(RetailETLPipeline::new(pipeline_id.clone(), run_date));
    let flow_id = uuid::Uuid::new_v4();

    println!("Pipeline ID: {}", pipeline_id);
    println!("Flow ID: {}", flow_id);
    println!("\nStarting ETL execution...\n");

    let start = std::time::Instant::now();
    let executor = Executor::new(flow_id, pipeline, Arc::clone(&storage));

    let (result, elapsed) = match executor
        .execute(|p| Box::pin(p.clone().run_etl_pipeline()))
        .await
    {
        FlowOutcome::Completed(Ok(result)) => (result, start.elapsed()),
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
    let json_filename = "workflow_parallel.json";
    std::fs::write(json_filename, json_output)?;

    println!("\n==========================================================");
    println!("                    EXECUTION RESULTS");
    println!("==========================================================");
    println!("\nWarehouse Load Status:");
    println!("  Success: {}", result.success);
    println!("  Records Loaded: {}", result.records_loaded);
    println!("  Load Duration: {}ms", result.load_duration_ms);
    println!("  Target Table: {}", result.warehouse_table);
    println!("\nJSON Report saved to: {}", json_filename);
    println!("\nTotal Pipeline Execution Time: {:?}", elapsed);

    println!("\n==========================================================");
    println!("                    PIPELINE METRICS");
    println!("==========================================================");

    let invocations = storage.get_invocations_for_flow(flow_id).await?;
    println!("\nTotal Steps Executed: {}", invocations.len());
    println!("\nStep Execution Details (with timestamps):");

    let step_groups = vec![
        (
            "EXTRACT",
            vec![
                "extract_customer_data",
                "extract_sales_data",
                "extract_product_data",
            ],
        ),
        (
            "TRANSFORM-SILVER",
            vec!["validate_customers", "cleanse_sales", "enrich_product_data"],
        ),
        (
            "TRANSFORM-GOLD",
            vec![
                "join_sales_with_products",
                "aggregate_by_region",
                "calculate_customer_metrics",
            ],
        ),
        (
            "LOAD",
            vec!["generate_analytics_report", "persist_to_warehouse"],
        ),
    ];

    for (phase, steps) in &step_groups {
        println!("\n  {} Phase:", phase);
        let mut phase_invocations: Vec<_> = steps
            .iter()
            .filter_map(|step_name| {
                invocations
                    .iter()
                    .find(|i| i.method_name().starts_with(step_name))
                    .map(|inv| (step_name, inv))
            })
            .collect();

        phase_invocations.sort_by_key(|(_, inv)| inv.timestamp());

        for (step_name, inv) in phase_invocations {
            println!(
                "    {:<35} - {:?} at {}",
                step_name,
                inv.status(),
                inv.timestamp().format("%H:%M:%S%.3f")
            );
        }
    }

    println!("\n==========================================================");
    println!("                 PARALLELISM ANALYSIS");
    println!("==========================================================");

    // Analyze which steps ran in parallel
    let extract_steps: Vec<_> = invocations
        .iter()
        .filter(|inv| {
            inv.method_name().starts_with("extract_customer_data")
                || inv.method_name().starts_with("extract_sales_data")
                || inv.method_name().starts_with("extract_product_data")
        })
        .collect();

    if extract_steps.len() == 3 {
        let timestamps: Vec<_> = extract_steps.iter().map(|inv| inv.timestamp()).collect();
        let min_time = timestamps.iter().min().unwrap();
        let max_time = timestamps.iter().max().unwrap();
        let time_diff = (*max_time - *min_time).num_milliseconds();

        println!("\nEXTRACT Phase (3 independent steps):");
        println!("  Start time spread: {}ms", time_diff);
    }

    // Analyze silver layer parallelism
    let silver_steps: Vec<_> = invocations
        .iter()
        .filter(|inv| {
            inv.method_name().starts_with("validate_customers")
                || inv.method_name().starts_with("cleanse_sales")
                || inv.method_name().starts_with("enrich_product_data")
        })
        .collect();

    if silver_steps.len() == 3 {
        let timestamps: Vec<_> = silver_steps.iter().map(|inv| inv.timestamp()).collect();
        let min_time = timestamps.iter().min().unwrap();
        let max_time = timestamps.iter().max().unwrap();
        let time_diff = (*max_time - *min_time).num_milliseconds();

        println!("\nTRANSFORM-SILVER Phase (3 independent steps):");
        println!("  Start time spread: {}ms", time_diff);
    }

    println!("\n==========================================================\n");

    Ok(())
}
