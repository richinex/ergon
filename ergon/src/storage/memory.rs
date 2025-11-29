use super::{error::Result, error::StorageError, params::InvocationStartParams, ExecutionLog};
use crate::core::{hash_params, Invocation, InvocationStatus};
use async_trait::async_trait;
use chrono::Utc;
use uuid::Uuid;

/// In-memory execution log using DashMap for concurrent access.
///
/// This implementation uses DashMap, a lock-free concurrent HashMap,
/// to provide thread-safe in-memory storage without the single-write
/// limitation of SQLite's shared cache mode. This is suitable for
/// both sequential and parallel DAG execution.
///
/// Reference: https://github.com/xacrimon/dashmap
/// DashMap provides concurrent access through sharding, eliminating
/// the bottleneck of a single mutex.
pub struct InMemoryExecutionLog {
    /// Concurrent storage for invocations keyed by (flow_id, step)
    invocations: dashmap::DashMap<(Uuid, i32), Invocation>,
}

impl InMemoryExecutionLog {
    /// Creates a new in-memory execution log.
    pub fn new() -> Self {
        Self {
            invocations: dashmap::DashMap::new(),
        }
    }
}

impl Default for InMemoryExecutionLog {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ExecutionLog for InMemoryExecutionLog {
    async fn log_invocation_start(&self, params: InvocationStartParams<'_>) -> Result<()> {
        // Destructure params
        let InvocationStartParams {
            id,
            step,
            class_name,
            method_name,
            delay,
            status,
            parameters,
        } = params;

        let params_hash = hash_params(parameters);
        let delay_ms = delay.map(|d| d.as_millis() as i64);

        let invocation = Invocation::new(
            id,
            step,
            Utc::now(),
            class_name.to_string(),
            method_name.to_string(),
            status,
            1,
            parameters.to_vec(),
            params_hash,
            None,
            delay_ms,
        );

        let key = (id, step);
        self.invocations.insert(key, invocation);
        Ok(())
    }

    async fn log_invocation_completion(
        &self,
        id: Uuid,
        step: i32,
        return_value: &[u8],
    ) -> Result<Invocation> {
        let key = (id, step);
        if let Some(mut entry) = self.invocations.get_mut(&key) {
            let delay_ms = entry.delay().map(|d| d.as_millis() as i64);

            let invocation = Invocation::new(
                entry.id(),
                entry.step(),
                entry.timestamp(),
                entry.class_name().to_string(),
                entry.method_name().to_string(),
                InvocationStatus::Complete,
                entry.attempts(),
                entry.parameters().to_vec(),
                entry.params_hash(),
                Some(return_value.to_vec()),
                delay_ms,
            );
            *entry = invocation.clone();
            Ok(invocation)
        } else {
            Err(StorageError::InvocationNotFound { id, step })
        }
    }

    async fn get_invocation(&self, id: Uuid, step: i32) -> Result<Option<Invocation>> {
        let key = (id, step);
        Ok(self
            .invocations
            .get(&key)
            .map(|entry| entry.value().clone()))
    }

    async fn get_latest_invocation(&self, id: Uuid) -> Result<Option<Invocation>> {
        let invocations = self.get_invocations_for_flow(id).await?;
        Ok(invocations.into_iter().max_by_key(|inv| inv.step()))
    }

    async fn get_invocations_for_flow(&self, id: Uuid) -> Result<Vec<Invocation>> {
        let mut invocations: Vec<Invocation> = self
            .invocations
            .iter()
            .filter(|entry| entry.key().0 == id)
            .map(|entry| entry.value().clone())
            .collect();

        invocations.sort_by_key(|inv| inv.step());
        Ok(invocations)
    }

    async fn get_incomplete_flows(&self) -> Result<Vec<Invocation>> {
        let invocations: Vec<Invocation> = self
            .invocations
            .iter()
            .filter(|entry| entry.value().status() != InvocationStatus::Complete)
            .map(|entry| entry.value().clone())
            .collect();

        Ok(invocations)
    }

    async fn reset(&self) -> Result<()> {
        self.invocations.clear();
        Ok(())
    }

    async fn close(&self) -> Result<()> {
        Ok(())
    }
}
