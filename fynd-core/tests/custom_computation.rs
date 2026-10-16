//! A computation written outside `fynd-core`, with only its public API.

use async_trait::async_trait;
use fynd_core::{
    derived::{
        ChangedComponents, ComputationError, ComputationId, ComputationOutput, DerivedComputation,
        DerivedData, SharedDerivedDataRef,
    },
    feed::market_data::MarketData,
    FyndBuilder,
};
use tycho_simulation::tycho_common::models::Chain;

/// Counts the components that each update adds.
struct AddedComponents;

#[async_trait]
impl DerivedComputation for AddedComponents {
    type Output = usize;
    const ID: ComputationId = "added_components";

    async fn compute(
        &self,
        _market: &MarketData,
        _store: &SharedDerivedDataRef,
        changed: &ChangedComponents,
    ) -> Result<ComputationOutput<Self::Output>, ComputationError> {
        Ok(ComputationOutput::success(changed.added.len()))
    }
}

#[tokio::test]
async fn test_computation_reads_the_changed_components() {
    let mut changed = ChangedComponents::default();
    changed
        .added
        .insert("pool".to_string(), Vec::new());

    let output = AddedComponents
        .compute(&MarketData::new_shared(), &DerivedData::new_shared(), &changed)
        .await
        .expect("the computation does not fail");

    assert_eq!(output.data, 1);
}

/// Compiles only while the builder takes a computation that is defined outside `fynd-core`.
#[test]
fn test_builder_takes_a_computation() {
    let _builder = FyndBuilder::new(Chain::Ethereum, "tycho", "http://rpc", Vec::new(), 0.0)
        .with_computation(AddedComponents);
}
