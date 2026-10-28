//! Request bodies for `eth_simulateV1` and `debug_traceCall`, with the field names of the
//! execution-apis spec.

use alloy::{
    primitives::{Address, B256, U256},
    rpc::types::{state::StateOverride, trace::geth::GethDebugTracingOptions, TransactionRequest},
};
use serde::Serialize;

/// The block fields that a simulation sets on the block it runs in.
///
/// Alloy's `BlockOverrides` serializes the fee recipient as `coinbase` and `prevrandao` as
/// `random`. The spec, and geth, which decodes strictly, name them `feeRecipient` and
/// `prevRandao`, and geth rejects the alloy names as unknown fields. Alloy accepts the spec names
/// only when it deserializes, so this type owns the serialization of what Fynd sends.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SimulationBlockEnvironment {
    /// The block number, or `None` to leave the numbering to the node.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) number: Option<U256>,
    /// The block timestamp, or `None` to take the node's.
    #[serde(skip_serializing_if = "Option::is_none", with = "alloy::serde::quantity::opt")]
    pub(crate) time: Option<u64>,
    /// The gas limit of the block.
    #[serde(skip_serializing_if = "Option::is_none", with = "alloy::serde::quantity::opt")]
    pub(crate) gas_limit: Option<u64>,
    /// The address that receives the block's fees.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) fee_recipient: Option<Address>,
    /// The `prevrandao` value of the block.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) prev_randao: Option<B256>,
}

/// One block of an `eth_simulateV1` request, holding the calls to run in it.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SimulationBlockCalls {
    pub(crate) block_overrides: SimulationBlockEnvironment,
    pub(crate) state_overrides: StateOverride,
    pub(crate) calls: Vec<TransactionRequest>,
}

/// The first parameter of `eth_simulateV1`.
///
/// Carries the same flags that alloy's `SimulatePayload` sends, all off.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SimulationRequest {
    pub(crate) block_state_calls: Vec<SimulationBlockCalls>,
    trace_transfers: bool,
    validation: bool,
    return_full_transactions: bool,
}

impl SimulationRequest {
    /// Returns a request for one block that holds `call` alone.
    pub(crate) fn single_call(
        call: TransactionRequest,
        state_overrides: StateOverride,
        block_overrides: SimulationBlockEnvironment,
    ) -> Self {
        Self {
            block_state_calls: vec![SimulationBlockCalls {
                block_overrides,
                state_overrides,
                calls: vec![call],
            }],
            trace_transfers: false,
            validation: false,
            return_full_transactions: false,
        }
    }
}

/// The third parameter of `debug_traceCall`.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TraceCallOptions {
    #[serde(flatten)]
    pub(crate) tracing_options: GethDebugTracingOptions,
    pub(crate) state_overrides: StateOverride,
    pub(crate) block_overrides: SimulationBlockEnvironment,
}
