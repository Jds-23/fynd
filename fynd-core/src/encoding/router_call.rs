//! The arguments of a Tycho router call, read back from its calldata.
//!
//! A calldata hook that signs over the router's arguments has to read them from the calldata the
//! router executes. The encoder converts some of them (the chain's native token becomes the
//! router's ETH address), and a hook may have written into the swaps. [`RouterCall::decode`] reads
//! every argument layout the [`Encoder`](super::encoder::Encoder) encodes, using the same choice of
//! layout, so the two cannot drift apart.

use alloy::{
    primitives::{keccak256, Address, Bytes as EvmBytes, U256},
    sol_types::SolValue,
};
use num_bigint::BigUint;
use tycho_execution::encoding::evm::approvals::permit2::PermitSingle;
use tycho_simulation::tycho_common::Bytes;

use crate::SolveError;

/// The argument layouts of the router's swap functions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RouterLayout {
    /// `singleSwap` and `sequentialSwap`, which take the same arguments.
    Sequential,
    /// `splitSwap`, which adds the number of tokens before the receiver.
    Split,
    /// A sequential layout that also takes a Permit2 permit and its signature.
    SequentialPermit2,
    /// A split layout that also takes a Permit2 permit and its signature.
    SplitPermit2,
}

impl RouterLayout {
    /// Returns the layout of the router function `function_signature`, or `None` for a function
    /// the router does not have. A vault variant takes the layout of the function it extends.
    pub(crate) fn of(function_signature: &str) -> Option<Self> {
        let split = function_signature.contains("splitSwap");
        if function_signature.contains("Permit2") {
            return Some(if split { Self::SplitPermit2 } else { Self::SequentialPermit2 });
        }
        if split {
            return Some(Self::Split);
        }
        let sequential = function_signature.contains("singleSwap") ||
            function_signature.contains("sequentialSwap");
        sequential.then_some(Self::Sequential)
    }
}

/// The client fee params of a router call, as the calldata carries them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedClientFee {
    fee_units: u32,
    receiver: Bytes,
    max_contribution: BigUint,
    deadline: BigUint,
    signature: Bytes,
}

impl EncodedClientFee {
    /// The router's `clientFeeBps` argument. It holds FeeCalculator fee units, not basis points:
    /// the encoder converts the basis points a client asks for.
    pub fn fee_units(&self) -> u32 {
        self.fee_units
    }

    /// The fee client: the address that receives the fee and signs the params. The zero address
    /// when the request carried no client fee.
    pub fn receiver(&self) -> &Bytes {
        &self.receiver
    }

    /// The most the receiver contributes from its vault.
    pub fn max_contribution(&self) -> &BigUint {
        &self.max_contribution
    }

    /// The Unix timestamp after which the router rejects the params.
    pub fn deadline(&self) -> &BigUint {
        &self.deadline
    }

    /// The receiver's signature over the params, or its placeholder.
    pub fn signature(&self) -> &Bytes {
        &self.signature
    }
}

/// The arguments of a router call that its client fee signature covers, and the client fee
/// params themselves.
#[derive(Debug, Clone)]
pub struct RouterCall {
    amount_in: BigUint,
    token_in: Bytes,
    token_out: Bytes,
    expected_amount_out: BigUint,
    min_amount_out: BigUint,
    receiver: Bytes,
    client_fee: EncodedClientFee,
    swaps: Bytes,
}

type ClientFeeArg = (u32, Address, U256, U256, EvmBytes);
type SequentialArgs = (U256, Address, Address, U256, U256, Address, ClientFeeArg, EvmBytes);
type SplitArgs = (U256, Address, Address, U256, U256, U256, Address, ClientFeeArg, EvmBytes);
type SequentialPermit2Args =
    (U256, Address, Address, U256, U256, Address, ClientFeeArg, PermitSingle, EvmBytes, EvmBytes);
type SplitPermit2Args = (
    U256,
    Address,
    Address,
    U256,
    U256,
    U256,
    Address,
    ClientFeeArg,
    PermitSingle,
    EvmBytes,
    EvmBytes,
);

impl RouterCall {
    /// Decodes the call that `calldata`, selector included, makes to the router function
    /// `function_signature`.
    ///
    /// Trailing bytes after the arguments, such as a calldata watermark, are ignored.
    ///
    /// # Errors
    ///
    /// Returns [`SolveError::FailedEncoding`] when the router has no such function, when the
    /// calldata calls another function, or when its arguments do not decode.
    pub fn decode(function_signature: &str, calldata: &[u8]) -> Result<Self, SolveError> {
        let failed = |reason: String| SolveError::FailedEncoding(reason);
        let layout = RouterLayout::of(function_signature).ok_or_else(|| {
            failed(format!("the Tycho router has no function {function_signature}"))
        })?;
        let selector = &keccak256(function_signature.as_bytes())[..4];
        let args = calldata
            .strip_prefix(selector)
            .ok_or_else(|| failed(format!("the calldata does not call {function_signature}")))?;
        let undecodable = |error: alloy::sol_types::Error| {
            failed(format!("cannot decode the arguments of {function_signature}: {error}"))
        };
        // Each layout is read down to the sequential one: the extra arguments carry no value the
        // client fee signature covers.
        let args: SequentialArgs = match layout {
            RouterLayout::Sequential => {
                SequentialArgs::abi_decode_params(args).map_err(undecodable)?
            }
            RouterLayout::Split => {
                let (amount_in, token_in, token_out, expected, min, _, receiver, fee, swaps) =
                    SplitArgs::abi_decode_params(args).map_err(undecodable)?;
                (amount_in, token_in, token_out, expected, min, receiver, fee, swaps)
            }
            RouterLayout::SequentialPermit2 => {
                let (amount_in, token_in, token_out, expected, min, receiver, fee, _, _, swaps) =
                    SequentialPermit2Args::abi_decode_params(args).map_err(undecodable)?;
                (amount_in, token_in, token_out, expected, min, receiver, fee, swaps)
            }
            RouterLayout::SplitPermit2 => {
                let (amount_in, token_in, token_out, expected, min, _, receiver, fee, _, _, swaps) =
                    SplitPermit2Args::abi_decode_params(args).map_err(undecodable)?;
                (amount_in, token_in, token_out, expected, min, receiver, fee, swaps)
            }
        };
        Ok(Self::from_args(args))
    }

    fn from_args(args: SequentialArgs) -> Self {
        let (
            amount_in,
            token_in,
            token_out,
            expected_amount_out,
            min_amount_out,
            receiver,
            (fee_units, fee_receiver, max_contribution, deadline, signature),
            swaps,
        ) = args;
        Self {
            amount_in: to_biguint(amount_in),
            token_in: to_bytes(token_in),
            token_out: to_bytes(token_out),
            expected_amount_out: to_biguint(expected_amount_out),
            min_amount_out: to_biguint(min_amount_out),
            receiver: to_bytes(receiver),
            client_fee: EncodedClientFee {
                fee_units,
                receiver: to_bytes(fee_receiver),
                max_contribution: to_biguint(max_contribution),
                deadline: to_biguint(deadline),
                signature: Bytes::from(signature.to_vec()),
            },
            swaps: Bytes::from(swaps.to_vec()),
        }
    }

    /// The exact amount the swap sells.
    pub fn amount_in(&self) -> &BigUint {
        &self.amount_in
    }

    /// The token the swap sells, as the router reads it: the router's ETH address for the native
    /// token.
    pub fn token_in(&self) -> &Bytes {
        &self.token_in
    }

    /// The token the swap buys, as the router reads it: the router's ETH address for the native
    /// token.
    pub fn token_out(&self) -> &Bytes {
        &self.token_out
    }

    /// The quoted amount out, which the router measures positive slippage against.
    pub fn expected_amount_out(&self) -> &BigUint {
        &self.expected_amount_out
    }

    /// The smallest amount out the router accepts.
    pub fn min_amount_out(&self) -> &BigUint {
        &self.min_amount_out
    }

    /// The address that receives the swap's output.
    pub fn receiver(&self) -> &Bytes {
        &self.receiver
    }

    /// The client fee params.
    pub fn client_fee(&self) -> &EncodedClientFee {
        &self.client_fee
    }

    /// The encoded swaps.
    pub fn swaps(&self) -> &Bytes {
        &self.swaps
    }
}

fn to_biguint(value: U256) -> BigUint {
    BigUint::from_bytes_be(&value.to_be_bytes::<32>())
}

fn to_bytes(address: Address) -> Bytes {
    Bytes::from(address.as_slice().to_vec())
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case::single("singleSwap(uint256)", Some(RouterLayout::Sequential))]
    #[case::sequential("sequentialSwap(uint256)", Some(RouterLayout::Sequential))]
    #[case::single_vault("singleSwapUsingVault(uint256)", Some(RouterLayout::Sequential))]
    #[case::split("splitSwap(uint256)", Some(RouterLayout::Split))]
    #[case::single_permit2("singleSwapPermit2(uint256)", Some(RouterLayout::SequentialPermit2))]
    #[case::split_permit2("splitSwapPermit2(uint256)", Some(RouterLayout::SplitPermit2))]
    #[case::unknown("deposit(uint256)", None)]
    fn test_router_layout_of(
        #[case] function_signature: &str,
        #[case] expected: Option<RouterLayout>,
    ) {
        assert_eq!(RouterLayout::of(function_signature), expected);
    }

    #[test]
    fn test_decode_unknown_function() {
        assert!(RouterCall::decode("deposit(uint256)", &[0u8; 36]).is_err());
    }

    #[test]
    fn test_decode_another_function() {
        let calldata = [0u8; 64];

        assert!(RouterCall::decode("singleSwap(uint256)", &calldata).is_err());
    }

    #[test]
    fn test_decode_truncated_arguments() {
        let signature = "singleSwap(uint256,address,address,uint256,uint256,address,\
                         (uint32,address,uint256,uint256,bytes),bytes)";
        let mut calldata = keccak256(signature.as_bytes())[..4].to_vec();
        calldata.extend([0u8; 64]);

        assert!(RouterCall::decode(signature, &calldata).is_err());
    }
}
