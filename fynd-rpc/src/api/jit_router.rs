//! Resolves FyndJITRouter encoding from the request headers.
//!
//! The proxy maps a designated API key to the header; Fynd then encodes the quote's transaction
//! to be sent to the chain's `FyndJITRouter` instead of the Tycho router, with the same calldata,
//! so a block builder can improve the route just in time. See `fynd_core::encoding::encoder` for
//! where the address comes from.

use actix_web::http::header::HeaderMap;
use fynd_core::QuoteRequest;

/// Header through which the authenticating proxy requests FyndJITRouter encoding.
const JIT_ROUTER_HEADER: &str = "x-jit-router";

/// Reads whether the authenticating proxy requested FyndJITRouter encoding.
///
/// Only the exact value `true` (case-insensitive) requests it; absent, malformed, and all other
/// values do not. Fynd does not authenticate callers itself, so this header is only meaningful
/// when the server is unreachable except through that proxy — otherwise a caller can set it
/// themselves.
pub fn from_headers(headers: &HeaderMap) -> bool {
    headers
        .get(JIT_ROUTER_HEADER)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.eq_ignore_ascii_case("true"))
}

/// Writes `requested` onto the request's encoding options, overwriting whatever was there.
///
/// The header is the only thing that turns the encoding on: the flag is set from it in both
/// directions, so a body that somehow arrived with it already set cannot keep it. A request
/// without encoding options passes through unchanged — there is nothing to encode.
#[must_use]
pub fn apply(request: QuoteRequest, requested: bool) -> QuoteRequest {
    let Some(encoding_options) = request.options().encoding_options() else {
        return request;
    };
    let options = request
        .options()
        .clone()
        .with_encoding_options(
            encoding_options
                .clone()
                .with_jit_router(requested),
        );
    request.with_options(options)
}

#[cfg(test)]
mod tests {
    use actix_web::http::header::{HeaderName, HeaderValue};
    use fynd_core::{EncodingOptions, Order, OrderSide, QuoteOptions};
    use num_bigint::BigUint;
    use rstest::rstest;
    use tycho_simulation::tycho_common::Bytes;

    use super::*;

    #[rstest]
    #[case::absent(None, false)]
    #[case::lowercase_true(Some("true"), true)]
    #[case::uppercase_true(Some("TRUE"), true)]
    #[case::explicit_false(Some("false"), false)]
    #[case::empty(Some(""), false)]
    #[case::numeric_one(Some("1"), false)]
    #[case::whitespace_padded(Some(" true "), false)]
    fn test_from_headers(#[case] header_value: Option<&str>, #[case] expected: bool) {
        let mut headers = HeaderMap::new();
        if let Some(value) = header_value {
            headers.insert(
                HeaderName::from_static(JIT_ROUTER_HEADER),
                HeaderValue::from_str(value).expect("test header value is valid"),
            );
        }

        assert_eq!(from_headers(&headers), expected);
    }

    fn request(encoding_options: Option<EncodingOptions>) -> QuoteRequest {
        let order = Order::new(
            Bytes::from([0xAAu8; 20]),
            Bytes::from([0xBBu8; 20]),
            BigUint::from(1_000_000_000_000_000_000u64),
            OrderSide::Sell,
            Bytes::from([0xCCu8; 20]),
        );
        let mut options = QuoteOptions::default();
        if let Some(encoding_options) = encoding_options {
            options = options.with_encoding_options(encoding_options);
        }
        QuoteRequest::new(vec![order], options)
    }

    #[rstest]
    #[case::requested(true)]
    #[case::not_requested(false)]
    fn test_apply_sets_flag_from_header(#[case] requested: bool) {
        let request = apply(request(Some(EncodingOptions::new(0.01))), requested);

        assert_eq!(
            request
                .options()
                .encoding_options()
                .expect("options present")
                .jit_router(),
            requested
        );
    }

    #[test]
    fn test_apply_clears_flag_set_in_the_body() {
        let preset = EncodingOptions::new(0.01).with_jit_router(true);

        let request = apply(request(Some(preset)), false);

        assert!(!request
            .options()
            .encoding_options()
            .expect("options present")
            .jit_router());
    }

    #[test]
    fn test_apply_without_encoding_options_is_a_no_op() {
        let request = apply(request(None), true);

        assert!(request
            .options()
            .encoding_options()
            .is_none());
    }
}
