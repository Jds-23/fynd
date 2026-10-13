//! Test doubles for post-processors.

use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use async_trait::async_trait;
use num_bigint::{BigInt, BigUint};
use tycho_simulation::tycho_core::models::protocol::ProtocolComponent;

use crate::{
    algorithm::test_utils::{component_with_protocol, token, MockProtocolSim},
    feed::{
        events::{EventError, MarketEvent, MarketEventHandler},
        market_data::MarketData,
    },
    post_process::{PostProcessRequest, RoutePostProcessor},
    types::{Route, RouteResult, Swap},
};

type Rewrite =
    Box<dyn Fn(&PostProcessRequest<'_>, RouteResult) -> Option<RouteResult> + Send + Sync>;

/// A post-processor that applies `rewrite` to a copy of the route after `delay`. It counts the
/// calls it receives and can fail every market event.
pub(crate) struct MockPostProcessor {
    rewrite: Rewrite,
    delay: Duration,
    fail_events: bool,
    pub(crate) initialize_calls: Arc<AtomicUsize>,
    pub(crate) event_count: Arc<AtomicUsize>,
}

impl MockPostProcessor {
    pub(crate) fn new(
        rewrite: impl Fn(RouteResult) -> RouteResult + Send + Sync + 'static,
    ) -> Self {
        Self::with_request(move |_, route| rewrite(route))
    }

    /// Like [`new`](Self::new), for a rewrite that reads the request.
    pub(crate) fn with_request(
        rewrite: impl Fn(&PostProcessRequest<'_>, RouteResult) -> RouteResult + Send + Sync + 'static,
    ) -> Self {
        Self::with_rewrite(Box::new(move |request, route| Some(rewrite(request, route))))
    }

    /// A post-processor that keeps every route.
    pub(crate) fn unchanged() -> Self {
        Self::with_rewrite(Box::new(|_, _| None))
    }

    fn with_rewrite(rewrite: Rewrite) -> Self {
        Self {
            rewrite,
            delay: Duration::ZERO,
            fail_events: false,
            initialize_calls: Arc::default(),
            event_count: Arc::default(),
        }
    }

    pub(crate) fn with_delay(mut self, delay: Duration) -> Self {
        self.delay = delay;
        self
    }

    pub(crate) fn failing_events(mut self) -> Self {
        self.fail_events = true;
        self
    }
}

#[async_trait]
impl MarketEventHandler for MockPostProcessor {
    async fn handle_event(&mut self, _event: &MarketEvent) -> Result<(), EventError> {
        self.event_count
            .fetch_add(1, Ordering::SeqCst);
        if self.fail_events {
            return Err(EventError::GraphErrors(Vec::new()));
        }
        Ok(())
    }
}

#[async_trait]
impl RoutePostProcessor for MockPostProcessor {
    async fn initialize(&mut self, _market: &MarketData) {
        self.initialize_calls
            .fetch_add(1, Ordering::SeqCst);
    }

    async fn process(
        &self,
        request: &PostProcessRequest<'_>,
        route: &RouteResult,
    ) -> Option<RouteResult> {
        tokio::time::sleep(self.delay).await;
        (self.rewrite)(request, route.clone())
    }
}

/// A single token 1 to token 2 swap through `component` that pays `amount_out`.
pub(crate) fn swap_through(component: ProtocolComponent, amount_out: u64) -> Swap {
    Swap::new(
        component.id.to_string(),
        component.protocol_system.clone(),
        token(0x01, "A").address,
        token(0x02, "B").address,
        BigUint::from(100u64),
        BigUint::from(amount_out),
        BigUint::from(1u64),
        component,
        Box::new(MockProtocolSim::new(2.0)),
    )
}

/// A single token 1 to token 2 swap through a pool of `protocol` that pays `amount_out`.
pub(crate) fn direct_swap(protocol: &str, amount_out: u64) -> Swap {
    let tokens = [token(0x01, "A"), token(0x02, "B")];
    swap_through(component_with_protocol("rfq-leg", protocol, &tokens), amount_out)
}

/// A route result of one swap that pays 100, with `net_amount_out`.
pub(crate) fn route_result(net_amount_out: i64) -> RouteResult {
    let route = Route::new(vec![direct_swap("uniswap_v2", 100)], []).expect("one swap");
    RouteResult::new(route, BigInt::from(net_amount_out), BigUint::from(1u64))
}

/// `result` with its net amount out replaced.
pub(crate) fn with_net_amount_out(result: RouteResult, net_amount_out: i64) -> RouteResult {
    RouteResult::new(
        result.route().clone(),
        BigInt::from(net_amount_out),
        result.gas_price().clone(),
    )
}
