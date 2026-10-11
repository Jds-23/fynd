//! Route post-processors: steps a pool runs on the route its algorithm returned.
//!
//! A pool config lists post-processors by name. Each worker builds one instance of each, keeps it
//! in step with the market events the worker sees, and runs the instances in config order on the
//! [`RouteResult`](crate::types::RouteResult) its algorithm returned. This happens before the
//! worker validates the route. The router ranks the quote after that.
//!
//! ```ignore
//! // In the deployment's binary:
//! let post_processors = PostProcessorRegistry::new().with_post_processor("rfq", Rfq::new)?;
//! let solver = FyndBuilder::new(..).with_post_processors(post_processors).build()?;
//! ```
//! ```toml
//! # In worker_pools.toml:
//! [pools.mine]
//! algorithm = "most_liquid"
//! post_processors = ["rfq"]
//! ```

mod chain;
pub mod registry;

use std::time::Instant;

use async_trait::async_trait;

pub(crate) use self::chain::PostProcessors;
pub use self::registry::{
    PostProcessorAlreadyRegistered, PostProcessorRegistry, UnknownPostProcessorError,
};
use crate::{
    derived::SharedDerivedDataRef,
    feed::{
        events::MarketEventHandler,
        market_data::{MarketData, StateLabel},
    },
    types::{Order, RouteExclusions, RouteResult},
};

/// A step that can replace the route a pool's algorithm returned.
///
/// A worker owns its instances and calls them from one task, so an instance may keep state. It
/// receives the market events the worker's graph manager receives through [`MarketEventHandler`].
/// Events for components the pool drops (exclusive components in a `public_only` pool, excluded
/// protocols) never arrive, but `initialize` sees the whole market.
///
/// The worker validates the route a processor returns. It fails the quote when the route is
/// invalid, uses a pool the request excluded, or uses a component the pool drops. A processor
/// owns `net_amount_out`: the worker does not recompute it, and the router ranks quotes on it.
/// For a sell order the worker also fails the quote when `net_amount_out` is above the route's
/// amount out.
#[async_trait]
pub trait RoutePostProcessor: MarketEventHandler + Send + Sync {
    /// The worker calls this when it builds its graph: at start, and after it falls behind on
    /// market events. Does nothing by default.
    async fn initialize(&mut self, _market: &MarketData) {}

    /// Returns the route to quote. A processor that cannot improve the route returns `route`
    /// unchanged, so a failed optimization never fails a quote.
    ///
    /// The worker stops waiting at [`PostProcessRequest::deadline`] and keeps the route it had
    /// before this call.
    async fn process(&self, request: &PostProcessRequest<'_>, route: RouteResult) -> RouteResult;
}

/// What a post-processor reads about the solve that produced the route.
pub struct PostProcessRequest<'a> {
    order: &'a Order,
    market: &'a MarketData,
    label: Option<&'a StateLabel>,
    derived: &'a SharedDerivedDataRef,
    exclusions: &'a RouteExclusions,
    deadline: Option<Instant>,
}

impl<'a> PostProcessRequest<'a> {
    /// A request with no deadline.
    pub fn new(
        order: &'a Order,
        market: &'a MarketData,
        label: Option<&'a StateLabel>,
        derived: &'a SharedDerivedDataRef,
        exclusions: &'a RouteExclusions,
    ) -> Self {
        Self { order, market, label, derived, exclusions, deadline: None }
    }

    /// Sets the time after which the worker stops waiting for the post-processor.
    #[must_use]
    pub fn with_deadline(mut self, deadline: Instant) -> Self {
        self.deadline = Some(deadline);
        self
    }

    /// The order the route solves.
    pub fn order(&self) -> &'a Order {
        self.order
    }

    /// The shared market. The solve may have read an overlay named by [`label`](Self::label);
    /// [`read_market`](crate::algorithm::paths::read_market) opens the right view.
    pub fn market(&self) -> &'a MarketData {
        self.market
    }

    /// The overlay the solve ran against, if the request named one. Pass it to
    /// [`read_market`](crate::algorithm::paths::read_market) to read the same state.
    pub fn label(&self) -> Option<&'a StateLabel> {
        self.label
    }

    /// The derived data: token prices, component depths.
    pub fn derived(&self) -> &'a SharedDerivedDataRef {
        self.derived
    }

    /// The pools and tokens this route must not use. The worker fails the quote when the returned
    /// route uses one.
    pub fn exclusions(&self) -> &'a RouteExclusions {
        self.exclusions
    }

    /// The time after which the worker stops waiting for the post-processor, if the request has
    /// one. The worker sets it to the deadline of the task.
    pub fn deadline(&self) -> Option<Instant> {
        self.deadline
    }
}
