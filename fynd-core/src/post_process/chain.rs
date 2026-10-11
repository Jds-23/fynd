//! The post-processors of one worker, run in config order.

use std::time::{Duration, Instant};

use tracing::warn;

use crate::{
    feed::{events::MarketEvent, market_data::MarketData},
    post_process::{PostProcessRequest, RoutePostProcessor},
    types::RouteResult,
};

/// The named post-processor instances of one worker, in config order.
#[derive(Default)]
pub(crate) struct PostProcessors {
    pool_name: String,
    instances: Vec<(String, Box<dyn RoutePostProcessor>)>,
}

impl PostProcessors {
    /// The instances of the pool `pool_name`, each with its registered name.
    pub(crate) fn new(
        pool_name: String,
        instances: Vec<(String, Box<dyn RoutePostProcessor>)>,
    ) -> Self {
        Self { pool_name, instances }
    }

    /// True when the pool sets no post-processors.
    pub(crate) fn is_empty(&self) -> bool {
        self.instances.is_empty()
    }

    /// Calls `initialize` on each post-processor.
    pub(crate) async fn initialize(&mut self, market: &MarketData) {
        for (_, post_processor) in &mut self.instances {
            post_processor.initialize(market).await;
        }
    }

    /// Sends `event` to each post-processor. An error is logged and the next one still gets the
    /// event.
    pub(crate) async fn handle_event(&mut self, event: &MarketEvent) {
        for (name, post_processor) in &mut self.instances {
            if let Err(e) = post_processor.handle_event(event).await {
                warn!(post_processor = name, "Error handling market event: {:?}", e);
            }
        }
    }

    /// Runs the post-processors on `route` in config order. Each one gets the route the one
    /// before returned.
    ///
    /// A post-processor that is still running at the request deadline loses its turn: this keeps
    /// the route it had before the call. Once the deadline has passed, the remaining
    /// post-processors do not run.
    pub(crate) async fn run(
        &self,
        request: &PostProcessRequest<'_>,
        mut route: RouteResult,
    ) -> RouteResult {
        let deadline = request.deadline();
        for (name, post_processor) in &self.instances {
            let started = Instant::now();
            if deadline.is_some_and(|deadline| started >= deadline) {
                warn!(
                    pool = %self.pool_name,
                    post_processor = name,
                    "task deadline passed; skipping the remaining post-processors"
                );
                break;
            }
            route = match deadline {
                Some(deadline) => {
                    let before = route.clone();
                    let processing = post_processor.process(request, route);
                    match tokio::time::timeout_at(deadline.into(), processing).await {
                        Ok(processed) => processed,
                        Err(_) => {
                            warn!(
                                pool = %self.pool_name,
                                post_processor = name,
                                "post-processor passed the task deadline; keeping its input route"
                            );
                            before
                        }
                    }
                }
                None => {
                    post_processor
                        .process(request, route)
                        .await
                }
            };
            record_post_processor_duration(&self.pool_name, name, started.elapsed());
        }
        route
    }

    /// The algorithm's name, followed by the name of each post-processor: who made the route.
    pub(crate) fn producer(&self, algorithm_name: &str) -> String {
        let mut producer = algorithm_name.to_string();
        for (name, _) in &self.instances {
            producer.push_str(" + ");
            producer.push_str(name);
        }
        producer
    }
}

/// Records the wall time of one post-processor run on a route.
fn record_post_processor_duration(pool_name: &str, post_processor: &str, duration: Duration) {
    metrics::histogram!(
        "worker_pool_post_processor_duration_seconds",
        "pool" => pool_name.to_string(),
        "post_processor" => post_processor.to_string()
    )
    .record(duration.as_secs_f64());
}
