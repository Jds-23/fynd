//! Replaces parts of a route with one RFQ swap when that raises the net amount out.

use std::{collections::BTreeSet, time::Instant};

use async_trait::async_trait;
use num_bigint::{BigInt, BigUint};
use rustc_hash::{FxHashMap, FxHashSet};
use tracing::{debug, trace, warn};
use tycho_simulation::tycho_common::models::{token::Token, Address};

use crate::{
    algorithm::paths::read_market,
    feed::{
        events::{EventError, MarketEvent, MarketEventHandler},
        market_data::{MarketData, MarketDataView},
        protocol_registry::RFQ_PREFIX,
    },
    post_process::{PostProcessRequest, RoutePostProcessor},
    replay::{simulate_swaps, ReplayError, SimulatedRoute, SimulationEnds},
    types::{ComponentId, Route, RouteResult, Swap},
};

/// Tries to replace swaps of the route with one swap through an RFQ component.
///
/// It replaces one swap, or all swaps of a segment: the run of swaps between two tokens that every
/// path of the order passes through. It keeps a replacement only when the net amount out rises.
/// Only sell orders without an exclusive leg change. It uses only RFQ components that the request
/// does not exclude and the pool does not drop. The server must index `rfq:` protocols for the
/// replacer to find components.
#[derive(Default)]
pub(crate) struct RfqSwapReplacer {
    market: Option<MarketData>,
    index: RfqIndex,
}

impl RfqSwapReplacer {
    /// The name that a pool configuration lists to run this post-processor.
    pub(crate) const NAME: &'static str = "rfq_swap_replacer";
}

#[async_trait]
impl MarketEventHandler for RfqSwapReplacer {
    async fn handle_event(&mut self, event: &MarketEvent) -> Result<(), EventError> {
        let Some(market) = &self.market else {
            return Ok(());
        };
        let MarketEvent::MarketUpdated { added_components, removed_components, .. } = event;
        for id in removed_components {
            self.index.remove(id);
        }
        if added_components.is_empty() {
            return Ok(());
        }
        let view = market.read().await;
        for (id, tokens) in added_components {
            self.index.remove(id);
            if view
                .get_component(id)
                .is_some_and(|component| is_rfq(&component.protocol_system))
            {
                self.index
                    .insert(id.clone(), tokens.clone());
            }
        }
        Ok(())
    }
}

#[async_trait]
impl RoutePostProcessor for RfqSwapReplacer {
    async fn initialize(&mut self, market: &MarketData) {
        let mut index = RfqIndex::default();
        {
            let view = market.read().await;
            let state = view.base_market_state();
            for system in state
                .component_counts_by_protocol()
                .keys()
                .filter(|system| is_rfq(system))
            {
                for id in state.components_by_protocol(system) {
                    if let Some(component) = state.get_component(id) {
                        index.insert(id.clone(), component.tokens.clone());
                    }
                }
            }
        }
        self.index = index;
        self.market = Some(market.clone());
    }

    async fn process(
        &self,
        request: &PostProcessRequest<'_>,
        route: &RouteResult,
    ) -> Option<RouteResult> {
        if self.index.is_empty() || !request.order().is_sell() || route.route().has_exclusive_leg()
        {
            return None;
        }
        let snapshot = match read_market(request.market(), request.label().cloned()).await {
            Ok(view) => Snapshot::take(&self.index, &view, request, route.route()),
            Err(error) => {
                debug!(%error, "cannot read the market; keeping the route");
                return None;
            }
        };
        Search::new(snapshot, request, route).and_then(Search::run)
    }
}

fn is_rfq(protocol_system: &str) -> bool {
    protocol_system.starts_with(RFQ_PREFIX)
}

/// The RFQ components of the market, by token.
#[derive(Default)]
struct RfqIndex {
    by_token: FxHashMap<Address, BTreeSet<ComponentId>>,
    tokens_of: FxHashMap<ComponentId, Vec<Address>>,
}

impl RfqIndex {
    fn is_empty(&self) -> bool {
        self.tokens_of.is_empty()
    }

    fn insert(&mut self, id: ComponentId, tokens: Vec<Address>) {
        for token in &tokens {
            self.by_token
                .entry(token.clone())
                .or_default()
                .insert(id.clone());
        }
        self.tokens_of.insert(id, tokens);
    }

    fn remove(&mut self, id: &str) {
        let Some(tokens) = self.tokens_of.remove(id) else {
            return;
        };
        for token in tokens {
            if let Some(ids) = self.by_token.get_mut(&token) {
                ids.remove(id);
                if ids.is_empty() {
                    self.by_token.remove(&token);
                }
            }
        }
    }

    /// The components that hold at least two of `tokens`, in id order.
    fn find_touching(&self, tokens: &FxHashSet<&Address>) -> BTreeSet<&ComponentId> {
        let mut counts: FxHashMap<&ComponentId, usize> = FxHashMap::default();
        for token in tokens {
            for id in self
                .by_token
                .get(*token)
                .into_iter()
                .flatten()
            {
                *counts.entry(id).or_default() += 1;
            }
        }
        let mut touching = BTreeSet::new();
        for (id, count) in counts {
            if count >= 2 {
                touching.insert(id);
            }
        }
        touching
    }
}

/// What a search reads from the market, copied under the read lock so the search holds no lock.
struct Snapshot {
    /// One swap with zero amounts for each RFQ component the search may use, in id order. Its
    /// token pair is a placeholder: `Search` sets the pair and the split before it simulates.
    rfq: Vec<Swap>,
    /// The tokens of the route.
    tokens: FxHashMap<Address, Token>,
}

impl Snapshot {
    /// Copies the tokens of `route` and the RFQ components containing at least two of those
    /// tokens. Omits components the request excludes or the pool drops.
    fn take(
        index: &RfqIndex,
        view: &MarketDataView<'_>,
        request: &PostProcessRequest<'_>,
        route: &Route,
    ) -> Self {
        let mut tokens = route.tokens().clone();
        let mut route_tokens = FxHashSet::default();
        for swap in route.swaps() {
            for address in [swap.token_in(), swap.token_out()] {
                route_tokens.insert(address);
                if let Some(token) = view.get_token(address) {
                    tokens
                        .entry(address.clone())
                        .or_insert_with(|| token.clone());
                }
            }
        }
        let mut rfq = Vec::new();
        for id in index.find_touching(&route_tokens) {
            if request.exclusions().excludes_pool(id) {
                continue;
            }
            let (Some(component), Some(state)) =
                (view.get_component(id), view.get_simulation_state(id))
            else {
                continue;
            };
            let [token_in, token_out, ..] = component.tokens.as_slice() else {
                continue;
            };
            if request.drops_component(component) {
                continue;
            }
            rfq.push(Swap::new(
                id.clone(),
                component.protocol_system.clone(),
                token_in.clone(),
                token_out.clone(),
                BigUint::ZERO,
                BigUint::ZERO,
                BigUint::ZERO,
                component.clone(),
                state.clone_box(),
            ));
        }
        Self { rfq, tokens }
    }
}

/// The gas cost of the original route in the output token, and the gas it used.
struct GasPricing {
    gas_cost_out: BigInt,
    gas: BigInt,
}

impl GasPricing {
    /// Prices the gas of `result` in `output_token`. `None` when the route uses no gas, so the
    /// cost has no price per unit.
    fn of(result: &RouteResult, output_token: &Address) -> Option<Self> {
        let gas = BigInt::from(result.route().total_gas());
        if gas == BigInt::ZERO {
            return None;
        }
        let amount_out = BigInt::from(result.route().amount_out(output_token));
        let gas_cost_out = (amount_out - result.net_amount_out()).max(BigInt::ZERO);
        Some(Self { gas_cost_out, gas })
    }

    /// Returns the net amount out of `simulated`, scaling the original gas cost by the ratio of
    /// simulated gas to original gas.
    fn net_amount_out(&self, simulated: &SimulatedRoute) -> BigInt {
        BigInt::from(simulated.amount_out.clone()) -
            &self.gas_cost_out * BigInt::from(simulated.gas.clone()) / &self.gas
    }
}

/// The swaps to replace, and the pair of tokens the replacement swap connects.
#[derive(Clone)]
struct Target {
    /// Positions of the swaps to replace, in ascending order.
    replaced: Vec<usize>,
    token_in: Address,
    token_out: Address,
    /// The split of the RFQ swap; `0.0` takes what remains of `token_in`.
    split: f64,
}

/// A target with the RFQ swap that replaces it.
struct Candidate {
    target: Target,
    rfq: Swap,
    simulated: SimulatedRoute,
    net_amount_out: BigInt,
}

/// Searches one route for RFQ swaps that raise the net amount out.
struct Search<'a> {
    original: &'a RouteResult,
    rfq: Vec<Swap>,
    deadline: Instant,
    pricing: GasPricing,
    tokens: FxHashMap<Address, Token>,
    input_token: Address,
    output_token: Address,
    total_in: BigUint,
}

impl<'a> Search<'a> {
    /// Prepares a search of `original` on `snapshot`. `None` when the route cannot be searched:
    /// it has no swaps, it starts and ends on one token, or it uses no gas.
    fn new(
        snapshot: Snapshot,
        request: &PostProcessRequest<'_>,
        original: &'a RouteResult,
    ) -> Option<Self> {
        let route = original.route();
        let (input_token, output_token) = (route.input_token()?, route.output_token()?);
        if input_token == output_token {
            return None;
        }
        let pricing = GasPricing::of(original, &output_token)?;
        Some(Self {
            original,
            rfq: snapshot.rfq,
            deadline: request.deadline(),
            pricing,
            tokens: snapshot.tokens,
            input_token,
            output_token,
            total_in: request.order().amount().clone(),
        })
    }

    /// Applies the best replacement until none raises the net amount out, the search has run one
    /// round per original swap, or the deadline passes. A candidate must beat the net amount out
    /// of the current route; the first round simulates the original route again. `None` when the
    /// route did not change, or when the final net amount out is not above the original one.
    fn run(mut self) -> Option<RouteResult> {
        let mut swaps = self.original.route().swaps().to_vec();
        let rounds = swaps.len();
        let mut net_amount_out = self.simulate_baseline(&swaps)?;
        let mut changed = false;
        for _ in 0..rounds {
            if self.deadline_passed() {
                break;
            }
            let Some(candidate) = self.best_candidate(&swaps, &net_amount_out) else {
                break;
            };
            net_amount_out = candidate.net_amount_out.clone();
            swaps = self.apply(&swaps, candidate);
            changed = true;
        }
        if !changed || net_amount_out <= *self.original.net_amount_out() {
            return None;
        }
        let route = Route::new(swaps, self.tokens)
            .inspect_err(|error| {
                warn!(%error, "cannot build the replacement route; keeping the original route");
            })
            .ok()?;
        Some(RouteResult::new(route, net_amount_out, self.original.gas_price().clone()))
    }

    /// The net amount out of `swaps`, simulated again with the pricing of the original route.
    /// `None` when a swap simulation fails.
    fn simulate_baseline(&self, swaps: &[Swap]) -> Option<BigInt> {
        let ordered: Vec<&Swap> = swaps.iter().collect();
        let simulated = self
            .simulate(&ordered)
            .inspect_err(|error| debug!(%error, "cannot simulate the original route"))
            .ok()?;
        Some(self.pricing.net_amount_out(&simulated))
    }

    fn deadline_passed(&self) -> bool {
        Instant::now() >= self.deadline
    }

    /// The candidate with the highest net amount out, if it beats `current_net`. Stops at the
    /// deadline with the best so far.
    fn best_candidate(&mut self, swaps: &[Swap], current_net: &BigInt) -> Option<Candidate> {
        let mut best: Option<Candidate> = None;
        let mut best_net = current_net.clone();
        for target in build_targets(swaps) {
            for position in 0..self.rfq.len() {
                if !fits(&self.rfq[position], &target, swaps) {
                    continue;
                }
                if self.deadline_passed() {
                    return best;
                }
                self.rfq[position].retarget(
                    target.token_in.clone(),
                    target.token_out.clone(),
                    target.split,
                );
                let rfq = &self.rfq[position];
                let Some((simulated, net_amount_out)) = self.evaluate(swaps, &target, rfq) else {
                    continue;
                };
                if net_amount_out > best_net {
                    best_net = net_amount_out.clone();
                    best = Some(Candidate {
                        target: target.clone(),
                        rfq: rfq.clone(),
                        simulated,
                        net_amount_out,
                    });
                }
            }
        }
        best
    }

    /// Simulates the route with `rfq` in place of the target swaps.
    /// `None` when a swap simulation fails.
    fn evaluate(
        &self,
        swaps: &[Swap],
        target: &Target,
        rfq: &Swap,
    ) -> Option<(SimulatedRoute, BigInt)> {
        let ordered = splice(swaps, &target.replaced, rfq);
        let simulated = self
            .simulate(&ordered)
            .inspect_err(|error| {
                trace!(
                    component = rfq.component_id(),
                    %error,
                    "dropped RFQ candidate: route simulation failed"
                );
            })
            .ok()?;
        let net_amount_out = self.pricing.net_amount_out(&simulated);
        Some((simulated, net_amount_out))
    }

    /// Simulates the swaps in `ordered` in topological order, using the snapshot's tokens and each
    /// swap's state.
    fn simulate<'s>(&'s self, ordered: &[&'s Swap]) -> Result<SimulatedRoute, ReplayError> {
        let ends = SimulationEnds {
            input_token: &self.input_token,
            output_token: &self.output_token,
            total_in: &self.total_in,
        };
        simulate_swaps(
            ordered,
            &ends,
            |address| self.tokens.get(address),
            |swap| Some(swap.protocol_state()),
        )
    }

    /// The swaps after the replacement, with the amounts and gas of the simulation.
    fn apply(&self, swaps: &[Swap], candidate: Candidate) -> Vec<Swap> {
        let Candidate { target, rfq, simulated, .. } = candidate;
        let ordered = splice(swaps, &target.replaced, &rfq);
        let mut replaced = Vec::with_capacity(ordered.len());
        for (swap, simulated) in ordered.into_iter().zip(simulated.swaps) {
            replaced.push(
                Swap::new(
                    swap.component_id().to_string(),
                    swap.protocol().to_string(),
                    swap.token_in().clone(),
                    swap.token_out().clone(),
                    simulated.amount_in,
                    simulated.amount_out,
                    simulated.gas,
                    swap.protocol_component().clone(),
                    swap.protocol_state().clone_box(),
                )
                .with_split(*swap.split()),
            );
        }
        replaced
    }
}

/// Whether the RFQ swap `leg` holds both tokens of `target` and `swaps` do not use its component.
fn fits(leg: &Swap, target: &Target, swaps: &[Swap]) -> bool {
    let tokens = &leg.protocol_component().tokens;
    tokens.contains(&target.token_in) &&
        tokens.contains(&target.token_out) &&
        !swaps
            .iter()
            .any(|swap| swap.component_id() == leg.component_id())
}

/// Returns `swaps` with the positions in `replaced` removed and `rfq` inserted at the first
/// removed position. The result keeps a topological order: `rfq` consumes only tokens that the
/// swaps before it make.
fn splice<'a>(swaps: &'a [Swap], replaced: &[usize], rfq: &'a Swap) -> Vec<&'a Swap> {
    let mut ordered = Vec::with_capacity(swaps.len());
    for (position, swap) in swaps.iter().enumerate() {
        if replaced.first() == Some(&position) {
            ordered.push(rfq);
        }
        if !replaced.contains(&position) {
            ordered.push(swap);
        }
    }
    ordered
}

/// Returns replacement targets for individual swaps, followed by targets for segments.
fn build_targets(swaps: &[Swap]) -> Vec<Target> {
    let mut targets = Vec::new();
    for (position, swap) in swaps.iter().enumerate() {
        targets.push(Target {
            replaced: vec![position],
            token_in: swap.token_in().clone(),
            token_out: swap.token_out().clone(),
            split: *swap.split(),
        });
    }
    targets.extend(build_segment_targets(swaps));
    targets
}

/// One target for each pair of cut tokens with more than one swap in the segment between them.
/// The pair of the order's input and output token covers the whole route.
fn build_segment_targets(swaps: &[Swap]) -> Vec<Target> {
    let cuts = find_cut_tokens(swaps);
    let forward: Vec<FxHashSet<Address>> = cuts
        .iter()
        .map(|cut| find_reachable(swaps, cut, Direction::Forward))
        .collect();
    let backward: Vec<FxHashSet<Address>> = cuts
        .iter()
        .map(|cut| find_reachable(swaps, cut, Direction::Backward))
        .collect();
    let mut targets = Vec::new();
    for (i, token_in) in cuts.iter().enumerate() {
        for (j, token_out) in cuts.iter().enumerate().skip(i + 1) {
            let replaced: Vec<usize> = swaps
                .iter()
                .enumerate()
                .filter(|(_, swap)| {
                    forward[i].contains(swap.token_in()) && backward[j].contains(swap.token_out())
                })
                .map(|(position, _)| position)
                .collect();
            if replaced.len() > 1 {
                targets.push(Target {
                    replaced,
                    token_in: token_in.clone(),
                    token_out: token_out.clone(),
                    split: 0.0,
                });
            }
        }
    }
    targets
}

/// The tokens that every path from the first input token to the last output token passes
/// through, in route order. The input and output token come first and last.
fn find_cut_tokens(swaps: &[Swap]) -> Vec<Address> {
    let (Some(first), Some(last)) = (swaps.first(), swaps.last()) else {
        return Vec::new();
    };
    let (input, output) = (first.token_in(), last.token_out());
    let mut seen = FxHashSet::default();
    let mut cuts = vec![input.clone()];
    for swap in swaps {
        let token = swap.token_out();
        let is_end = token == output;
        if !is_end && seen.insert(token) && !connects(swaps, input, output, token) {
            cuts.push(token.clone());
        }
    }
    cuts.push(output.clone());
    cuts
}

/// Whether swaps lead from `from` to `to` without a swap that ends on `skip`.
fn connects(swaps: &[Swap], from: &Address, to: &Address, skip: &Address) -> bool {
    let mut reached = FxHashSet::default();
    reached.insert(from);
    for swap in swaps {
        let (token_in, token_out) = (swap.token_in(), swap.token_out());
        if token_out != skip && reached.contains(token_in) {
            reached.insert(token_out);
        }
    }
    reached.contains(to)
}

enum Direction {
    Forward,
    Backward,
}

/// The tokens that swaps connect to `start`, and `start` itself. `Forward` follows the swaps
/// from input to output, `Backward` the other way.
fn find_reachable(swaps: &[Swap], start: &Address, direction: Direction) -> FxHashSet<Address> {
    let mut reached = FxHashSet::default();
    reached.insert(start.clone());
    match direction {
        Direction::Forward => {
            for swap in swaps {
                if reached.contains(swap.token_in()) {
                    reached.insert(swap.token_out().clone());
                }
            }
        }
        Direction::Backward => {
            for swap in swaps.iter().rev() {
                if reached.contains(swap.token_out()) {
                    reached.insert(swap.token_in().clone());
                }
            }
        }
    }
    reached
}

