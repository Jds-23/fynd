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
        protocol_registry::{is_rfq_protocol, strip_fallback_prefix},
    },
    post_process::{PostProcessRequest, RoutePostProcessor},
    replay::{simulate_swaps, ReplayError, SimulatedRoute, SimulationEnds},
    types::{ComponentId, Route, RouteResult, Swap},
};

/// Tries to replace swaps of the route with one swap through an RFQ component.
///
/// It replaces one swap, or all swaps of a segment: the run of swaps between two tokens that every
/// path of the order passes through. It keeps a replacement only when the net amount out rises.
/// Only sell orders change. It uses only RFQ components that the request
/// does not exclude and the pool does not drop. An RFQ protocol that cannot fill partially replaces
/// only swaps that start at the sell token. The server must index `rfq:` or `fallback:rfq:`
/// protocols for the replacer to find components.
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
        for (id, topology) in added_components {
            self.index.remove(id);
            if view
                .get_component(id)
                .is_some_and(|component| is_rfq_protocol(&component.protocol_system))
            {
                self.index
                    .insert(id.clone(), topology.tokens.clone());
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
                .filter(|system| is_rfq_protocol(system))
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
        if self.index.is_empty() || !request.order().is_sell() {
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

/// RFQ protocols whose signed quotes support input amounts smaller than the quoted amount. The
/// replacer uses other RFQ protocols only for swaps starting at the sell token, where the input
/// amount matches the quoted amount exactly.
const PARTIAL_FILL_PROTOCOLS: &[&str] = &["rfq:bebop", "rfq:hashflow", "rfq:native"];

/// True when `protocol_system` is in [`PARTIAL_FILL_PROTOCOLS`], through its own executor or its
/// fallback router.
fn fills_partially(protocol_system: &str) -> bool {
    PARTIAL_FILL_PROTOCOLS.contains(&strip_fallback_prefix(protocol_system))
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
                if !fits(&self.rfq[position], &target, swaps, &self.input_token) {
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
/// A leg whose protocol cannot fill partially fits only a target that starts at `sell_token`.
fn fits(leg: &Swap, target: &Target, swaps: &[Swap], sell_token: &Address) -> bool {
    let tokens = &leg.protocol_component().tokens;
    (fills_partially(leg.protocol()) || target.token_in == *sell_token) &&
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use rstest::rstest;
    use tycho_simulation::tycho_common::{
        models::protocol::ProtocolComponent, simulation::protocol_sim::ProtocolSim,
    };

    use super::*;
    use crate::{
        algorithm::test_utils::{
            component, component_with_protocol, order, setup_market_weighted, token,
            MockProtocolSim,
        },
        derived::DerivedData,
        types::{Order, OrderSide, RouteExclusions},
    };

    fn far_deadline() -> Instant {
        Instant::now() + Duration::from_secs(60)
    }

    fn tokens() -> (Token, Token, Token) {
        (token(0x0A, "A"), token(0x0B, "B"), token(0x0C, "C"))
    }

    fn market() -> MarketData {
        setup_market_weighted(Vec::new()).0
    }

    async fn add_component(
        market: &MarketData,
        id: &str,
        protocol: &str,
        component_tokens: &[&Token],
        sim: MockProtocolSim,
    ) {
        let component_tokens: Vec<Token> = component_tokens
            .iter()
            .map(|token| (*token).clone())
            .collect();
        let mut state = market.write().await;
        state.upsert_components(std::iter::once(component_with_protocol(
            id,
            protocol,
            &component_tokens,
        )));
        state.update_states([(id.to_string(), Box::new(sim) as Box<dyn ProtocolSim>)]);
        state.upsert_tokens(component_tokens);
    }

    async fn add_rfq(
        market: &MarketData,
        id: &str,
        component_tokens: &[&Token],
        sim: MockProtocolSim,
    ) {
        add_component(market, id, "rfq:bebop", component_tokens, sim).await;
    }

    async fn replacer(market: &MarketData) -> RfqSwapReplacer {
        let mut replacer = RfqSwapReplacer::default();
        replacer.initialize(market).await;
        replacer
    }

    /// A swap as returned by the algorithm, with gas priced at one output-token unit per gas unit.
    fn swap(id: &str, pair: (&Token, &Token), amounts: (u64, u64), split: f64, price: f64) -> Swap {
        Swap::new(
            id.to_string(),
            "uniswap_v2".to_string(),
            pair.0.address.clone(),
            pair.1.address.clone(),
            BigUint::from(amounts.0),
            BigUint::from(amounts.1),
            BigUint::from(10u64),
            component(id, &[pair.0.clone(), pair.1.clone()]),
            Box::new(MockProtocolSim::new(price).with_gas(10)),
        )
        .with_split(split)
    }

    fn result(swaps: Vec<Swap>, all_tokens: &[&Token]) -> RouteResult {
        let gas: u64 = swaps.len() as u64 * 10;
        let output = swaps
            .last()
            .expect("the test route has swaps")
            .token_out()
            .clone();
        let route = Route::new(
            swaps,
            all_tokens
                .iter()
                .map(|token| (token.address.clone(), (*token).clone())),
        )
        .expect("the test swaps form a route");
        let amount_out = BigInt::from(route.amount_out(&output));
        RouteResult::new(route, amount_out - gas, BigUint::from(1u64))
    }

    fn sequential_route() -> RouteResult {
        let (a, b, c) = tokens();
        result(
            vec![
                swap("ab", (&a, &b), (1000, 2000), 0.0, 2.0),
                swap("bc", (&b, &c), (2000, 4000), 0.0, 2.0),
            ],
            &[&a, &b, &c],
        )
    }

    fn sell_order(from: &Token, to: &Token) -> Order {
        order(from, to, 1000, OrderSide::Sell)
    }

    async fn run(
        replacer: &RfqSwapReplacer,
        market: &MarketData,
        order: &Order,
        exclusions: &RouteExclusions,
        route: RouteResult,
    ) -> Option<RouteResult> {
        let derived = DerivedData::new_shared();
        let request = PostProcessRequest::new(order, market, &derived, exclusions, far_deadline());
        replacer.process(&request, &route).await
    }

    fn replaced(out: Option<RouteResult>) -> RouteResult {
        out.expect("the replacer replaces the route")
    }

    /// The component ids of the replacement, or none when the replacer keeps the route.
    fn replaced_ids(out: &Option<RouteResult>) -> Vec<&str> {
        out.as_ref()
            .map(component_ids)
            .unwrap_or_default()
    }

    fn component_ids(result: &RouteResult) -> Vec<&str> {
        result
            .route()
            .swaps()
            .iter()
            .map(Swap::component_id)
            .collect()
    }

    #[tokio::test]
    async fn test_rfq_hop_with_downstream_swaps() {
        let (a, b, c) = tokens();
        let market = market();
        add_rfq(&market, "rfq_ab", &[&a, &b], MockProtocolSim::new(3.0).with_gas(10)).await;
        let replacer = replacer(&market).await;

        let out = replaced(
            run(
                &replacer,
                &market,
                &sell_order(&a, &c),
                &RouteExclusions::default(),
                sequential_route(),
            )
            .await,
        );

        assert_eq!(component_ids(&out), ["rfq_ab", "bc"]);
        let swaps = out.route().swaps();
        assert_eq!(swaps[0].amount_out(), &BigUint::from(3000u64));
        assert_eq!(swaps[1].amount_in(), &BigUint::from(3000u64));
        assert_eq!(swaps[1].amount_out(), &BigUint::from(6000u64));
        assert_eq!(out.net_amount_out(), &BigInt::from(5980));
    }

    #[tokio::test]
    async fn test_rfq_lower_amount_out() {
        let (a, b, c) = tokens();
        let market = market();
        add_rfq(&market, "rfq_ab", &[&a, &b], MockProtocolSim::new(1.5).with_gas(10)).await;
        let replacer = replacer(&market).await;

        let out = run(
            &replacer,
            &market,
            &sell_order(&a, &c),
            &RouteExclusions::default(),
            sequential_route(),
        )
        .await;

        assert!(out.is_none());
    }

    #[tokio::test]
    async fn test_rfq_for_whole_order() {
        let (a, _, c) = tokens();
        let market = market();
        add_rfq(&market, "rfq_ac", &[&a, &c], MockProtocolSim::new(5.0).with_gas(5)).await;
        let replacer = replacer(&market).await;

        let out = replaced(
            run(
                &replacer,
                &market,
                &sell_order(&a, &c),
                &RouteExclusions::default(),
                sequential_route(),
            )
            .await,
        );

        assert_eq!(component_ids(&out), ["rfq_ac"]);
        assert_eq!(out.route().swaps()[0].amount_out(), &BigUint::from(5000u64));
        assert_eq!(out.net_amount_out(), &BigInt::from(4995));
    }

    fn split_route() -> RouteResult {
        let (a, b, _) = tokens();
        result(
            vec![
                swap("p1", (&a, &b), (600, 1200), 0.6, 2.0),
                swap("p2", (&a, &b), (400, 800), 0.0, 2.0),
            ],
            &[&a, &b],
        )
    }

    #[tokio::test]
    async fn test_rfq_for_parallel_swaps() {
        let (a, b, _) = tokens();
        let market = market();
        add_rfq(&market, "rfq_ab", &[&a, &b], MockProtocolSim::new(3.0).with_gas(10)).await;
        let replacer = replacer(&market).await;

        let out = replaced(
            run(
                &replacer,
                &market,
                &sell_order(&a, &b),
                &RouteExclusions::default(),
                split_route(),
            )
            .await,
        );

        assert_eq!(component_ids(&out), ["rfq_ab"]);
        assert_eq!(out.route().swaps()[0].amount_in(), &BigUint::from(1000u64));
        assert_eq!(out.net_amount_out(), &BigInt::from(2990));
    }

    #[tokio::test]
    async fn test_rfq_for_one_leg_with_split() {
        let (a, b, _) = tokens();
        let market = market();
        let limited = MockProtocolSim::new(3.0)
            .with_gas(10)
            .with_liquidity(1800);
        add_rfq(&market, "rfq_ab", &[&a, &b], limited).await;
        let replacer = replacer(&market).await;

        let out = replaced(
            run(
                &replacer,
                &market,
                &sell_order(&a, &b),
                &RouteExclusions::default(),
                split_route(),
            )
            .await,
        );

        assert_eq!(component_ids(&out), ["rfq_ab", "p2"]);
        let swaps = out.route().swaps();
        assert_eq!(*swaps[0].split(), 0.6);
        assert_eq!(swaps[0].amount_out(), &BigUint::from(1800u64));
        assert_eq!(swaps[1].amount_out(), &BigUint::from(800u64));
        assert_eq!(out.net_amount_out(), &BigInt::from(2580));
    }

    #[tokio::test]
    async fn test_rfq_excluded_component() {
        let (a, b, c) = tokens();
        let market = market();
        add_rfq(&market, "rfq_ab", &[&a, &b], MockProtocolSim::new(3.0).with_gas(10)).await;
        let replacer = replacer(&market).await;
        let exclusions = RouteExclusions::default().with_pools(["rfq_ab".to_string()]);

        let out =
            run(&replacer, &market, &sell_order(&a, &c), &exclusions, sequential_route()).await;

        assert!(out.is_none());
    }

    fn added(id: &str, pair: &[&Token]) -> MarketEvent {
        MarketEvent::MarketUpdated {
            added_components: [(
                id.to_string(),
                pair.iter()
                    .map(|token| token.address.clone())
                    .collect::<Vec<_>>()
                    .into(),
            )]
            .into_iter()
            .collect(),
            removed_components: Vec::new(),
            updated_components: Vec::new(),
        }
    }

    #[tokio::test]
    async fn test_rfq_component_added_after_initialize() {
        let (a, b, c) = tokens();
        let market = market();
        let mut replacer = replacer(&market).await;
        add_rfq(&market, "rfq_ab", &[&a, &b], MockProtocolSim::new(3.0).with_gas(10)).await;

        replacer
            .handle_event(&added("rfq_ab", &[&a, &b]))
            .await
            .expect("adding an RFQ component");

        let out = replaced(
            run(
                &replacer,
                &market,
                &sell_order(&a, &c),
                &RouteExclusions::default(),
                sequential_route(),
            )
            .await,
        );
        assert_eq!(component_ids(&out), ["rfq_ab", "bc"]);
    }

    #[tokio::test]
    async fn test_rfq_component_removed_after_initialize() {
        let (a, b, c) = tokens();
        let market = market();
        add_rfq(&market, "rfq_ab", &[&a, &b], MockProtocolSim::new(3.0).with_gas(10)).await;
        let mut replacer = replacer(&market).await;
        let removed = MarketEvent::MarketUpdated {
            added_components: FxHashMap::default(),
            removed_components: vec!["rfq_ab".to_string()],
            updated_components: Vec::new(),
        };

        replacer
            .handle_event(&removed)
            .await
            .expect("removing an RFQ component");

        let out = run(
            &replacer,
            &market,
            &sell_order(&a, &c),
            &RouteExclusions::default(),
            sequential_route(),
        )
        .await;
        assert!(out.is_none());
    }

    #[tokio::test]
    async fn test_non_rfq_component_added_after_initialize() {
        let (a, b, c) = tokens();
        let market = market();
        let mut replacer = replacer(&market).await;
        add_component(
            &market,
            "v2_ab",
            "uniswap_v2",
            &[&a, &b],
            MockProtocolSim::new(3.0).with_gas(10),
        )
        .await;

        replacer
            .handle_event(&added("v2_ab", &[&a, &b]))
            .await
            .expect("adding a non-RFQ component");

        let out = run(
            &replacer,
            &market,
            &sell_order(&a, &c),
            &RouteExclusions::default(),
            sequential_route(),
        )
        .await;
        assert!(out.is_none());
    }

    #[tokio::test]
    async fn test_event_before_initialize() {
        let (a, b, c) = tokens();
        let market = market();
        add_rfq(&market, "rfq_ab", &[&a, &b], MockProtocolSim::new(3.0).with_gas(10)).await;
        let mut replacer = RfqSwapReplacer::default();

        replacer
            .handle_event(&added("rfq_ab", &[&a, &b]))
            .await
            .expect("an event before initialize");

        let out = run(
            &replacer,
            &market,
            &sell_order(&a, &c),
            &RouteExclusions::default(),
            sequential_route(),
        )
        .await;
        assert!(out.is_none());
    }

    #[rstest]
    #[case::cheap_gas(50, Some(2450))]
    #[case::costly_gas(600, None)]
    #[tokio::test]
    async fn test_rfq_gas_cost(#[case] rfq_gas: u64, #[case] expected_net: Option<i64>) {
        let (a, b, _) = tokens();
        let market = market();
        add_rfq(&market, "rfq_ab", &[&a, &b], MockProtocolSim::new(2.5).with_gas(rfq_gas)).await;
        let replacer = replacer(&market).await;
        let route = result(vec![swap("ab", (&a, &b), (1000, 2000), 0.0, 2.0)], &[&a, &b]);

        let out =
            run(&replacer, &market, &sell_order(&a, &b), &RouteExclusions::default(), route).await;

        match expected_net {
            Some(net) => {
                let out = replaced(out);
                assert_eq!(component_ids(&out), ["rfq_ab"]);
                assert_eq!(out.net_amount_out(), &BigInt::from(net));
            }
            None => assert!(out.is_none()),
        }
    }

    #[tokio::test]
    async fn test_rfq_for_two_hops() {
        let (a, b, c) = tokens();
        let market = market();
        add_rfq(&market, "rfq_ab", &[&a, &b], MockProtocolSim::new(3.0).with_gas(10)).await;
        add_rfq(&market, "rfq_bc", &[&b, &c], MockProtocolSim::new(3.0).with_gas(10)).await;
        let replacer = replacer(&market).await;

        let out = replaced(
            run(
                &replacer,
                &market,
                &sell_order(&a, &c),
                &RouteExclusions::default(),
                sequential_route(),
            )
            .await,
        );

        assert_eq!(component_ids(&out), ["rfq_ab", "rfq_bc"]);
        let swaps = out.route().swaps();
        assert_eq!(swaps[0].amount_out(), &BigUint::from(3000u64));
        assert_eq!(swaps[1].amount_in(), &BigUint::from(3000u64));
        assert_eq!(swaps[1].amount_out(), &BigUint::from(9000u64));
        assert_eq!(out.net_amount_out(), &BigInt::from(8980));
    }

    #[tokio::test]
    async fn test_rfq_component_covering_two_legs() {
        let (a, b, c) = tokens();
        let market = market();
        add_rfq(&market, "rfq_abc", &[&a, &b, &c], MockProtocolSim::new(3.0).with_gas(10)).await;
        let replacer = replacer(&market).await;

        let out = replaced(
            run(
                &replacer,
                &market,
                &sell_order(&a, &c),
                &RouteExclusions::default(),
                sequential_route(),
            )
            .await,
        );

        assert_eq!(component_ids(&out), ["rfq_abc", "bc"]);
    }

    #[tokio::test]
    async fn test_rfq_deadline_in_the_past() {
        let (a, b, c) = tokens();
        let market = market();
        add_rfq(&market, "rfq_ab", &[&a, &b], MockProtocolSim::new(3.0).with_gas(10)).await;
        let replacer = replacer(&market).await;
        let derived = DerivedData::new_shared();
        let order = sell_order(&a, &c);
        let exclusions = RouteExclusions::default();
        let request =
            PostProcessRequest::new(&order, &market, &derived, &exclusions, Instant::now());

        let out = replacer
            .process(&request, &sequential_route())
            .await;

        assert!(out.is_none());
    }

    #[tokio::test]
    async fn test_rfq_unknown_state_label() {
        let (a, b, c) = tokens();
        let market = market();
        add_rfq(&market, "rfq_ab", &[&a, &b], MockProtocolSim::new(3.0).with_gas(10)).await;
        let replacer = replacer(&market).await;
        let derived = DerivedData::new_shared();
        let order = sell_order(&a, &c);
        let exclusions = RouteExclusions::default();
        let label = "missing".to_string();
        let request =
            PostProcessRequest::new(&order, &market, &derived, &exclusions, far_deadline())
                .with_label(Some(&label));

        let out = replacer
            .process(&request, &sequential_route())
            .await;

        assert!(out.is_none());
    }

    #[tokio::test]
    async fn test_route_without_gas() {
        let (a, b, _) = tokens();
        let market = market();
        add_rfq(&market, "rfq_ab", &[&a, &b], MockProtocolSim::new(3.0).with_gas(10)).await;
        let replacer = replacer(&market).await;
        let free = Swap::new(
            "ab".to_string(),
            "uniswap_v2".to_string(),
            a.address.clone(),
            b.address.clone(),
            BigUint::from(1000u64),
            BigUint::from(2000u64),
            BigUint::ZERO,
            component("ab", &[a.clone(), b.clone()]),
            Box::new(MockProtocolSim::new(2.0)),
        );
        let route = Route::new(vec![free], [(a.address.clone(), a.clone())]).expect("one swap");
        let route = RouteResult::new(route, BigInt::from(2000), BigUint::from(1u64));

        let out =
            run(&replacer, &market, &sell_order(&a, &b), &RouteExclusions::default(), route).await;

        assert!(out.is_none());
    }

    /// Only a protocol that fills partially replaces a swap after the sell token.
    #[rstest]
    #[case::partial_fill_after_sell_token("rfq:bebop", false, &["ab", "rfq"])]
    #[case::partial_fill_via_fallback_router("fallback:rfq:bebop", false, &["ab", "rfq"])]
    #[case::partial_fill_hashflow("rfq:hashflow", false, &["ab", "rfq"])]
    #[case::partial_fill_native("rfq:native", false, &["ab", "rfq"])]
    #[case::exact_fill_after_sell_token("rfq:metric", false, &[])]
    #[case::exact_fill_via_fallback_router("fallback:rfq:metric", false, &[])]
    #[case::exact_fill_at_sell_token("rfq:metric", true, &["rfq", "bc"])]
    #[tokio::test]
    async fn test_rfq_partial_fill(
        #[case] protocol: &str,
        #[case] at_sell_token: bool,
        #[case] expected: &[&str],
    ) {
        let (a, b, c) = tokens();
        let pair = if at_sell_token { [&a, &b] } else { [&b, &c] };
        let market = market();
        add_component(&market, "rfq", protocol, &pair, MockProtocolSim::new(3.0).with_gas(10))
            .await;
        let replacer = replacer(&market).await;

        let out = run(
            &replacer,
            &market,
            &sell_order(&a, &c),
            &RouteExclusions::default(),
            sequential_route(),
        )
        .await;

        assert_eq!(replaced_ids(&out), expected);
    }

    #[tokio::test]
    async fn test_rfq_two_components_same_pair() {
        let (a, b, c) = tokens();
        let market = market();
        add_rfq(&market, "rfq_1", &[&a, &b], MockProtocolSim::new(3.0).with_gas(10)).await;
        add_rfq(&market, "rfq_2", &[&a, &b], MockProtocolSim::new(2.5).with_gas(10)).await;
        let replacer = replacer(&market).await;

        let out = replaced(
            run(
                &replacer,
                &market,
                &sell_order(&a, &c),
                &RouteExclusions::default(),
                sequential_route(),
            )
            .await,
        );

        assert_eq!(component_ids(&out), ["rfq_1", "bc"]);
    }

    #[tokio::test]
    async fn test_rfq_component_dropped_by_pool() {
        let (a, b, c) = tokens();
        let market = market();
        add_rfq(&market, "rfq_ab", &[&a, &b], MockProtocolSim::new(3.0).with_gas(10)).await;
        let replacer = replacer(&market).await;
        let derived = DerivedData::new_shared();
        let order = sell_order(&a, &c);
        let exclusions = RouteExclusions::default();
        let drops = |component: &ProtocolComponent| component.id == "rfq_ab";
        let request =
            PostProcessRequest::new(&order, &market, &derived, &exclusions, far_deadline())
                .with_pool_drops(&drops);

        let out = replacer
            .process(&request, &sequential_route())
            .await;

        assert!(out.is_none());
    }

    #[tokio::test]
    async fn test_rfq_stale_original_route() {
        let (a, b, _) = tokens();
        let market = market();
        add_rfq(&market, "rfq_ab", &[&a, &b], MockProtocolSim::new(2.0).with_gas(10)).await;
        let replacer = replacer(&market).await;
        let stale = result(vec![swap("ab", (&a, &b), (1000, 1500), 0.0, 2.0)], &[&a, &b]);

        let out =
            run(&replacer, &market, &sell_order(&a, &b), &RouteExclusions::default(), stale).await;

        assert!(out.is_none());
    }

    /// The original route claims a net above what a new simulation gives, so a replacement that
    /// beats the simulation can still pay less than the original route.
    #[tokio::test]
    async fn test_rfq_below_original_net() {
        let (a, b, _) = tokens();
        let market = market();
        add_rfq(&market, "rfq_ab", &[&a, &b], MockProtocolSim::new(2.2).with_gas(10)).await;
        let replacer = replacer(&market).await;
        let optimistic = result(vec![swap("ab", (&a, &b), (1000, 2500), 0.0, 2.0)], &[&a, &b]);

        let out =
            run(&replacer, &market, &sell_order(&a, &b), &RouteExclusions::default(), optimistic)
                .await;

        assert!(out.is_none());
    }

    fn legs(spec: &[(char, char)]) -> Vec<Swap> {
        let mut swaps = Vec::new();
        for (position, (from, to)) in spec.iter().enumerate() {
            swaps.push(swap(
                &format!("s{position}"),
                (&letter(*from), &letter(*to)),
                (1, 1),
                0.0,
                1.0,
            ));
        }
        swaps
    }

    fn letter(letter: char) -> Token {
        token(letter as u8 - b'A' + 0x0A, &letter.to_string())
    }

    #[rstest]
    #[case::sequential(&[('A', 'B'), ('B', 'C'), ('C', 'D')], "ABCD", &[&[0, 1][..], &[0, 1, 2], &[1, 2]])]
    #[case::split_and_merge(
        &[('A', 'B'), ('A', 'C'), ('B', 'D'), ('C', 'D')],
        "AD",
        &[&[0, 1, 2, 3][..]]
    )]
    #[case::split_before_hop(&[('A', 'B'), ('A', 'B'), ('B', 'C')], "ABC", &[&[0, 1][..], &[0, 1, 2]])]
    fn test_cut_tokens_and_segments(
        #[case] spec: &[(char, char)],
        #[case] expected_cuts: &str,
        #[case] expected_replaced: &[&[usize]],
    ) {
        let swaps = legs(spec);

        let cuts = find_cut_tokens(&swaps);
        let targets = build_segment_targets(&swaps);

        let expected: Vec<Address> = expected_cuts
            .chars()
            .map(|c| letter(c).address)
            .collect();
        assert_eq!(cuts, expected);
        let replaced: Vec<&[usize]> = targets
            .iter()
            .map(|target| target.replaced.as_slice())
            .collect();
        assert_eq!(replaced, expected_replaced);
    }
}
