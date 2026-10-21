//! The search that the replacing post-processors share.
//!
//! A search takes a route and a set of swaps with zero amounts, one for each market component it
//! may use. It tries each swap in place of a target of the route and keeps the replacement that
//! raises the net amount out most.

use std::{collections::BTreeSet, time::Instant};

use num_bigint::{BigInt, BigUint};
use rustc_hash::{FxHashMap, FxHashSet};
use tracing::{debug, trace, warn};
use tycho_simulation::tycho_common::models::{protocol::ProtocolComponent, token::Token, Address};

use crate::{
    feed::{
        events::MarketEvent,
        market_data::{MarketData, MarketDataView},
    },
    post_process::PostProcessRequest,
    replay::{simulate_swaps, ReplayError, SimulatedRoute, SimulationEnds},
    types::{ComponentId, Route, RouteResult, Swap},
};

/// The components of the market that a replacer may use, by token.
#[derive(Default)]
pub(super) struct ComponentIndex {
    by_token: FxHashMap<Address, BTreeSet<ComponentId>>,
    tokens_of: FxHashMap<ComponentId, Vec<Address>>,
}

impl ComponentIndex {
    pub(super) fn is_empty(&self) -> bool {
        self.tokens_of.is_empty()
    }

    pub(super) fn insert(&mut self, id: ComponentId, tokens: Vec<Address>) {
        for token in &tokens {
            self.by_token
                .entry(token.clone())
                .or_default()
                .insert(id.clone());
        }
        self.tokens_of.insert(id, tokens);
    }

    pub(super) fn remove(&mut self, id: &str) {
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

/// Whether `swap` can replace `target`: its pool trades both tokens of the target, and the route
/// does not already use that pool.
pub(super) fn can_replace(swap: &Swap, target: &ReplacementTarget, route_swaps: &[Swap]) -> bool {
    let tokens = &swap.protocol_component().tokens;
    tokens.contains(&target.token_in) &&
        tokens.contains(&target.token_out) &&
        !route_swaps
            .iter()
            .any(|route_swap| route_swap.component_id() == swap.component_id())
}

/// The route with `target` replaced by `swap` alone, if its net amount out is above
/// `min_net_amount_out`. The swap takes the token pair and the split of the target.
pub(super) fn replace_with_swap(
    swap: &Swap,
    target: &ReplacementTarget,
    route_swaps: &[Swap],
    evaluator: &RouteEvaluator<'_>,
    min_net_amount_out: &BigInt,
) -> Option<CandidateRoute> {
    let mut replacement = swap.clone();
    replacement.retarget(target.token_in.clone(), target.token_out.clone(), target.split);
    let (simulated, net_amount_out) = evaluator.evaluate(route_swaps, target, &replacement)?;
    (net_amount_out > *min_net_amount_out).then(|| CandidateRoute {
        target: target.clone(),
        swap: replacement,
        simulated,
        net_amount_out,
    })
}

/// Indexes the components of the base market state that pass `is_member`.
pub(super) async fn build_index(
    market: &MarketData,
    is_member: impl Fn(&ProtocolComponent) -> bool,
) -> ComponentIndex {
    let mut index = ComponentIndex::default();
    let view = market.read().await;
    let state = view.base_market_state();
    for system in state
        .component_counts_by_protocol()
        .keys()
    {
        for id in state.components_by_protocol(system) {
            if let Some(component) = state
                .get_component(id)
                .filter(|component| is_member(component))
            {
                index.insert(id.clone(), component.tokens.clone());
            }
        }
    }
    index
}

/// Removes the components `event` removes from `index`, and indexes the components it adds that
/// pass `is_member`.
pub(super) async fn update_index(
    index: &mut ComponentIndex,
    market: &MarketData,
    event: &MarketEvent,
    is_member: impl Fn(&ProtocolComponent) -> bool,
) {
    let MarketEvent::MarketUpdated { added_components, removed_components, .. } = event;
    for id in removed_components {
        index.remove(id);
    }
    if added_components.is_empty() {
        return;
    }
    let view = market.read().await;
    for (id, topology) in added_components {
        index.remove(id);
        if view
            .get_component(id)
            .is_some_and(&is_member)
        {
            index.insert(id.clone(), topology.tokens.clone());
        }
    }
}

/// The swaps the search may use as replacements for one route, and the route's tokens. Copied
/// from the market so the search holds no lock.
pub(super) struct ReplacementSnapshot {
    /// One swap with zero amounts for each component the search may use, in id order. Its token
    /// pair is a placeholder: the replacement finder sets the pair and the split before it
    /// simulates.
    swaps: Vec<Swap>,
    /// The tokens of the route.
    tokens: FxHashMap<Address, Token>,
}

impl ReplacementSnapshot {
    /// Copies the tokens of `route` and the components of `index` containing at least two of those
    /// tokens. Omits components the request excludes or the pool drops.
    pub(super) fn take(
        index: &ComponentIndex,
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
        let mut swaps = Vec::new();
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
            swaps.push(Swap::new(
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
        Self { swaps, tokens }
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

/// The part of a route that one replacement takes the place of: one swap, or a segment of swaps
/// between two tokens that every path of the route passes through.
#[derive(Clone)]
pub(super) struct ReplacementTarget {
    /// Positions of the swaps to replace, in ascending order.
    pub(super) replaced: Vec<usize>,
    pub(super) token_in: Address,
    pub(super) token_out: Address,
    /// The split of the replacement swap; `0.0` takes what remains of `token_in`.
    pub(super) split: f64,
}

/// A route with one replacement applied and simulated, which the search keeps if it beats the
/// current route.
pub(super) struct CandidateRoute {
    target: ReplacementTarget,
    swap: Swap,
    simulated: SimulatedRoute,
    net_amount_out: BigInt,
}

/// Decides which swaps may replace a target, and finds the best replacement through one swap.
/// [`ReplacementSearcher`] runs it.
pub(super) trait ReplacementFinder {
    /// Whether `swap` may replace `target` of `route_swaps`.
    fn accepts(
        &self,
        swap: &Swap,
        target: &ReplacementTarget,
        route_swaps: &[Swap],
        evaluator: &RouteEvaluator<'_>,
    ) -> bool;

    /// Returns the replacement of `target` through `swap` that has the highest net amount out, if
    /// that amount is above `min_net_amount_out`. Evaluates each replacement with `evaluator`. A
    /// rule that restricts the routes it returns, or commits them to an amount, does so here.
    fn find(
        &self,
        swap: &Swap,
        target: &ReplacementTarget,
        route_swaps: &[Swap],
        evaluator: &RouteEvaluator<'_>,
        min_net_amount_out: &BigInt,
    ) -> Option<CandidateRoute>;
}

/// Simulates replacements on the route under search: holds the original route, its gas pricing,
/// the tokens and the deadline.
pub(super) struct RouteEvaluator<'a> {
    original: &'a RouteResult,
    deadline: Instant,
    pricing: GasPricing,
    tokens: FxHashMap<Address, Token>,
    input_token: Address,
    output_token: Address,
    total_in: BigUint,
}

impl RouteEvaluator<'_> {
    pub(super) fn input_token(&self) -> &Address {
        &self.input_token
    }

    pub(super) fn deadline_passed(&self) -> bool {
        Instant::now() >= self.deadline
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

    /// Simulates the route with `replacement` in place of the target route_swaps.
    /// `None` when a swap simulation fails.
    pub(super) fn evaluate(
        &self,
        route_swaps: &[Swap],
        target: &ReplacementTarget,
        swap: &Swap,
    ) -> Option<(SimulatedRoute, BigInt)> {
        let ordered = splice(route_swaps, &target.replaced, swap);
        let simulated = self
            .simulate(&ordered)
            .inspect_err(|error| {
                trace!(
                    component = swap.component_id(),
                    %error,
                    "dropped candidate: route simulation failed"
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
}

/// Searches one route for replacements that raise the net amount out.
pub(super) struct ReplacementSearcher<'a, F> {
    evaluator: RouteEvaluator<'a>,
    swaps: Vec<Swap>,
    finder: F,
}

impl<'a, F: ReplacementFinder> ReplacementSearcher<'a, F> {
    /// Prepares a search of `original` on `snapshot`. `None` when the route cannot be searched:
    /// there are no replacement swaps, the route has no swaps, it starts and ends on one token, or
    /// it uses no gas.
    pub(super) fn new(
        snapshot: ReplacementSnapshot,
        request: &PostProcessRequest<'_>,
        original: &'a RouteResult,
        finder: F,
    ) -> Option<Self> {
        if snapshot.swaps.is_empty() {
            return None;
        }
        let route = original.route();
        let (input_token, output_token) = (route.input_token()?, route.output_token()?);
        if input_token == output_token {
            return None;
        }
        let pricing = GasPricing::of(original, &output_token)?;
        let evaluator = RouteEvaluator {
            original,
            deadline: request.deadline(),
            pricing,
            tokens: snapshot.tokens,
            input_token,
            output_token,
            total_in: request.order().amount().clone(),
        };
        Some(Self { evaluator, swaps: snapshot.swaps, finder })
    }

    /// Applies the best replacement until none raises the net amount out, the search has run one
    /// round per original swap, or the deadline passes. A candidate must beat the net amount out
    /// of the current route; the first round simulates the original route again. `None` when the
    /// route did not change, or when the final net amount out is not above the original one.
    pub(super) fn run(self) -> Option<RouteResult> {
        let original = self.evaluator.original;
        let mut swaps = original.route().swaps().to_vec();
        let rounds = swaps.len();
        let mut net_amount_out = self
            .evaluator
            .simulate_baseline(&swaps)?;
        let mut changed = false;
        for _ in 0..rounds {
            if self.evaluator.deadline_passed() {
                break;
            }
            let Some(candidate) = self.best_route(&swaps, &net_amount_out) else {
                break;
            };
            net_amount_out = candidate.net_amount_out.clone();
            swaps = self.apply(&swaps, candidate);
            changed = true;
        }
        if !changed || net_amount_out <= *original.net_amount_out() {
            return None;
        }
        let route = Route::new(swaps, self.evaluator.tokens)
            .inspect_err(|error| {
                warn!(%error, "cannot build the replacement route; keeping the original route");
            })
            .ok()?;
        Some(RouteResult::new(route, net_amount_out, original.gas_price().clone()))
    }

    /// The candidate with the highest net amount out that the finder returns, if it beats
    /// `current_net`. Stops at the deadline with the best so far.
    fn best_route(&self, route_swaps: &[Swap], current_net: &BigInt) -> Option<CandidateRoute> {
        let mut best: Option<CandidateRoute> = None;
        let mut best_net = current_net.clone();
        for target in build_targets(route_swaps) {
            for swap in &self.swaps {
                if !self
                    .finder
                    .accepts(swap, &target, route_swaps, &self.evaluator)
                {
                    continue;
                }
                if self.evaluator.deadline_passed() {
                    return best;
                }
                let Some(candidate) =
                    self.finder
                        .find(swap, &target, route_swaps, &self.evaluator, &best_net)
                else {
                    continue;
                };
                best_net = candidate.net_amount_out.clone();
                best = Some(candidate);
            }
        }
        best
    }

    /// The route_swaps after the replacement, with the amounts and gas of the simulation.
    fn apply(&self, route_swaps: &[Swap], candidate: CandidateRoute) -> Vec<Swap> {
        let CandidateRoute { target, swap, simulated, .. } = candidate;
        let ordered = splice(route_swaps, &target.replaced, &swap);
        let mut replaced = Vec::with_capacity(ordered.len());
        for (route_swap, simulated) in ordered.into_iter().zip(simulated.swaps) {
            replaced.push(
                Swap::new(
                    route_swap.component_id().to_string(),
                    route_swap.protocol().to_string(),
                    route_swap.token_in().clone(),
                    route_swap.token_out().clone(),
                    simulated.amount_in,
                    simulated.amount_out,
                    simulated.gas,
                    route_swap.protocol_component().clone(),
                    route_swap.protocol_state().clone_box(),
                )
                .with_split(*route_swap.split()),
            );
        }
        replaced
    }
}

/// Returns `swaps` with the positions in `replaced` removed and `replacement` inserted at the first
/// removed position. The result keeps a topological order: `replacement` consumes only tokens that
/// the swaps before it make.
fn splice<'a>(swaps: &'a [Swap], replaced: &[usize], replacement: &'a Swap) -> Vec<&'a Swap> {
    let mut ordered = Vec::with_capacity(swaps.len());
    for (position, route_swap) in swaps.iter().enumerate() {
        if replaced.first() == Some(&position) {
            ordered.push(replacement);
        }
        if !replaced.contains(&position) {
            ordered.push(route_swap);
        }
    }
    ordered
}

/// Returns replacement targets for individual swaps, followed by targets for segments.
fn build_targets(swaps: &[Swap]) -> Vec<ReplacementTarget> {
    let mut targets = Vec::new();
    for (position, swap) in swaps.iter().enumerate() {
        targets.push(ReplacementTarget {
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
fn build_segment_targets(swaps: &[Swap]) -> Vec<ReplacementTarget> {
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
                targets.push(ReplacementTarget {
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
    use rstest::rstest;
    use tycho_simulation::tycho_common::models::{token::Token, Address};

    use super::*;
    use crate::{algorithm::test_utils::token, post_process::test_utils::swap};

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
