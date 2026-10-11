//! Post-processor factories, registered by name.

use std::{collections::HashMap, sync::Arc};

use crate::post_process::RoutePostProcessor;

/// Builds one post-processor. Called once per worker.
type Factory = Arc<dyn Fn() -> Box<dyn RoutePostProcessor> + Send + Sync>;

/// Registering a post-processor under a name that is already taken.
#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
#[error("post-processor '{name}' is already registered")]
pub struct PostProcessorAlreadyRegistered {
    /// The name that clashes.
    pub name: String,
}

/// A pool configuration listed a post-processor that no registry holds.
#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
#[error(
    "worker pool '{pool}' lists unknown post-processor '{name}'. Registered: {}",
    if registered.is_empty() { "none".to_string() } else { registered.join(", ") }
)]
pub struct UnknownPostProcessorError {
    pool: String,
    name: String,
    registered: Vec<String>,
}

impl UnknownPostProcessorError {
    /// The name that was not found.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The worker pool that listed the name.
    pub fn pool(&self) -> &str {
        &self.pool
    }
}

/// The factories behind the post-processor names of one pool, in config order.
#[derive(Clone, Default)]
pub(crate) struct PostProcessorFactories(Vec<(String, Factory)>);

impl PostProcessorFactories {
    /// One new instance of each post-processor with its registered name, in config order.
    pub(crate) fn build(&self) -> Vec<(String, Box<dyn RoutePostProcessor>)> {
        self.0
            .iter()
            .map(|(name, factory)| (name.clone(), factory()))
            .collect()
    }
}

impl std::fmt::Debug for PostProcessorFactories {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list()
            .entries(self.0.iter().map(|(name, _)| name))
            .finish()
    }
}

/// Post-processor factories, keyed by the name a pool configuration lists. Empty by default.
#[derive(Default, Clone)]
pub struct PostProcessorRegistry {
    by_name: HashMap<String, Factory>,
}

impl PostProcessorRegistry {
    /// Creates an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers `factory` under `name`, so a pool listing that name runs the post-processor.
    ///
    /// The factory runs once per worker, on the worker thread.
    ///
    /// # Errors
    ///
    /// [`PostProcessorAlreadyRegistered`] when an earlier call took `name`.
    pub fn with_post_processor<P, F>(
        mut self,
        name: impl Into<String>,
        factory: F,
    ) -> Result<Self, PostProcessorAlreadyRegistered>
    where
        P: RoutePostProcessor + 'static,
        F: Fn() -> P + Send + Sync + 'static,
    {
        let name = name.into();
        if self.by_name.contains_key(&name) {
            return Err(PostProcessorAlreadyRegistered { name });
        }
        self.by_name
            .insert(name, Arc::new(move || Box::new(factory())));
        Ok(self)
    }

    /// The names this registry holds, in no fixed order.
    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.by_name.keys().map(String::as_str)
    }

    /// The factories behind `names`, in the same order. `pool` is the pool that listed them.
    ///
    /// # Errors
    ///
    /// [`UnknownPostProcessorError`] for the first name that is not registered. Raised when the
    /// solver is built, so a misspelled name fails at start and not at the first quote.
    pub(crate) fn resolve(
        &self,
        pool: &str,
        names: &[String],
    ) -> Result<PostProcessorFactories, UnknownPostProcessorError> {
        let mut factories = Vec::with_capacity(names.len());
        for name in names {
            let Some(factory) = self.by_name.get(name) else {
                let mut registered: Vec<String> = self
                    .names()
                    .map(str::to_string)
                    .collect();
                registered.sort();
                return Err(UnknownPostProcessorError {
                    pool: pool.to_string(),
                    name: name.clone(),
                    registered,
                });
            };
            factories.push((name.clone(), factory.clone()));
        }
        Ok(PostProcessorFactories(factories))
    }
}

impl std::fmt::Debug for PostProcessorRegistry {
    /// Names only: the factories behind them cannot be printed.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut names: Vec<&str> = self.names().collect();
        names.sort_unstable();
        f.debug_struct("PostProcessorRegistry")
            .field("names", &names)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;

    use super::*;
    use crate::{
        feed::events::{EventError, MarketEvent, MarketEventHandler},
        post_process::PostProcessRequest,
        types::RouteResult,
    };

    struct Noop;

    #[async_trait]
    impl MarketEventHandler for Noop {
        async fn handle_event(&mut self, _event: &MarketEvent) -> Result<(), EventError> {
            Ok(())
        }
    }

    #[async_trait]
    impl RoutePostProcessor for Noop {
        async fn process(
            &self,
            _request: &PostProcessRequest<'_>,
            route: RouteResult,
        ) -> RouteResult {
            route
        }
    }

    fn registry_with_post_processor(name: &str) -> PostProcessorRegistry {
        PostProcessorRegistry::new()
            .with_post_processor(name, || Noop)
            .expect("the name is free")
    }

    #[test]
    fn test_registry_duplicate_name() {
        let error = registry_with_post_processor("mine")
            .with_post_processor("mine", || Noop)
            .expect_err("the name is taken");

        assert_eq!(error, PostProcessorAlreadyRegistered { name: "mine".to_string() });
    }

    #[test]
    fn test_registry_debug() {
        let printed = format!("{:?}", registry_with_post_processor("mine"));

        assert_eq!(printed, r#"PostProcessorRegistry { names: ["mine"] }"#);
    }

    #[test]
    fn test_resolve_empty_names() {
        let factories = PostProcessorRegistry::new()
            .resolve("pool", &[])
            .expect("nothing to look up");

        assert!(factories.build().is_empty(), "expected no post-processors");
    }

    #[test]
    fn test_resolve_keeps_config_order() {
        let registry = registry_with_post_processor("a")
            .with_post_processor("b", || Noop)
            .expect("the name is free");

        let factories = registry
            .resolve("pool", &["b".to_string(), "a".to_string()])
            .expect("both names are registered");

        let names: Vec<String> = factories
            .build()
            .into_iter()
            .map(|(name, _)| name)
            .collect();
        assert_eq!(names, ["b", "a"]);
    }

    #[test]
    fn test_resolve_builds_an_instance_per_build_call() {
        let calls = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&calls);
        let registry = PostProcessorRegistry::new()
            .with_post_processor("a", move || {
                counted.fetch_add(1, Ordering::SeqCst);
                Noop
            })
            .expect("the name is free");
        let factories = registry
            .resolve("pool", &["a".to_string()])
            .expect("the name is registered");

        factories.build();
        factories.build();

        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn test_resolve_unknown_name() {
        let error = registry_with_post_processor("zeta")
            .with_post_processor("alpha", || Noop)
            .expect("the name is free")
            .resolve("main", &["zeta".to_string(), "missing".to_string()])
            .expect_err("the second name is not registered");

        assert_eq!(error.name(), "missing");
        assert_eq!(error.pool(), "main");
        assert_eq!(
            error.to_string(),
            "worker pool 'main' lists unknown post-processor 'missing'. Registered: alpha, zeta"
        );
    }

    #[test]
    fn test_resolve_unknown_name_empty_registry() {
        let error = PostProcessorRegistry::new()
            .resolve("main", &["x".to_string()])
            .expect_err("nothing is registered");

        assert!(
            error
                .to_string()
                .ends_with("Registered: none"),
            "expected the message to say nothing is registered, got: {error}"
        );
    }
}
