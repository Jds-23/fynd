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
}
