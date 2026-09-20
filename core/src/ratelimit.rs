//! Join-attempt throttling.
//!
//! Someone who knows (or is guessing at) a room code should not be able to
//! hammer the host with connection attempts. Each remote identity gets a small
//! budget per window, and the whole room gets a larger one.

use std::collections::HashMap;
use std::time::{Duration, Instant};

pub struct RateLimiter {
    window: Duration,
    per_source: usize,
    global: usize,
    hits: HashMap<String, Vec<Instant>>,
    all: Vec<Instant>,
}

impl RateLimiter {
    pub fn new(per_source: usize, global: usize, window: Duration) -> Self {
        Self {
            window,
            per_source,
            global,
            hits: HashMap::new(),
            all: Vec::new(),
        }
    }

    /// Default policy: 5 attempts per source and 30 overall per minute.
    pub fn default_policy() -> Self {
        Self::new(5, 30, Duration::from_secs(60))
    }

    /// Record an attempt. `false` means "refuse this one".
    pub fn allow(&mut self, source: &str) -> bool {
        let now = Instant::now();
        let window = self.window;
        self.all.retain(|t| now.duration_since(*t) < window);
        let entry = self.hits.entry(source.to_string()).or_default();
        entry.retain(|t| now.duration_since(*t) < window);

        if entry.len() >= self.per_source || self.all.len() >= self.global {
            return false;
        }
        entry.push(now);
        self.all.push(now);
        true
    }

    /// Drop bookkeeping for sources with nothing left in the window.
    pub fn sweep(&mut self) {
        let now = Instant::now();
        let window = self.window;
        self.hits
            .retain(|_, v| v.iter().any(|t| now.duration_since(*t) < window));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn per_source_budget_is_enforced() {
        let mut rl = RateLimiter::new(2, 100, Duration::from_secs(60));
        assert!(rl.allow("a"));
        assert!(rl.allow("a"));
        assert!(!rl.allow("a"));
        assert!(rl.allow("b"));
    }

    #[test]
    fn global_budget_is_enforced() {
        let mut rl = RateLimiter::new(10, 2, Duration::from_secs(60));
        assert!(rl.allow("a"));
        assert!(rl.allow("b"));
        assert!(!rl.allow("c"));
    }
}
