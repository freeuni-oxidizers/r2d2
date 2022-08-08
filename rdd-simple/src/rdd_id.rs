use std::sync::atomic::{AtomicUsize, Ordering};

use serde::{Serialize, Deserialize};

#[derive(Copy, Clone, Hash, Eq, PartialEq, PartialOrd, Ord, Debug, Serialize, Deserialize)]
pub struct RddId(usize);

impl RddId {
    pub fn new() -> RddId {
        static COUNTER:AtomicUsize = AtomicUsize::new(1);
        RddId(COUNTER.fetch_add(1, Ordering::Relaxed))
    }
}
