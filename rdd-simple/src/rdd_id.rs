use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Copy, Clone, Hash, Eq, PartialEq, PartialOrd, Ord, Debug)]
pub struct RddId(usize);

pub(crate) fn new_rdd_id() -> RddId {
    static COUNTER:AtomicUsize = AtomicUsize::new(1);
    RddId(COUNTER.fetch_add(1, Ordering::Relaxed))
}
