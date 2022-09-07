use std::{collections::HashMap, fmt::Debug};

use serde::{Deserialize, Serialize};

use super::rdd::{RddBase, RddId};

// TODO: can't easily have custom deserialization for hashmap value sadge :(
#[derive(Clone, Serialize, Deserialize)]
struct RddHolder(#[serde(with = "serde_traitobject")] Box<dyn RddBase>);

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct Graph {
    /// All the rdd's are stored in the context in here
    rdds: HashMap<RddId, RddHolder>,
    // #[serde(skip)]
    // cache: ResultCache,
}

impl Debug for Graph {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Graph")
            .field("n_nodes", &self.rdds.len())
            .finish()
    }
}

impl Graph {
    pub fn store_new_rdd<R: RddBase + 'static>(&mut self, rdd: R) {
        self.rdds.insert(rdd.id(), RddHolder(Box::new(rdd)));
    }

    pub fn get_rdd(&self, id: RddId) -> Option<&dyn RddBase> {
        self.rdds.get(&id).map(|x| x.0.as_ref())
    }

    pub fn contains(&self, id: RddId) -> bool {
        self.rdds.contains_key(&id)
    }
}
