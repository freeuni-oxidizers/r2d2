use std::sync::atomic::{AtomicUsize, Ordering};

use crate::Config;
use crate::{master, worker};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
#[derive(Copy, Clone, Hash, Eq, PartialEq, PartialOrd, Ord, Debug, Serialize, Deserialize)]
pub struct RddId(usize);
use crate::r2d2::{Task, TaskAction};
use tokio::sync::broadcast;

impl RddId {
    pub fn new() -> RddId {
        static COUNTER: AtomicUsize = AtomicUsize::new(1);
        RddId(COUNTER.fetch_add(1, Ordering::Relaxed))
    }
}

use std::{any::Any, collections::HashMap, marker::PhantomData};

pub trait Data: Serialize + DeserializeOwned + 'static {}

impl<T> Data for T where T: Serialize + DeserializeOwned + 'static {}

#[derive(Default)]
pub struct ResultCache {
    data: HashMap<RddId, Box<dyn Any>>,
}

impl ResultCache {
    pub fn has(&self, id: RddId) -> bool {
        self.data.contains_key(&id)
    }

    pub fn put(&mut self, id: RddId, data: Box<dyn Any>) {
        self.data.insert(id, data);
    }

    pub fn get_as<T: Data>(&self, rdd: RddIndex<T>) -> Option<&[T]> {
        self.data
            .get(&rdd.id)
            .map(|b| b.downcast_ref::<Vec<T>>().unwrap().as_slice())
    }
}

/// methods independent of `Item` type
pub trait RddBase: serde_traitobject::Serialize + serde_traitobject::Deserialize {
    /// This expects that all the deps have already been put inside the cache
    /// Ownership of results is passed back to context!
    fn work(&self, cache: &ResultCache) -> Box<dyn Any>;

    // TODO: maybe api like this possible
    // fn work2(&self, ctx: &SparkContext) -> Box<dyn Any>;

    /// serialize data returned by this rdd in a form which can be sent over the network
    fn serialize_raw_data(&self, raw_data: &dyn Any) -> String;
    /// deserialize data in a way which can be ingested into following rdds
    fn deserialize_raw_data(&self, serialized_data: String) -> *const ();

    /// Fetch unique id for this rdd
    fn id(&self) -> RddId;

    /// rdd dependencies
    fn deps(&self) -> Vec<RddId>;
}

/// methods for Rdd which are dependent on `Item` type
// TODO: do we need this???
// trait Rdd: RddBase {
//     type Item: Data;
// }

#[derive(Serialize, Deserialize)]
pub struct DataRdd<T> {
    id: RddId,
    data: Vec<T>,
}

impl<T> RddBase for DataRdd<T>
where
    T: Data + Clone,
{
    fn serialize_raw_data(&self, _raw_data: &dyn Any) -> String {
        todo!()
    }

    fn deserialize_raw_data(&self, _serialized_data: String) -> *const () {
        todo!()
    }

    fn id(&self) -> RddId {
        self.id
    }

    fn deps(&self) -> Vec<RddId> {
        vec![]
    }

    fn work(&self, _cache: &ResultCache) -> Box<dyn Any> {
        Box::new(self.data.clone())
    }
}

#[derive(Serialize, Deserialize)]
pub struct MapRdd<T, U> {
    id: RddId,
    prev: RddIndex<T>,
    #[serde(with = "serde_fp")]
    map_fn: fn(&T) -> U,
}

impl<T, U> RddBase for MapRdd<T, U>
where
    T: Data,
    U: Data,
{
    fn serialize_raw_data(&self, _raw_data: &dyn Any) -> String {
        todo!()
    }

    fn deserialize_raw_data(&self, _serialized_data: String) -> *const () {
        todo!()
    }

    fn id(&self) -> RddId {
        self.id
    }

    fn deps(&self) -> Vec<RddId> {
        vec![self.prev.id]
    }

    fn work(&self, cache: &ResultCache) -> Box<dyn Any> {
        let v = cache.get_as(self.prev).unwrap();
        let g: Vec<U> = v.iter().map(self.map_fn).collect();
        Box::new(g)
    }
}

// TODO: maybe add uuid so that we can't pass one rdd index to another context
#[derive(Serialize, Deserialize)]
pub struct RddIndex<T> {
    pub id: RddId,
    #[serde(skip)]
    _data: PhantomData<T>,
}

impl<T> Clone for RddIndex<T> {
    fn clone(&self) -> RddIndex<T> {
        RddIndex {
            id: self.id,
            _data: PhantomData::default(),
        }
    }
}

impl<T> Copy for RddIndex<T> {}

/// All these methods use interior mutability to keep state
pub trait Context: 'static {
    // fn resolve<T: Data>(&mut self, rdd: RddIndex<T>);
    fn collect<T: Data>(&mut self, rdd: RddIndex<T>) -> &[T];
    fn map<T: Data, U: Data>(&mut self, rdd: RddIndex<T>, f: fn(&T) -> U) -> RddIndex<U>;
    fn new_from_list<T: Data + Clone>(&mut self, data: Vec<T>) -> RddIndex<T>;

    // fn store_rdd<T: RddBase + 'static>(&self, rdd: T) -> Rc<T>;
    // fn receive_serialized(&self, id: RddId, serialized_data: String);
}

// TODO: can't easily have custom deserialization for hashmap value sadge :(
#[derive(Serialize, Deserialize)]
pub struct RddHolder(#[serde(with = "serde_traitobject")] Box<dyn RddBase>);

#[derive(Default, Serialize, Deserialize)]
pub struct SparkContext {
    /// All the rdd's are stored in the context in here
    rdds: HashMap<RddId, RddHolder>,
    /// this field is basically storing Vec<T>s where T can be different for each id we are not
    /// doing Vec<Any> for perf reasons. downcasting is not free
    /// This should not be serialized because all workers have this is just a cache
    #[serde(skip)]
    cache: ResultCache,
}

// TODO(zvikinoza): extract appropriate fn s to Spark
impl SparkContext {
    pub fn new() -> Self {
        Self::default()
    }

    /// TODO: proper error handling
    fn resolve(&mut self, id: RddId) {
        assert!(self.rdds.contains_key(&id), "id not found in context");
        if self.cache.has(id) {
            return;
        }
        // First resolve all the deps
        for dep in self.rdds.get(&id).unwrap().0.deps() {
            self.resolve(dep);
        }
        let res = self.rdds.get(&id).unwrap().0.work(&self.cache);
        self.cache.put(id, res);
    }

    fn store_new_rdd<R: RddBase + 'static>(&mut self, rdd: R) {
        self.rdds.insert(rdd.id(), RddHolder(Box::new(rdd)));
    }
}

impl Context for SparkContext {
    fn collect<T: Data>(&mut self, rdd: RddIndex<T>) -> &[T] {
        self.resolve(rdd.id);
        self.cache.get_as(rdd).unwrap()
    }

    fn map<T: Data, U: Data>(&mut self, rdd: RddIndex<T>, f: fn(&T) -> U) -> RddIndex<U> {
        let id = RddId::new();
        self.store_new_rdd(MapRdd {
            id,
            prev: rdd,
            map_fn: f,
        });
        RddIndex {
            id,
            _data: PhantomData::default(),
        }
    }

    fn new_from_list<T: Data + Clone>(&mut self, data: Vec<T>) -> RddIndex<T> {
        let id = RddId::new();
        self.store_new_rdd(DataRdd { id, data });
        RddIndex {
            id,
            _data: PhantomData::default(),
        }
    }
}

// TODO(zvikinoza): extract this to sep file
// and use SparkContext as lib from rdd-simple
pub struct Spark {
    sc: SparkContext,
    tx: broadcast::Sender<Task>,
    wait_handle: Option<tokio::task::JoinHandle<()>>,
}

impl Spark {
    pub async fn new(config: Config) -> Self {
        let (tx, _) = broadcast::channel(16);
        let mut sc = Self {
            sc: SparkContext::new(),
            tx,
            wait_handle: None,
        };
        if config.master {
            sc.wait_handle = Some(master::start(config.n_workers, &sc.tx).await);
        } else {
            // better if we tokio::block of await? (to make Self::new sync)
            worker::start(config.id).await; // this fn call never returns
        }
        sc
    }

    // TODO(zvikinoza): port this to fn drop
    // problem is Drop can't be async and await must be async
    pub async fn termiante(&mut self) {
        match self.wait_handle {
            Some(ref mut wh) => wh.await.unwrap(),
            None => panic!("worker shouldn't be here"),
        };
        println!("\n\nmaster shutting down\n\n");
    }
}

impl Context for Spark {
    fn collect<T: Data>(&mut self, rdd: RddIndex<T>) -> &[T] {
        let sc = serde_json::to_string_pretty(&self.sc).unwrap();
        let sr = serde_json::to_string_pretty(&rdd).unwrap();
        let _res = self.tx.send(Task {
            action: TaskAction::Work as i32,
            context: sc,
            rdd: sr,
        });
        self.sc.collect(rdd)
    }

    fn map<T: Data, U: Data>(&mut self, rdd: RddIndex<T>, f: fn(&T) -> U) -> RddIndex<U> {
        self.sc.map(rdd, f)
    }

    fn new_from_list<T: Data + Clone>(&mut self, data: Vec<T>) -> RddIndex<T> {
        self.sc.new_from_list(data)
    }
}
