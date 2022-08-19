mod rdd_id;
use std::{any::Any, collections::HashMap, marker::PhantomData};

use rdd_id::RddId;

use serde::{de::DeserializeOwned, Deserialize, Serialize};

trait Data: Serialize + DeserializeOwned + 'static {}

impl<T> Data for T where T: Serialize + DeserializeOwned + 'static {}

#[derive(Default)]
struct ResultCache {
    data: HashMap<RddPartitionId, Box<dyn Any>>,
}

enum RddType {
    Narrow,
    Wide,
}

impl ResultCache {
    pub fn has(&self, id: RddPartitionId) -> bool {
        self.data.contains_key(&id)
    }

    pub fn put(&mut self, id: RddPartitionId, data: Box<dyn Any>) {
        self.data.insert(id, data);
    }

    pub fn get_as<T: Data>(&self, rdd: RddIndex<T>, partition_id: usize) -> Option<&[T]> {
        self.data
            .get(&RddPartitionId {
                rdd_id: rdd.id,
                partition_id: partition_id,
            })
            .map(|b| b.downcast_ref::<Vec<T>>().unwrap().as_slice())
    }
}

/// methods independent of `Item` type
trait RddBase: serde_traitobject::Serialize + serde_traitobject::Deserialize {
    /// This expects that all the deps have already been put inside the cache
    /// Ownership of results is passed back to context!
    /// For narrow transformations only
    fn work(&self, cache: &ResultCache, partition_id: usize) -> Box<dyn Any>;

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

    fn rdd_type(&self) -> RddType;

    fn partitions_num(&self) -> usize;
}

/// methods for Rdd which are dependent on `Item` type
// TODO: do we need this???
// trait Rdd: RddBase {
//     type Item: Data;
// }

#[derive(Serialize, Deserialize)]
struct DataRdd<T> {
    id: RddId,
    partitions_num: usize,
    data: Vec<T>,
}

impl<T> RddBase for DataRdd<T>
where
    T: Data + Clone,
{
    fn serialize_raw_data(&self, raw_data: &dyn Any) -> String {
        todo!()
    }

    fn deserialize_raw_data(&self, serialized_data: String) -> *const () {
        todo!()
    }

    fn id(&self) -> RddId {
        self.id
    }

    fn deps(&self) -> Vec<RddId> {
        vec![]
    }

    fn work(&self, cache: &ResultCache, partition_id: usize) -> Box<dyn Any> {
        Box::new(self.data.clone())
    }

    fn rdd_type(&self) -> RddType {
        RddType::Narrow
    }

    fn partitions_num(&self) -> usize {
        self.partitions_num
    }
}

#[derive(Serialize, Deserialize)]
struct MapRdd<T, U> {
    id: RddId,
    partitions_num: usize,
    prev: RddIndex<T>,
    #[serde(with = "serde_fp")]
    map_fn: fn(&T) -> U,
}

impl<T, U> RddBase for MapRdd<T, U>
where
    T: Data,
    U: Data,
{
    fn serialize_raw_data(&self, raw_data: &dyn Any) -> String {
        todo!()
    }

    fn deserialize_raw_data(&self, serialized_data: String) -> *const () {
        todo!()
    }

    fn id(&self) -> RddId {
        self.id
    }

    fn deps(&self) -> Vec<RddId> {
        vec![self.prev.id]
    }

    fn work(&self, cache: &ResultCache, partition_id: usize) -> Box<dyn Any> {
        let v = cache.get_as(self.prev, partition_id).unwrap();
        let g: Vec<U> = v.iter().map(self.map_fn).collect();
        Box::new(g)
    }

    fn rdd_type(&self) -> RddType {
        RddType::Narrow
    }

    fn partitions_num(&self) -> usize {
        self.partitions_num
    }
}

// TODO: maybe add uuid so that we can't pass one rdd index to another context
#[derive(Serialize, Deserialize)]
struct RddIndex<T> {
    pub id: RddId,
    #[serde(skip)]
    _data: PhantomData<T>,
}

#[derive(Copy, Clone, Hash, Eq, PartialEq, PartialOrd, Ord, Debug, Serialize, Deserialize)]
struct RddPartitionId {
    pub rdd_id: RddId,
    pub partition_id: usize,
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
trait Context: 'static {
    // fn resolve<T: Data>(&mut self, rdd: RddIndex<T>);
    fn collect<T: Data>(&mut self, rdd: RddIndex<T>) -> &[T];
    fn map<T: Data, U: Data>(
        &mut self,
        rdd: RddIndex<T>,
        f: fn(&T) -> U,
        partitions_num: usize,
    ) -> RddIndex<U>;
    fn new_from_list<T: Data + Clone>(
        &mut self,
        data: Vec<T>,
        partitions_num: usize,
    ) -> RddIndex<T>;

    // fn store_rdd<T: RddBase + 'static>(&self, rdd: T) -> Rc<T>;
    // fn receive_serialized(&self, id: RddId, serialized_data: String);
}

// TODO: can't easily have custom deserialization for hashmap value sadge :(
#[derive(Serialize, Deserialize)]
struct RddHolder(#[serde(with = "serde_traitobject")] Box<dyn RddBase>);

#[derive(Default, Serialize, Deserialize)]
struct SparkContext {
    /// All the rdd's are stored in the context in here
    rdds: HashMap<RddId, RddHolder>,
    /// this field is basically storing Vec<T>s where T can be different for each id we are not
    /// doing Vec<Any> for perf reasons. downcasting is not free
    /// This should not be serialized because all workers have this is just a cache
    #[serde(skip)]
    cache: ResultCache,
}

impl SparkContext {
    fn new() -> Self {
        Self::default()
    }

    fn resolve(&mut self, id: RddId) {
        assert!(self.rdds.contains_key(&id), "id not found in context");
        let rdd = self.rdds.get(&id).unwrap().0;
        match rdd.rdd_type() {
            RddType::Narrow => self.resolve_narrow(id),
            RddType::Wide => self.resolve_wide(id),
        }
    }

    fn resolve_narrow(&mut self, id: RddId) {}

    fn resolve_wide(&mut self, id: RddId) {}

    /// TODO: proper error handling
    /// TODO: think if we need different types of resolve for narrow and wide rdd-s
    // fn resolve(&mut self, id: RddId) {
    //     assert!(self.rdds.contains_key(&id), "id not found in context");

    //     let rdd = self.rdds.get(&id).unwrap().0;
    //     match rdd.rdd_type() {
    //         RddType::Narrow => {
    //             for partition_id in 0..rdd.partitions_num() {
    //                 let id = RddPartitionId {
    //                     rdd_id: rdd.id(),
    //                     partition_id,
    //                 };

    //                 if self.cache.has(id) {
    //                     continue;
    //                 }
    //             }
    //         }
    //         RddType::Wide => todo!(),
    //     };
    //     // First resolve all the deps
    //     for dep in self.rdds.get(&id).unwrap().0.deps() {
    //         self.resolve(dep);
    //     }
    //     let res = self.rdds.get(&id).unwrap().0.work(&self.cache);
    //     self.cache.put(id, res);
    // }

    fn store_new_rdd<R: RddBase + 'static>(&mut self, rdd: R) {
        self.rdds.insert(rdd.id(), RddHolder(Box::new(rdd)));
    }
}

impl Context for SparkContext {
    fn collect<T: Data>(&mut self, rdd: RddIndex<T>) -> &[T] {
        self.resolve(rdd.id);
        self.cache.get_as(rdd).unwrap()
    }

    fn map<T: Data, U: Data>(
        &mut self,
        rdd: RddIndex<T>,
        f: fn(&T) -> U,
        partitions_num: usize,
    ) -> RddIndex<U> {
        let id = RddId::new();
        self.store_new_rdd(MapRdd {
            id,
            partitions_num,
            prev: rdd,
            map_fn: f,
        });
        RddIndex {
            id,
            _data: PhantomData::default(),
        }
    }

    fn new_from_list<T: Data + Clone>(
        &mut self,
        data: Vec<T>,
        partitions_num: usize,
    ) -> RddIndex<T> {
        let id = RddId::new();
        self.store_new_rdd(DataRdd {
            id,
            partitions_num,
            data,
        });
        RddIndex {
            id,
            _data: PhantomData::default(),
        }
    }
}

fn main() {
    let mut sc = SparkContext::new();

    let rdd = sc.new_from_list(vec![1, 2, 3, 4], 4);
    let r2 = sc.map(rdd, |x| 2 * x, 4);
    // let d2 = sc.collect(r2);
    // println!("{:?}", d2);
    let json = serde_json::to_string_pretty(&sc).unwrap();
    println!("serialized = {}", &json);
    let mut sc: SparkContext = serde_json::from_str(&json).unwrap();
    let res = sc.collect(r2);
    println!("{:?}", res);
}
