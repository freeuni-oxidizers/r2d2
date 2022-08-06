mod rdd_id;
use std::{
    any::Any,
    borrow::{Borrow, BorrowMut},
    cell::RefCell,
    collections::HashMap,
    rc::Rc,
};

use rdd_id::{new_rdd_id, RddId};

use serde::{de::DeserializeOwned, Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
struct Point {
    x: i32,
    y: i32,
}

// TODO: do we need Clone here
trait Data: Serialize + DeserializeOwned + Clone + 'static {}

impl<T> Data for T where T: Serialize + DeserializeOwned + Clone + 'static {}

/// methods independent of `Item` type
trait RddBase {
    /// this function is intended to be called from context
    /// it will ask context for input data and call context
    /// to put results back
    fn work(&self, data_deps: Vec<&dyn Any>) -> Box<dyn Any>;

    /// serialize data returned by this rdd in a form which can be sent over the network
    fn serialize_raw_data(&self, raw_data: *const ()) -> String;
    /// deserialize data in a way which can be ingested by work
    fn deserialize_raw_data(&self, serialized_data: String) -> *const ();

    /// Fetch unique id for this id
    fn id(&self) -> RddId;

    fn deps(&self) -> Vec<RddId>;
}

/// methods for Rdd which are dependent on `Item` type
trait Rdd: RddBase {
    type Item: Data;
    type Ctx: Context;

    fn map<U: Data>(
        &self,
        f: fn(&Self::Item) -> U,
    ) -> Rc<MapRdd<fn(&Self::Item) -> U, Rc<Self::Ctx>>> {
        self.ctx().store_rdd(MapRdd {
            id: new_rdd_id(),
            prev: self.id(),
            map_fn: f,
            ctx: self.ctx(),
        })
    }

    fn ctx(&self) -> Rc<Self::Ctx>;
}

struct DataRdd<T, C> {
    id: RddId,
    data: Vec<T>,
    ctx: C,
}

impl<T, C> RddBase for DataRdd<T, Rc<C>>
where
    T: Data,
    C: Context,
{
    fn work(&self, data_deps: Vec<&dyn Any>) -> Box<dyn Any> {
        Box::new(self.data.clone())
    }

    fn serialize_raw_data(&self, raw_data: *const ()) -> String {
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
}

impl<T, C> Rdd for DataRdd<T, Rc<C>>
where
    T: Data,
    C: Context,
{
    type Item = T;

    type Ctx = C;

    fn ctx(&self) -> Rc<Self::Ctx> {
        self.ctx.clone()
    }
}

struct MapRdd<F, C> {
    id: RddId,
    prev: RddId,
    map_fn: F,
    ctx: C,
}

impl<T, U, C> RddBase for MapRdd<fn(&T) -> U, Rc<C>>
where
    T: Data,
    U: Data,
    C: Context,
{
    fn work(&self, data_deps: Vec<&dyn Any>) -> Box<dyn Any> {
        let v = data_deps[0].downcast_ref::<Vec<T>>().unwrap();
        let g: Vec<U> = v.iter().map(self.map_fn).collect();
        Box::new(g)
    }

    fn serialize_raw_data(&self, raw_data: *const ()) -> String {
        todo!()
    }

    fn deserialize_raw_data(&self, serialized_data: String) -> *const () {
        todo!()
    }

    fn id(&self) -> RddId {
        self.id
    }

    fn deps(&self) -> Vec<RddId> {
        vec![self.prev]
    }
}

impl<T, U, C> Rdd for MapRdd<fn(&T) -> U, Rc<C>>
where
    T: Data,
    U: Data,
    C: Context,
{
    type Item = T;

    type Ctx = C;

    fn ctx(&self) -> Rc<Self::Ctx> {
        self.ctx.clone()
    }
}

/// All these methods use interior mutability to keep state
trait Context: 'static {
    fn resolve(&self, id: RddId);
    fn get_data_of_id(&mut self, id: RddId) -> &dyn Any;
    fn store_rdd<T: RddBase + 'static>(&self, rdd: T) -> Rc<T>;
    fn receive_serialized(&self, id: RddId, serialized_data: String);
    // fn new_from_list<T: Data>(self, data: Vec<T>) -> Rc<DataRdd<T, Rc<Self>>>
    // where
    //     Self: Sized;
}

#[derive(Default)]
struct SparkContext {
    rdds: RefCell<HashMap<RddId, Rc<dyn RddBase>>>,
    /// this field is basically storing Vec<T>s where T can be different for each id we are not
    /// doing Vec<Any> for perf reasons. downcasting is not free
    data: RefCell<HashMap<RddId, Box<dyn Any>>>,
}

impl SparkContext {
    fn new() -> Self {
        Self::default()
    }
}

impl Context for SparkContext {
    fn resolve(&self, id: RddId) {
        {
            for dep in self.rdds.borrow().get(&id).unwrap().deps() {
                self.resolve(dep);
            }
        }
        let mut data = self.data.borrow_mut();
        let mut data_deps = Vec::new();
        {
            for dep in self.rdds.borrow().get(&id).unwrap().deps() {
                data_deps.push(data.get(&dep).unwrap().as_ref());
            }
        }
        let res = self.rdds.borrow().get(&id).unwrap().work(data_deps);
        data.insert(id, res);
    }

    fn get_data_of_id(&mut self, id: RddId) -> &dyn Any {
        self.resolve(id);
        self.data.get_mut().get(&id).unwrap()
    }

    /// since we want SparkContext to be the owner of our rdd's we must pass them as 'static and
    /// move them inside the hashmap
    fn store_rdd<T: RddBase + 'static>(&self, rdd: T) -> Rc<T> {
        // TODO: figure out a way to store rdd as Box<dyn RddBase> but return value as &T
        let mut inner = self.rdds.borrow_mut();
        let k = Rc::new(rdd);
        inner.insert(k.id(), k.clone());
        k
    }

    fn receive_serialized(&self, id: RddId, serialized_data: String) {
        unimplemented!();
    }

    // fn new_from_list<T: Data>(self, data: Vec<T>) -> Rc<DataRdd<T, Rc<Self>>>
    // where
    //     Self: Sized,
    // {
    //     DataRdd { id: new_rdd_id(), data: data, ctx: Rc::new(self) }
    // }


}

fn main() {
    let sc = Rc::new(SparkContext::new());

    let rdd = sc.store_rdd(DataRdd { id: new_rdd_id(), data: vec![1, 2, 3, 4], ctx: sc.clone() });
    // let rdd = sc.new_from_list(vec![1, 2, 3, 4]);
    let rdd = rdd.map(|x|2*x);
    rdd.ctx().resolve(rdd.id);
    {
        let data = sc.data.borrow_mut();
        let res = data.get(&rdd.id).unwrap().downcast_ref::<Vec<i32>>().unwrap();
        println!("{:?}", res);
    }
}
