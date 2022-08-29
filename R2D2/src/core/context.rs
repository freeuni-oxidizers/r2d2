use super::rdd::{Data, RddIndex};

/// All these methods use interior mutability to keep state
pub trait Context {
    // fn resolve<T: Data>(&mut self, rdd: RddIndex<T>);
    // fn collect<T: Data>(&mut self, rdd: RddIndex<T>) -> Vec<T>;
    fn map<T: Data, U: Data>(&mut self, rdd: RddIndex<T>, f: fn(&T) -> U) -> RddIndex<U>;
    fn filter<T: Data>(&mut self, rdd: RddIndex<T>, f: fn(&T) -> bool) -> RddIndex<T>;
    fn new_from_list<T: Data + Clone>(&mut self, data: Vec<Vec<T>>) -> RddIndex<T>;

    // fn store_rdd<T: RddBase + 'static>(&self, rdd: T) -> Rc<T>;
    // fn receive_serialized(&self, id: RddId, serialized_data: String);
}
