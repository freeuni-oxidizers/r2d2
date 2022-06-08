use std::{
    fmt::Debug,
    fs::File,
    io::Write,
};

use anyhow::Result;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

trait Data: Serialize + DeserializeOwned + Clone + Debug + 'static {}

impl<T> Data for T where T: Serialize + DeserializeOwned + Clone + Debug + 'static {}

/// Rdd Independent of `Item` associated type
trait RddBase: serde_traitobject::Serialize + serde_traitobject::Deserialize {
    fn do_stuff(&self);
}

trait Rdd: RddBase {
    type Item;

    fn collect(&self) -> Self::Item;
}

#[derive(Serialize, Deserialize, Debug)]
struct DataRdd<T> {
    data: T,
}

impl<T> RddBase for DataRdd<T>
where
    T: Data,
{
    fn do_stuff(&self) {
        unreachable!();
    }
}

impl<T> Rdd for DataRdd<T>
where
    T: Data,
{
    type Item = T;

    fn collect(&self) -> Self::Item {
        self.data.clone()
    }
}

#[derive(Serialize, Deserialize)]
struct MapRdd<T: Data, U: Data> {
    // Does it have to be like this?
    // What limitations does it give us?
    #[serde(with = "serde_traitobject")]
    prev: Box<dyn Rdd<Item = T>>,

    #[serde(with = "serde_fp")]
    f: fn(T) -> U,
}

impl<T: Data, U: Data> RddBase for MapRdd<T, U> {
    fn do_stuff(&self) {
        println!("res: {:?}", (self.f)(self.prev.collect()));
    }
}

impl<T: Data, U: Data> Rdd for MapRdd<T, U> {
    type Item = U;

    fn collect(&self) -> Self::Item {
        (self.f)(self.prev.collect())
    }
}

/// final message being sent over network
#[derive(Serialize, Deserialize)]
struct Message {
    #[serde(with = "serde_traitobject")]
    tobj: Box<dyn RddBase>,
}

fn main() -> Result<()> {
    // example of reading back serialized linuxtrait object.
    // This gives us confidence that we work with different
    // ASLR bases.

    // {
    //     if let Ok(mut f) = File::open("./wow.json") {
    //         let mut json = String::new();
    //         f.read_to_string(&mut json)?;
    //         let m: Message = serde_json::from_str(&json)?;
    //         println!("Running from deser in file:");
    //         m.tobj.do_stuff();
    //     }
    // }

    let data_rdd = DataRdd {
        data: "AAAAAAAAAAA".to_string(),
    };
    let map_rdd = MapRdd {
        prev: Box::new(data_rdd),
        f: |s| s.len(),
    };

    let msg = Message {
        tobj: Box::new(map_rdd),
    };

    msg.tobj.do_stuff();
    let json = serde_json::to_string_pretty(&msg)?;
    println!("serialized trait object: {}", json);

    {
        let mut f = File::create("./wow.json")?;
        f.write_all(json.as_bytes())?;
    }

    let msg_deser: Message = serde_json::from_str(&json)?;
    msg_deser.tobj.do_stuff();

    Ok(())
}
