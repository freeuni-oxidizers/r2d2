use std::intrinsics::transmute;

use lazy_static::lazy_static;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[doc(hidden)]
#[used]
#[no_mangle]
static BINARY_BASE_VAL: &() = &();

lazy_static! {
    static ref BINARY_BASE: usize = BINARY_BASE_VAL as *const () as usize;
}

pub trait FunctionPointer: Copy + Send + Sync {
    fn as_usize(self) -> usize;
    unsafe fn from_usize(fp: usize) -> Self;
}

// Impls for function pointers
macro_rules! fnptr_impls_safety_abi {
    ($FnTy: ty, $($Arg: ident),*) => {

        impl<Ret, $($Arg),*> FunctionPointer for $FnTy {
            #[inline]
            fn as_usize(self) -> usize {
                self as usize
            }

            #[inline]
            unsafe fn from_usize(fp: usize) -> Self {
                transmute(fp)
            }
        }
    }
}

macro_rules! fnptr_impls_args {
    ($($Arg: ident),+) => {
        fnptr_impls_safety_abi! { extern "Rust" fn($($Arg),+) -> Ret, $($Arg),+ }
        fnptr_impls_safety_abi! { extern "C" fn($($Arg),+) -> Ret, $($Arg),+ }
        fnptr_impls_safety_abi! { extern "C" fn($($Arg),+ , ...) -> Ret, $($Arg),+ }
        fnptr_impls_safety_abi! { unsafe extern "Rust" fn($($Arg),+) -> Ret, $($Arg),+ }
        fnptr_impls_safety_abi! { unsafe extern "C" fn($($Arg),+) -> Ret, $($Arg),+ }
        fnptr_impls_safety_abi! { unsafe extern "C" fn($($Arg),+ , ...) -> Ret, $($Arg),+ }
    };
    () => {
        // No variadic functions with 0 parameters
        fnptr_impls_safety_abi! { extern "Rust" fn() -> Ret, }
        fnptr_impls_safety_abi! { extern "C" fn() -> Ret, }
        fnptr_impls_safety_abi! { unsafe extern "Rust" fn() -> Ret, }
        fnptr_impls_safety_abi! { unsafe extern "C" fn() -> Ret, }
    };
}

fnptr_impls_args! {}
fnptr_impls_args! { A }
fnptr_impls_args! { A, B }
fnptr_impls_args! { A, B, C }
fnptr_impls_args! { A, B, C, D }
fnptr_impls_args! { A, B, C, D, E }
fnptr_impls_args! { A, B, C, D, E, F }
fnptr_impls_args! { A, B, C, D, E, F, G }
fnptr_impls_args! { A, B, C, D, E, F, G, H }
fnptr_impls_args! { A, B, C, D, E, F, G, H, I }
fnptr_impls_args! { A, B, C, D, E, F, G, H, I, J }
fnptr_impls_args! { A, B, C, D, E, F, G, H, I, J, K }
fnptr_impls_args! { A, B, C, D, E, F, G, H, I, J, K, L }

impl<T, Ret> FunctionPointer for fn(&T) -> Ret {
    #[inline]
    fn as_usize(self) -> usize {
        self as usize
    }

    #[inline]
    unsafe fn from_usize(fp: usize) -> Self {
        transmute(fp)
    }
}

// #[derive(Serialize, Deserialize)]
// pub struct SFp<T: FunctionPointer> {
//     rel_fp: usize,
//     #[serde(skip, default)]
//     function_type: PhantomData<fn(T)>,
// }
//
// impl<T: FunctionPointer> SFp<T> {
//     pub fn new(f: T) -> Self {
//         Self {
//             rel_fp: f.as_usize().overflowing_sub(*BINARY_BASE).0,
//             function_type: PhantomData,
//         }
//     }
//
//     pub fn back(&self) -> T {
//         unsafe { FunctionPointer::from_usize(self.rel_fp.overflowing_add(*BINARY_BASE).0) }
//     }
// }

pub fn serialize<T: FunctionPointer, S>(fp: &T, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    Serialize::serialize(&fp.as_usize().overflowing_sub(*BINARY_BASE).0, s)
}

pub fn deserialize<'de, T: FunctionPointer, D>(d: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
{
    let res: usize = Deserialize::deserialize(d)?;
    unsafe {
        Ok(FunctionPointer::from_usize(
            res.overflowing_add(*BINARY_BASE).0,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // #[test]
    // fn single_arg() {
    //     let double: fn(i32) -> i32 = |x| 2 * x;
    //     let wow: SFp<fn(i32) -> i32> = SFp::new(double);
    //     let double_copy = wow.back();
    //     assert_eq!(double(100), double_copy(100));
    // }
    //
    // #[test]
    // fn two_args() {
    //     let sum_fn: fn(i32, i32) -> i32 = |x, y| x + y;
    //     let wow = SFp::new(sum_fn);
    //     let sum_fn_copy = wow.back();
    //     assert_eq!(sum_fn(50, 100), sum_fn_copy(50, 100));
    // }
    //
    // #[test]
    // fn single_arg_serde() {
    //     let double: fn(i32) -> i32 = |x| 2 * x;
    //     let wow: SFp<fn(i32) -> i32> = SFp::new(double);
    //
    //     let fp_json = serde_json::to_string_pretty(&wow).unwrap();
    //     println!("{}", fp_json);
    //
    //     let wow_serde: SFp<fn(i32) -> i32> = serde_json::from_str(&fp_json).unwrap();
    //     let double_copy = wow_serde.back();
    //
    //     assert_eq!(double(100), double_copy(100));
    // }

    #[derive(Serialize, Deserialize)]
    struct FpContainer {
        #[serde(with = "super")]
        f: fn(i32, i32) -> i32,
    }

    #[test]
    fn single_arg_serde_with_functions() {
        let sum_fp: fn(i32, i32) -> i32 = |x, y| x + y;
        let c = FpContainer { f: sum_fp };
        let c_json = serde_json::to_string_pretty(&c).unwrap();
        println!("{}", c_json);

        let c_serde: FpContainer = serde_json::from_str(&c_json).unwrap();

        assert_eq!(sum_fp(100, 200), (c_serde.f)(100, 200));
    }

    #[derive(Serialize, Deserialize)]
    struct Mapper<T, U> {
        d: T,
        #[serde(with = "super")]
        f: fn(T) -> U,
    }
    #[test]
    fn test_serialize_mapper() {
        let mapper = Mapper {
            d: ("aaaaaaaa").to_string(),
            f: |s| s.len(),
        };

        let json = serde_json::to_string_pretty(&mapper).unwrap();

        println!("Serialized mapper: {}", json);

        let mapper_serde: Mapper<String, usize> = serde_json::from_str(&json).unwrap();

        assert_eq!((mapper.f)(mapper.d), (mapper_serde.f)(mapper_serde.d));
    }
}
