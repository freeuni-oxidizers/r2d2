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
    /// # Safety
    ///
    /// should only be called with numbers created from `as_usize`
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
