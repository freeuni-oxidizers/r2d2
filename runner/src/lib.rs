use std::path::PathBuf;

/// 
/// personal notes: logging shoudl be done in a separate module, 
/// master & worker should be able to share the same logging module??
/// 
#[allow(unused_variables)]
pub fn run(code_path: PathBuf, input_path: PathBuf, output_path: PathBuf) {
    // 1. run master
    // 2. run worker
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
