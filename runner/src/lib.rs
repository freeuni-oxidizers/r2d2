use std::{path::PathBuf, process::ExitStatus};
use std::io;
use std::process::Command;

#[allow(unused_variables)]
pub fn run(code_path: &PathBuf, input_path: &PathBuf, output_path: &PathBuf) -> io::Result<ExitStatus> {
    // TODO(zvikinoza): add master worker support

    let p = code_path.to_str().unwrap();
    let input_arg = input_path.to_str().unwrap();
    let output_arg = output_path.to_str().unwrap();
    Command::new("cargo").args(["run", "-p", p, "--", "-i", input_arg, "-o", output_arg]).status()
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
