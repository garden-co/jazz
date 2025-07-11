
#[cxx::bridge]
mod ffi {
    extern "Rust" {
        fn rust_no_args_return_string() -> String;
        fn rust_args_return_string(arg1: String) -> String;
        fn rust_no_args_return_ab() -> Vec<u8>;
        fn rust_args_return_ab(arg1: Vec<u8>) -> Vec<u8>;
    }
}

fn rust_no_args_return_string() -> String {
    "Hello from Rust!".to_string()
}

fn rust_args_return_string(arg1: String) -> String {
    format!("Hello, {}!", arg1)
}

fn rust_no_args_return_ab() -> Vec<u8> {
    vec![10, 20, 30, 40, 50]
}

fn rust_args_return_ab(arg1: Vec<u8>) -> Vec<u8> {
    arg1.into_iter().map(|x| x.wrapping_add(10)).collect()
}
