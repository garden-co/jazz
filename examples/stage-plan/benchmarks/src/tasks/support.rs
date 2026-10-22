pub fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

pub fn csv_usizes(name: &str, default: &str) -> Vec<usize> {
    std::env::var(name)
        .unwrap_or_else(|_| default.to_owned())
        .split(',')
        .map(|value| {
            value
                .trim()
                .parse::<usize>()
                .unwrap_or_else(|_| panic!("invalid {name} value: {value}"))
        })
        .collect()
}
