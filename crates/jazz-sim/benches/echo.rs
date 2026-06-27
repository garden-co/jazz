use jazz_sim::{PeerProfile, run_echo_deterministic, run_echo_threaded};

fn main() {
    let seed = env_u64("JAZZ_SEED", 1);
    let rounds = env_u64("JAZZ_ECHO_ROUNDS", 100);
    let one_way_ms = env_u64("JAZZ_LINK_ONE_WAY_MS", 1);
    let jitter_ms = env_u64("JAZZ_LINK_JITTER_MS", 0);
    let overhead_ms = env_u64("JAZZ_LINK_OVERHEAD_MS", 0);
    let profile_name = std::env::var("JAZZ_PROFILE").unwrap_or_else(|_| "local".to_owned());
    let profile = PeerProfile::new(profile_name, one_way_ms, jitter_ms, overhead_ms);

    run_echo_deterministic(seed, rounds, profile.clone()).emit();
    run_echo_threaded(seed, rounds, profile).emit();
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}
