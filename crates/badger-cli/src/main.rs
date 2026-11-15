//! Command-line tools for badger databases (`info`, `backup`, `restore`, ...).
//!
//! The commands arrive in Phase 8 of `PLAN.md`.

fn main() {
    println!(
        "badger-cli {} (Go Badger {} format): no commands yet",
        env!("CARGO_PKG_VERSION"),
        badger::GO_BADGER_VERSION
    );
}
