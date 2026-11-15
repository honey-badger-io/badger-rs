//! The fixtures must come from the Go Badger release this crate targets.

#[test]
fn fixtures_match_targeted_go_badger_version() {
    let fx = badger_fixtures::Fixtures::load().expect("fixtures missing: run `make fixtures`");
    assert_eq!(fx.index.badger_version, badger::GO_BADGER_VERSION);
}
