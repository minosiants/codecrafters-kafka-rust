use trybuild;

#[test]
fn ui() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/newtype/fail/*.rs");
    t.pass("tests/newtype/pass/*.rs");
}
