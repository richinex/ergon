#[test]
fn ui() {
    let t = trybuild::TestCases::new();

    // Pass tests - valid macro usage
    t.pass("tests/ui/flow_valid.rs");
    t.pass("tests/ui/step_valid.rs");

    // Compile-fail tests - invalid macro usage
    t.compile_fail("tests/ui/flow_not_async.rs");
    t.compile_fail("tests/ui/step_not_async.rs");
}
