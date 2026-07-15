# Testing the Configuration System

## Running Unit Tests

Run all configuration tests:

```bash
cargo test --lib config::tests
```

Run a specific test:

```bash
cargo test --lib config::tests::test_valid_simple_config
```

## Test Coverage

The test suite includes:

### Validation Tests
1. **test_valid_simple_config** - Valid minimal configuration
2. **test_duplicate_umem_id** - Duplicate UMEM ID detection
3. **test_invalid_frame_size_not_power_of_two** - Frame size validation
4. **test_nonexistent_interface_reference** - Interface reference validation
5. **test_nonexistent_umem_reference** - UMEM reference validation
6. **test_queue_id_exceeds_interface_queues** - Queue range validation
7. **test_core_affinity_length_mismatch** - Core affinity length validation
8. **test_invalid_rxq_size_not_power_of_two** - Queue size validation
9. **test_head_room_exceeds_frame_size** - Head room validation
10. **test_mixed_umem_config** - Mixed UMEM architecture validation
11. **test_empty_queues_list** - Empty queues detection

### Example File Tests
12. **test_example_simple_toml** - Validates examples/simple.toml
13. **test_example_mixed_umem_toml** - Validates examples/mixed_umem.toml
14. **test_example_shared_umem_toml** - Validates examples/shared_umem.toml
15. **test_example_multi_interface_toml** - Validates examples/multi_interface.toml

## Manual Testing

Test with an example configuration:

```bash
# Build the project
cargo build --bin wabrix


```

## Expected Test Output

All tests should pass:

```
test result: ok. 15 passed; 0 failed; 0 ignored; 0 measured
```

## Adding New Tests

To add a new validation test, add to `src/config/mod.rs`:

```rust
#[test]
fn test_your_validation() {
    let toml_str = r#"
        [wabrix]
        // ... your test config
    "#;
    
    let config: XBricksConfig = toml::from_str(toml_str).unwrap();
    let result = config.validate();
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("expected error"));
}
```
