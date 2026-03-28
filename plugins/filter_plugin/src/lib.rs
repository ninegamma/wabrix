// 1. Tell the macro to read the .wit file and generate the bindings
wit_bindgen::generate!({
    path: "../../wit/inspector.wit",
    world: "inspector",
});

struct MyPlugin;

// 2. Implement the generated trait
// 2. Implement the generated trait
impl Guest for MyPlugin {
    fn inspect_batch(ports: Vec<u32>) -> Vec<u32> {
        let mut actions = Vec::with_capacity(ports.len());

        for port in ports {
            if port == 22 {
                actions.push(0); // Drop SSH
            } else {
                actions.push(1); // Keep
            }
        }

        actions
    }
}

// 3. Export the struct to the Wasm engine
export!(MyPlugin);