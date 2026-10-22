* test more multi-worker and multi-queue examples
* implement flow direction
* implement the bypass logic: e.g
```toml
  [[pipelines]]
  name = "multi_bridge"
  workers = [0, 1]
  lifecycle = "Running"
  mode = "Bypass"

  [[pipelines.nodes]]
  id = "rx_eth0"
  type = "BatchRxNode"
  routes = ["deep_inspection_engine"]
  bypass_routes = ["tx_eth1"]
  queues = [0, 1]
  [pipelines.nodes.args]
  interface = "eth0"

  [[pipelines.nodes]]
  id = "rx_eth1"
  type = "BatchRxNode"
  routes = ["deep_inspection_engine"]
  bypass_routes = ["tx_eth0"]
  queues = [0, 1]
  [pipelines.nodes.args]
  interface = "eth1"

  [[pipelines.nodes]]
  id = "tx_eth0"
  type = "BatchTxNode"
  queues = [0, 1]
  [pipelines.nodes.args]
  interface = "eth0"

  [[pipelines.nodes]]
  id = "tx_eth1"
  type = "BatchTxNode"
  queues = [0, 1]
  [pipelines.nodes.args]
  interface = "eth1"
```