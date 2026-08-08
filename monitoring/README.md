# Prometheus and Grafana Setup Guide

This guide describes how to start and configure Prometheus and Grafana for monitoring the Wabrix engine.

## Port Numbers

* **Wabrix Engine Metrics:** `9090` (Local engine metrics endpoint)
* **Prometheus UI:** `9091` (Exposed externally to avoid port conflicts with the engine)
* **Grafana UI:** `3000` (Exposed externally for dashboard visualization)

---

## Getting Started

* **Prerequisites:** Ensure Docker and Docker Compose are installed on your system.
* **Step 1: Configuration Verification**
  Verify that the `docker-compose.yml` and `prometheus.yml` files are located in your working directory and correctly reference port `9090` for the Wabrix engine target.
* **Step 2: Start the Monitoring Stack**
  Run the following command from the directory containing the `docker-compose.yml` file to start both containers in the background:
```bash
docker compose up -d

```


* **Step 3: Access the UIs**
* **Prometheus:** Open your browser and navigate to `http://localhost:9091` to verify metric scraping.
* **Grafana:** Open your browser and navigate to `http://localhost:3000`. Log in using the default credentials (`admin` / `admin`).


* **Step 4: Stop the Services**
  To stop and remove the monitoring containers, execute:
```bash
docker compose down

```