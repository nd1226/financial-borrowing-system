# Financial Borrowing System

This repository contains the microservices for the Financial Borrowing System.

## Architecture

*   **Loan Origination Service** (Port 3000): Accepts loan applications and publishes events.
*   **Credit Decision Service** (Port 3002): Subscribes to events, fetches credit scores, and makes decisions.
*   **Partner Mock Service** (Port 3001): Simulates an external Credit Bureau API.
*   **Message Broker**: Apache Kafka + Zookeeper
*   **Observability**: Prometheus & Grafana

## Running Locally with Docker Compose

For a quick setup, you can use Docker Compose to run all services, Kafka, and Prometheus.

```bash
docker-compose up --build -d
```

### Endpoints (Local)
- Loan Service: `http://localhost:3000`
- Partner Mock: `http://localhost:3001`
- Credit Service: `http://localhost:3002`
- Grafana: `http://localhost:3003`
- Prometheus: `http://localhost:9090`

### Testing the Workflow

1.  **Submit a Loan Application:**
    ```bash
    curl -X POST http://localhost:3000/api/v1/loans \
      -H "Content-Type: application/json" \
      -d '{"amount": 5000, "term": 12, "nationalId": "123456789", "name": "John Doe"}'
    ```
    *You will receive an `applicationId`.*

2.  **Check Application Status:**
    ```bash
    curl http://localhost:3000/api/v1/loans/<applicationId>
    ```
    *If successful, the status should change from `PENDING` to `APPROVED` or `REJECTED` based on the mocked credit score.*

## Deploying to Kubernetes

The `k8s/` directory contains basic Kubernetes deployment and service manifests.

To deploy to a local cluster (like Docker Desktop, minikube, or kind):

1.  **Build the Docker images locally (if using Docker Desktop/kind):**
    ```bash
    docker build -t loan-origination-service:latest ./loan-origination-service
    docker build -t credit-decision-service:latest ./credit-decision-service
    docker build -t partner-mock-service:latest ./partner-mock-service
    ```

2.  **Apply Kafka/Zookeeper:**
    ```bash
    kubectl apply -f k8s/kafka/kafka-zookeeper.yaml
    ```

3.  **Apply Microservices:**
    ```bash
    kubectl apply -f k8s/apps/partner-mock.yaml
    kubectl apply -f k8s/apps/loan-origination.yaml
    kubectl apply -f k8s/apps/credit-decision.yaml
    ```

### Note on Advanced Infrastructure (Kong, Keycloak, EFK)
For advanced deployments like API Gateways (Kong), Identity (Keycloak), and Logging (EFK), it is highly recommended to use **Helm charts** rather than raw YAML manifests due to their complexity.
Example: `helm repo add bitnami https://charts.bitnami.com/bitnami && helm install keycloak bitnami/keycloak`

## Documentation

Detailed project documentation is available in the `docs/` folder. Start with the Project Details:

```text
docs/PROJECT_DETAIL.md
```

Or view the docs index: `docs/README.md`
