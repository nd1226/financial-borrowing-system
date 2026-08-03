# Financial Borrowing System

This repository contains the microservices for the Financial Borrowing System, secured with Keycloak OpenID Connect / OAuth2 Identity Provider.

## Architecture

*   **Loan Origination Service** (Port 3000): Accepts loan applications and publishes events. Protected by Keycloak JWT authentication.
*   **Credit Decision Service** (Port 3002): Subscribes to events, fetches credit scores, and makes decisions.
*   **Partner Mock Service** (Port 3001): Simulates an external Credit Bureau API.
*   **Identity & Access Management (Keycloak)** (Port 8080): OpenID Connect (OIDC) identity provider.
*   **Message Broker**: Apache Kafka + Zookeeper
*   **Observability**: Prometheus & Grafana

## Keycloak Security Configuration

Keycloak runs pre-configured with the **`financial-realm`** imported automatically from `infra/keycloak/realm-export.json`.

*   **Keycloak Admin Console**: `http://localhost:8080` (Username: `admin`, Password: `admin`)
*   **Realm**: `financial-realm`
*   **Client ID**: `loan-service`
*   **Pre-configured Test Users**:
    *   **Applicant**: `john_doe` / `password123` (Role: `loan-applicant`)
    *   **Manager**: `jane_manager` / `password123` (Role: `loan-manager`)

---

## Running Locally with Docker Compose

For a quick setup, you can use Docker Compose to run all microservices, Keycloak, Kafka, and Prometheus.

```bash
docker-compose up --build -d
```

### Endpoints (Local)
- Keycloak: `http://localhost:8080`
- Loan Service: `http://localhost:3000`
- Partner Mock: `http://localhost:3001`
- Credit Service: `http://localhost:3002`
- Grafana: `http://localhost:3003`
- Prometheus: `http://localhost:9090`

---

## Testing the Authenticated Workflow

### 1. Obtain an Access Token from Keycloak

**Using Shell Script (Linux/macOS):**
```bash
chmod +x ./scripts/get-token.sh
./scripts/get-token.sh john_doe password123
```

**Using PowerShell (Windows):**
```powershell
.\scripts\get-token.ps1 -Username john_doe -Password password123
```

**Direct `curl` command:**
```bash
curl -X POST http://localhost:8080/realms/financial-realm/protocol/openid-connect/token \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=password" \
  -d "client_id=loan-service" \
  -d "username=john_doe" \
  -d "password=password123"
```

---

### 2. Submit a Secured Loan Application

Unauthenticated requests (`POST /api/v1/loans` without token) will return `401 Unauthorized`.

Pass the obtained access token as a `Bearer` token:

```bash
curl -X POST http://localhost:3000/api/v1/loans \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer <YOUR_ACCESS_TOKEN>" \
  -d '{"amount": 5000, "term": 12, "nationalId": "123456789", "name": "John Doe"}'
```

*Response:*
```json
{
  "message": "Loan application received",
  "applicationId": "c3b9b4a1-...",
  "status": "PENDING",
  "createdBy": "john_doe"
}
```

---

### 3. Check Application Status

```bash
curl http://localhost:3000/api/v1/loans/<applicationId> \
  -H "Authorization: Bearer <YOUR_ACCESS_TOKEN>"
```

---

## Deploying to Kubernetes

The `k8s/` directory contains Kubernetes deployment and service manifests including Keycloak.

1.  **Build Docker images:**
    ```bash
    docker build -t loan-origination-service:latest ./loan-origination-service
    docker build -t credit-decision-service:latest ./credit-decision-service
    docker build -t partner-mock-service:latest ./partner-mock-service
    ```

2.  **Apply Kafka & Keycloak Infrastructure:**
    ```bash
    kubectl apply -f k8s/kafka/kafka-zookeeper.yaml
    kubectl apply -f k8s/infra/keycloak.yaml
    ```

3.  **Apply Microservices:**
    ```bash
    kubectl apply -f k8s/apps/partner-mock.yaml
    kubectl apply -f k8s/apps/loan-origination.yaml
    kubectl apply -f k8s/apps/credit-decision.yaml
    ```
