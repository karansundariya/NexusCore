# NexusCore Production Deployment & Cloud Hosting Guide

This guide covers everything required to deploy, host, scale, and monitor **NexusCore** across multiple cloud environments—from a single Cloud VPS to high-availability Kubernetes clusters.

---

## 1. Hosting Architecture Overview

NexusCore consists of three tiers:
1. **Edge / Routing Tier**: Java 21 Spring Boot 3 WebFlux Gateway (Stateless, horizontally scalable).
2. **State & Event Streaming Tier**:
   - **Redis Stack**: In-memory vector similarity cache (HNSW) and distributed rate limiter.
   - **Apache Kafka (KRaft)**: Event queue for asynchronous prompt processing and telemetry.
   - **PostgreSQL 16**: Persistent audit logs and API key storage.
3. **Reasoning Worker Tier**: Python 3.12 LangGraph Agentic Worker (Horizontally scalable by Kafka partition count).

---

## 2. Option A: Single-Node Cloud VPS Deployment (Quick & Cost-Effective)

Ideal for staging, internal enterprise pilots, and cost-efficient production ($20–$40/month).

### Recommended Cloud Specs:
* **Host**: AWS EC2 (`t3.xlarge` or `c6i.xlarge`), DigitalOcean Droplet (8GB RAM, 4 vCPUs), or Hetzner (`CPX31`).
* **OS**: Ubuntu 22.04 LTS / Debian 12.

### Step-by-Step Deployment:

1. **Clone the repository on the server**:
   ```bash
   git clone <YOUR_GITHUB_REPO_URL>
   cd "Random Ideas and apps"
   ```

2. **Configure Environment Variables**:
   Create a root `.env` file:
   ```env
   # Database & Redis Credentials
   POSTGRES_USER=nexuscore
   POSTGRES_PASSWORD=your_secure_password_here
   POSTGRES_DB=nexuscore
   REDIS_HOST=localhost
   REDIS_PORT=6379

   # Kafka Config
   KAFKA_BOOTSTRAP_SERVERS=localhost:9092

   # Worker API Keys (Optional if running in mock mode)
   ANTHROPIC_API_KEY=sk-ant-...
   GOOGLE_API_KEY=AIzaSy...
   MISTRAL_API_KEY=...
   ```

3. **Start Core Infrastructure (Docker Compose)**:
   ```bash
   docker compose up -d
   ```

4. **Build & Run Services**:
   * **Java Gateway (Systemd Service or Container)**:
     ```bash
     cd gateway-service
     mvn clean package -DskipTests
     java -jar target/gateway-service-1.0.0-SNAPSHOT.jar &
     ```
   * **Python Worker (Systemd Service or Container)**:
     ```bash
     cd ../agentic-worker
     python3 -m venv venv
     source venv/bin/activate
     pip install -r requirements.txt
     python main.py &
     ```

5. **Nginx Reverse Proxy & SSL (Let's Encrypt)**:
   Install Nginx and Certbot:
   ```bash
   sudo apt update && sudo apt install -y nginx certbot python3-certbot-nginx
   ```
   Nginx configuration (`/etc/nginx/sites-available/nexuscore`):
   ```nginx
   server {
       server_name api.yourdomain.com;

       location / {
           proxy_pass http://localhost:8080;
           proxy_http_version 1.1;
           proxy_set_header Upgrade $http_upgrade;
           proxy_set_header Connection 'upgrade';
           proxy_set_header Host $host;
           proxy_cache_bypass $http_upgrade;
           proxy_read_timeout 60s;
       }
   }
   ```
   Enable SSL:
   ```bash
   sudo ln -s /etc/nginx/sites-available/nexuscore /etc/nginx/sites-enabled/
   sudo certbot --nginx -d api.yourdomain.com
   ```

---

## 3. Option B: High-Availability Cloud Deployment (AWS / GCP)

For enterprise-scale production requiring 99.99% uptime and automatic elasticity:

```
                          [AWS Route 53 / Cloudflare DNS]
                                         |
                                         v
                         [Application Load Balancer (ALB)]
                                         |
                         +---------------+---------------+
                         |                               |
                         v                               v
             [ECS Gateway Task 1]              [ECS Gateway Task 2]
             (Java 21 WebFlux)                 (Java 21 WebFlux)
                         |                               |
        +----------------+-------------------------------+----------------+
        |                                 |                               |
        v                                 v                               v
[ElastiCache / Redis Stack]      [Amazon MSK (Kafka)]           [Amazon RDS Postgres]
(VSS HNSW & Rate Limiting)       (KRaft Streaming)              (Audit Trail DB)
                                          |
                         +----------------+----------------+
                         |                                 |
                         v                                 v
             [ECS Worker Task 1]               [ECS Worker Task 2]
             (Python LangGraph)                (Python LangGraph)
```

### Managed Cloud Services:
* **Gateway & Workers**: AWS ECS Fargate or Google Cloud Run (Containerized, auto-scales on CPU/memory).
* **Semantic Vector Cache**: Redis Enterprise Cloud or AWS ElastiCache for Valkey/Redis.
* **Event Streaming**: Amazon MSK (Managed Streaming for Kafka) or Confluent Cloud.
* **Audit Database**: Amazon RDS for PostgreSQL 16 (Multi-AZ enabled).

---

## 4. Option C: Kubernetes (EKS / GKE / On-Prem)

Deploy using standard Kubernetes manifests or Helm:
* Deploy Gateway as a `Deployment` with a `HorizontalPodAutoscaler` (HPA) targeting 70% CPU utilization.
* Deploy Worker as a `Deployment` with replicas matching Kafka partition count (e.g. 3 or 6).
* Deploy Redis, Kafka, and Postgres via Strimzi Kafka Operator and CloudNativePG.

---

## 5. Automated CI/CD Pipeline (GitHub Actions)

Add `.github/workflows/ci-cd.yml` to automate builds and testing:
```yaml
name: NexusCore CI/CD

on:
  push:
    branches: [ main ]
  pull_request:
    branches: [ main ]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Set up JDK 21
        uses: actions/setup-java@v4
        with:
          java-version: '21'
          distribution: 'temurin'

      - name: Build & Test Gateway
        run: |
          cd gateway-service
          mvn test

      - name: Set up Python 3.12
        uses: actions/setup-python@v5
        with:
          python-version: '3.12'

      - name: Install Python Dependencies
        run: |
          cd agentic-worker
          pip install -r requirements.txt
```

---

## 6. Production Security Checklist

* [ ] Rotate default API keys (`nexuscore-dev-key-12345` &rarr; cryptographically generated 256-bit keys).
* [ ] Store database passwords and LLM provider tokens in AWS Secrets Manager or HashiCorp Vault.
* [ ] Enforce HTTPS / TLS 1.3 on all public inbound routes.
* [ ] Restrict Redis (6379), Kafka (9092), and Postgres (5432) to VPC private subnets only.
* [ ] Configure Prometheus + Grafana alerts on gateway error rates (`HTTP 5xx > 1%`) and Kafka consumer lag.
