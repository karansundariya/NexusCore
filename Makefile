# =============================================================================
# NexusCore — Root Makefile
# =============================================================================
# PURPOSE:
#   Provides a single, consistent CLI interface for all project lifecycle
#   operations. Using `make` rather than raw docker/maven commands means:
#   1. New developers run ONE command to start the whole stack.
#   2. CI/CD pipelines call the same targets — no configuration drift.
#   3. Complex multi-step operations are encapsulated behind a single name.
#
# USAGE:
#   make infra-up      — Start all infrastructure services (Redis, Kafka, Postgres)
#   make infra-down    — Stop and remove all containers (data volumes preserved)
#   make infra-destroy — Stop containers AND delete all data volumes (fresh start)
#   make init-db       — Initialize Redis VSS index and verify Postgres schema
#   make init-kafka    — Create required Kafka topics explicitly
#   make gateway-build — Compile and test the Java Gateway service
#   make gateway-run   — Run the Gateway service locally
#   make worker-install— Install Python Agentic Worker dependencies
#   make worker-run    — Start the Python LangGraph worker
#   make e2e-test      — Run the full end-to-end test suite
#   make logs          — Tail logs from all running containers
#   make status        — Show health status of all containers
#   make clean         — Remove build artifacts
#   make help          — Display this help message
# =============================================================================

# Use bash explicitly for shell features (arrays, pipefail, etc.)
SHELL := /bin/bash

# Project metadata
PROJECT_NAME := nexuscore
COMPOSE_FILE := docker-compose.yml

# Color codes for pretty terminal output
GREEN  := \033[0;32m
YELLOW := \033[0;33m
RED    := \033[0;31m
BLUE   := \033[0;34m
RESET  := \033[0m

# Make all targets phony (not real files) — prevents conflicts with same-named files
.PHONY: help infra-up infra-down infra-destroy init-db init-kafka \
        gateway-build gateway-run worker-install worker-run \
        e2e-test logs status clean

# Default target when `make` is run with no arguments
.DEFAULT_GOAL := help

# =============================================================================
# HELP: Print available targets with descriptions
# =============================================================================
help:
	@echo ""
	@printf "$(BLUE)╔══════════════════════════════════════════════════════════╗$(RESET)\n"
	@printf "$(BLUE)║         NexusCore — Enterprise GenAI Platform            ║$(RESET)\n"
	@printf "$(BLUE)╚══════════════════════════════════════════════════════════╝$(RESET)\n"
	@echo ""
	@printf "$(YELLOW)Infrastructure:$(RESET)\n"
	@printf "  $(GREEN)make infra-up$(RESET)        Start all services (Redis, Kafka, Postgres)\n"
	@printf "  $(GREEN)make infra-down$(RESET)      Stop all services (data preserved)\n"
	@printf "  $(GREEN)make infra-destroy$(RESET)   Stop services and DELETE all data volumes\n"
	@printf "  $(GREEN)make init-db$(RESET)         Init Redis VSS index and verify Postgres\n"
	@printf "  $(GREEN)make init-kafka$(RESET)      Create Kafka topics (ai.prompts.unprocessed, ai.telemetry.events)\n"
	@printf "  $(GREEN)make status$(RESET)          Show container health status\n"
	@printf "  $(GREEN)make logs$(RESET)            Tail logs from all containers\n"
	@echo ""
	@printf "$(YELLOW)Gateway Service (Java / Spring Boot):$(RESET)\n"
	@printf "  $(GREEN)make gateway-build$(RESET)   Build and test the Gateway service\n"
	@printf "  $(GREEN)make gateway-run$(RESET)     Run the Gateway service on port 8080\n"
	@echo ""
	@printf "$(YELLOW)Agentic Worker (Python / LangGraph):$(RESET)\n"
	@printf "  $(GREEN)make worker-install$(RESET)  Install Python dependencies\n"
	@printf "  $(GREEN)make worker-run$(RESET)      Start the LangGraph Kafka consumer worker\n"
	@echo ""
	@printf "$(YELLOW)Testing:$(RESET)\n"
	@printf "  $(GREEN)make e2e-test$(RESET)        Run the full end-to-end test suite\n"
	@printf "  $(GREEN)make clean$(RESET)           Remove all build artifacts\n"
	@echo ""

# =============================================================================
# INFRASTRUCTURE TARGETS
# =============================================================================

## infra-up: Start all Docker Compose services and wait for health checks
infra-up:
	@printf "$(BLUE)▶ Starting NexusCore infrastructure...$(RESET)\n"
	@docker compose -f $(COMPOSE_FILE) up -d --build
	@echo ""
	@printf "$(BLUE)▶ Waiting for all services to pass health checks...$(RESET)\n"
	@echo ""
	@# Wait for Redis
	@printf "  Checking Redis Stack... "
	@timeout 60 bash -c 'until docker exec nexuscore-redis redis-cli ping 2>/dev/null | grep -q PONG; do sleep 2; done'
	@printf "$(GREEN)✓ HEALTHY$(RESET)\n"
	@# Wait for Postgres
	@printf "  Checking PostgreSQL... "
	@timeout 60 bash -c 'until docker exec nexuscore-postgres pg_isready -U nexuscore -d nexuscore 2>/dev/null; do sleep 2; done' > /dev/null
	@printf "$(GREEN)✓ HEALTHY$(RESET)\n"
	@# Wait for Kafka (longer startup time)
	@printf "  Checking Kafka KRaft... "
	@timeout 120 bash -c 'until docker exec nexuscore-kafka kafka-topics.sh --bootstrap-server kafka:9092 --list 2>/dev/null; do sleep 3; done' > /dev/null
	@printf "$(GREEN)✓ HEALTHY$(RESET)\n"
	@echo ""
	@printf "$(GREEN)✓ All services are up and healthy!$(RESET)\n"
	@echo ""
	@printf "  $(YELLOW)Service Endpoints:$(RESET)\n"
	@printf "  • Redis:         redis://localhost:6379\n"
	@printf "  • RedisInsight:  http://localhost:8001\n"
	@printf "  • Kafka:         kafka://localhost:9092\n"
	@printf "  • PostgreSQL:    postgresql://nexuscore:nexuscore_secret@localhost:5432/nexuscore\n"
	@echo ""
	@printf "  $(YELLOW)Next step:$(RESET) Run $(GREEN)make init-db$(RESET) to initialize the Redis VSS index.\n"

## infra-down: Stop all services (keeps data volumes)
infra-down:
	@printf "$(YELLOW)▶ Stopping NexusCore infrastructure (data preserved)...$(RESET)\n"
	@docker compose -f $(COMPOSE_FILE) down
	@printf "$(GREEN)✓ All containers stopped.$(RESET)\n"

## infra-destroy: Stop all services AND delete all data volumes (clean slate)
infra-destroy:
	@printf "$(RED)▶ DESTROYING all NexusCore containers and data volumes...$(RESET)\n"
	@printf "$(RED)  WARNING: This deletes ALL Redis, Kafka, and Postgres data!$(RESET)\n"
	@read -p "  Type 'yes' to confirm: " confirm && [ "$$confirm" = "yes" ]
	@docker compose -f $(COMPOSE_FILE) down -v --remove-orphans
	@printf "$(GREEN)✓ All containers and volumes destroyed.$(RESET)\n"

## init-db: Initialize the Redis VSS index and verify Postgres schema
init-db:
	@printf "$(BLUE)▶ Initializing Redis Vector Similarity Search index...$(RESET)\n"
	@# Run the VSS init script inside the Redis container (avoids needing redis-cli locally)
	@docker exec nexuscore-redis bash -c "\
		redis-cli FT.CREATE idx:prompt_vss ON HASH PREFIX 1 prompt: SCHEMA \
		vector VECTOR HNSW 6 TYPE FLOAT32 DIM 1536 DISTANCE_METRIC COSINE \
		prompt_text TEXT \
		response_text TEXT \
		model_used TAG \
		created_at NUMERIC 2>&1 || echo 'Index already exists, skipping.'"
	@echo ""
	@printf "$(BLUE)▶ Verifying Redis VSS index...$(RESET)\n"
	@docker exec nexuscore-redis redis-cli FT.INFO idx:prompt_vss | head -20
	@echo ""
	@printf "$(BLUE)▶ Verifying PostgreSQL schema...$(RESET)\n"
	@docker exec nexuscore-postgres psql -U nexuscore -d nexuscore -c "\dt" 2>/dev/null
	@printf "$(GREEN)✓ Database initialization complete!$(RESET)\n"

## init-kafka: Create required Kafka topics
init-kafka:
	@printf "$(BLUE)▶ Creating Kafka topics...$(RESET)\n"
	@docker exec nexuscore-kafka /opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server localhost:9092 \
		--create --if-not-exists \
		--topic ai.prompts.unprocessed \
		--partitions 3 \
		--replication-factor 1 \
		--config retention.ms=604800000 \
		--config max.message.bytes=10485760
	@docker exec nexuscore-kafka /opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server localhost:9092 \
		--create --if-not-exists \
		--topic ai.telemetry.events \
		--partitions 3 \
		--replication-factor 1 \
		--config retention.ms=604800000
	@echo ""
	@printf "$(BLUE)▶ Listing all Kafka topics:$(RESET)\n"
	@docker exec nexuscore-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
	@printf "$(GREEN)✓ Kafka topics created!$(RESET)\n"

## status: Show health and status of all containers
status:
	@printf "$(BLUE)▶ NexusCore Container Status:$(RESET)\n"
	@docker compose -f $(COMPOSE_FILE) ps
	@echo ""
	@printf "$(BLUE)▶ Redis Memory Usage:$(RESET)\n"
	@docker exec nexuscore-redis redis-cli INFO memory | grep used_memory_human || echo "Redis not running"
	@echo ""
	@printf "$(BLUE)▶ Kafka Topic List:$(RESET)\n"
	@docker exec nexuscore-kafka kafka-topics.sh --bootstrap-server kafka:9092 --list 2>/dev/null || echo "Kafka not running"

## logs: Tail logs from all containers
logs:
	@printf "$(BLUE)▶ Tailing logs from all NexusCore services (Ctrl+C to stop)...$(RESET)\n"
	@docker compose -f $(COMPOSE_FILE) logs -f --tail=50

# =============================================================================
# GATEWAY SERVICE TARGETS (Java / Spring Boot)
# =============================================================================

## gateway-build: Build, compile, and run unit tests for the Gateway service
gateway-build:
	@printf "$(BLUE)▶ Building NexusCore Gateway Service...$(RESET)\n"
	@cd gateway-service && mvn clean test -q
	@printf "$(GREEN)✓ Gateway build and tests passed!$(RESET)\n"

## gateway-run: Run the Gateway service locally (requires infra to be running)
gateway-run:
	@printf "$(BLUE)▶ Starting NexusCore Gateway Service on port 8080...$(RESET)\n"
	@cd gateway-service && mvn spring-boot:run

# =============================================================================
# AGENTIC WORKER TARGETS (Python / LangGraph)
# =============================================================================

## worker-install: Install Python agentic worker dependencies
worker-install:
	@printf "$(BLUE)▶ Installing Python agentic worker dependencies...$(RESET)\n"
	@cd agentic-worker && pip install -r requirements.txt
	@printf "$(GREEN)✓ Python dependencies installed!$(RESET)\n"

## worker-run: Start the LangGraph Kafka consumer worker
worker-run:
	@printf "$(BLUE)▶ Starting NexusCore Agentic Worker...$(RESET)\n"
	@cd agentic-worker && python main.py

# =============================================================================
# TESTING TARGETS
# =============================================================================

## e2e-test: Run the full end-to-end test suite
e2e-test:
	@printf "$(BLUE)▶ Running NexusCore E2E Test Suite...$(RESET)\n"
	@chmod +x scripts/test_e2e.sh
	@./scripts/test_e2e.sh

## clean: Remove build artifacts
clean:
	@printf "$(YELLOW)▶ Cleaning build artifacts...$(RESET)\n"
	@cd gateway-service && mvn clean -q 2>/dev/null || true
	@find . -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true
	@find . -name "*.pyc" -delete 2>/dev/null || true
	@printf "$(GREEN)✓ Clean complete.$(RESET)\n"
