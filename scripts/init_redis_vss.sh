#!/usr/bin/env bash
# =============================================================================
# init_redis_vss.sh — Initialize Redis Stack Vector Similarity Search Index
# =============================================================================
# PURPOSE:
#   Creates the HNSW (Hierarchical Navigable Small World) vector index that
#   powers NexusCore's semantic cache. This script is idempotent: running it
#   multiple times will not fail — it detects if the index already exists.
#
# HOW HNSW WORKS:
#   HNSW is a graph-based Approximate Nearest Neighbor (ANN) algorithm.
#   It builds a multi-layer proximity graph over all stored vectors. At query
#   time it traverses the graph layers top-down to find the k-nearest neighbors
#   in O(log n) time, achieving >99% recall with <1ms overhead vs brute force.
#
# WHY 1536 DIMENSIONS?
#   OpenAI's text-embedding-ada-002 and text-embedding-3-small models output
#   1536-dimensional float vectors. This is the industry standard for semantic
#   embeddings used in RAG and caching pipelines.
#
# WHY COSINE DISTANCE?
#   Cosine similarity measures the angle between two vectors, ignoring magnitude.
#   This is ideal for text embeddings because two semantically equivalent
#   sentences may have different word counts (different magnitudes) but should
#   still be detected as similar (small angle between their vectors).
#
# USAGE:
#   ./scripts/init_redis_vss.sh
#   Or called from: make init-db
# =============================================================================

set -euo pipefail  # Exit on error, unset variables are errors, pipe failures propagate

# ---------------------------------------------------------------------------
# Configuration — override with environment variables if needed
# ---------------------------------------------------------------------------
REDIS_HOST="${REDIS_HOST:-localhost}"
REDIS_PORT="${REDIS_PORT:-6379}"
INDEX_NAME="idx:prompt_vss"
KEY_PREFIX="prompt:"

echo "============================================================"
echo " NexusCore — Redis VSS Index Initialization"
echo " Target: redis://${REDIS_HOST}:${REDIS_PORT}"
echo "============================================================"

# ---------------------------------------------------------------------------
# Step 1: Wait for Redis to be ready (important in CI/CD pipelines)
# ---------------------------------------------------------------------------
echo "[1/3] Waiting for Redis to be ready..."
MAX_WAIT=60
WAITED=0
until redis-cli -h "${REDIS_HOST}" -p "${REDIS_PORT}" PING 2>/dev/null | grep -q PONG; do
  if [ "${WAITED}" -ge "${MAX_WAIT}" ]; then
    echo "ERROR: Redis did not become ready within ${MAX_WAIT} seconds. Exiting."
    exit 1
  fi
  echo "  ... Redis not ready yet, waiting 2s (${WAITED}s elapsed)"
  sleep 2
  WAITED=$((WAITED + 2))
done
echo "  ✓ Redis is ready."

# ---------------------------------------------------------------------------
# Step 2: Check if index already exists (idempotency guard)
# ---------------------------------------------------------------------------
echo "[2/3] Checking if index '${INDEX_NAME}' already exists..."
# FT.INFO returns index details or an error if the index doesn't exist.
# We redirect stderr to /dev/null and capture the exit code.
if redis-cli -h "${REDIS_HOST}" -p "${REDIS_PORT}" FT.INFO "${INDEX_NAME}" > /dev/null 2>&1; then
  echo "  ✓ Index '${INDEX_NAME}' already exists. Skipping creation."
  echo ""
  echo "[3/3] Verifying existing index..."
else
  echo "  Index not found. Creating VSS index now..."
  echo ""

  # ---------------------------------------------------------------------------
  # FT.CREATE — The Core Index Definition
  # ---------------------------------------------------------------------------
  # FT.CREATE idx:prompt_vss
  #   Creates a new full-text + vector search index named 'idx:prompt_vss'.
  #
  # ON HASH
  #   Index Redis HASH data structures (as opposed to JSON documents).
  #   Each cached prompt is stored as a Redis HASH at key `prompt:<uuid>`.
  #
  # PREFIX 1 prompt:
  #   Only index keys that start with "prompt:". The '1' means one prefix.
  #   This is a performance guard: Redis won't scan every key in the database.
  #
  # SCHEMA fields:
  #
  #   vector VECTOR HNSW 6 TYPE FLOAT32 DIM 1536 DISTANCE_METRIC COSINE
  #     - VECTOR: this field holds a vector embedding
  #     - HNSW: use Hierarchical Navigable Small World graph index
  #     - 6: number of HNSW construction parameters that follow
  #     - TYPE FLOAT32: vectors are arrays of 32-bit floats (4 bytes each)
  #                     Total bytes per vector: 1536 × 4 = 6144 bytes
  #     - DIM 1536: exactly 1536 dimensions per vector (OpenAI ada-002 output)
  #     - DISTANCE_METRIC COSINE: measure angular distance between vectors
  #
  #   prompt_text TEXT
  #     - Full-text searchable field (tokenized, stopwords removed)
  #     - Stored for display and debugging purposes
  #
  #   response_text TEXT
  #     - The cached LLM response. Retrieved on cache hit and returned to client.
  #
  #   model_used TAG
  #     - TAG field: exact-match filtering (e.g., @model_used:{gpt-4o})
  #     - Used to filter cache hits by the model that generated the response
  #
  #   created_at NUMERIC
  #     - Unix timestamp of cache entry creation
  #     - Enables TTL-aware queries: filter out stale entries
  #     - Supports SORTBY for chronological ordering
  # ---------------------------------------------------------------------------
  redis-cli -h "${REDIS_HOST}" -p "${REDIS_PORT}" \
    FT.CREATE "${INDEX_NAME}" \
    ON HASH \
    PREFIX 1 "${KEY_PREFIX}" \
    SCHEMA \
      vector VECTOR HNSW 6 TYPE FLOAT32 DIM 1536 DISTANCE_METRIC COSINE \
      prompt_text TEXT \
      response_text TEXT \
      model_used TAG \
      created_at NUMERIC

  echo "  ✓ Index '${INDEX_NAME}' created successfully."
  echo ""
fi

# ---------------------------------------------------------------------------
# Step 3: Verify the index is healthy by running FT.INFO
# ---------------------------------------------------------------------------
echo "[3/3] Index information:"
echo "------------------------------------------------------------"
redis-cli -h "${REDIS_HOST}" -p "${REDIS_PORT}" FT.INFO "${INDEX_NAME}"
echo "------------------------------------------------------------"

echo ""
echo "============================================================"
echo " ✓ Redis VSS initialization complete!"
echo " Index '${INDEX_NAME}' is ready for semantic cache queries."
echo "============================================================"
