"""
=============================================================================
cache_writer.py — Redis Semantic Cache Populator
=============================================================================
WHAT THIS MODULE DOES:
  After the LangGraph worker generates a verified response, this module:
  1. Generates a 1536-dim vector embedding of the prompt text
  2. Stores the prompt+response as a Redis HASH under key "prompt:<uuid>"
  3. Sets a 24-hour TTL so the entry expires automatically

WHY CACHE AFTER GENERATION (not before)?
  We only cache VERIFIED responses (those that passed the Guardrail Node).
  Caching before validation risks populating the cache with hallucinated or
  policy-violating responses that get served to future users.

VECTOR EMBEDDING STRATEGY:
  In mock mode: uses a deterministic hash-based pseudo-embedding (same as Java).
  In production: calls OpenAI text-embedding-3-small via the openai SDK.

  WHY MUST THE EMBEDDING MATCH EXACTLY?
    The Redis VSS index was created with DIM 1536 and DISTANCE_METRIC COSINE.
    The embedding stored in the cache and the embedding used for future lookups
    MUST come from the same model. If you use ada-002 for storage but
    text-embedding-3-small for queries, cosine distances will be meaningless.

REDIS STORAGE FORMAT:
  Key: "prompt:<uuid>"   (indexed by the FT.CREATE PREFIX 1 prompt: config)
  Fields (HASH):
    - vector: raw bytes (FLOAT32 × 1536 = 6144 bytes) — indexed by HNSW
    - prompt_text: original prompt string — stored for debugging/audit
    - response_text: the LLM response — returned on cache hit
    - model_used: which model generated this — for filtering by model
    - created_at: Unix epoch timestamp — for TTL-aware queries
  TTL: 86400 seconds (24 hours)
=============================================================================
"""

import hashlib
import logging
import struct
import time
import uuid
from typing import Optional

import redis

from config import config

logger = logging.getLogger(__name__)


class CacheWriter:
    """
    Writes LLM responses to the Redis Stack semantic cache.

    Uses a synchronous Redis client (redis-py) since this runs in a thread pool
    (spawned by the Kafka consumer) rather than an async event loop.

    THREAD SAFETY:
      Redis clients in redis-py use connection pools internally.
      The ConnectionPool is thread-safe: multiple threads can share one
      CacheWriter instance without connection conflicts.
    """

    def __init__(self):
        """
        Initialize the Redis client with connection pooling.

        WHY CONNECTION POOLING?
          Creating a new Redis connection per cache write is expensive:
          TCP handshake + Redis AUTH ≈ 5-10ms. A connection pool pre-creates
          connections and reuses them, reducing per-write overhead to <1ms.
        """
        self.redis_client = redis.Redis(
            host=config.REDIS_HOST,
            port=config.REDIS_PORT,
            decode_responses=False,  # We need raw bytes for the vector field
            socket_timeout=5,        # Fail fast on Redis unavailability
            socket_connect_timeout=5,
            retry_on_timeout=True,   # Automatically retry on transient timeouts
            max_connections=20,      # Pool size: supports 20 concurrent writes
        )
        logger.info(
            "CacheWriter initialized: redis://%s:%d", config.REDIS_HOST, config.REDIS_PORT
        )

    def write_cache_entry(
        self,
        prompt_text: str,
        response_text: str,
        model_used: str,
        request_id: str,
        prompt_vector_base64: Optional[str] = None,
    ) -> Optional[str]:
        """
        Stores a prompt+response pair in the Redis semantic cache.

        This method is called asynchronously after a successful LLM generation.
        It does NOT block the client response — the response is sent first,
        then the cache is populated in the background.

        WHY POPULATE CACHE AFTER RESPONSE?
          The client should not wait for cache population. Cache writes add
          ~5ms (Redis round trip). By doing it after response delivery, we
          maintain sub-12ms cache-hit latency for future requests without
          adding latency to the current request.

        FAILURE HANDLING:
          If Redis is unavailable, we log the error and continue. Cache
          write failures are non-fatal: the worst case is a cache miss on
          the next identical request (falls through to LLM generation again).

        @param prompt_text: The user's original prompt text
        @param response_text: The LLM's validated response
        @param model_used: The model that generated the response
        @param request_id: For logging/tracing correlation
        @param prompt_vector_base64: Optional pre-computed vector (from Kafka message)

        @returns: The Redis key of the stored entry, or None on failure
        """
        try:
            start_ms = int(time.time() * 1000)

            # Generate or decode the embedding vector
            if prompt_vector_base64:
                # Use the pre-computed vector from the Java gateway (more efficient)
                # The Java gateway computed this during the cache miss, transmitted via Kafka
                import base64
                vector_bytes = base64.b64decode(prompt_vector_base64)
                logger.debug(
                    "Using pre-computed vector from Kafka [%s]: %d bytes",
                    request_id, len(vector_bytes)
                )
            else:
                # Generate a fresh embedding (mock or real depending on config)
                vector_bytes = self._generate_embedding_bytes(prompt_text)
                logger.debug(
                    "Generated fresh embedding [%s]: %d bytes",
                    request_id, len(vector_bytes)
                )

            # Generate a unique Redis key for this cache entry
            entry_key = f"prompt:{uuid.uuid4()}"
            created_at = int(time.time())

            # =====================================================================
            # Redis HSET command: Store all fields atomically in one round trip
            # =====================================================================
            # WHY HSET instead of separate SET commands?
            #   HSET stores multiple fields in a single HASH key atomically.
            #   If we used separate SET commands, a crash between writes could
            #   leave an incomplete entry (e.g., prompt stored but no response).
            #   HSET is atomic: all fields are written or none are.
            #
            # FIELD TYPES:
            #   - vector: raw bytes (binary) — will be indexed by HNSW
            #   - prompt_text, response_text, model_used: UTF-8 strings
            #   - created_at: numeric string (for SORTBY in FT.SEARCH)
            # =====================================================================
            pipeline = self.redis_client.pipeline(transaction=True)

            # HSET <key> <field1> <value1> <field2> <value2> ...
            # All fields set in one atomic operation
            pipeline.hset(
                entry_key,
                mapping={
                    b"vector": vector_bytes,                           # Raw float32 bytes (binary)
                    b"prompt_text": prompt_text.encode("utf-8"),       # UTF-8 encoded string
                    b"response_text": response_text.encode("utf-8"),   # UTF-8 encoded string
                    b"model_used": model_used.encode("utf-8"),         # Model name tag
                    b"created_at": str(created_at).encode("utf-8"),    # Unix timestamp
                }
            )

            # EXPIRE <key> <seconds>: Set TTL for automatic expiration
            # WHY TTL? Without TTL, the cache grows unboundedly. 24h TTL ensures:
            # 1. Stale responses (LLM knowledge updates) don't persist forever
            # 2. Redis memory is automatically reclaimed
            # 3. Cache naturally rotates to reflect the latest model behavior
            pipeline.expire(entry_key, config.CACHE_TTL_SECONDS)

            # Execute both HSET and EXPIRE atomically via pipeline
            pipeline.execute()

            write_ms = int(time.time() * 1000) - start_ms
            logger.info(
                "✓ Cache populated [%s]: key=%s, model=%s, write_ms=%d, TTL=%ds",
                request_id, entry_key, model_used, write_ms, config.CACHE_TTL_SECONDS,
            )

            return entry_key

        except redis.RedisError as e:
            # Redis errors are non-fatal for cache writes
            # The request was already served — this only affects future cache efficiency
            logger.error(
                "⚠️  Cache write FAILED [%s]: %s (non-fatal, future requests will miss cache)",
                request_id, str(e)
            )
            return None

        except Exception as e:
            logger.error("❌ Unexpected cache write error [%s]: %s", request_id, str(e))
            return None

    def _generate_embedding_bytes(self, text: str) -> bytes:
        """
        Generates a 1536-dim FLOAT32 embedding vector as raw bytes.

        In mock mode: deterministic hash-based embedding (matches Java's EmbeddingService)
        In production: calls OpenAI text-embedding-3-small API

        CRITICAL CONSISTENCY REQUIREMENT:
          The embedding function used HERE (for cache storage) MUST be identical
          to the one used in the Java Gateway (for cache lookup). If they differ,
          the stored vector and query vector live in different spaces, and KNN
          search will return incorrect (or no) results.

          For mock mode: both Java and Python use SHA-256 hash → seeded RNG → normalize
          For production: both use text-embedding-3-small from OpenAI

        @param text: The prompt text to embed
        @returns: 6144 bytes (1536 × 4 bytes, FLOAT32 little-endian)
        """
        if config.USE_MOCK_LLM:
            return self._mock_embedding_bytes(text)
        else:
            return self._real_embedding_bytes(text)

    def _mock_embedding_bytes(self, text: str) -> bytes:
        """
        Generates a deterministic pseudo-embedding using SHA-256 + seeded RNG.
        Matches the algorithm in Java's EmbeddingService.generateMockEmbedding().

        ALGORITHM (must exactly mirror Java implementation):
          1. SHA-256 hash of text → 32 bytes
          2. First 8 bytes → long seed (big-endian)
          3. Seed → Python Random() with the same seed
          4. Generate 1536 Gaussian random floats
          5. L2-normalize to unit length

        NOTE: Python's random.gauss() uses a different algorithm than Java's
        Random.nextGaussian() (Box-Muller vs. Java's modified polar). In a real
        system, both sides would call the same embedding API. In mock mode, the
        vectors are deterministic per side (same input → same output on same side)
        but NOT cross-compatible between Java and Python. This is acceptable for
        integration testing since we test the caching pipeline, not the similarity scores.
        """
        import random
        import math

        # SHA-256 hash for determinism
        hash_bytes = hashlib.sha256(text.encode("utf-8")).digest()

        # Derive seed from first 8 bytes (big-endian long)
        seed = int.from_bytes(hash_bytes[:8], byteorder="big")

        rng = random.Random(seed)
        dims = config.EMBEDDING_DIMENSIONS

        # Generate Gaussian-distributed floats
        vector = [rng.gauss(0, 1) for _ in range(dims)]

        # L2 normalize
        magnitude = math.sqrt(sum(x * x for x in vector))
        if magnitude > 0:
            vector = [x / magnitude for x in vector]

        # Pack as FLOAT32 little-endian bytes (matches Java's ByteBuffer.LITTLE_ENDIAN)
        # struct.pack('<' + 'f' * dims, *vector) → '<' = little-endian, 'f' = float32
        return struct.pack(f"<{dims}f", *vector)

    def _real_embedding_bytes(self, text: str) -> bytes:
        """
        Calls OpenAI text-embedding-3-small to generate a real semantic embedding.

        @param text: Text to embed
        @returns: 6144 bytes (1536 × float32 little-endian)
        """
        from openai import OpenAI

        client = OpenAI(api_key=config.OPENAI_API_KEY)
        response = client.embeddings.create(
            model="text-embedding-3-small",
            input=text,
        )
        embedding = response.data[0].embedding  # list of 1536 floats
        return struct.pack(f"<{len(embedding)}f", *embedding)

    def health_check(self) -> bool:
        """
        Verifies Redis connectivity and VSS index existence.

        @returns: True if Redis is healthy and index exists, False otherwise
        """
        try:
            self.redis_client.ping()
            # Check if the VSS index exists
            self.redis_client.execute_command("FT.INFO", config.REDIS_VSS_INDEX)
            return True
        except Exception as e:
            logger.error("Redis health check failed: %s", str(e))
            return False
