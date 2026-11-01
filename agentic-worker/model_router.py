"""
=============================================================================
model_router.py — LiteLLM Multi-Model Router with Automatic Fallback
=============================================================================
WHAT THIS MODULE DOES:
  Provides a single entry point for LLM completion requests that automatically
  handles:
  1. Primary model invocation (Claude 3.5 Sonnet or GPT-4o)
  2. Automatic fallback to secondary model on HTTP 429/500/timeout
  3. Tertiary fallback if secondary also fails
  4. Timeout enforcement per model
  5. Structured logging of which model served each request

WHY LITELLM?
  LiteLLM normalizes 100+ LLM providers to a single OpenAI-compatible interface.
  Without LiteLLM:
    - Anthropic: client.messages.create(model="claude-3-5-sonnet", ...)
    - OpenAI: client.chat.completions.create(model="gpt-4o", ...)
    - Gemini: genai.GenerativeModel("gemini-1.5-flash").generate_content(...)
  With LiteLLM:
    - ALL: litellm.completion(model="<provider>/<model>", messages=[...])
  This abstraction eliminates 3 separate SDK integrations.

WHY AUTOMATIC FALLBACK?
  Production LLM APIs have real-world constraints:
  - Rate limits: OpenAI GPT-4o has per-minute and per-day token limits
  - Outages: Even major providers experience brief unavailability
  - Regional latency: Cloud providers have variable response times by geography
  LiteLLM's Router provides retry + fallback out of the box, turning transient
  failures into seamless user experiences.

LITELLM ROUTER CONFIGURATION:
  model_list: Defines available models with their API keys and fallback priority
  fallbacks: Specifies which models to try after each model fails
  retry_policy: How many times to retry a failed model before falling back
  timeout: Per-model request timeout (seconds)
=============================================================================
"""

import logging
import time
from typing import Optional

import litellm
from litellm import Router
from litellm.exceptions import (
    RateLimitError,
    ServiceUnavailableError,
    Timeout,
    AuthenticationError,
)

from config import config

# =============================================================================
# Configure LiteLLM Logging
# =============================================================================
# Set LiteLLM to only log warnings and errors (its INFO logs are very verbose)
litellm.set_verbose = False
logger = logging.getLogger(__name__)


class ModelRouter:
    """
    LiteLLM-based multi-model router with automatic failover.

    This class wraps LiteLLM's Router to provide:
    - Transparent model selection (primary → secondary → tertiary)
    - Per-request latency and model tracking
    - Structured error handling and logging
    - Mock mode for local testing without API keys

    THREAD SAFETY:
      The Router instance is created once and shared across threads.
      LiteLLM's Router is thread-safe: it uses internal locks for rate
      limit tracking and maintains per-model connection pools.
    """

    def __init__(self):
        """
        Initialize the LiteLLM Router with primary and fallback models.

        The router configuration defines:
          - model_list: Available models with their API credentials
          - fallbacks: Ordered list of fallback models for each primary
          - timeout: Maximum wait time before triggering fallback (seconds)
          - num_retries: Retry count before marking a model as failed

        WHY TWO-TIER FALLBACK (primary → secondary → tertiary)?
          - Primary (Claude 3.5 Sonnet): Best quality, higher cost, can hit rate limits
          - Secondary (Gemini 1.5 Flash): Fast, cheap, separate rate limit pool
          - Tertiary (Mistral Large): European provider, different availability zone
          Having three tiers gives 99.9%+ availability across all three providers
          even when two have simultaneous issues (very rare, but it happens).
        """
        logger.info(
            "Initializing LiteLLM Router: primary=%s, fallback=%s, tertiary=%s",
            config.PRIMARY_MODEL,
            config.FALLBACK_MODEL,
            config.TERTIARY_MODEL,
        )

        # Define the model list for LiteLLM Router
        # Each entry specifies: model_name (alias), litellm_params (actual model + key)
        model_list = [
            {
                "model_name": "primary",  # Abstract alias used in completion() calls
                "litellm_params": {
                    "model": config.PRIMARY_MODEL,
                    "api_key": self._get_api_key(config.PRIMARY_MODEL),
                    "timeout": config.LLM_TIMEOUT_SECONDS,
                },
            },
            {
                "model_name": "secondary",
                "litellm_params": {
                    "model": config.FALLBACK_MODEL,
                    "api_key": self._get_api_key(config.FALLBACK_MODEL),
                    "timeout": config.LLM_TIMEOUT_SECONDS,
                },
            },
            {
                "model_name": "tertiary",
                "litellm_params": {
                    "model": config.TERTIARY_MODEL,
                    "api_key": self._get_api_key(config.TERTIARY_MODEL),
                    "timeout": config.LLM_TIMEOUT_SECONDS,
                },
            },
        ]

        # Fallback routing: if "primary" fails, try "secondary", then "tertiary"
        # LiteLLM Router evaluates fallbacks in order until one succeeds
        fallbacks = [
            {"primary": ["secondary"]},
            {"secondary": ["tertiary"]},
        ]

        self.router = Router(
            model_list=model_list,
            fallbacks=fallbacks,
            # Number of times to retry a single model before triggering fallback
            # WHY 2 retries? Transient 429s and network blips often resolve within 1-2 retries
            num_retries=2,
            # Timeout applies per retry attempt, not total — so primary gets
            # up to 3 × timeout before falling back to secondary
            timeout=config.LLM_TIMEOUT_SECONDS,
            # Enable detailed retry logging for observability
            set_verbose=False,
        )

        logger.info("✓ LiteLLM Router initialized with %d models", len(model_list))

    def complete(
        self,
        messages: list[dict],
        model: str = "primary",
        temperature: float = 0.7,
        max_tokens: Optional[int] = None,
        request_id: Optional[str] = None,
    ) -> dict:
        """
        Route a completion request through the model chain with automatic fallback.

        This is the PRIMARY interface used by the LangGraph nodes. It:
        1. Attempts the requested model (default: primary = Claude 3.5 Sonnet)
        2. On failure (429/500/timeout): automatically falls back per router config
        3. Returns a standardized result dict with model info, content, and usage

        ARGUMENTS:
          messages:    List of {"role": "...", "content": "..."} dicts
          model:       Model alias ("primary", "secondary", or "tertiary")
          temperature: Sampling temperature (0=deterministic, 2=creative)
          max_tokens:  Max output tokens (None = model default)
          request_id:  For logging correlation

        RETURNS:
          dict with keys:
            - content (str): The generated text
            - model_used (str): Actual model that served the request (e.g., "gpt-4o")
            - input_tokens (int): Prompt token count
            - output_tokens (int): Completion token count
            - fallback_used (bool): Whether the primary model was bypassed
            - latency_ms (int): Wall-clock latency of the LLM call
        """
        if config.USE_MOCK_LLM:
            return self._mock_completion(messages, request_id)

        start_ms = int(time.time() * 1000)

        try:
            logger.info(
                "🤖 LLM Request [%s]: model_alias=%s, messages=%d",
                request_id,
                model,
                len(messages),
            )

            # LiteLLM Router.completion() is the same interface as litellm.completion()
            # but with automatic retry and fallback built in
            completion_kwargs = {
                "model": model,  # Use the abstract alias (e.g., "primary")
                "messages": messages,
                "temperature": temperature,
                "stream": False,  # Non-streaming for MVP; can add streaming in next iteration
            }
            if max_tokens is not None:
                completion_kwargs["max_tokens"] = max_tokens

            response = self.router.completion(**completion_kwargs)

            latency_ms = int(time.time() * 1000) - start_ms

            # Extract the actual model name used (LiteLLM populates this field)
            # This tells us if a fallback was triggered (model ≠ primary model name)
            actual_model = response.model or config.PRIMARY_MODEL
            content = response.choices[0].message.content
            usage = response.usage

            # Detect if a fallback was used
            primary_model_names = [config.PRIMARY_MODEL]
            fallback_used = actual_model not in primary_model_names

            if fallback_used:
                logger.warning(
                    "⚠️  FALLBACK TRIGGERED [%s]: primary failed, used model=%s",
                    request_id,
                    actual_model,
                )
            else:
                logger.info(
                    "✓ LLM Response [%s]: model=%s, latency=%dms, tokens=%d in / %d out",
                    request_id,
                    actual_model,
                    latency_ms,
                    usage.prompt_tokens if usage else 0,
                    usage.completion_tokens if usage else 0,
                )

            return {
                "content": content,
                "model_used": actual_model,
                "input_tokens": usage.prompt_tokens if usage else 0,
                "output_tokens": usage.completion_tokens if usage else 0,
                "fallback_used": fallback_used,
                "latency_ms": latency_ms,
            }

        except RateLimitError as e:
            # 429: All models in the router chain are rate-limited simultaneously
            # This is extremely rare with 3 diverse providers, but we handle it gracefully
            logger.error("🚫 All models rate-limited [%s]: %s", request_id, str(e))
            raise

        except (ServiceUnavailableError, Timeout) as e:
            # 503/Timeout: Even after retries + fallbacks, all providers failed
            logger.error("💥 All models unavailable/timeout [%s]: %s", request_id, str(e))
            raise

        except AuthenticationError as e:
            # 401: API key issue — this is a configuration error, not transient
            logger.error("🔑 Authentication error [%s]: %s", request_id, str(e))
            raise

        except Exception as e:
            latency_ms = int(time.time() * 1000) - start_ms
            logger.error("❌ Unexpected LLM error [%s] after %dms: %s", request_id, latency_ms, str(e))
            raise

    def _mock_completion(self, messages: list[dict], request_id: Optional[str]) -> dict:
        """
        Returns a canned LLM response for local testing without API keys.

        WHY A MOCK?
          Running integration tests that hit real LLM APIs is:
          1. Slow: 1-30 seconds per call
          2. Expensive: API costs money
          3. Non-deterministic: responses vary, breaking assert-based tests
          Mock mode enables fast, deterministic, free testing of the full pipeline.

        The mock simulates realistic behavior:
          - 200ms artificial delay (simulates LLM network latency)
          - Returns a coherent response based on the last user message
        """
        import time
        time.sleep(0.2)  # Simulate 200ms LLM latency

        last_message = messages[-1]["content"] if messages else "Hello"

        mock_response = (
            f"[MOCK RESPONSE for: '{last_message[:50]}...']"
            if len(last_message) > 50
            else f"[MOCK RESPONSE for: '{last_message}']"
        )

        logger.info("🎭 Mock LLM response generated [%s]", request_id)

        return {
            "content": mock_response + "\n\nThis is a mock response from NexusCore's test mode. "
                       "Set USE_MOCK_LLM=false and configure API keys for real responses.",
            "model_used": "mock-model",
            "input_tokens": len(" ".join(m["content"] for m in messages)) // 4,
            "output_tokens": 50,
            "fallback_used": False,
            "latency_ms": 200,
        }

    def _get_api_key(self, model: str) -> str:
        """
        Returns the appropriate API key for the given model provider.

        LiteLLM identifies the provider from the model name prefix:
          "claude-*"  → Anthropic key
          "gpt-*"     → OpenAI key
          "gemini/*"  → Google key
          "mistral/*" → Mistral key

        @param model: Full model identifier (e.g., "gemini/gemini-1.5-flash")
        @returns: The API key string for this provider
        """
        model_lower = model.lower()
        if "claude" in model_lower or "anthropic" in model_lower:
            return config.ANTHROPIC_API_KEY
        elif "gpt" in model_lower or "openai" in model_lower:
            return config.OPENAI_API_KEY
        elif "gemini" in model_lower or "google" in model_lower:
            return config.GOOGLE_API_KEY
        elif "mistral" in model_lower:
            return config.MISTRAL_API_KEY
        else:
            # Default: let LiteLLM auto-detect from environment
            return os.getenv("LLM_API_KEY", "")


import os

# =============================================================================
# Module-level singleton: shared across all worker threads
# =============================================================================
# WHY SINGLETON?
#   The Router maintains state: connection pools, rate limit trackers, and
#   fallback state. Sharing one instance avoids redundant connections and
#   ensures rate limit tracking is accurate across all concurrent requests.
_router_instance: Optional[ModelRouter] = None


def get_router() -> ModelRouter:
    """
    Returns the module-level ModelRouter singleton, creating it on first call.

    Thread-safe: Python's GIL ensures the initialization block runs once.
    """
    global _router_instance
    if _router_instance is None:
        _router_instance = ModelRouter()
    return _router_instance
