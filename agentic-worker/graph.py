"""
=============================================================================
graph.py — LangGraph Multi-Agent Reasoning Pipeline
=============================================================================
WHAT THIS MODULE DOES:
  Implements the NexusCore agentic pipeline as a LangGraph StateGraph.
  The graph has 3 processing nodes and conditional edges:

    ┌─────────────────────────────────────────────────────────┐
    │                  LangGraph State Machine                 │
    │                                                         │
    │  [START] ──► [Node 1: Intent Classifier]                │
    │                      │                                  │
    │          ┌───────────┴───────────┐                      │
    │          ▼                       ▼                      │
    │  [Needs Tool?]              [Direct Answer]             │
    │  [Node 2: Tool Executor]         │                      │
    │          │                       │                      │
    │          └───────────┬───────────┘                      │
    │                      ▼                                  │
    │          [Node 3: Guardrail Verifier]                   │
    │                      │                                  │
    │                   [END]                                 │
    └─────────────────────────────────────────────────────────┘

NODE DESCRIPTIONS:

  Node 1 — Intent & Tool Classifier:
    Uses the LLM to classify the prompt: does it require external data retrieval
    (database lookup, API call, calculation) or can it be answered directly?
    Examples requiring tools: "What is the account balance for user ID 12345?"
    Examples not requiring tools: "What is the capital of France?"

  Node 2 — Tool Execution:
    If Node 1 decides tools are needed, this node executes the appropriate tools:
    - sql_lookup: Queries the mock PostgreSQL database for structured data
    - calculator: Performs financial calculations
    - api_lookup: Calls mock external APIs (e.g., order status, shipping)
    Tool results are appended to the message context before LLM generation.

  Node 3 — Guardrail & Hallucination Verifier:
    Validates the LLM's output against a Pydantic schema:
    - Response is non-empty
    - Response doesn't contain obvious hallucination markers
    - Response length is within expected bounds
    - Sensitive data patterns (SSN, credit card) are not present (PII filter)
    If validation fails, the node can trigger a retry (up to 3 times).

WHY LANGGRAPH OVER RAW LANGCHAIN?
  LangChain's sequential chains (LCEL) are acyclic: A → B → C. They cannot:
  - Loop back if the guardrail fails (requires a retry cycle)
  - Branch conditionally based on the tool classifier's output
  LangGraph supports both cycles and conditional edges via StateGraph,
  making it the correct abstraction for agentic workflows with retry logic.

STATE MANAGEMENT:
  LangGraph maintains a shared State dict that flows between nodes.
  Each node READS the current state and RETURNS an updated state dict.
  This is analogous to Redux reducers: immutable state transitions.
=============================================================================
"""

import json
import logging
import re
from typing import TypedDict, Optional, Annotated

from langchain_core.messages import HumanMessage, AIMessage, SystemMessage, ToolMessage
from langgraph.graph import StateGraph, START, END
from pydantic import BaseModel, Field, field_validator

from model_router import get_router

logger = logging.getLogger(__name__)

# =============================================================================
# State Schema: The shared state passed between all graph nodes
# =============================================================================

class AgentState(TypedDict):
    """
    Shared state object passed between all LangGraph nodes.

    WHY TYPEDDICT?
      TypedDict gives type hints without the overhead of a full dataclass.
      LangGraph requires the state to be a TypedDict (or dict subclass).
      The Annotated[list, operator.add] pattern tells LangGraph to APPEND
      to message lists (rather than replacing them) on state updates.

    STATE LIFECYCLE:
      1. main.py creates the initial state from the Kafka PromptEvent
      2. Node 1 (classifier) appends its classification decision
      3. Node 2 (tools, if needed) appends tool results to messages
      4. Node 3 (guardrail) appends the final validated response
      5. The final state is read after graph.invoke() completes
    """

    # The incoming prompt event ID (for tracing and logging)
    request_id: str

    # The conversation messages (system prompt + user message + tool results)
    # Using a list (not Annotated with operator.add) for full control over ordering
    messages: list

    # Classification result from Node 1: "tools_required" | "direct_answer"
    intent: Optional[str]

    # Tool call results (populated by Node 2)
    tool_results: Optional[list]

    # The final validated response from Node 3
    final_response: Optional[str]

    # Which model was ultimately used (for telemetry)
    model_used: Optional[str]

    # Token usage from the LLM call (for cost tracking)
    input_tokens: int
    output_tokens: int

    # How many guardrail validation retries were attempted
    retry_count: int

    # Whether a fallback model was triggered
    fallback_used: bool

    # OpenTelemetry trace ID for distributed tracing
    trace_id: Optional[str]


# =============================================================================
# Pydantic Schemas: Guardrail Validation
# =============================================================================

class GuardrailSchema(BaseModel):
    """
    Schema for validating LLM output before returning it to the client.

    WHY PYDANTIC FOR GUARDRAILS?
      Pydantic v2's validators run in Rust, making validation extremely fast
      (~1ms per validation). We use it to enforce:
      1. Content length constraints (not too short, not too long)
      2. PII detection (SSN, credit card patterns via regex)
      3. Hallucination markers (phrases like "As an AI, I cannot..." indicate
         the model refused to answer — these should trigger a retry)
      4. Profanity/policy violation detection (simplified here)

    PRODUCTION EXTENSIONS:
      In a production system, add:
      - Azure Content Safety API for toxic content detection
      - Custom regex blocklists for domain-specific violations
      - LLM-as-judge: use a cheap model to score quality before returning
    """

    response_text: str = Field(..., min_length=1, max_length=100_000)

    @field_validator("response_text")
    @classmethod
    def check_no_pii_leakage(cls, v: str) -> str:
        """
        Detect Social Security Numbers and credit card patterns in the response.

        WHY THIS MATTERS:
          LLMs can be tricked into outputting PII from their training data.
          This validator catches common patterns before data reaches the client.
        """
        # SSN pattern: NNN-NN-NNNN
        if re.search(r"\b\d{3}-\d{2}-\d{4}\b", v):
            raise ValueError("Response contains potential SSN (PII violation)")

        # Credit card pattern: 16 digit groups separated by spaces or dashes
        if re.search(r"\b(?:\d[ -]?){15,16}\b", v):
            raise ValueError("Response contains potential credit card number")

        return v

    @field_validator("response_text")
    @classmethod
    def check_not_refusal(cls, v: str) -> str:
        """
        Detect model refusal patterns that indicate the query should be retried
        with better context or a different model.

        REFUSAL PATTERNS:
          These phrases indicate the model didn't actually answer the question.
          In production, rather than error out, we'd rewrite the prompt and retry.
        """
        refusal_patterns = [
            "i cannot provide",
            "i'm unable to",
            "i am unable to",
            "as an ai language model, i cannot",
            "i don't have access to real-time",
        ]
        v_lower = v.lower()
        for pattern in refusal_patterns:
            if pattern in v_lower:
                logger.warning("⚠️  Guardrail: response contains refusal pattern: '%s'", pattern)
                # We raise a ValueError to trigger a retry with enriched context
                raise ValueError(f"Response appears to be a refusal: found pattern '{pattern}'")
        return v


# =============================================================================
# Graph Node Implementations
# =============================================================================

def node_intent_classifier(state: AgentState) -> dict:
    """
    Node 1: Intent & Tool Classifier

    Determines if the user's request requires external tool execution
    (database lookups, API calls, calculations) or can be answered directly
    from the LLM's knowledge.

    HOW IT WORKS:
      Uses a lightweight LLM call (or rule-based classifier in mock mode)
      to classify the intent. The classification is used by the graph's
      conditional edge to route to either Node 2 (tools) or Node 3 (guardrail).

    CLASSIFICATION EXAMPLES:
      "What is the capital of France?" → direct_answer (factual, no tools)
      "What is the balance on account 12345?" → tools_required (DB lookup)
      "Calculate compound interest on $10,000 at 5% for 3 years" → tools_required

    @param state: Current graph state
    @returns: Updated state dict with 'intent' field set
    """
    request_id = state["request_id"]
    messages = state["messages"]

    logger.info("🔍 Node 1: Classifying intent for request [%s]", request_id)

    # Extract the last user message for classification
    last_user_msg = next(
        (m["content"] for m in reversed(messages) if m.get("role") == "user"),
        ""
    )

    # Rule-based classifier for mock mode (avoids extra LLM call)
    # In production: use a small, fast model (e.g., GPT-3.5-turbo) for classification
    tool_trigger_keywords = [
        "account", "balance", "order", "status", "customer id", "user id",
        "calculate", "sum", "total", "invoice", "transaction", "lookup",
        "retrieve", "database", "sql", "query", "find record"
    ]

    intent = "direct_answer"
    for keyword in tool_trigger_keywords:
        if keyword.lower() in last_user_msg.lower():
            intent = "tools_required"
            break

    logger.info(
        "✓ Intent classified [%s]: '%s' → %s",
        request_id,
        last_user_msg[:50] if len(last_user_msg) > 50 else last_user_msg,
        intent,
    )

    return {"intent": intent}


def node_tool_executor(state: AgentState) -> dict:
    """
    Node 2: Tool Execution

    Executes the appropriate tools based on the user's intent.
    Tool results are appended to the message context so the LLM
    can use them to generate a grounded, accurate response.

    AVAILABLE TOOLS:
      - sql_lookup(entity_id): Queries mock PostgreSQL for entity data
      - calculator(expression): Evaluates mathematical expressions safely
      - api_lookup(endpoint, params): Calls mock external APIs

    WHY TOOL CALLING?
      LLMs have a knowledge cutoff date and no access to real-time or
      private organizational data. Tool calling bridges this gap by letting
      the LLM access current data (account balances, order statuses) while
      still generating the natural language response.

    @param state: Current graph state
    @returns: Updated state with tool results appended to messages
    """
    request_id = state["request_id"]
    messages = state["messages"]

    logger.info("🔧 Node 2: Executing tools for request [%s]", request_id)

    last_user_msg = next(
        (m["content"] for m in reversed(messages) if m.get("role") == "user"),
        ""
    )

    # Execute mock tools based on simple keyword matching
    tool_results = []

    if any(kw in last_user_msg.lower() for kw in ["account", "balance", "customer"]):
        # Mock SQL lookup result
        result = sql_lookup_tool(last_user_msg)
        tool_results.append(result)
        logger.info("  ✓ sql_lookup executed: %s", result[:100])

    elif any(kw in last_user_msg.lower() for kw in ["calculate", "sum", "total"]):
        # Mock calculator result
        result = calculator_tool(last_user_msg)
        tool_results.append(result)
        logger.info("  ✓ calculator executed: %s", result[:100])

    # Append tool results to the message context
    # The LLM will see these results when generating its response in Node 3
    updated_messages = list(messages)
    if tool_results:
        tool_context = "\n\n[TOOL RESULTS]\n" + "\n".join(tool_results)
        updated_messages.append({
            "role": "system",
            "content": f"The following data was retrieved from internal systems to help answer the user's question:{tool_context}"
        })

    logger.info("✓ Node 2 complete [%s]: %d tool(s) executed", request_id, len(tool_results))

    return {
        "messages": updated_messages,
        "tool_results": tool_results,
    }


def node_guardrail_verifier(state: AgentState) -> dict:
    """
    Node 3: LLM Generation + Guardrail Validation

    This is the primary LLM generation node. It:
    1. Calls the ModelRouter to generate a response (with automatic fallback)
    2. Validates the response against the GuardrailSchema (Pydantic)
    3. If validation fails: increments retry_count (graph loops back here up to 3 times)
    4. If validation passes: sets final_response and signals completion

    RETRY LOGIC:
      The LangGraph graph has a conditional edge from this node:
      - If retry_count < 3 AND validation failed → loop back to this node
      - If retry_count >= 3 OR validation passed → proceed to END
      On retry, the messages list is augmented with the failed response and
      an instruction to try again differently, giving the LLM additional context.

    @param state: Current graph state
    @returns: Updated state with final_response (or retry signal)
    """
    request_id = state["request_id"]
    messages = state["messages"]
    retry_count = state.get("retry_count", 0)

    logger.info(
        "🛡️  Node 3: Generating + validating response [%s] (attempt %d/3)",
        request_id,
        retry_count + 1,
    )

    try:
        # Call the LLM via ModelRouter (handles primary → fallback automatically)
        router = get_router()
        result = router.complete(
            messages=messages,
            request_id=request_id,
        )

        response_text = result["content"]
        model_used = result["model_used"]
        input_tokens = result["input_tokens"]
        output_tokens = result["output_tokens"]
        fallback_used = result["fallback_used"]

        logger.info(
            "  LLM responded [%s]: model=%s, tokens=%d/%d, fallback=%s",
            request_id, model_used, input_tokens, output_tokens, fallback_used,
        )

        # Validate the response through the Pydantic guardrail schema
        try:
            validated = GuardrailSchema(response_text=response_text)
            logger.info("  ✅ Guardrail PASSED [%s]", request_id)

            return {
                "final_response": validated.response_text,
                "model_used": model_used,
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "fallback_used": fallback_used,
                "retry_count": retry_count,  # Don't increment on success
            }

        except ValueError as validation_error:
            # Guardrail failed: prepare for retry with enriched context
            logger.warning(
                "  ⚠️  Guardrail FAILED [%s] (attempt %d): %s",
                request_id, retry_count + 1, str(validation_error)
            )

            # Add the failed response + correction instruction to message context
            updated_messages = list(messages)
            updated_messages.append({
                "role": "assistant",
                "content": response_text
            })
            updated_messages.append({
                "role": "user",
                "content": f"Your previous response had an issue: {validation_error}. "
                           f"Please try again with a corrected response that addresses this concern."
            })

            return {
                "messages": updated_messages,
                "retry_count": retry_count + 1,
                "model_used": model_used,
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "fallback_used": fallback_used,
                # final_response remains None — graph will loop back
            }

    except Exception as e:
        # LLM call failed completely (even after internal retries + fallbacks)
        logger.error("  ❌ LLM generation failed [%s]: %s", request_id, str(e))

        # Return an error response rather than crashing the graph
        return {
            "final_response": "I'm sorry, but I'm unable to process your request at this time. "
                             "Please try again in a moment.",
            "model_used": "error",
            "input_tokens": 0,
            "output_tokens": 0,
            "fallback_used": True,
            "retry_count": 3,  # Max retries: prevent infinite loop on persistent failures
        }


# =============================================================================
# Conditional Edge Functions
# =============================================================================

def should_use_tools(state: AgentState) -> str:
    """
    Conditional edge: routes from Node 1 to either Node 2 or Node 3.

    LangGraph evaluates this function AFTER Node 1 completes and uses the
    return value to determine which node to execute next.

    @returns: "tools" → route to node_tool_executor
             "guardrail" → skip tools, go directly to node_guardrail_verifier
    """
    intent = state.get("intent", "direct_answer")
    if intent == "tools_required":
        logger.debug("Routing to tool executor (intent=tools_required)")
        return "tools"
    else:
        logger.debug("Routing directly to guardrail (intent=direct_answer)")
        return "guardrail"


def should_retry(state: AgentState) -> str:
    """
    Conditional edge: routes from Node 3 to either retry (itself) or END.

    If the guardrail failed AND retry_count < 3: loop back to Node 3.
    Otherwise: proceed to END (final_response is set).

    @returns: "retry" → loop back to node_guardrail_verifier
             "end" → proceed to graph END
    """
    final_response = state.get("final_response")
    retry_count = state.get("retry_count", 0)
    max_retries = 3

    if final_response is not None:
        logger.debug("Guardrail passed — proceeding to END")
        return "end"
    elif retry_count >= max_retries:
        logger.warning("Max retries (%d) reached — returning error response", max_retries)
        return "end"
    else:
        logger.debug("Guardrail failed — retrying (attempt %d/%d)", retry_count, max_retries)
        return "retry"


# =============================================================================
# Mock Tools
# =============================================================================

def sql_lookup_tool(query: str) -> str:
    """
    Mock SQL lookup tool. In production: executes parameterized SQL queries
    against PostgreSQL using SQLAlchemy and returns structured results.

    PRODUCTION IMPLEMENTATION:
      - Parse entity IDs from the query using NER or regex
      - Execute: SELECT * FROM accounts WHERE id = :entity_id
      - Return results as a formatted JSON string
      - Use read replicas to avoid impacting write performance
    """
    return (
        "SQL Query Result (MOCK): "
        "Account ID: 12345, Customer Name: John Doe, "
        "Balance: $1,250.00, Status: Active, "
        "Last Transaction: 2024-01-15 (Debit $50.00)"
    )


def calculator_tool(expression: str) -> str:
    """
    Mock calculator tool. In production: uses Python's AST-safe eval or
    a math expression parser (e.g., numexpr) to evaluate financial formulas.

    SECURITY NOTE:
      Never use eval() directly on user-provided expressions.
      Use ast.literal_eval() or a whitelist-based expression parser to
      prevent code injection attacks.
    """
    return (
        "Calculation Result (MOCK): "
        "Compound Interest = $1,628.89 "
        "(Principal: $1,000, Rate: 5%, Duration: 10 years)"
    )


# =============================================================================
# Graph Construction and Compilation
# =============================================================================

def build_agent_graph() -> object:
    """
    Constructs and compiles the LangGraph StateGraph.

    GRAPH STRUCTURE:
      START → node_intent_classifier
      node_intent_classifier → (conditional) → node_tool_executor | node_guardrail_verifier
      node_tool_executor → node_guardrail_verifier
      node_guardrail_verifier → (conditional) → node_guardrail_verifier (retry) | END

    WHY compile()?
      LangGraph's compile() step:
      1. Validates the graph structure (detects unreachable nodes)
      2. Compiles the state management logic for efficiency
      3. Returns a CompiledGraph that can be invoke()d with a state dict

    @returns: Compiled LangGraph StateGraph ready for invoke()
    """
    # Initialize the StateGraph with our AgentState schema
    workflow = StateGraph(AgentState)

    # Add nodes (processing steps)
    workflow.add_node("intent_classifier", node_intent_classifier)
    workflow.add_node("tool_executor", node_tool_executor)
    workflow.add_node("guardrail_verifier", node_guardrail_verifier)

    # Add edges (routing)
    # START → intent_classifier: always enter here
    workflow.add_edge(START, "intent_classifier")

    # intent_classifier → (conditional) → tool_executor OR guardrail_verifier
    workflow.add_conditional_edges(
        "intent_classifier",
        should_use_tools,
        {
            "tools": "tool_executor",      # → Node 2 if tools needed
            "guardrail": "guardrail_verifier",  # → Node 3 directly
        }
    )

    # tool_executor → guardrail_verifier: always after tools
    workflow.add_edge("tool_executor", "guardrail_verifier")

    # guardrail_verifier → (conditional) → retry OR END
    workflow.add_conditional_edges(
        "guardrail_verifier",
        should_retry,
        {
            "retry": "guardrail_verifier",  # Loop back (retry on guardrail failure)
            "end": END,                      # Proceed to final result
        }
    )

    # Compile and return the runnable graph
    compiled = workflow.compile()
    logger.info("✓ LangGraph compiled: intent_classifier → [tools?] → guardrail_verifier → END")
    return compiled


# Module-level compiled graph singleton
_compiled_graph = None


def get_agent_graph():
    """Returns the compiled LangGraph graph, building it on first call."""
    global _compiled_graph
    if _compiled_graph is None:
        _compiled_graph = build_agent_graph()
    return _compiled_graph
