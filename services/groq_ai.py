"""
CreatorFlow — services/groq_ai.py
AI-powered YouTube metadata generator.

Uses the Groq API (llama3-8b-8192) in JSON mode to produce:
  • A viral, SEO-optimised YouTube title (≤ 100 chars, ideally ≤ 60)
  • A 2-paragraph description with strong hook and CTA

Supports optional category + context tags injected into the system prompt
for highly targeted metadata when the user provides sync context.

Supports optional aiConfig dict from the AI Generation settings page,
injected with field-length caps for defense-in-depth against Firestore
prompt injection (Pydantic validates API requests; this covers the
drive_poller path which loads aiConfig directly from Firestore).
"""

import asyncio
import json
import logging
import os
from typing import Final

from groq import Groq

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
GROQ_MODEL:        Final[str]   = os.environ.get("GROQ_MODEL", "llama-3.1-8b-instant")
GROQ_MAX_TOKENS:   Final[int]   = 600
GROQ_TEMPERATURE:  Final[float] = 0.85

# Field-length caps for Firestore-sourced aiConfig dicts.
# These mirror the Pydantic limits in main.py but apply here as a
# second layer so drive_poller can never inject oversized strings.
_FIELD_LIMITS: Final[dict[str, int]] = {
    "channelNiche":      50,
    "tone":              50,
    "titleStyle":        50,
    "targetAudience":    200,
    "alwaysInclude":     300,
    "alwaysAvoid":       300,
    "descriptionCTA":    200,
    "customInstruction": 500,
}

BASE_SYSTEM_PROMPT: Final[str] = """
You are an elite YouTube growth strategist with deep expertise in:
  - Viral content optimisation and audience psychology
  - YouTube SEO and search intent
  - Compelling copywriting for maximum click-through rate

Your task: given a video topic or sanitised filename, produce engaging YouTube metadata.

STRICT OUTPUT FORMAT — respond ONLY with a valid JSON object, no markdown, no backticks:
{
  "title": "<string: compelling title, UNDER 100 characters, ideally under 60>",
  "description": "<string: exactly two paragraphs separated by a blank line. Paragraph 1: hook viewers with the value proposition (2-3 sentences). Paragraph 2: call-to-action + relevant SEO keywords (2-3 sentences).>"
}

Rules for the title:
  • Must be under 100 characters (hard limit).
  • Transform raw filenames into clean, human-readable titles (e.g., turn "gameplay 1080p.mp4" into "Epic Gameplay").
  • NEVER include technical video details, file extensions, resolutions, or codecs (e.g., completely ignore .mp4, 360p, 720p, 1080p, 4k, h264, mkv, fps).
  • Use power words, numbers, or curiosity gaps where natural.
  • Never use clickbait that misrepresents the content.
  • Avoid ALL CAPS words.

Rules for the description:
  • Each paragraph is 2-3 sentences (not bullet points, not headers).
  • Naturally weave in 3-5 relevant keywords for search.
  • End with a soft CTA (like, subscribe, comment, etc.).
""".strip()


# ---------------------------------------------------------------------------
# Prompt builder
# ---------------------------------------------------------------------------

def _build_system_prompt(
    category: str | None = None,
    tags:     list[str] | None = None,
    config:   dict | None = None,
) -> str:
    """
    Dynamically appends user-supplied context to the base system prompt.

    Priority / sources:
      1. category + tags  — from the Sync Drive drawer (per-sync context)
      2. config dict      — from the AI Generation settings page (persistent rules)

    All string values are truncated before injection so an oversized Firestore
    document can never overflow the prompt or cause unexpected behaviour.

    Returns the base prompt unchanged if no context is provided.
    """
    tags = tags or []
    has_context = bool(category or tags or config)

    if not has_context:
        return BASE_SYSTEM_PROMPT

    context_lines = [
        "",
        "═══════════════════════════════════════",
        "CREATOR CONTEXT — USE THIS TO PERSONALISE THE OUTPUT:",
    ]

    # ── 1. Sync-time context (Sync Drive drawer) ──────────────────────
    if category:
        context_lines.append(
            f"  • Content Category: {str(category)[:50].upper()} "
            "— tailor tone, keywords, and hooks specifically for this niche."
        )

    if tags:
        safe_tags = [str(t)[:60] for t in tags[:10]]  # cap count + length
        context_lines.append(
            f"  • Content Style / Themes: {', '.join(safe_tags)} "
            "— reflect these naturally in the title and description."
        )

    # ── 2. Persistent AI config (AI Generation settings page) ─────────
    if config:

        def _safe(key: str) -> str:
            """Return a truncated, stripped string value from config, or ''."""
            val   = config.get(key, "")
            limit = _FIELD_LIMITS.get(key, 100)
            return str(val).strip()[:limit] if val else ""

        if _safe("channelNiche"):
            context_lines.append(
                f"  • Niche/Category: {_safe('channelNiche').upper()}"
            )
        if _safe("tone"):
            context_lines.append(
                f"  • Writing Tone: {_safe('tone').upper()}"
            )
        if _safe("titleStyle"):
            context_lines.append(
                f"  • Title Style: {_safe('titleStyle').upper()}"
            )
        if _safe("targetAudience"):
            context_lines.append(
                f"  • Target Audience: {_safe('targetAudience')}"
            )
        if _safe("alwaysInclude"):
            context_lines.append(
                f"  • MUST Include Keywords: {_safe('alwaysInclude')}"
            )
        if _safe("alwaysAvoid"):
            context_lines.append(
                f"  • MUST AVOID Words: {_safe('alwaysAvoid')}"
            )
        if _safe("descriptionCTA"):
            context_lines.append(
                f"  • Description CTA: End the description with EXACTLY: "
                f"'{_safe('descriptionCTA')}'"
            )
        if config.get("includeHashtags"):
            context_lines.append(
                "  • Add 3-5 relevant hashtags at the very end of the description."
            )
        if config.get("includeEmojis"):
            context_lines.append(
                "  • Use highly relevant emojis in the title and description."
            )
        if _safe("customInstruction"):
            context_lines.append(
                f"  • EXTRA INSTRUCTIONS: {_safe('customInstruction')}"
            )

    context_lines += [
        "",
        "IMPORTANT: The above context OVERRIDES generic assumptions.",
        "Do not produce generic metadata — follow the creator's exact rules.",
        "═══════════════════════════════════════",
    ]

    return BASE_SYSTEM_PROMPT + "\n" + "\n".join(context_lines)


# ---------------------------------------------------------------------------
# GroqGenerator
# ---------------------------------------------------------------------------

class GroqGenerator:
    """
    Wraps the Groq Python SDK to generate YouTube metadata.

    The underlying SDK is synchronous; all calls are offloaded to a thread
    pool executor so they never block the asyncio event loop.
    """

    def __init__(self) -> None:
        api_key = os.environ.get("GROQ_API_KEY")
        if not api_key:
            raise EnvironmentError("GROQ_API_KEY environment variable is not set.")
        self._client = Groq(api_key=api_key)
        logger.info("[Groq] Client initialised with model '%s'.", GROQ_MODEL)

    # ------------------------------------------------------------------
    # Public async interface
    # ------------------------------------------------------------------

    async def generate_metadata(
        self,
        clean_topic: str,
        category:    str | None       = None,
        tags:        list[str] | None = None,
        config:      dict | None      = None,
    ) -> tuple[str, str]:
        """
        Generate a YouTube title and description for the given topic.

        Args:
            clean_topic: Sanitised video topic / filename (no extension).
            category:    Optional content category from the Sync Drive drawer
                         e.g. "gaming", "tutorial".
            tags:        Optional context tags from the Sync Drive drawer
                         e.g. ["Funny", "Walkthrough"].
            config:      Optional aiConfig dict from the AI Generation page
                         (loaded from Firestore by drive_poller, or sent as
                         a validated Pydantic model via the preview endpoint).

        Returns:
            (title, description) as a tuple of strings.

        Raises:
            ValueError:  If clean_topic is empty.
            RuntimeError: If the Groq API call fails or returns malformed JSON.
        """
        if not clean_topic.strip():
            raise ValueError("clean_topic must not be empty.")

        tags = tags or []

        logger.debug(
            "[Groq] Generating metadata | topic='%s' | category=%s | tags=%s | config keys=%s",
            clean_topic,
            category,
            tags,
            list(config.keys()) if config else "None",
        )

        system_prompt = _build_system_prompt(
            category=category,
            tags=tags,
            config=config,
        )

        loop = asyncio.get_event_loop()
        title, description = await loop.run_in_executor(
            None, self._call_groq_sync, clean_topic, system_prompt
        )
        return title, description

    # ------------------------------------------------------------------
    # Synchronous implementation (runs in thread pool)
    # ------------------------------------------------------------------

    def _call_groq_sync(
        self,
        clean_topic:   str,
        system_prompt: str,
    ) -> tuple[str, str]:
        """
        Blocking Groq API call.
        Wrapped inside a thread executor by the async caller.
        Accepts a pre-built system_prompt so all context injection
        happens before hitting the thread pool.
        """
        user_message = (
            f"Generate YouTube metadata for a video about: \"{clean_topic}\"\n\n"
            "Remember: title must be under 100 characters (aim for under 60)."
        )

        try:
            completion = self._client.chat.completions.create(
                model=GROQ_MODEL,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user",   "content": user_message},
                ],
                temperature=GROQ_TEMPERATURE,
                max_tokens=GROQ_MAX_TOKENS,
                response_format={"type": "json_object"},
            )
        except Exception as exc:
            logger.error(
                "[Groq] API request failed for topic '%s': %s", clean_topic, exc
            )
            raise RuntimeError(f"Groq API error: {exc}") from exc

        raw_content: str = completion.choices[0].message.content or ""
        logger.debug("[Groq] Raw response: %s", raw_content)

        return self._parse_response(raw_content, clean_topic)

    # ------------------------------------------------------------------
    # Response parser
    # ------------------------------------------------------------------

    def _parse_response(self, raw_content: str, topic: str) -> tuple[str, str]:
        """
        Parse the JSON response from Groq and validate the output.

        Args:
            raw_content: The raw string returned by the model.
            topic:       The original topic (used in error messages).

        Returns:
            (title, description)

        Raises:
            RuntimeError: If JSON is malformed or required fields are missing.
        """
        # Strip accidental markdown fences just in case the model misbehaves
        cleaned = (
            raw_content.strip()
            .lstrip("```json")
            .lstrip("```")
            .rstrip("```")
            .strip()
        )

        try:
            data: dict = json.loads(cleaned)
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                f"Groq returned invalid JSON for topic '{topic}': {exc}\n"
                f"Raw content: {raw_content!r}"
            ) from exc

        title:       str = str(data.get("title",       "")).strip()
        description: str = str(data.get("description", "")).strip()

        if not title:
            raise RuntimeError(
                f"Groq returned an empty title for topic '{topic}'. "
                f"Full response: {data}"
            )
        if not description:
            raise RuntimeError(
                f"Groq returned an empty description for topic '{topic}'. "
                f"Full response: {data}"
            )

        # Enforce hard title length limit
        if len(title) > 100:
            logger.warning(
                "[Groq] Title exceeded 100 chars (%d); truncating.", len(title)
            )
            title = title[:97].rstrip() + "…"

        logger.info("[Groq] ✓ Title generated: '%s'", title)
        return title, description
