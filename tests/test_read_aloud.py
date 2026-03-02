"""Tests for the Read Aloud player controls.

Validates HTML structure, JS state machine logic, and paragraph discoverability
for each section that has a .ra-bar. Uses stdlib html.parser to parse the
dashboard HTML and regex to validate the JavaScript behavior.
"""

import re
from html.parser import HTMLParser
from pathlib import Path

import pytest

HTML_PATH = Path("src/web/index.html")


# ── HTML parsing helpers ─────────────────────────────────────────────────────

class RaBarParser(HTMLParser):
    """Extract all .ra-bar elements and their child controls from HTML."""

    def __init__(self):
        super().__init__()
        self.bars = []           # list of dicts, one per ra-bar
        self._in_bar = False
        self._current_bar = None
        self._depth = 0          # track nesting depth within a bar
        self._section_id = None  # nearest analysis-body id
        self._pending_section_id = None

    def handle_starttag(self, tag, attrs):
        attr_dict = dict(attrs)
        classes = attr_dict.get("class", "").split()

        # Track the nearest .analysis-body id for context
        if "analysis-body" in classes:
            self._pending_section_id = attr_dict.get("id", "unknown")

        if "ra-bar" in classes:
            self._in_bar = True
            self._depth = 1
            self._current_bar = {
                "section_id": self._pending_section_id,
                "buttons": [],
                "selects": [],
                "spans": [],
                "button_classes": [],
                "select_classes": [],
                "span_classes": [],
                "disabled_buttons": [],
            }
            return

        if self._in_bar:
            self._depth += 1

            if tag == "button":
                self._current_bar["buttons"].append(attr_dict)
                self._current_bar["button_classes"].append(classes)
                if "disabled" in attr_dict:
                    self._current_bar["disabled_buttons"].append(classes)

            elif tag == "select":
                self._current_bar["selects"].append(attr_dict)
                self._current_bar["select_classes"].append(classes)

            elif tag == "span":
                self._current_bar["spans"].append(attr_dict)
                self._current_bar["span_classes"].append(classes)

    def handle_endtag(self, tag):
        if self._in_bar:
            self._depth -= 1
            if self._depth == 0:
                self._in_bar = False
                self.bars.append(self._current_bar)
                self._current_bar = None


class SectionContentParser(HTMLParser):
    """Extract readable content elements from each .analysis-body section.

    Mirrors the getParagraphs() JS function to count discoverable paragraphs.
    """

    READABLE_TAGS = {"p", "h2", "h3", "h4", "li", "td"}
    EXCLUDED_PARENTS = {"ra-bar", "read-aloud-bar", "footnote", "chart-card",
                        "pipeline-svg-wrap"}

    def __init__(self):
        super().__init__()
        self.sections = {}       # section_id -> list of text chunks
        self._in_section = False
        self._section_id = None
        self._section_depth = 0
        self._ancestor_classes = []  # stack of class sets for ancestor elements
        self._capture_text = False
        self._current_text = ""

    def handle_starttag(self, tag, attrs):
        attr_dict = dict(attrs)
        classes = set(attr_dict.get("class", "").split())
        self._ancestor_classes.append(classes)

        if "analysis-body" in classes:
            self._in_section = True
            self._section_id = attr_dict.get("id", "unknown")
            self._section_depth = 1
            self.sections[self._section_id] = []
            return

        if self._in_section:
            self._section_depth += 1

            # Check if we're inside an excluded parent
            excluded = any(
                cls in ancestor
                for ancestor in self._ancestor_classes
                for cls in self.EXCLUDED_PARENTS
            )

            if not excluded and tag in self.READABLE_TAGS:
                self._capture_text = True
                self._current_text = ""

    def handle_data(self, data):
        if self._capture_text:
            self._current_text += data

    def handle_endtag(self, tag):
        if self._capture_text and tag in self.READABLE_TAGS:
            text = self._current_text.strip()
            if text and len(text) > 2:
                self.sections.get(self._section_id, []).append(text)
            self._capture_text = False
            self._current_text = ""

        if self._ancestor_classes:
            self._ancestor_classes.pop()

        if self._in_section:
            self._section_depth -= 1
            if self._section_depth == 0:
                self._in_section = False


# ── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def html_content():
    return HTML_PATH.read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def ra_bars(html_content):
    parser = RaBarParser()
    parser.feed(html_content)
    return parser.bars


@pytest.fixture(scope="module")
def section_content(html_content):
    parser = SectionContentParser()
    parser.feed(html_content)
    return parser.sections


@pytest.fixture(scope="module")
def js_source(html_content):
    """Extract the Read Aloud IIFE JavaScript from the HTML."""
    # Match the IIFE from "// ── Read Aloud" to the closing "})();"
    match = re.search(
        r'// ── Read Aloud \(Web Speech API\).*?^\}\)\(\);',
        html_content,
        re.DOTALL | re.MULTILINE,
    )
    assert match, "Could not find Read Aloud IIFE in HTML"
    return match.group(0)


# ── HTML Structure Tests ─────────────────────────────────────────────────────

class TestRaBarCount:
    """Verify the expected number of ra-bars exist."""

    def test_has_ra_bars(self, ra_bars):
        assert len(ra_bars) >= 8, f"Expected at least 8 ra-bars, found {len(ra_bars)}"

    def test_each_bar_has_section(self, ra_bars):
        for i, bar in enumerate(ra_bars):
            assert bar["section_id"] is not None, (
                f"ra-bar #{i} is not inside an .analysis-body"
            )


class TestRaBarControls:
    """Every ra-bar must have the full set of player controls."""

    REQUIRED_BUTTON_CLASSES = [
        "ra-play", "ra-pause", "ra-stop", "ra-prev", "ra-next",
    ]
    REQUIRED_SELECT_CLASSES = ["ra-speed", "ra-voice"]

    def _find_class(self, class_lists, target):
        return any(target in classes for classes in class_lists)

    def test_each_bar_has_play_button(self, ra_bars):
        for i, bar in enumerate(ra_bars):
            assert self._find_class(bar["button_classes"], "ra-play"), (
                f"ra-bar #{i} ({bar['section_id']}) missing play button"
            )

    def test_each_bar_has_pause_button(self, ra_bars):
        for i, bar in enumerate(ra_bars):
            assert self._find_class(bar["button_classes"], "ra-pause"), (
                f"ra-bar #{i} ({bar['section_id']}) missing pause button"
            )

    def test_each_bar_has_stop_button(self, ra_bars):
        for i, bar in enumerate(ra_bars):
            assert self._find_class(bar["button_classes"], "ra-stop"), (
                f"ra-bar #{i} ({bar['section_id']}) missing stop button"
            )

    def test_each_bar_has_prev_button(self, ra_bars):
        for i, bar in enumerate(ra_bars):
            assert self._find_class(bar["button_classes"], "ra-prev"), (
                f"ra-bar #{i} ({bar['section_id']}) missing prev button"
            )

    def test_each_bar_has_next_button(self, ra_bars):
        for i, bar in enumerate(ra_bars):
            assert self._find_class(bar["button_classes"], "ra-next"), (
                f"ra-bar #{i} ({bar['section_id']}) missing next button"
            )

    def test_each_bar_has_speed_select(self, ra_bars):
        for i, bar in enumerate(ra_bars):
            assert self._find_class(bar["select_classes"], "ra-speed"), (
                f"ra-bar #{i} ({bar['section_id']}) missing speed select"
            )

    def test_each_bar_has_voice_select(self, ra_bars):
        for i, bar in enumerate(ra_bars):
            assert self._find_class(bar["select_classes"], "ra-voice"), (
                f"ra-bar #{i} ({bar['section_id']}) missing voice select"
            )

    def test_each_bar_has_status_span(self, ra_bars):
        for i, bar in enumerate(ra_bars):
            assert self._find_class(bar["span_classes"], "ra-status"), (
                f"ra-bar #{i} ({bar['section_id']}) missing status span"
            )

    def test_all_controls_present_per_bar(self, ra_bars):
        """Summary check: every bar has all 5 buttons + 2 selects + status."""
        for i, bar in enumerate(ra_bars):
            sid = bar["section_id"]
            for cls in self.REQUIRED_BUTTON_CLASSES:
                assert self._find_class(bar["button_classes"], cls), (
                    f"ra-bar #{i} ({sid}) missing button .{cls}"
                )
            for cls in self.REQUIRED_SELECT_CLASSES:
                assert self._find_class(bar["select_classes"], cls), (
                    f"ra-bar #{i} ({sid}) missing select .{cls}"
                )


class TestSkipButtonsNotDisabled:
    """Design hole fix: prev/next must NOT be disabled in initial HTML."""

    def test_prev_not_disabled_in_html(self, ra_bars):
        for i, bar in enumerate(ra_bars):
            for btn_classes in bar["disabled_buttons"]:
                assert "ra-prev" not in btn_classes, (
                    f"ra-bar #{i} ({bar['section_id']}): "
                    f"ra-prev should not have disabled attribute in HTML"
                )

    def test_next_not_disabled_in_html(self, ra_bars):
        for i, bar in enumerate(ra_bars):
            for btn_classes in bar["disabled_buttons"]:
                assert "ra-next" not in btn_classes, (
                    f"ra-bar #{i} ({bar['section_id']}): "
                    f"ra-next should not have disabled attribute in HTML"
                )

    def test_pause_disabled_initially(self, ra_bars):
        """Pause should be disabled by default (nothing to pause)."""
        for i, bar in enumerate(ra_bars):
            has_disabled_pause = any(
                "ra-pause" in cls for cls in bar["disabled_buttons"]
            )
            assert has_disabled_pause, (
                f"ra-bar #{i} ({bar['section_id']}): "
                f"ra-pause should be disabled initially"
            )

    def test_stop_disabled_initially(self, ra_bars):
        """Stop should be disabled by default (nothing to stop)."""
        for i, bar in enumerate(ra_bars):
            has_disabled_stop = any(
                "ra-stop" in cls for cls in bar["disabled_buttons"]
            )
            assert has_disabled_stop, (
                f"ra-bar #{i} ({bar['section_id']}): "
                f"ra-stop should be disabled initially"
            )


class TestSpeedOptions:
    """Speed select must offer the standard playback rates."""

    EXPECTED_SPEEDS = ["0.75", "1", "1.25", "1.5", "2"]

    def test_speed_options(self, html_content):
        # Extract all <option> values from the first ra-speed select
        pattern = r'<select[^>]*class="[^"]*ra-speed[^"]*"[^>]*>(.*?)</select>'
        match = re.search(pattern, html_content, re.DOTALL)
        assert match, "Could not find ra-speed select"

        values = re.findall(r'value="([^"]+)"', match.group(1))
        assert values == self.EXPECTED_SPEEDS, (
            f"Expected speeds {self.EXPECTED_SPEEDS}, got {values}"
        )

    def test_default_speed_is_1x(self, html_content):
        pattern = r'<select[^>]*class="[^"]*ra-speed[^"]*"[^>]*>(.*?)</select>'
        match = re.search(pattern, html_content, re.DOTALL)
        assert match
        # Find the option with "selected" — attribute may appear before or after value
        selected = re.findall(
            r'<option[^>]*value="([^"]+)"[^>]*selected', match.group(1)
        )
        assert selected == ["1"], f"Default speed should be 1x, got {selected}"


# ── JavaScript State Machine Tests ───────────────────────────────────────────

class TestJsEnsureParagraphs:
    """Verify the ensureParagraphs() function exists and implements lazy discovery."""

    def test_ensure_paragraphs_exists(self, js_source):
        assert "function ensureParagraphs" in js_source

    def test_calls_get_paragraphs(self, js_source):
        # ensureParagraphs should call getParagraphs to discover content
        fn = re.search(
            r'function ensureParagraphs.*?^    \}',
            js_source,
            re.DOTALL | re.MULTILINE,
        )
        assert fn, "Could not extract ensureParagraphs function"
        body = fn.group(0)
        assert "getParagraphs" in body, (
            "ensureParagraphs must call getParagraphs for lazy discovery"
        )

    def test_stops_other_bar_if_switching(self, js_source):
        fn = re.search(
            r'function ensureParagraphs.*?^    \}',
            js_source,
            re.DOTALL | re.MULTILINE,
        )
        body = fn.group(0)
        assert "fullStop" in body, (
            "ensureParagraphs must call fullStop when switching bars"
        )


class TestJsSkipHandlers:
    """Verify prev/next handlers use ensureParagraphs for skip-before-play."""

    def test_prev_calls_ensure_paragraphs(self, js_source):
        # Find the prevBtn click handler
        match = re.search(
            r"prevBtn\.addEventListener\('click'.*?\}\);",
            js_source,
            re.DOTALL,
        )
        assert match, "Could not find prevBtn click handler"
        handler = match.group(0)
        assert "ensureParagraphs" in handler, (
            "prevBtn handler must call ensureParagraphs for skip-before-play"
        )

    def test_next_calls_ensure_paragraphs(self, js_source):
        match = re.search(
            r"nextBtn\.addEventListener\('click'.*?\}\);",
            js_source,
            re.DOTALL,
        )
        assert match, "Could not find nextBtn click handler"
        handler = match.group(0)
        assert "ensureParagraphs" in handler, (
            "nextBtn handler must call ensureParagraphs for skip-before-play"
        )

    def test_next_bounds_check(self, js_source):
        """Next should clamp to last paragraph, not overflow."""
        match = re.search(
            r"nextBtn\.addEventListener\('click'.*?\}\);",
            js_source,
            re.DOTALL,
        )
        handler = match.group(0)
        assert "Math.min" in handler or "paragraphs.length - 1" in handler, (
            "nextBtn must clamp index to prevent overflow past last paragraph"
        )

    def test_prev_bounds_check(self, js_source):
        """Prev should clamp to 0, not go negative."""
        match = re.search(
            r"prevBtn\.addEventListener\('click'.*?\}\);",
            js_source,
            re.DOTALL,
        )
        handler = match.group(0)
        assert "Math.max" in handler or "0" in handler, (
            "prevBtn must clamp index to prevent negative index"
        )


class TestJsSkipButtonsAlwaysEnabled:
    """Verify the JS never disables skip buttons during normal operation."""

    def test_update_ui_does_not_disable_skip(self, js_source):
        """updateUI should not set prev/next to disabled."""
        fn = re.search(
            r'function updateUI.*?^    \}',
            js_source,
            re.DOTALL | re.MULTILINE,
        )
        assert fn, "Could not extract updateUI function"
        body = fn.group(0)
        assert "prevBtn.disabled" not in body, (
            "updateUI should not touch prevBtn.disabled"
        )
        assert "nextBtn.disabled" not in body, (
            "updateUI should not touch nextBtn.disabled"
        )

    def test_reset_all_bars_does_not_disable_skip(self, js_source):
        """resetAllBars should not disable prev/next."""
        fn = re.search(
            r'function resetAllBars.*?^    \}',
            js_source,
            re.DOTALL | re.MULTILINE,
        )
        assert fn, "Could not extract resetAllBars function"
        body = fn.group(0)
        assert "ra-prev" not in body, (
            "resetAllBars should not reference ra-prev"
        )
        assert "ra-next" not in body, (
            "resetAllBars should not reference ra-next"
        )


class TestJsPlaybackStates:
    """Verify the JS state machine has correct state transitions."""

    def test_play_sets_speaking_true(self, js_source):
        fn = re.search(
            r'function speakIndex.*?^    \}',
            js_source,
            re.DOTALL | re.MULTILINE,
        )
        assert fn
        body = fn.group(0)
        assert "speaking = true" in body

    def test_play_sets_paused_false(self, js_source):
        fn = re.search(
            r'function speakIndex.*?^    \}',
            js_source,
            re.DOTALL | re.MULTILINE,
        )
        body = fn.group(0)
        assert "paused = false" in body

    def test_fullstop_resets_state(self, js_source):
        fn = re.search(
            r'function fullStop.*?^    \}',
            js_source,
            re.DOTALL | re.MULTILINE,
        )
        assert fn
        body = fn.group(0)
        assert "speaking = false" in body
        assert "paused = false" in body
        assert "paragraphs = []" in body
        assert "activeBar = null" in body

    def test_pause_handler_sets_paused(self, js_source):
        match = re.search(
            r"pauseBtn\.addEventListener\('click'.*?\}\);",
            js_source,
            re.DOTALL,
        )
        assert match
        handler = match.group(0)
        assert "paused = true" in handler

    def test_play_handler_resumes_from_pause(self, js_source):
        match = re.search(
            r"playBtn\.addEventListener\('click'.*?\}\);",
            js_source,
            re.DOTALL,
        )
        assert match
        handler = match.group(0)
        assert "speechSynthesis.resume" in handler


class TestJsVoiceScoring:
    """Verify the voice quality scoring algorithm."""

    def test_quality_keywords_exist(self, js_source):
        assert "QUALITY_KEYWORDS" in js_source

    def test_scores_english_voices_higher(self, js_source):
        fn = re.search(
            r'function scoreVoice.*?^    \}',
            js_source,
            re.DOTALL | re.MULTILINE,
        )
        assert fn
        body = fn.group(0)
        assert "lang.startsWith('en')" in body

    def test_filters_to_english(self, js_source):
        assert "filter(v => v.lang.startsWith('en'))" in js_source


class TestJsSpeedSync:
    """Verify speed changes sync across all bars and restart current paragraph."""

    def test_speed_change_restarts_paragraph(self, js_source):
        # Use greedy match to capture the full handler (contains nested });)
        match = re.search(
            r"speedSel\.addEventListener\('change'.*?speakIndex.*?\}\);",
            js_source,
            re.DOTALL,
        )
        assert match, "Speed change handler should call speakIndex to restart"
        handler = match.group(0)
        assert "speakIndex(currentIdx)" in handler

    def test_speed_syncs_globally(self, js_source):
        match = re.search(
            r"speedSel\.addEventListener\('change'.*?speakIndex.*?\}\);",
            js_source,
            re.DOTALL,
        )
        assert match
        handler = match.group(0)
        assert "querySelectorAll('.ra-speed')" in handler


# ── Paragraph Discovery Tests ────────────────────────────────────────────────

class TestParagraphDiscovery:
    """Verify each section has discoverable readable content."""

    EXPECTED_SECTIONS_WITH_BARS = [
        "analysis-body",        # Overview / main analysis
        "burden-body",          # Who Bears the Burden
        "revenue-body",         # Revenue Impact by Income Segment
        "lawprocess-body",      # How Tax Law Is Changed
        "currentlaw-body",      # Current Law
        "proposed-body",        # Proposed Statutory Language
        "architecture-body",    # Architecture & Methodology
        "assumptions-body",     # Model Assumptions
        "relevant-data-body",   # Relevant Data
        "glossary-body",        # Glossary
    ]

    def test_all_sections_have_content(self, section_content):
        """Every section with an ra-bar should have at least 1 readable paragraph."""
        for section_id in self.EXPECTED_SECTIONS_WITH_BARS:
            if section_id in section_content:
                paragraphs = section_content[section_id]
                assert len(paragraphs) > 0, (
                    f"Section '{section_id}' has an ra-bar but no readable paragraphs"
                )

    def test_sections_have_meaningful_content(self, section_content):
        """Paragraphs should have substantive text (not just labels or numbers)."""
        for section_id, paragraphs in section_content.items():
            if not paragraphs:
                continue
            # At least one paragraph should be a real sentence (> 30 chars)
            long_paragraphs = [p for p in paragraphs if len(p) > 30]
            assert len(long_paragraphs) > 0, (
                f"Section '{section_id}' has {len(paragraphs)} paragraphs "
                f"but none longer than 30 chars"
            )

    def test_paragraph_count_known_before_play(self, section_content):
        """Verify the paragraph count is deterministic from the HTML structure.

        This validates the design fix: paragraph discovery (via getParagraphs
        or ensureParagraphs) should be callable at any time, not only after
        pressing play. The content is in the DOM — we just need to read it.
        """
        total_sections = 0
        for section_id in self.EXPECTED_SECTIONS_WITH_BARS:
            if section_id in section_content:
                count = len(section_content[section_id])
                assert count > 0, (
                    f"Section '{section_id}': paragraph count should be "
                    f"deterministic from HTML (got {count})"
                )
                total_sections += 1
        assert total_sections >= 8, (
            f"Expected at least 8 sections with content, found {total_sections}"
        )


class TestJsGetParagraphsSelector:
    """Verify the getParagraphs selector is consistent with actual content."""

    def test_selector_includes_p_tags(self, js_source):
        fn = re.search(
            r'function getParagraphs.*?^    \}',
            js_source,
            re.DOTALL | re.MULTILINE,
        )
        assert fn
        body = fn.group(0)
        assert "'p," in body or '"p,' in body or "p," in body

    def test_excludes_ra_bar_content(self, js_source):
        fn = re.search(
            r'function getParagraphs.*?^    \}',
            js_source,
            re.DOTALL | re.MULTILINE,
        )
        body = fn.group(0)
        assert ".ra-bar" in body or "read-aloud-bar" in body, (
            "getParagraphs must exclude ra-bar content from readable text"
        )

    def test_excludes_chart_cards(self, js_source):
        fn = re.search(
            r'function getParagraphs.*?^    \}',
            js_source,
            re.DOTALL | re.MULTILINE,
        )
        body = fn.group(0)
        assert ".chart-card" in body, (
            "getParagraphs must exclude chart-card content"
        )

    def test_minimum_length_filter(self, js_source):
        """getParagraphs should skip very short text fragments."""
        fn = re.search(
            r'function getParagraphs.*?^    \}',
            js_source,
            re.DOTALL | re.MULTILINE,
        )
        body = fn.group(0)
        assert "length > 2" in body or "length >= 3" in body, (
            "getParagraphs should filter out text fragments <= 2 chars"
        )


# ── Docs sync test ───────────────────────────────────────────────────────────

class TestDocsSync:
    """Verify docs/index.html is synced with src/web/index.html."""

    def test_docs_html_matches_src(self):
        src = Path("src/web/index.html").read_text(encoding="utf-8")
        docs = Path("docs/index.html").read_text(encoding="utf-8")
        assert src == docs, (
            "docs/index.html is out of sync with src/web/index.html"
        )
