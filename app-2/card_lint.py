"""
Lark card schema linter.

The `note` bug shipped because nothing checked the card before Lark did. Lark
answers an invalid card with a bare 400 and no field name, so a single wrong
tag anywhere silently downgrades the whole card to the plain-text fallback --
which is exactly what the team saw in chat.

This module encodes the documented card JSON 2.0 and 1.0 schemas as
allowlists and validates a card dict against them. It is pure data, no
network, so the test suite runs it on every card the bot can produce.

Sources (Lark Open Platform, card JSON 2.0):
  - component JSON v2.0 overview     -- the full component/tag list
  - Card JSON 2.0 structure          -- config / header / body fields, limits
  - Column set, Button, Single select dropdown, Table  -- per-component fields
"""

# ---------------------------------------------------------------------------
# Card JSON 2.0
# ---------------------------------------------------------------------------

V2_CONTAINERS = {"column_set", "form", "interactive_container",
                 "collapsible_panel"}
V2_DISPLAY = {"div", "markdown", "img", "img_combination", "person",
              "person_list", "chart", "table", "hr"}
V2_INTERACTIVE = {"input", "button", "overflow", "select_static",
                  "multi_select_static", "select_person", "multi_select_person",
                  "date_picker", "picker_time", "picker_datetime",
                  "select_img", "checker"}
V2_TAGS = V2_CONTAINERS | V2_DISPLAY | V2_INTERACTIVE

# Attributes every 2.0 component may carry.
V2_COMMON = {"tag", "element_id", "margin"}

V2_FIELDS = {
    "markdown": V2_COMMON | {"content", "text_align", "text_size", "icon",
                             "href"},
    "div": V2_COMMON | {"text", "icon"},
    "hr": V2_COMMON,
    "column_set": V2_COMMON | {"horizontal_spacing", "horizontal_align",
                               "flex_mode", "background_style", "action",
                               "columns"},
    "column": {"tag", "element_id", "background_style", "width", "weight",
               "horizontal_spacing", "horizontal_align", "vertical_align",
               "vertical_spacing", "direction", "padding", "margin", "action",
               "elements"},
    "button": V2_COMMON | {"type", "size", "width", "text", "icon",
                           "hover_tips", "disabled", "disabled_tips", "confirm",
                           "behaviors", "name", "form_action_type",
                           "url", "multi_url", "value"},
    "select_static": V2_COMMON | {"type", "name", "required", "disabled",
                                  "initial_option", "placeholder", "width",
                                  "behaviors", "options", "confirm"},
    "table": V2_COMMON | {"page_size", "row_height", "row_max_height",
                          "header_style", "freeze_first_column", "columns",
                          "rows"},
    "img": V2_COMMON | {"img_key", "alt", "title", "corner_radius", "scale_type",
                        "size", "transparent", "preview", "behaviors"},
}

V2_CONFIG = {"streaming_mode", "streaming_config", "summary", "locales",
             "enable_forward", "update_multi", "width_mode",
             "use_custom_translation", "enable_forward_interaction", "style"}
V2_HEADER = {"title", "subtitle", "text_tag_list", "i18n_text_tag_list",
             "template", "icon", "padding"}
V2_BODY = {"direction", "padding", "horizontal_spacing", "horizontal_align",
           "vertical_spacing", "vertical_align", "elements"}

V2_TEMPLATES = {"blue", "wathet", "turquoise", "green", "yellow", "orange",
                "red", "carmine", "violet", "purple", "indigo", "grey",
                "default"}
V2_BUTTON_TYPES = {"default", "primary", "danger", "text", "primary_text",
                   "danger_text", "primary_filled", "danger_filled", "laser"}
V2_BUTTON_SIZES = {"tiny", "small", "medium", "large"}
V2_FLEX_MODES = {"none", "stretch", "flow", "bisect", "trisect"}

# "Up to 200 elements or components for a card" -- Card JSON 2.0 structure.
V2_MAX_COMPONENTS = 200

# ---------------------------------------------------------------------------
# Card JSON 1.0 (the fallback layout)
# ---------------------------------------------------------------------------

V1_TAGS = {"div", "markdown", "hr", "img", "note", "action", "column_set",
           "column", "button", "select_static", "overflow", "date_picker",
           "picker_time", "picker_datetime", "person", "table", "chart"}


def _walk(node, path, out, tags):
    """Collect every component dict in the tree, with its JSON path."""
    if isinstance(node, dict):
        if "tag" in node and node["tag"] in tags or "tag" in node:
            out.append((path, node))
        for key, value in node.items():
            _walk(value, "%s.%s" % (path, key), out, tags)
    elif isinstance(node, list):
        for i, item in enumerate(node):
            _walk(item, "%s[%d]" % (path, i), out, tags)


def lint_v2(card):
    """Return a list of human-readable problems with a card 2.0 dict."""
    problems = []

    if card.get("schema") != "2.0":
        problems.append("schema must be the string '2.0'")

    for key in card:
        if key not in {"schema", "config", "card_link", "header", "body",
                       "i18n_header", "i18n_body"}:
            problems.append("unknown top-level key %r" % key)

    for key in card.get("config", {}):
        if key not in V2_CONFIG:
            problems.append("config.%s is not a card 2.0 config field" % key)

    header = card.get("header", {})
    for key in header:
        if key not in V2_HEADER:
            problems.append("header.%s is not a card 2.0 header field" % key)
    if header and "title" not in header:
        problems.append("header.title is required when a header is present")
    if header.get("template") and header["template"] not in V2_TEMPLATES:
        problems.append("header.template %r is not a Lark template colour"
                        % header["template"])

    body = card.get("body", {})
    for key in body:
        if key not in V2_BODY:
            problems.append("body.%s is not a card 2.0 body field" % key)

    # --- every component in the tree ---------------------------------------
    components = []
    _walk(body.get("elements", []), "body.elements", components, V2_TAGS)

    # Text objects ({"tag": "plain_text"|"lark_md"}) are values, not components.
    text_tags = {"plain_text", "lark_md", "standard_icon", "custom_icon",
                 "text_tag", "column"}
    real = [(p, c) for p, c in components if c.get("tag") not in text_tags]

    for path, comp in real:
        tag = comp.get("tag")
        if tag not in V2_TAGS:
            problems.append(
                "%s: tag %r does not exist in card JSON 2.0 (valid: %s)"
                % (path, tag, ", ".join(sorted(V2_TAGS))))
            continue
        allowed = V2_FIELDS.get(tag)
        if allowed:
            for key in comp:
                if key not in allowed:
                    problems.append("%s: %r is not a field of the %s component"
                                    % (path, key, tag))
        if tag == "button":
            if comp.get("type") and comp["type"] not in V2_BUTTON_TYPES:
                problems.append("%s: button type %r is invalid"
                                % (path, comp["type"]))
            if comp.get("size") and comp["size"] not in V2_BUTTON_SIZES:
                problems.append("%s: button size %r is invalid"
                                % (path, comp["size"]))
            if "value" in comp and "behaviors" not in comp:
                problems.append("%s: card 2.0 buttons carry callbacks in "
                                "'behaviors', not 'value'" % path)
        if tag == "column_set":
            if comp.get("flex_mode") and comp["flex_mode"] not in V2_FLEX_MODES:
                problems.append("%s: flex_mode %r is invalid"
                                % (path, comp["flex_mode"]))
            for i, col in enumerate(comp.get("columns", [])):
                if col.get("tag") != "column":
                    problems.append("%s.columns[%d]: must have tag 'column'"
                                    % (path, i))
                for key in col:
                    if key not in V2_FIELDS["column"]:
                        problems.append("%s.columns[%d]: %r is not a field of "
                                        "the column component" % (path, i, key))
        if tag == "select_static":
            if not comp.get("options"):
                problems.append("%s: select_static needs at least one option"
                                % path)
            for i, opt in enumerate(comp.get("options", [])):
                if "value" not in opt or "text" not in opt:
                    problems.append("%s.options[%d]: needs both text and value"
                                    % (path, i))

    if len(real) > V2_MAX_COMPONENTS:
        problems.append("card has %d components; Lark's limit is %d"
                        % (len(real), V2_MAX_COMPONENTS))

    return problems


def lint_v1(card):
    """Return a list of problems with a card 1.0 dict."""
    problems = []
    if card.get("schema") == "2.0":
        problems.append("card declares schema 2.0 but is being linted as 1.0")
    if "elements" not in card:
        problems.append("card 1.0 needs a top-level 'elements' list")

    components = []
    _walk(card.get("elements", []), "elements", components, V1_TAGS)
    text_tags = {"plain_text", "lark_md", "standard_icon", "custom_icon"}
    for path, comp in components:
        tag = comp.get("tag")
        if tag in text_tags:
            continue
        if tag not in V1_TAGS:
            problems.append("%s: tag %r does not exist in card JSON 1.0"
                            % (path, tag))
    return problems


def assert_valid(card, schema="2.0"):
    """Raise ValueError listing every problem. Used by the tests."""
    problems = lint_v2(card) if schema == "2.0" else lint_v1(card)
    if problems:
        raise ValueError("Invalid Lark card (%s):\n  - %s"
                         % (schema, "\n  - ".join(problems)))
    return True
