from collections import Counter
import re

from bs4 import BeautifulSoup, Comment


BLOCKED_TAGS = {
    "script",
    "style",
    "title",
    "iframe",
    "form",
    "object",
    "embed",
    "link",
    "meta",
}

TRACKING_STYLE_MARKERS = (
    "width:1px",
    "height:1px",
    "position:absolute",
    "left:-",
    "top:-",
)


def _attr_text(value) -> str:
    if isinstance(value, (list, tuple)):
        return " ".join(str(v) for v in value)
    return str(value or "")


def _extract_px_number(value) -> int | None:
    if value is None:
        return None
    match = re.search(r"(\d+)", str(value))
    return int(match.group(1)) if match else None


def _is_tracking_style(style_text: str) -> bool:
    normalized = style_text.lower().replace(" ", "")
    return all(marker in normalized for marker in ("width:1px", "height:1px")) and any(
        marker in normalized for marker in TRACKING_STYLE_MARKERS
    )


def _is_tiny_tracking_image(tag) -> bool:
    width = _extract_px_number(tag.get("width"))
    height = _extract_px_number(tag.get("height"))
    if width is not None and height is not None:
        return width <= 3 and height <= 3

    style_text = _attr_text(tag.attrs.get("style", ""))
    if style_text and _is_tracking_style(style_text):
        return True

    return False


def _fragment_html(soup: BeautifulSoup) -> str:
    return "".join(str(node) for node in soup.contents)


def sanitize_detail_html(raw_html: str) -> tuple[str, dict]:
    """Clean wholesaler HTML into a safer fragment for Naver detailContent."""
    html = (raw_html or "").strip()
    if not html:
        return "", {}

    soup = BeautifulSoup(html, "html.parser")
    stats = Counter()

    for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
        comment.extract()
        stats["removed_comments"] += 1

    for tag in soup.find_all(BLOCKED_TAGS):
        stats[f"removed_tag_{tag.name}"] += 1
        tag.decompose()

    for tag in list(soup.find_all(True)):
        if not tag.parent:
            continue

        if tag.name == "a":
            stats["unwrapped_anchor"] += 1
            tag.unwrap()
            continue

        if tag.name == "img" and _is_tiny_tracking_image(tag):
            tag.decompose()
            stats["removed_tiny_tracking_img"] += 1
            continue

        remove_tag = False
        for attr_name in list(tag.attrs.keys()):
            lower = attr_name.lower()
            attr_text = _attr_text(tag.attrs.get(attr_name))

            if lower.startswith("on"):
                del tag.attrs[attr_name]
                stats["removed_event_attr"] += 1
                continue

            if lower == "href":
                del tag.attrs[attr_name]
                stats["removed_href_attr"] += 1
                continue

            if lower == "style":
                if tag.name == "img" and _is_tracking_style(attr_text):
                    remove_tag = True
                    stats["removed_tracking_style_img"] += 1
                    break
                if "expression(" in attr_text.lower():
                    del tag.attrs[attr_name]
                    stats["removed_style_attr"] += 1
                continue

            if tag.name == "img" and lower == "src":
                src = attr_text.strip()
                src_lower = src.lower()
                if not src:
                    remove_tag = True
                    stats["removed_img_without_src"] += 1
                    break
                if src_lower.startswith("data:image/"):
                    remove_tag = True
                    stats["removed_data_image"] += 1
                    break
                if src_lower.startswith("javascript:"):
                    remove_tag = True
                    stats["removed_javascript_image"] += 1
                    break

        if remove_tag and tag.parent:
            tag.decompose()

    cleaned = _fragment_html(soup)
    cleaned = re.sub(r"\n\s*\n\s*\n+", "\n\n", cleaned).strip()
    return cleaned, dict(sorted(stats.items()))
