import json
import re

from deja import events as ev
from deja.recorder import Recorder
from deja.report import build, write_report


def sample_events():
    return [
        (ev.BATCH, 1, 1_000_000),
        (ev.REQUEST, 1, "r0", "POST", "/buy", [["content-type", "application/json"]], b'{"a":1}', 1_000_000),
        (ev.REQUEST, 2, "r0", "POST", "/buy", [], b'{"a":1}', 1_100_000),
        (ev.TOUCH, "q1", "wallet", "read", 100, 2_000_000),
        (ev.TOUCH, "q2", "wallet", "read", 100, 3_000_000),
        (ev.TOUCH, "q1", "wallet", "write", 0, 4_000_000),
        (ev.TOUCH, "q2", "wallet", "write", 0, 5_000_000),
        (ev.RESPONSE, 1, 200, b"<ok>", 6_000_000),
        (ev.RESPONSE, 2, 200, b"<ok>", 6_500_000),
    ]


def embedded(page):
    m = re.search(r'<script id="data" type="application/json">(.*?)</script>', page, re.S)
    return json.loads(m.group(1))


def test_report_embeds_story_moments_and_requests():
    page = build({"reason": "charged 200", "labels": {"POST /buy": "purchase"}}, sample_events(), source="x.deja")
    data = embedded(page)
    assert "lost update on wallet" in data["story"]
    assert [m["text"] for m in data["moments"]][:2] == ["A reads wallet → 100", "B reads wallet → 100"]
    assert {r["id"] for r in data["requests"]} == {"q1", "q2"}
    assert data["conflicts"][0]["pattern"] == "lost-update"
    assert data["replay"] is None and data["source"] == "x.deja"


def test_embedded_data_cannot_break_out_of_the_script_tag():
    events = sample_events() + [
        (ev.REQUEST, 3, "r0", "GET", "/x</script><script>alert(1)</script>", [], b"", 7_000_000)
    ]
    page = build({}, events, source="x.deja")
    assert "</script><script>alert(1)" not in page
    assert any("alert(1)" in r["path"] for r in embedded(page)["requests"])


def test_write_report_next_to_recording(tmp_path):
    rec = Recorder()
    rec.buffer.extend(sample_events())
    path = rec.dump(str(tmp_path / "r.deja"))
    out = write_report(path, replay={"times": 3, "matched": 3, "summary": "3/3"})
    page = open(out, encoding="utf-8").read()
    assert out.endswith("r.html") and "Déjà" in page and embedded(page)["replay"]["matched"] == 3
