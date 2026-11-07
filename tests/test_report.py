from deja import events as ev
from deja.recorder import Recorder
from deja.report import build, write_report


def sample_events():
    return [
        (ev.BATCH, 1), (ev.REQUEST, 0, "r0", "POST", "/purchase", [["content-type", "application/json"]], b'{"a":1}'),
        (ev.BATCH, 1), (ev.STEP, "q0"), (ev.IO_END, "q0", "pg.fetchval", "d", "ok", 100),
        (ev.BATCH, 1), (ev.STEP, "r5"),
        (ev.BATCH, 1), (ev.STEP, "q0"), (ev.IO_END, "q0", "http.send", "d", "ok", {"__t": "httpx", "status": 502}),
        (ev.FLAG, "q0", "balance negative"), (ev.RESPONSE, 0, 500, b"<oops>"),
    ]


def test_report_shows_requests_io_and_flags():
    page = build({"reason": "status 500", "python": "3.14"}, sample_events(), source="x.deja")
    assert "POST" in page and "/purchase" in page
    assert "pg.fetchval &rarr; 100" in page and "HTTP 502" in page
    assert "flag: balance negative" in page
    assert "1 pass without request activity" in page  # the r5-only pass is folded
    assert "&lt;oops&gt;" in page and "<oops>" not in page  # response bodies are escaped
    assert "deja replay x.deja" in page


def test_write_report_next_to_recording(tmp_path):
    rec = Recorder()
    rec.buffer.extend(sample_events())
    path = rec.dump(str(tmp_path / "r.deja"))
    out = write_report(path)
    assert out.endswith("r.html") and "Déjà recording" in open(out, encoding="utf-8").read()
