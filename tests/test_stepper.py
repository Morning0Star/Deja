"""deja step: pause before a recorded step and inspect the request that's about to run."""

import fixable_app
from deja.replay import replay
from deja.stepper import Stepper
from test_verify import record


def test_step_to_a_point_and_inspect(tmp_path):
    path = record(tmp_path)
    # step 1: q1 starts; step 2: q0 is about to resume in take(), after its read of the stock
    said, answers = [], iter(["n", "w", "l", "p n + 100", "t", "c"])
    stepper = Stepper(task="q1", ask=lambda prompt: next(answers), say=said.append)
    r = replay(path, fixable_app.make_racy, deadline_s=5, on_step=stepper)
    assert r.followed and r.same_responses, r.summary()
    text = "\n".join(map(str, said))
    assert "q1 is about to run" in text
    assert "fixable_app.py:15 in take" in text  # where it will resume
    assert "'n': 1" in text and "101" in text  # it read stock 1 before the other request wrote
    assert "stock[widget]" in text  # recent accesses


def test_quit_stops_the_replay(tmp_path):
    path = record(tmp_path)
    r = replay(path, fixable_app.make_racy, deadline_s=5,
               on_step=Stepper(ask=lambda prompt: "q", say=lambda *a: None))
    assert r.divergence == "stopped by the user"
