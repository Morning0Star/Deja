"""The gallery is Déjà's acceptance suite, so its bugs must be real and its safe variants must be clean.

Buggy variants: at ~24-52% per trial, P(no violation in 60 trials) is below 1e-7, so "at least one" is stable.
"""

import asyncio

import pytest

from gallery import stress

TRIALS = 60


@pytest.mark.parametrize("scenario", list(stress.SCENARIOS))
def test_bug_is_real(scenario):
    hits, _, example = asyncio.run(stress.run(scenario, TRIALS, safe=False, url=None, stagger_ms=60))
    assert hits > 0, f"{scenario}: race never happened in {TRIALS} trials"
    assert hits < TRIALS, f"{scenario}: race happened every time; it should be intermittent"


@pytest.mark.parametrize("scenario", list(stress.SCENARIOS))
def test_safe_variant_is_clean(scenario):
    hits, _, example = asyncio.run(stress.run(scenario, TRIALS, safe=True, url=None, stagger_ms=60))
    assert hits == 0, f"{scenario}: safe variant violated: {example}"
