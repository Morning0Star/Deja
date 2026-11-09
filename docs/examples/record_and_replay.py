"""Record a race in a test, replay it, and check whether it is fixed."""

import asyncio

import httpx

import deja
from deja.replay import replay
from deja.verify import verify

from wallet import create_app


async def two_purchases(directory: str) -> list[str]:
    app = create_app(directory)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        await asyncio.gather(client.post("/purchase"), client.post("/purchase"))
    await asyncio.sleep(0.1)  # recordings are written off the event loop
    return asyncio.get_running_loop().deja.dumps


def main(directory: str = "deja-recordings") -> str:
    # 1. Record: run the requests on the recording loop.
    #    The second purchase calls deja.flag(), which saves a recording.
    saved = asyncio.run(two_purchases(directory), loop_factory=deja.recording_loop)
    recording = saved[0]

    # 2. Replay: the same interleaving, the same answers.
    result = replay(recording, lambda: create_app(directory + "/replay"))
    assert result.followed and result.same_responses, result.summary()

    # 3. Verify: against this (unfixed) code the race is still there.
    verdict = verify(recording, lambda: create_app(directory + "/replay"), times=3)
    assert not verdict.fixed, verdict.summary()
    return recording


if __name__ == "__main__":
    print(main())
