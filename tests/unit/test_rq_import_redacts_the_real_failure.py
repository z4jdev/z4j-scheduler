"""The guard has to cover the call that actually connects.

``Redis.from_url`` and the rq ``Scheduler`` constructor perform no I/O: redis-py
connects lazily. The importer's try/except wrapped only those two, so no
connection failure could reach it. Two things followed.

The RESP3 hint added for a pre-6.0 server paired with redis-py 6+ was
unreachable, which is how the sweep found this. The more serious one is that the
redaction was unreachable too: the block exists so a refused connection, a DNS
failure or an auth failure does not put the Redis URL, password included, into a
traceback someone pastes into a public issue. The driver's own message does
exactly that, and it was raised from the iteration below the guard.
"""

from __future__ import annotations

import pytest

# The importer is an optional surface.  Keep the base scheduler suite green
# without silently pretending these are base-install tests, and let the
# dedicated ``z4j-scheduler[rq-import]`` lane exercise the whole module.
pytest.importorskip("redis", reason="requires z4j-scheduler[rq-import]")
pytest.importorskip("rq_scheduler", reason="requires z4j-scheduler[rq-import]")

PASSWORD = "hunter2-do-not-leak"
URL = f"redis://:{PASSWORD}@127.0.0.1:6399/0"


def test_constructing_a_client_performs_no_io() -> None:
    """The premise. If this ever fails, the guard placement can be revisited."""
    from redis import Redis

    Redis.from_url(URL)  # a dead port, and yet no exception


def test_a_dead_server_is_reported_without_the_password() -> None:
    """The failure the guard exists for, through the shipped entry point."""
    from z4j_scheduler.importers.rq import read_rq_scheduler

    with pytest.raises(RuntimeError) as caught:
        read_rq_scheduler(redis_url=URL, project_slug="p")

    message = str(caught.value)
    assert PASSWORD not in message, "the Redis password reached the error message"
    assert "could not connect to Redis" in message


def test_the_password_is_absent_from_the_whole_chain() -> None:
    """``from None`` is what keeps the driver's message out of the traceback.

    Asserting on the raised exception alone would pass even if the original
    were chained, because the leak would sit in ``__cause__`` and print anyway.
    """
    from z4j_scheduler.importers.rq import read_rq_scheduler

    with pytest.raises(RuntimeError) as caught:
        read_rq_scheduler(redis_url=URL, project_slug="p")

    current: BaseException | None = caught.value
    seen = 0
    while current is not None:
        assert PASSWORD not in str(current), f"password leaked at depth {seen}"
        current = current.__cause__ or current.__context__
        seen += 1
        if seen > 10:
            break


def test_a_job_decoding_failure_is_not_reported_as_a_connection_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Pulling the iteration inside the guard made the redaction reachable.

    It also put every job-decoding failure under a message blaming the
    connection. rq raises ValueError from its own ``restore`` on an unknown job
    status, and the operator was then told "could not connect to Redis" about a
    broker they were plainly connected to, with ``from None`` suppressing the
    real cause.
    """
    import z4j_scheduler.importers.rq as importer

    class FakeScheduler:
        def __init__(self, connection=None, **_kw) -> None: ...

        def get_jobs(self, *, with_times=False):
            assert with_times is True
            message = "'canceled' is not a valid JobStatus"
            raise ValueError(message)
            yield  # pragma: no cover - generator shape only

    def fake_from_url(_url):
        return object()

    from inspect import getattr_static

    import rq_scheduler
    from redis import Redis

    # MonkeyPatch records the raw class dictionary entry, so teardown restores
    # Redis.from_url as a classmethod descriptor rather than a bound method.
    original_from_url = getattr_static(Redis, "from_url")
    with monkeypatch.context() as patcher:
        patcher.setattr(Redis, "from_url", staticmethod(fake_from_url))
        patcher.setattr(rq_scheduler, "Scheduler", FakeScheduler)
        with pytest.raises(RuntimeError) as caught:
            importer.read_rq_scheduler(redis_url=URL, project_slug="p")
    assert getattr_static(Redis, "from_url") is original_from_url

    message = str(caught.value)
    assert "could not connect" not in message, (
        "a decoding failure is being reported as a connection failure"
    )
    assert "could not read the rq-scheduler jobs" in message
    assert PASSWORD not in message
    # The cause is kept for this class, unlike the connection case where the
    # driver's message embeds the URL and is deliberately chopped.
    assert isinstance(caught.value.__cause__, ValueError)
    assert "JobStatus" in str(caught.value.__cause__)
