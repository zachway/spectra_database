from ingest import add_star


def test_launch_gaia_job_retries_transient_failure(monkeypatch):
    """A couple of bad responses (the HTML-error-page failure mode seen live
    against Gaia's TAP+ endpoint) shouldn't kill the whole sync — retry
    should clear it once the transient blip passes."""
    monkeypatch.setattr(add_star.time, "sleep", lambda _seconds: None)

    calls = []

    def flaky_launch_job(query):
        calls.append(query)
        if len(calls) < 3:
            raise ValueError("Not a gzipped file (b'<h')")
        return "ok"

    monkeypatch.setattr(add_star.Gaia, "launch_job", flaky_launch_job)

    result = add_star._launch_gaia_job("SELECT 1")

    assert result == "ok"
    assert len(calls) == 3


def test_launch_gaia_job_raises_after_exhausting_attempts(monkeypatch):
    monkeypatch.setattr(add_star.time, "sleep", lambda _seconds: None)

    def always_fails(query):
        raise ValueError("Not a gzipped file (b'<h')")

    monkeypatch.setattr(add_star.Gaia, "launch_job", always_fails)

    try:
        add_star._launch_gaia_job("SELECT 1")
        assert False, "expected _launch_gaia_job to raise"
    except ValueError:
        pass
