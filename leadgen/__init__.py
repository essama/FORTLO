"""ICP lead engine. Reads CRM and profile records. Does not write them back."""

__all__ = ["run"]


def run(**kwargs):
    from leadgen.pipeline import run as _run

    return _run(**kwargs)
