"""Run neurosynth-compose meta-analyses."""

from importlib.metadata import PackageNotFoundError, version as _distribution_version

try:
    # hatch-vcs stamps the distribution metadata from the git tag at build
    # time, so this names the release that is actually installed and running.
    __version__ = _distribution_version("compose-runner")
except PackageNotFoundError:
    # Imported from a source tree that was never installed; the build hook's
    # generated file is the next best thing, and may not exist either.
    try:
        from compose_runner._version import __version__
    except ImportError:
        __version__ = "unknown"

__all__ = ["__version__"]
