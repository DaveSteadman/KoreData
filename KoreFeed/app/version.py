# ====================================================================================================
# MARK: OVERVIEW
# ====================================================================================================
# Single source of truth for the MiniFeed version string.
#
# Versioning scheme: [build / release]
#
# - Build number is a simple forever incrementing integer.
#   0001        - incrementing build number
#
# - Release is a human-sensible version number: a tagged release (X.Y) or development version (X.Y+dev).
#   0.1        - tagged release
#   0.1+dev    - active development after that release
#   0.2        - next tagged release
#   1.0-rc1    - release candidate
#
# Bump __version__ to X.Y+dev immediately after tagging a release,
# and to X.Y (no suffix) just before tagging the next one.
# Bump build number on any code change.
# ====================================================================================================

__version__ = "[0005 / 0.1+dev]"
