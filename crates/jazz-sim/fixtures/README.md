# jazz-sim fixtures

Large trace fixtures are stored as public GitHub Release assets instead of being
committed to git:

https://github.com/garden-co/jazz/releases/tag/jazz-sim-fixtures-v1

That release is only a storage vessel for reproducible benchmark inputs. The
benchmark code records the asset URLs and expected sha256 hashes, downloads a
missing fixture on demand, and verifies the bytes before parsing.
