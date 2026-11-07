# Runtime-only AL2023 userspace (Vercel's build image) for the packed smoke.
# It never builds the binding; the Bullseye image in the parent directory does.
# Both digests identify multi-architecture indexes (x64 and ARM64).
FROM node:24.13.0-bullseye@sha256:8036dbe5b1f465e3acb8b866031cd06e4f84c31b0e83dabbdc59397a40dbe288 AS node
FROM amazonlinux:2023@sha256:065856aabb1ddab0441f4024dfbba116cc760286858a53ec74e6a1218af979ee
COPY --from=node /usr/local/ /usr/local/
# AL2023 pins its package repository release through /etc/dnf/vars/releasever.
RUN dnf install -y libstdc++ && dnf clean all
