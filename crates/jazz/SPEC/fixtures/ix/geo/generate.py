#!/usr/bin/env python3
"""Independent reference for SPEC 20.10.3 (encoding jazz-ix-geo-z62/1).

Writes points-v1.json beside this file. It is written from the SPEC text,
not from the Rust code:

- cells by exact rational arithmetic: u = floor((lng + 180) / 360 * 2^31),
  v = floor((lat + 90) / 180 * 2^31), clamped to 2^31 - 1, u in the even
  bits and v in the odd bits;
- unit vectors with the SPEC's trigonometry (exact reduction in degrees,
  one rounded conversion to radians, fdlibm's sin/cos kernels in binary64),
  each component checked against mpmath to within 2 ulp of 1;
- chord thresholds (2 sin(m / 2R))^2 the same way.

Points include, besides named places and edge values, the doubles nearest
to grid boundaries and their neighbours: there a rounded evaluation of the
quantization (in either association) can differ from the exact floor.

Run: python3 -I generate.py   (needs mpmath)
"""
import json
import math
import random
import struct
from fractions import Fraction
from pathlib import Path

import mpmath

mpmath.mp.prec = 200
STEPS = 1 << 31
R = 6371008.8


def bits(x: float) -> str:
    return "%016x" % struct.unpack(">Q", struct.pack(">d", x))[0]


def from_bits(h: int) -> float:
    return struct.unpack(">d", struct.pack(">Q", h))[0]


# fdlibm's kernel coefficients, as published (s_sin.c / k_sin.c / k_cos.c).
S1, S2, S3, S4, S5, S6 = (
    from_bits(0xBFC5555555555549), from_bits(0x3F8111111110F8A6),
    from_bits(0xBF2A01A019C161D5), from_bits(0x3EC71DE357B1FE7D),
    from_bits(0xBE5AE5E68A2B9CEB), from_bits(0x3DE5D93A5ACFD57C))
C1, C2, C3, C4, C5, C6 = (
    from_bits(0x3FA555555555554C), from_bits(0xBF56C16C16C15177),
    from_bits(0x3EFA01A019CB1590), from_bits(0xBE927E4F809C52AD),
    from_bits(0x3E21EE9EBDB4B1C4), from_bits(0xBDA8FAE9BE8838D4))
DEG_TO_RAD = math.pi / 180.0


def kernel_sin(x):
    z = x * x
    v = z * x
    r = S2 + z * (S3 + z * (S4 + z * (S5 + z * S6)))
    return x + v * (S1 + z * r)


def kernel_cos(x):
    z = x * x
    w = z * z
    r = z * (C1 + z * (C2 + z * C3)) + w * w * (C4 + z * (C5 + z * C6))
    hz = 0.5 * z
    w = 1.0 - hz
    return w + (((1.0 - w) - hz) + z * r)


def sin_cos_deg(d):
    # Reduce to [-45, 45] by an exact multiple of 90.
    if d > 135.0:
        q, r = 2, d - 180.0
    elif d > 45.0:
        q, r = 1, d - 90.0
    elif d >= -45.0:
        q, r = 0, d
    elif d >= -135.0:
        q, r = 3, d + 90.0
    else:
        q, r = 2, d + 180.0
    assert Fraction(r) == Fraction(d) - 90 * {0: 0, 1: 1, 2: (2 if d > 0 else -2), 3: -1}[q]
    x = r * DEG_TO_RAD
    s, c = kernel_sin(x), kernel_cos(x)
    return {0: (s, c), 1: (c, -s), 2: (-s, -c), 3: (-c, s)}[q]


def unit(lat, lng):
    sl, cl = sin_cos_deg(lat)
    sn, cn = sin_cos_deg(lng)
    return [cl * cn, cl * sn, sl]


def exact_step(value, origin, span):
    q = (Fraction(value) - origin) * STEPS // span
    return max(0, min(STEPS - 1, int(q)))


def interleave(u, v):
    out = 0
    for i in range(31):
        out |= ((u >> i) & 1) << (2 * i)
        out |= ((v >> i) & 1) << (2 * i + 1)
    return out


def cell(lat, lng):
    return interleave(exact_step(lng, -180, 360), exact_step(lat, -90, 180))


def check_unit(lat, lng, got):
    la, ln = mpmath.radians(mpmath.mpf(lat)), mpmath.radians(mpmath.mpf(lng))
    true = [mpmath.cos(la) * mpmath.cos(ln), mpmath.cos(la) * mpmath.sin(ln), mpmath.sin(la)]
    for g, t in zip(got, true):
        assert abs(mpmath.mpf(g) - t) <= 2 * mpmath.mpf(2) ** -53, (lat, lng, g, t)


def chord2_for_meters(m):
    c = 2.0 * kernel_sin(m / (2.0 * R))
    return c * c


def rounded_steps(value, origin, span):
    """The two rounded evaluations a careless port might use."""
    x = value - origin
    return (min(STEPS - 1, max(0, math.floor(x / span * STEPS))),
            min(STEPS - 1, max(0, math.floor(x * (STEPS / span)))))


def main():
    rng = random.Random(20261117)
    points = [
        (0.0, 0.0), (-0.0, -0.0), (90.0, 0.0), (-90.0, 0.0), (90.0, 180.0),
        (-90.0, -180.0), (0.0, 180.0), (0.0, -180.0), (45.0, 45.0), (-45.0, -135.0),
        (51.477928, -0.001545), (37.774929, -122.419416), (-33.86882, 151.209296),
        (64.146582, -21.942635), (-77.846323, 166.668235), (1e-300, -1e-300),
        (5e-324, -5e-324), (89.999999999, 179.999999999), (-89.999999999, -179.999999999),
        (math.nextafter(90.0, 0.0), math.nextafter(180.0, 0.0)),
        (math.nextafter(-90.0, 0.0), math.nextafter(-180.0, 0.0)),
    ]
    for _ in range(40):
        points.append((rng.uniform(-90, 90), rng.uniform(-180, 180)))
    # Doubles nearest to grid boundaries, and their neighbours.
    discriminating = {"divide_then_scale": 0, "scale_by_rounded_constant": 0}
    for axis in ("lng", "lat"):
        origin, span = (-180, 360) if axis == "lng" else (-90, 180)
        found = 0
        tries = 0
        while found < 60 and tries < 200000:
            tries += 1
            k = rng.randrange(0, STEPS + 1)
            boundary = float(Fraction(origin) + Fraction(k * span, STEPS))
            for value in (math.nextafter(boundary, -math.inf), boundary,
                          math.nextafter(boundary, math.inf)):
                if not (origin <= value <= origin + span):
                    continue
                exact = exact_step(value, origin, span)
                a, b = rounded_steps(value, origin, span)
                differs = a != exact or b != exact
                if differs or found < 20:
                    other = rng.uniform(-90, 90) if axis == "lng" else rng.uniform(-180, 180)
                    points.append((other, value) if axis == "lng" else (value, other))
                    found += 1
                discriminating["divide_then_scale"] += a != exact
                discriminating["scale_by_rounded_constant"] += b != exact
    cases = []
    killed = {"divide_then_scale": 0, "scale_by_rounded_constant": 0}
    for lat, lng in points:
        u = unit(lat, lng)
        check_unit(lat, lng, u)
        for name, i in (("divide_then_scale", 0), ("scale_by_rounded_constant", 1)):
            if (rounded_steps(lng, -180, 360)[i] != exact_step(lng, -180, 360)
                    or rounded_steps(lat, -90, 180)[i] != exact_step(lat, -90, 180)):
                killed[name] += 1
        cases.append({
            "lat": bits(lat), "lng": bits(lng), "cell": str(cell(lat, lng)),
            "x": bits(u[0]), "y": bits(u[1]), "z": bits(u[2]),
        })
    # Both rounded evaluations must be told apart from the exact one.
    assert killed["divide_then_scale"] > 0 and killed["scale_by_rounded_constant"] > 0, killed
    thresholds = []
    for m in [0.0, 1e-3, 1.0, 250.0, 1000.0, 12345.678, 100000.0, 1000000.0]:
        c2 = chord2_for_meters(m)
        true = (2 * mpmath.sin(mpmath.mpf(m) / (2 * mpmath.mpf(R)))) ** 2
        assert abs(mpmath.mpf(c2) - true) <= 4 * mpmath.mpf(2) ** -53 * max(true, mpmath.mpf(2) ** -1022)
        thresholds.append({"meters": bits(m), "chord2": bits(c2)})
    out = {
        "encoding": "jazz-ix-geo-z62/1",
        "note": "SPEC 20.10.3. f64 values are the 16 hex digits of their IEEE-754 bits; "
                "cell is the decimal i64. Generated by generate.py, an independent Python "
                "reference: exact rational cells, the SPEC's fdlibm trigonometry checked "
                "against mpmath. Points that a rounded quantization gets wrong: "
                + json.dumps(killed) + ".",
        "points": cases,
        "chord_thresholds": thresholds,
    }
    path = Path(__file__).with_name("points-v1.json")
    path.write_text(json.dumps(out, indent=1) + "\n")
    print(f"{len(cases)} points, {killed}")


if __name__ == "__main__":
    main()
