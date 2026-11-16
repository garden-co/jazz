---
"jazz-tools": patch
---

Add a `point` column type for geographic positions. Declare it as `"point"` (or `"point?"`) in `jazz.app.json`. Its value is `[lng, lat]` in GeoJSON order, longitude first: TypeScript sees `readonly [lng: number, lat: number]` and Rust sees `jazz_app::Point { lng, lat }`. A write with a non-finite coordinate, a longitude outside [-180, 180] or a latitude outside [-90, 90] is refused. Filters compare whole points bit for bit (`eq`, `ne`, `in`, `notIn`); searching by distance or inside a box goes through a geo index (SPEC 20). Thanks to carlosbensant, an outside contributor whose issue [#3991](https://github.com/garden-co/jazz/issues/3991) shaped this API.
