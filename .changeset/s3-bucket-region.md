---
"jazz-tools": patch
---

The `s3()` blob store now works out its bucket's region. On Vercel, `AWS_REGION` is set to the function's own region unless the project sets it, so a bucket in another region used to fail every request. Now, when the bucket answers that it is in another region, the core signs for that region from then on and logs a warning suggesting you set the region explicitly.
