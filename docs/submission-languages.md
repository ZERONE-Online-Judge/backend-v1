# Problem-specific submission languages

`Problem.allowed_languages` is a nonempty, unique subset of:
`cpp17`, `python313`, `java8`, `c99`, `pypy3`, `kotlin`.

Omitted values on creation/import default to all six languages. Migration
0051 also enables all six for existing problems, preserving the previous
unrestricted policy. Explicit choices survive problem copy and ZIP roundtrip.
Empty, duplicate, null and unknown selections are rejected.

Operators with `contest.problem.manage` configure the list per problem in
contest settings or the problem editor. The contestant API exposes it and
the submission UI filters its language choices. Regular and mock submissions
also enforce it on the server. Preview submissions follow the same rule.

Changing the list affects new participant submissions only. Existing queued
submissions, operator verification and historical rejudging keep their saved
language. It never rewrites source code or changes a previous submission.

`ProblemAsset.source_language` preserves the language of exported verification
solutions, particularly PyPy `.py` files, which cannot be distinguished from
CPython by extension. Ordinary old assets keep filename-based inference.

See the judge-agent and Editor `docs/icpc-languages.md` for pinned toolchains,
resource accounting and capability-gated deployment.
