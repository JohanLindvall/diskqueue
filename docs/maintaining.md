# Maintaining diskqueue

[Contributor setup](../CONTRIBUTING.md) · [User guide](guide.md)

## Triage and review

Use the existing `bug`, `enhancement`, `documentation`, and `question` labels
to make issues easier to find. Apply `good first issue` only when the scope is
small, the expected outcome is clear, and a contributor can get started from
the description. Use `help wanted` for work open to outside contributions.

For a report, first identify its expected behavior and reproducer. Avoid
requesting production queue contents; these can contain private application
data. Follow [SECURITY.md](../SECURITY.md) for private vulnerability reports.

Review changes against the documented delivery and durability contracts.
CI's minimum-Go, race, fault-injection, 32-bit, fuzz, allocation, and cross-build
checks serve different purposes; a passing subset does not replace the rest.

## Keep the development tools aligned

`GOLANGCI_LINT_VERSION` in [Makefile](../Makefile) and the version in
[ci.yml](../.github/workflows/ci.yml) must match. Update them together when
upgrading Go or the linter. Dependabot checks the Go dependency and GitHub
Actions weekly; it does not update the linter's `version` input or Makefile pin.

The minimum Go version is declared in `go.mod` and tested by the CI floor job.
Change the documentation and matrix together if raising it.

## Prepare a release

CI creates a patch tag after all checks pass on `main`. Tags are Go module
versions; they do not automatically create a GitHub Release. Keep published
tags immutable and use a new version for a fix.

For a release worth announcing:

1. Choose an existing green tag from the repository's tags.
2. Open **Releases → Draft a new release**, select that tag and the previous
   release, and generate release notes.
3. Review the notes. Add a short description of user-visible behavior,
   compatibility changes, upgrade steps, and validation. Generated notes use
   merged pull requests; include relevant direct commits manually.
4. Publish when the notes are ready. A Go library needs no binary attachments.

[release.yml](../.github/release.yml) groups generated notes by the existing
PR labels. [GitHub's release-note documentation](https://docs.github.com/en/repositories/releasing-projects-on-github/automatically-generated-release-notes)
describes the generation and review steps.

## Help people find and evaluate the project

Keep the repository's About description focused on its purpose, link to the
Go API documentation, and retain relevant topics such as `go`,
`persistent-queue`, `disk-queue`, and `work-queue`.
[GitHub topics](https://docs.github.com/en/repositories/managing-your-repositorys-settings-and-features/customizing-your-repository/classifying-your-repository-with-topics)
help people find repositories by subject.

Keep the README quick start executable, make tradeoffs explicit, and publish
benchmark results with enough environment detail to reproduce them. Ask before
listing a user's project or logo as an adopter. Review comparison claims
against upstream sources before promoting them; the comparison document is a
design snapshot, not a continuously updated ranking.
