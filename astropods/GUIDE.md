# Astropods Bifrost Fork Guide

> Copy of the team guide, exported 2026-11-13. The live, editable version is the [Astropods Bifrost Fork Guide](https://claude.ai/artifact/UJEPMz2XnMVb3qoWcj44Fo); update both when you change one.

We run Bifrost from our own fork, [astropods/bifrost](https://github.com/astropods/bifrost). `main` is our release branch: upstream Bifrost plus our commits, and we deploy only from tags on it.

## Why we fork

Bifrost OSS keeps virtual keys, budgets and rate limits in each node's memory and reads Postgres only at startup. With several replicas, a key created on one node is rejected by the others until they restart. Enterprise fixes this with gossip clustering; we fix it in our fork with Postgres `LISTEN/NOTIFY`.

What the fork adds, all behind flags so it behaves like upstream when they are off:

- **Virtual key sync** (`BIFROST_OSS_SYNC=1`): changes made on any node reach every node within milliseconds, with catch-up and reconcile loops for missed notifications.
- **Counter sync** (`BIFROST_OSS_SYNC_COUNTERS=1`): budget and rate-limit usage shared across nodes, so a limit holds for the cluster instead of per node, within about one second.
- **Our plugins** (planned): compiled into the gateway rather than loaded as `.so` files.

Background and design: the [architecture page](https://claude.ai/artifact/9RL3YSsRbLSPmeP9UF1c8A).

## Branches

`main` is the only long-lived branch we own. Everything else is short-lived and merges into it by PR.

| Branch | What it holds | Branches from | Merges into |
| --- | --- | --- | --- |
| `main` (default, protected) | Upstream `main` plus our commits. Always deployable. | — | — |
| `feat/*` | A change of ours, e.g. `feat/oss-vk-sync` | `main` | `main` by PR |
| `fix/*` | An urgent fix to what we run | `main` | `main` by PR, then a new tag |
| `sync/vX.Y.Z` | Bringing in an upstream release | `main` | `main` by PR |
| `release/X.Y` | Patching an older version after `main` moved on. Create only when needed. | the version's tag | nowhere; tag from it |

We do not use `dev`. Upstream works on `dev` and releases from `main`; `dev` is unreleased code that changes daily. Never commit to `dev` or build from it. If the fork has a `dev` branch, it only mirrors upstream.

## How changes flow

```mermaid
flowchart LR
    subgraph upstream["maximhq/bifrost main"]
        U1(["transports/v2.5.1"]) --> U2(["transports/v2.6.0"])
    end
    subgraph ours["astropods main"]
        M0["main created from v2.5.1"] --> M1["feat/* merged"] --> M2["sync/v2.6.0 merged"]
    end
    U1 -. "create main once" .-> M0
    F["feat/* and fix/*"] -- "PR into main" --> M1
    U2 -- "merge release tag" --> S["sync/vX.Y.Z"] -- "PR into main" --> M2
    M2 -- "bump modules/bifrost in astro" --> R["astro CI builds the image, e.g. v2.5.1-ast.0fb93426bea0"] --> ST["Staging"] -- "same tag" --> P["Prod"]
```

Upstream release tags are merged into our `main` through a `sync/*` PR; our own work comes in through `feat/*` and `fix/*` PRs. To ship `main`, bump the `modules/bifrost` submodule in astro: astro's deploy workflows build the image, and astro-infra pins it for preview, then prod.

## Making a change

1. Update `main` and branch from it: `git switch main && git pull` then `git switch -c feat/<name>`.
2. Put new files in `astropods/` (see Rules). Go code that has to sit in an upstream package goes in **new files** named after our feature (like `transports/bifrost-http/server/ossync.go`). Edit upstream files only for a few lines of wiring, and mark each edit with a comment naming our feature.
3. Put new behaviour behind an env flag that defaults to off, so the fork behaves like upstream when it is unset.
4. Build the way a release does, without the Go workspace: `cd transports && GOWORK=off go build ./bifrost-http`.
5. Run the upstream tests for any package you touched (`go test ./<package>/...`). Nothing runs them for you: there is no CI.
6. Open a PR into `main`: `gh pr create -R astropods/bifrost --base main`. Always pass `-R astropods/bifrost`; without it `gh` can target the upstream repo.
7. Squash-merge, so each feature stays one commit that is easy to find when we sync upstream.

## Releasing

The fork has no build of its own: GitHub Actions is disabled on it and stays off. astro builds the gateway image from the `modules/bifrost` submodule, and astro-infra deploys it.

1. **Merge** the change into the fork's `main`.
2. **Bump the submodule** in astro: `git -C modules/bifrost fetch && git -C modules/bifrost checkout origin/main`, commit the new pointer, open a PR. A change to `modules/bifrost` is what triggers the build.
3. **Build:** when that PR merges, `Deploy: Preview` builds the `bifrost` service with the fork's own `transports/Dockerfile` and pushes it to `preview-bifrost`. It is tagged `:<astro commit>` and `:v<upstream version>-ast.<fork commit>`, e.g. `:v2.5.1-ast.0fb93426bea0`, and the binary reports the same version. `scripts/bifrost-version.sh` in astro computes it. Prod builds `prod-bifrost` the same way on its next scheduled or manual `Deploy: Prod` run.
4. **Deploy:** point astro-infra at the image (`helm/values/common/bifrost.yaml.tpl` and `bifrost_image_tag`), preview first, then prod. The run summary also prints the digest; pin it where a mutable tag is not enough.
5. **Roll back:** pin the previous `v…-ast.…` tag in astro-infra. Never move or delete a pushed tag.

## Taking an upstream release

Sync within a week or two of each upstream release (a new `transports/vX.Y.Z` tag on [maximhq/bifrost releases](https://github.com/maximhq/bifrost/releases)). Small, regular syncs keep conflicts small. Sync right after a release, not at random times: upstream sometimes merges hotfixes into `main` just before tagging.

1. Fetch upstream: `git fetch upstream --tags`.
2. Branch and merge the release tag: `git switch -c sync/v2.6.0 main` then `git merge transports/v2.6.0`. Merge, never rebase: `main` is shared, so history must not be rewritten.
3. Resolve conflicts in our patched files. Keep upstream's code and re-apply our wiring. Turn on `git config rerere.enabled true` so git remembers each resolution for next time.
4. Build with `GOWORK=off`, run the upstream tests.
5. Open a PR into `main` and merge it, then release it as above (bump `modules/bifrost` in astro).

GitHub's "Sync fork" button merges upstream `main` (not a tag) into ours. It is fine right after a release, when the two match; prefer merging the tag.

## Rules

- **Our files live in `astropods/`.** Upstream never touches that folder, so nothing in it can conflict on a sync. It holds this guide, `GUIDE.md`, and any design notes we add. The only exception is Go code that must sit inside an upstream package; it goes in new files named after our feature, like `server/ossync.go`.
- **Keep our footprint small.** Every line we change in an upstream file is a possible conflict on every sync, so keep those edits to a few lines of wiring.
- **Everything behind a flag, off by default.** With our flags unset the fork must behave exactly like upstream.
- **Keep GitHub Actions disabled.** The fork carries all of upstream's workflows, including ones that release and publish on every push to `main`. With Actions off none of them can run, so we leave the workflow files untouched and syncs stay conflict-free. Never re-enable Actions or click "enable workflows" on the Actions tab.
- **Watch the `GOWORK` gotcha.** Release builds use `GOWORK=off`, so `transports` pulls `core`, `framework` and the plugins from the Go module proxy at the versions in `transports/go.mod`, not from our folders. A change under `plugins/` or `framework/` is silently left out unless `transports/go.mod` has a `replace` line pointing at the local folder, e.g. `replace github.com/maximhq/bifrost/plugins/governance => ../plugins/governance`. Before you release such a change, check that the built image contains it.
- **The repo is public.** A fork of a public repo cannot be private. No secrets, customer names or internal hostnames in code, commits or PRs. If we need private code, it goes in a separate private repo.
- **Send wiring upstream when we can.** If maximhq accepts an extension point we need (for example, passing our own callbacks into `Bootstrap`), our patch shrinks and syncs get easier.

## Roadmap

The first release goes to preview once step 2 merges and astro bumps `modules/bifrost`.

| Step | What | Done when | Status |
| --- | --- | --- | --- |
| 1. Set up the fork | `main` from upstream, default branch, Actions disabled, image built by astro CI (astropods/astro#3796, #3800); branch protection on `main` still to add | astro CI pushes a `v…-ast.…` image for a submodule bump | In progress |
| 2. Virtual key sync | `feat/oss-vk-sync` PR; sync health exported as Prometheus metrics | A key made on one node works on all nodes within 1 s, no restarts | In progress |
| 3. Harden sync | Budget resets, gap-aware catch-up, full reconcile and a sweep for teams, customers and model configs (in #1); a Postgres failover on preview | A failover on preview leaves every node consistent | In progress |
| 4. Counter sync | Shared budget and rate-limit usage through an own-usage table, no governance plugin change (`feat/oss-counter-sync`) | A limit across replicas stops at the limit plus at most 1 s of traffic | In progress |
| 5. Our plugins | Compiled into the fork | Plugins ship in the standard image | Not started |
| 6. First upstream sync | Merge the next `transports/v2.x` release | Tagged and running in staging | Not started |

Step 2 status: [astropods/bifrost#1](https://github.com/astropods/bifrost/pull/1) is merged and goes to preview for testing. Provider configuration and virtual MCPs are not synced. Live usage counters are shared only with counter sync (step 4).

## One-time setup

The fork was created with only `dev` and no tags, so `main` has to be created once. Whoever does it runs:

```bash
git clone https://github.com/astropods/bifrost.git
cd bifrost
git remote add upstream https://github.com/maximhq/bifrost.git
git fetch upstream main --tags
git switch -c main upstream/main
git push -u origin main
gh repo edit astropods/bifrost --default-branch main
```

Then protect `main` in the repo settings (PRs only, at least one approval). Everyone else just clones and adds the `upstream` remote:

```bash
git clone https://github.com/astropods/bifrost.git && cd bifrost && git remote add upstream https://github.com/maximhq/bifrost.git
```

To build locally with every module from your checkout, run `make setup-workspace` once (it creates a `go.work`, which is not committed). Release builds run without it.
