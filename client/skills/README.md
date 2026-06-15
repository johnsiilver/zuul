# Zuul client skills

[Claude Code](https://claude.com/claude-code) skills for working with Zuul. Each
subdirectory with a `SKILL.md` is one skill:

| Skill | Invoke as | Purpose |
| --- | --- | --- |
| [`zuul-client-design`](zuul-client-design/SKILL.md) | `/zuul-client-design` | Design Go client interactions for a use case — distributed locks, leader election, dialing the elected master, lease-bound sessions — with path keys, identity/ACLs, and failover. |
| [`zuul-k8s-deploy`](zuul-k8s-deploy/SKILL.md) | `/zuul-k8s-deploy` | Deploy a Zuul cluster on Kubernetes (StatefulSet + DNS discovery) and layer on a security posture: plaintext/dev, server-TLS + token/OIDC, mutual TLS, gossip, and ACLs. |

## Install

Run the installer; it symlinks each skill into a Claude Code skills directory.

```bash
./install.sh            # project scope: <repo>/.claude/skills (default)
./install.sh --user     # user scope:    ~/.claude/skills
./install.sh --copy     # copy instead of symlink
./install.sh --uninstall
```

Symlinks are the default, so editing a `SKILL.md` here updates the installed skill in
place. After installing, invoke a skill in Claude Code with its `/name` (e.g.
`/zuul-client-design`). Claude also auto-loads a skill when a request matches its
description.

These two skills cross-reference each other: design the client with
`/zuul-client-design`, stand up and secure the cluster it talks to with
`/zuul-k8s-deploy`.
