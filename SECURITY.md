# Security policy

## Release signing

Release tags `v*` are signed with the project SSH signing key:

- fingerprint: `SHA256:RKtM0n6ob0U7GcFKq9FVum3BifnM/pPvaZf+Vsn8OxU`
- public key: [`security/release-signing-key.pub`](security/release-signing-key.pub)

Verify a tag:

```bash
git -c gpg.ssh.allowedSignersFile=security/allowed_signers verify-tag v0.1.0
```

`install.sh` verifies `v*` tags with the embedded public key before installing;
`main` is treated as a development ref and produces a warning.

## Plugin trust

Plugins are code. With `plugins.sandbox = "require"` (the default), the worker
does not start a plugin unless bubblewrap/user namespaces work. The sandbox hides
`data_dir` and the worker config, and disables networking for plugins without
`permissions.network = true`. `auto` is an explicit compatibility fallback;
`off` disables isolation.

## Reporting

Please report vulnerabilities privately via GitHub Security Advisories
("Report a vulnerability") rather than opening a public issue.
