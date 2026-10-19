<p align="center">
    <a href="https://wippy.ai" target="_blank">
        <picture>
            <source media="(prefers-color-scheme: dark)" srcset="https://github.com/wippyai/.github/blob/main/logo/wippy-text-dark.svg?raw=true">
            <img width="30%" align="center" src="https://github.com/wippyai/.github/blob/main/logo/wippy-text-light.svg?raw=true" alt="Wippy logo">
        </picture>
    </a>
</p>
<h1 align="center">Dataflow</h1>
<div align="center">

[![Latest Release](https://img.shields.io/github/v/release/wippyai/dataflow?style=flat-square)][releases-page]
[![License](https://img.shields.io/github/license/wippyai/dataflow?style=flat-square)](LICENSE)
[![Documentation](https://img.shields.io/badge/Wippy-Documentation-brightgreen.svg?style=flat-square)][wippy-documentation]

</div>


[wippy-documentation]: https://docs.wippy.ai
[releases-page]: https://github.com/wippyai/dataflow/releases
[packcli]: https://github.com/wippyai/wippy-releases/releases
[modules-registry]: https://modules.wippy.ai

## Live execution authority

A host with mutable subject authority binds its existing native identity resolver through the Dataflow dependency:

```yaml
parameters:
  - name: userspace.dataflow:execution_authority_imports
    value:
      execution_identity_resolver: app:execution_authority
  - name: userspace.dataflow:refresh_perms
    value: true
```

The imported `library.lua` exports two methods:

```lua
capture(actor, scope) -> authority_context, error
resolve({subject_id, authority_only = true, actor_meta, authority_context}) -> actor, scope, error
```

`capture` receives the current native actor and scope and returns a durable plain table. The host includes the references or descriptors that preserve every explicit narrowing ceiling. It returns an error when a restriction has no durable representation. Dataflow bounds, copies and persists this table as the optional `authority_context` field of its execution frame. The host keeps credentials and authority in its own stores; the frame contains no bearer token.

`resolve` uses the host's existing live authority mechanism, such as native token validation backed by `subject_lookup`. It checks current subject status, memberships and credential restrictions and returns native Security actor/scope objects. The actor ID matches `subject_id`. The scope includes every captured explicit ceiling. Dataflow passes that scope to execution unchanged, which preserves native restrictions attached to its policies. Captured policy IDs do not supply authority when this binding is present.

With both the binding and `refresh_perms: true`, the overseer resolves identity on activation, including Await resume and runtime recovery. Synchronous resolution also uses the host resolver when the ambient frame matches the stored frame. These are execution boundaries; Dataflow does not poll during running nodes or add a provider lookup to each persistence operation. Resolver errors, changed principals, malformed results and missing resolvers for saved restriction contexts fail execution without a policy snapshot fallback.

Both provider methods return errors through their declared result tuple. Dataflow calls them directly and returns provider errors unchanged, preserving native kind, retryability, details and cause. The provider returns an error for operational failures; it does not raise an exception. Dataflow returns `Invalid` for changed principals or malformed binding/results and `Unavailable` when a saved authority context has no configured provider.

The library runs under the calling execution frame. Its declared modules/imports and the host's narrow reconstruction permissions determine access to its authority provider; Dataflow adds no wildcard authority or host-specific permission. The host uses a module-owned authority boundary for protected reads. Calling scopes with a resolver binding allow `registry.get` for `userspace.dataflow:execution_authority_settings`; the module's overseer includes this exact read permission.

The binding defaults to an empty import map and `refresh_perms` defaults to false. With either absent, capture and resolution make no provider calls and existing snapshot frames retain their actor/policy reconstruction behavior. Enabling both also covers existing frames: `authority_context` is absent, and the host uses the existing actor metadata to resolve any stored credential restriction or returns an error if it cannot preserve it. No schema migration or stored-data rewrite occurs. A host keeps its resolver and refresh setting enabled while frames containing its restriction contexts remain runnable; disabling either refuses those frames instead of discarding their saved restrictions.
