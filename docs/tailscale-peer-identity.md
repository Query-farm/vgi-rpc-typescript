# Tailscale peer identity

The Node/Bun entry point exports two opt-in providers. Neither changes application authentication unless a peer-authentication policy consumes its evidence.

## Tailscale Serve

`tailscaleServeIdentityProvider` accepts Serve headers only when `immediatePeer` is one of the configured exact IP addresses. CIDRs and hostnames are deliberately unsupported. It rejects duplicate headers, controls, malformed RFC 2047 UTF-8 Q encoding, duplicate JSON keys, and malformed application-capability shapes. A `Tailscale-Funnel-Request: ?1` request is `not_applicable`; any other value is invalid.

```ts
import { tailscaleServeIdentityProvider } from "@query-farm/vgi-rpc";

const provider = tailscaleServeIdentityProvider({
  issuer: "tailnet:example.com",
  trustedProxyAddresses: ["127.0.0.1", "::1"],
});
```

Serve supplies a login name rather than the stable numeric tailnet user ID. The provider therefore marks it `subjectStability: "login"`: it is verified proxy evidence and can bind an existing application identity, but it is not eligible for `peerIdentityPrimary`. Application capabilities can be returned without a user and remain usable as verified capability evidence.

## LocalAPI WhoIs

`tailscaleLocalApiIdentityProvider` performs a fresh `GET /localapi/v0/whois` for every resolution. It never invokes the `tailscale` CLI, caches a result, follows redirects, honors proxy environment variables, or falls back to another transport. The WhoIs source precedence is `assertedPeer`, then `sourceEndpoint`, then `immediatePeer`; service and destination-IP scopes use the official `svc_name` and `dst_ip` query parameters.

The Fetch API does not expose the accepted socket's source port, so the generic HTTP handler cannot derive `sourceEndpoint` itself. A runtime adapter or application-specific Node/Bun listener must supply the original `IP:port` through `peerResolutionContext` before native HTTP LocalAPI WhoIs can be relied on. An IP-only `immediatePeer` remains useful for exact trusted-proxy checks, but is not a substitute for the connection tuple required by WhoIs.

```ts
import { tailscaleLocalApiIdentityProvider } from "@query-farm/vgi-rpc";

const provider = tailscaleLocalApiIdentityProvider({
  issuer: "tailnet:example.com",
  unixSocket: "/var/run/tailscale/tailscaled.sock",
});
```

An explicit local HTTP/token endpoint is also supported:

```ts
const provider = tailscaleLocalApiIdentityProvider({
  issuer: "tailnet:example.com",
  endpoint: "http://127.0.0.1:49152",
  password: process.env.TAILSCALE_LOCALAPI_TOKEN,
});
```

Only plain HTTP origins without userinfo, path, query, or fragment are accepted. The password is sent as Basic authentication with an empty username and is valid only with an explicit endpoint.

### Native runtime coverage

- Linux defaults to `/var/run/tailscale/tailscaled.sock`; any Unix socket can be configured explicitly.
- macOS Tailscale GUI same-user-proof discovery is not implemented in this SDK. Configure the discovered local HTTP endpoint and token explicitly; the SDK does not inspect GUI state or invoke platform tools.
- Windows named-pipe LocalAPI is not implemented. Node can open some named pipes, but the SDK does not claim native Tailscale pipe discovery or protocol support. Use an explicit local HTTP endpoint where your deployment provides one.
- Browser and Worker builds do not export either provider because they cannot safely access the local daemon or a trusted immediate-peer address.

LocalAPI responses are size- and header-bounded, require exactly one JSON content type, and use strict bounded JSON decoding. Cancellation, the provider timeout, `PeerResolutionContext.deadline`, and its monotonic budget all constrain the same request.

## Raw TCP behind an L4 proxy

`serveTcp` can require PROXY protocol v2 before Arrow framing and LocalAPI
identity resolution. It accepts only TCP over IPv4 or IPv6 from an exact,
operator-configured immediate proxy address. The asserted source is passed to
WhoIs, while `proxyAddress` records the immediate sender.

```ts
const worker = await serveTcp(protocol, {
  host: "127.0.0.1",
  port: 9400,
  proxyProtocolV2Required: true,
  trustedProxyAddresses: ["127.0.0.1"],
  proxyPreambleTimeoutMs: 500,
  maximumProxyPreambleBytes: 536,
  peerIdentityProviders: [provider],
  peerAuthenticationPolicy: peerIdentityPrimary("tailscale"),
});
```

Trust is checked before any preamble byte is consumed. CIDRs and hostnames are
not accepted as trusted senders. `LOCAL`, `UNSPEC`, UDP, Unix-family,
truncated, malformed, and oversized preambles fail closed; bounded unknown
TLVs are ignored. The backend must be unreachable except through the trusted
proxy, because PROXY protocol authenticates neither its own header nor VGI
traffic. Use TLS or a private network for confidentiality and integrity.
