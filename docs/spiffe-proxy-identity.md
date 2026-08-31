# SPIFFE identity from trusted HTTP proxies

The SPIFFE providers produce provider-neutral peer evidence from proxy-managed
headers. They do not connect to the SPIFFE Workload API or validate a
certificate chain themselves. The immediate proxy must validate the chain
against the intended SPIFFE bundle, replace every configured identity header,
and be the only route to the worker backend.

All providers require an exact immediate-peer allowlist and report
`configured_proxy` assurance. Duplicate raw headers, noncanonical SPIFFE IDs,
untrusted peers, malformed values, and ambiguous identities fail closed. In a
Node HTTP adapter, populate `peerResolutionContext.headers` from
`IncomingMessage.rawHeaders`; merged Fetch `Headers` cannot prove that a value
was not duplicated.

```ts
import {
  headersFromNodeRawHeaders,
  nginxSpiffeProvider,
  peerIdentityPrimary,
} from "@query-farm/vgi-rpc";

const peerIdentityProviders = [
  nginxSpiffeProvider({
    trustDomains: ["example.org"],
    trustedProxyAddresses: ["127.0.0.1"],
  }),
];

const options = {
  peerIdentityProviders,
  peerAuthenticationPolicy: peerIdentityPrimary("spiffe"),
  peerResolutionContext: () => ({
    immediatePeer: socket.remoteAddress,
    headers: headersFromNodeRawHeaders(request.rawHeaders),
  }),
};
```

Supported profiles:

- `envoyXfccSpiffeProvider`: one strict text XFCC element from an adjacent
  Envoy configured with mTLS and `forward_client_cert_details: SANITIZE_SET`.
- `nginxSpiffeProvider`: an escaped client certificate plus an exact `SUCCESS`
  verification header from nginx.
- `awsAlbSpiffeProvider`: the ALB verify-mode leaf header. ALB emits no
  per-request `verified=true` signal, so the operator must guarantee verify
  mode, header replacement, backend isolation, and the correct ALB trust store.
- `gcpLoadBalancerSpiffeProvider`: custom frontend-mTLS headers for certificate
  presence, chain verification, SPIFFE ID, and validation error.
- `azureApplicationGatewaySpiffeProvider`: strict-mode Application Gateway
  `client_certificate` and `client_certificate_verification` server variables
  mapped to replacement headers.
- `spiffeX509HeaderProvider`: a generic certificate plus mandatory positive
  chain-verification header.

Certificate-header providers additionally validate the leaf SVID profile:
validity, exactly one URI SAN, subjectless/critical-SAN rule, non-CA basic
constraints, critical digital-signature key usage without certificate/CRL
signing, and client/server authentication EKUs when EKU is present. PEM parsing
uses `node:crypto`, loaded only when one of those providers is called. Envoy
XFCC and GCP SPIFFE-ID providers remain usable without Node certificate APIs.
