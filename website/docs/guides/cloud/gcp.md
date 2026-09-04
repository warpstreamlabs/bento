---
title: Google Cloud Platform
description: Find out about GCP components in Bento
---

There are many components within Bento which utilise Google Cloud Platform (GCP) services. You will find that each of
these components require valid credentials.

When running Bento inside a Google Cloud environment that has a
[default service account](https://cloud.google.com/iam/docs/service-accounts#default), it can automatically retrieve the
service account credentials to call Google Cloud APIs through a library called Application Default Credentials (ADC).

Otherwise, if your application runs outside Google Cloud environments that provide a default service account, you need
to manually create one. Once you have a service account set up which has the required permissions, you can
[create](https://console.cloud.google.com/apis/credentials/serviceaccountkey) a new Service Account Key and download it
as a JSON file. Then all you need to do set the path to this JSON file in the `GOOGLE_APPLICATION_CREDENTIALS`
environment variable.

Please refer to [this document](https://cloud.google.com/docs/authentication/production) for details.

## Service Account Impersonation

Application Default Credentials are resolved once per process, which means every GCP component in a Bento instance
shares the same identity. When running multiple streams in a single instance (for example in
[streams mode](/docs/guides/streams_mode/about)) it is often desirable for each stream to act as its own, least-privileged
service account instead.

Every GCP component exposes an advanced `credentials` object that supports
[service account impersonation](https://cloud.google.com/iam/docs/service-account-impersonation). When
`impersonate_service_account` is set the component obtains short-lived tokens for that service account using the base
Application Default Credentials, which must be granted `roles/iam.serviceAccountTokenCreator` on the target:

```yaml
input:
  gcp_pubsub:
    project: my-project
    subscription: my-subscription
    credentials:
      impersonate_service_account: stream-a@my-project.iam.gserviceaccount.com
```

Delegated impersonation chains can be expressed with `impersonate_delegates`, where each service account must be
granted `roles/iam.serviceAccountTokenCreator` on the next one in the chain, and the final delegate on the target:

```yaml
input:
  gcp_pubsub:
    project: my-project
    subscription: my-subscription
    credentials:
      impersonate_service_account: target@my-project.iam.gserviceaccount.com
      impersonate_delegates:
        - hop-one@my-project.iam.gserviceaccount.com
        - hop-two@my-project.iam.gserviceaccount.com
```

Impersonated tokens are requested with the `https://www.googleapis.com/auth/cloud-platform` scope and are refreshed
automatically before they expire.
