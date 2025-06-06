---
title: websocket
slug: websocket
type: input
status: stable
categories: ["Network"]
---

<!--
     THIS FILE IS AUTOGENERATED!

     To make changes please edit the corresponding source file under internal/impl/<provider>.
-->

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Connects to a websocket server and continuously receives messages.


<Tabs defaultValue="common" values={[
  { label: 'Common', value: 'common', },
  { label: 'Advanced', value: 'advanced', },
]}>

<TabItem value="common">

```yml
# Common config fields, showing default values
input:
  label: ""
  websocket:
    url: ws://localhost:4195/get/ws # No default (required)
    auto_replay_nacks: true
```

</TabItem>
<TabItem value="advanced">

```yml
# All config fields, showing default values
input:
  label: ""
  websocket:
    url: ws://localhost:4195/get/ws # No default (required)
    proxy_url: "" # No default (optional)
    open_messages: [] # No default (optional)
    open_message_type: binary
    auto_replay_nacks: true
    tls:
      enabled: false
      skip_cert_verify: false
      enable_renegotiation: false
      root_cas: ""
      root_cas_file: ""
      client_certs: []
    connection:
      max_retries: -1 # No default (optional)
    oauth:
      enabled: false
      consumer_key: ""
      consumer_secret: ""
      access_token: ""
      access_token_secret: ""
    basic_auth:
      enabled: false
      username: ""
      password: ""
    jwt:
      enabled: false
      private_key_file: ""
      signing_method: ""
      claims: {}
      headers: {}
```

</TabItem>
</Tabs>

It is possible to configure an `open_message`, which when set to a non-empty string will be sent to the websocket server each time a connection is first established.

## Fields

### `url`

The URL to connect to.


Type: `string`  

```yml
# Examples

url: ws://localhost:4195/get/ws
```

### `proxy_url`

An optional HTTP proxy URL.


Type: `string`  

### `open_messages`

An optional list of messages to send to the server upon connection. This field replaces `open_message`, which will be removed in a future version.


Type: `array`  

### `open_message_type`

An optional flag to indicate the data type of open_message.


Type: `string`  
Default: `"binary"`  

| Option | Summary |
|---|---|
| `binary` | Binary data open_message. |
| `text` | Text data open_message. The text message payload is interpreted as UTF-8 encoded text data. |


### `auto_replay_nacks`

Whether messages that are rejected (nacked) at the output level should be automatically replayed indefinitely, eventually resulting in back pressure if the cause of the rejections is persistent. If set to `false` these messages will instead be deleted. Disabling auto replays can greatly improve memory efficiency of high throughput streams as the original shape of the data can be discarded immediately upon consumption and mutation.


Type: `bool`  
Default: `true`  

### `tls`

Custom TLS settings can be used to override system defaults.


Type: `object`  

### `tls.enabled`

Whether custom TLS settings are enabled.


Type: `bool`  
Default: `false`  

### `tls.skip_cert_verify`

Whether to skip server side certificate verification.


Type: `bool`  
Default: `false`  

### `tls.enable_renegotiation`

Whether to allow the remote server to repeatedly request renegotiation. Enable this option if you're seeing the error message `local error: tls: no renegotiation`.


Type: `bool`  
Default: `false`  
Requires version 1.0.0 or newer  

### `tls.root_cas`

An optional root certificate authority to use. This is a string, representing a certificate chain from the parent trusted root certificate, to possible intermediate signing certificates, to the host certificate.
:::warning Secret
This field contains sensitive information that usually shouldn't be added to a config directly, read our [secrets page for more info](/docs/configuration/secrets).
:::


Type: `string`  
Default: `""`  

```yml
# Examples

root_cas: |-
  -----BEGIN CERTIFICATE-----
  ...
  -----END CERTIFICATE-----
```

### `tls.root_cas_file`

An optional path of a root certificate authority file to use. This is a file, often with a .pem extension, containing a certificate chain from the parent trusted root certificate, to possible intermediate signing certificates, to the host certificate.


Type: `string`  
Default: `""`  

```yml
# Examples

root_cas_file: ./root_cas.pem
```

### `tls.client_certs`

A list of client certificates to use. For each certificate either the fields `cert` and `key`, or `cert_file` and `key_file` should be specified, but not both.


Type: `array`  
Default: `[]`  

```yml
# Examples

client_certs:
  - cert: foo
    key: bar

client_certs:
  - cert_file: ./example.pem
    key_file: ./example.key
```

### `tls.client_certs[].cert`

A plain text certificate to use.


Type: `string`  
Default: `""`  

### `tls.client_certs[].key`

A plain text certificate key to use.
:::warning Secret
This field contains sensitive information that usually shouldn't be added to a config directly, read our [secrets page for more info](/docs/configuration/secrets).
:::


Type: `string`  
Default: `""`  

### `tls.client_certs[].cert_file`

The path of a certificate to use.


Type: `string`  
Default: `""`  

### `tls.client_certs[].key_file`

The path of a certificate key to use.


Type: `string`  
Default: `""`  

### `tls.client_certs[].password`

A plain text password for when the private key is password encrypted in PKCS#1 or PKCS#8 format. The obsolete `pbeWithMD5AndDES-CBC` algorithm is not supported for the PKCS#8 format. Warning: Since it does not authenticate the ciphertext, it is vulnerable to padding oracle attacks that can let an attacker recover the plaintext.
:::warning Secret
This field contains sensitive information that usually shouldn't be added to a config directly, read our [secrets page for more info](/docs/configuration/secrets).
:::


Type: `string`  
Default: `""`  

```yml
# Examples

password: foo

password: ${KEY_PASSWORD}
```

### `connection`

Customise how websocket connection attempts are made.


Type: `object`  

### `connection.max_retries`

An optional limit to the number of consecutive retry attempts that will be made before abandoning the connection altogether and gracefully terminating the input. When all inputs terminate in this way the service (or stream) will shut down. If set to zero connections will never be reattempted upon a failure. If set below zero this field is ignored (effectively unset).


Type: `int`  

```yml
# Examples

max_retries: -1

max_retries: 10
```

### `oauth`

Allows you to specify open authentication via OAuth version 1.


Type: `object`  

### `oauth.enabled`

Whether to use OAuth version 1 in requests.


Type: `bool`  
Default: `false`  

### `oauth.consumer_key`

A value used to identify the client to the service provider.


Type: `string`  
Default: `""`  

### `oauth.consumer_secret`

A secret used to establish ownership of the consumer key.
:::warning Secret
This field contains sensitive information that usually shouldn't be added to a config directly, read our [secrets page for more info](/docs/configuration/secrets).
:::


Type: `string`  
Default: `""`  

### `oauth.access_token`

A value used to gain access to the protected resources on behalf of the user.


Type: `string`  
Default: `""`  

### `oauth.access_token_secret`

A secret provided in order to establish ownership of a given access token.
:::warning Secret
This field contains sensitive information that usually shouldn't be added to a config directly, read our [secrets page for more info](/docs/configuration/secrets).
:::


Type: `string`  
Default: `""`  

### `basic_auth`

Allows you to specify basic authentication.


Type: `object`  

### `basic_auth.enabled`

Whether to use basic authentication in requests.


Type: `bool`  
Default: `false`  

### `basic_auth.username`

A username to authenticate as.


Type: `string`  
Default: `""`  

### `basic_auth.password`

A password to authenticate with.
:::warning Secret
This field contains sensitive information that usually shouldn't be added to a config directly, read our [secrets page for more info](/docs/configuration/secrets).
:::


Type: `string`  
Default: `""`  

### `jwt`

BETA: Allows you to specify JWT authentication.


Type: `object`  

### `jwt.enabled`

Whether to use JWT authentication in requests.


Type: `bool`  
Default: `false`  

### `jwt.private_key_file`

A file with the PEM encoded via PKCS1 or PKCS8 as private key.


Type: `string`  
Default: `""`  

### `jwt.signing_method`

A method used to sign the token such as RS256, RS384, RS512 or EdDSA.


Type: `string`  
Default: `""`  

### `jwt.claims`

A value used to identify the claims that issued the JWT.


Type: `object`  
Default: `{}`  

### `jwt.headers`

Add optional key/value headers to the JWT.


Type: `object`  
Default: `{}`  


