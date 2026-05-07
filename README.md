# haraka-plugin-gelf

A [Haraka](https://haraka.github.io/) plugin that forwards log messages to a [Graylog](https://graylog.org/) server via GELF UDP.

## Features

- Forwards all Haraka log messages to Graylog in GELF format
- UDP transport with automatic IPv4/IPv6 detection
- Chunked UDP support for large messages (GELF chunking spec)
- Optional gzip compression
- Per-plugin configuration with independent URL, fields, and routing
- Configurable custom GELF fields with variable interpolation
- Exposes a `loggelf` API on `server.notes` for other plugins to send structured GELF messages directly

## AI Usage Disclaimer

This project makes limited use of generative AI tools during development:

- Generative AI was used for brainstorming, test generation, documentation and code review assistance
- All production code was written, reviewed, and validated by a human
- Final design decisions and implementations are human-driven
- Any defects or limitations in the code are the responsibility of the human authors

## Installation

```
npm install haraka-plugin-gelf
```

Add to `config/plugins`:

```
gelf
```

## Configuration

Create `config/gelf.yaml`. A minimal configuration:

```yaml
url: 'udp://graylog.example.com:12201'
```

Full configuration with all options:

```yaml
# Enable/disable GELF logging
enabled: true

# Enable/disable Haraka log hook
#   true  - all Haraka log messages are forwarded to Graylog
#   false - only messages sent via the server.notes.loggelf API are forwarded
log_hook_enabled: false

# GELF UDP endpoint. Supports udp://, udp4://, udp6:// schemes
url: 'udp://graylog.example.com:12201'

# Compress messages with gzip
compress: true

# Maximum UDP packet/chunk size in bytes (64–65475)
# 1420 suits standard Ethernet MTU; 8192 suits jumbo frames
max_chunk_size: 1420

# Override the hostname reported in GELF messages (defaults to os.hostname())
# hostname: mail.example.com

# If true, no further log plugins will be called after this one
last: false

# Custom fields added to every GELF message (see Custom Fields below)
fields:
  logger: '${logger}'
  connection: '${connection_uuid}'
  transaction: '${transaction_uuid}'

# Per-plugin overrides (see Per-plugin Configuration below)
# plugins:
#   rspamd:
#     url: 'udp4://rspamd-graylog.example.com:12201'
#     fields:
#       component: 'rspamd'
```

## URL scheme

The `url` setting controls both the transport endpoint and the preferred address family:

| Scheme    | Behaviour                                                            |
|-----------|----------------------------------------------------------------------|
| `udp://`  | Dual-stack; uses system address family preference (IPv4 or IPv6)     |
| `udp4://` | Forces IPv4                                                          |
| `udp6://` | Forces IPv6 only                                                     |

When a hostname resolves to multiple addresses, DNS round-robin provides basic load distribution.

## Custom Fields

The `fields` section lets you add fixed or dynamic fields to every GELF message. Field values are strings and support variable interpolation using `${variable}` syntax.

The following variables are available:

| Variable           | Value                                          |
|--------------------|------------------------------------------------|
| `${logger}`        | Name of the plugin that emitted the log entry  |
| `${connection_uuid}` | Haraka connection UUID                       |
| `${transaction_uuid}` | Haraka transaction UUID (includes `.N` suffix) |

If a variable is not available in context (e.g. `${transaction_uuid}` outside of a transaction), the field is omitted from the message entirely.

Example:

```yaml
fields:
  logger: '${logger}'
  connection: '${connection_uuid}'
  transaction: '${transaction_uuid}'
  environment: 'production'
  facility: 'smtp'
```

To explicitly suppress a field that would be inherited from the main config in a per-plugin override, set it to `null`:

```yaml
plugins:
  rspamd:
    fields:
      transaction: null   # omit transaction field for rspamd messages
      component: 'rspamd'
```

## Per-plugin Configuration

Any top-level setting can be overridden per Haraka plugin under the `plugins` key. Plugin names match `plugin.name` in Haraka (e.g. `karma`, `dkim`, `rcpt_to.in_host_list`).

Per-plugin `fields` are merged with (not replaced by) the top-level `fields`.

```yaml
plugins:
  karma:
    url: 'udp://karma-graylog.example.com:12201'
    last: true
    fields:
      component: 'karma'

  rspamd:
    enabled: false

  wildduck:
    log_hook_enabled: true
    url: 'udp4://wildduck-graylog.example.com:12201'
    fields:
      component: 'mx'
      transaction: null
      queue_id: '${transaction_uuid}'
```

## API for other plugins

When loaded, the plugin exposes `server.notes.loggelf` for structured GELF logging from other plugins.

### `message(callerPlugin, msg)`

Send a raw GELF message object. Returns `cfg.last` (boolean) — when `true`, no further log plugins will be called.

```javascript
exports.hook_queue = function (next, connection) {
    connection.server.notes.loggelf?.message(this, {
        short_message: 'Mail queued',
        level: 6, // INFO
        _recipient: connection.transaction.rcpt_to.toString(),
        _sender: connection.transaction.mail_from.toString(),
    });

    next(OK);
};
```

### `log(callerPlugin, connection, level, shortMessage, additionalFields)`

Send a structured log message. Automatically formats `short_message` as `[transaction_uuid] [plugin_name] message` to match Haraka's log format, and populates `connection_uuid` and `transaction_uuid` template variables from the connection object.

```javascript
gelf?.log(this, connection, 6, 'Mail queued', { _queue: 'outbound' });
```

### Convenience log methods

All accept `(callerPlugin, connection, shortMessage, additionalFields)`. `connection` may be `null`.

```javascript
const gelf = connection.server.notes.loggelf;

gelf?.emergency(this, connection, 'System failure');
gelf?.alert(this, connection, 'Disk almost full');
gelf?.critical(this, connection, 'Database unreachable');
gelf?.error(this, connection, 'Delivery failed', { _recipient: 'user@example.com' });
gelf?.warning(this, connection, 'Rate limit approached');
gelf?.notice(this, connection, 'New connection');
gelf?.info(this, connection, 'Message accepted');
gelf?.debug(this, connection, 'Processing step', { _detail: 'some value' });
```

### Additional fields in `msg`

Fields in `msg` beyond the standard GELF fields are included as additional fields, automatically prefixed with `_` if not already. Reserved GELF field names (`host`, `version`, `short_message`, `full_message`, `timestamp`, `level`, `facility`, `file`, `line`, `id`) are never duplicated as additional fields.

| Type      | Behaviour                    |
|-----------|------------------------------|
| `string`  | Passed as-is                 |
| `number`  | Passed as-is                 |
| `boolean` | Converted to `"true"`/`"false"` |
| `Date`    | Converted to ISO 8601 string |
| Other     | JSON round-tripped           |

## GELF message format

Messages are sent as GELF 1.1 JSON over UDP. Supported standard fields:

| Field           | Source                                         |
|-----------------|------------------------------------------------|
| `version`       | Always `"1.1"`                                 |
| `host`          | `msg.host`, `hostname` config, or `os.hostname()` |
| `short_message` | Required                                       |
| `full_message`  | Optional                                       |
| `timestamp`     | `msg.timestamp` (Date or number) or `Date.now()` |
| `level`         | Syslog severity (0–7), defaults to INFO (6)    |
| `facility`      | Optional                                       |
| `file`          | Optional                                       |
| `line`          | Optional integer                               |

## Log levels

| Level     | Value |
|-----------|-------|
| emergency | 0     |
| alert     | 1     |
| critical  | 2     |
| error     | 3     |
| warning   | 4     |
| notice    | 5     |
| info      | 6     |
| debug     | 7     |

Haraka log levels `DATA` and `PROTOCOL` are mapped to `DEBUG` (7).

## License

EUPL-1.1+
