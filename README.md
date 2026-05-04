# haraka-plugin-gelf

A [Haraka](https://haraka.github.io/) plugin that forwards log messages to a [Graylog](https://graylog.org/) server via GELF UDP.

## Features

- Forwards all Haraka log messages to Graylog in GELF format
- UDP transport with automatic IPv4/IPv6 detection
- Chunked UDP support for large messages (GELF chunking spec)
- Optional gzip compression
- Per-plugin log routing with independent configuration
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

Create `config/gelf.ini`:

```ini
[main]
; Enable or disable the plugin
enabled = true

; Enable or disable Haraka log hook (send all Haraka log to graylog)
log_hook_enabled = true

; GELF UDP endpoint
; Supports udp://, udp4://, udp6:// schemes
url = udp://graylog.example.com:12201

; Compress messages with gzip
compress = true

; Maximum UDP packet/chunk size in bytes (64–65475)
; Set to 1420 for standard Ethernet MTU, 8192 for jumbo frames
max_chunk_size = 1420

; Override the hostname reported in GELF messages
; Defaults to os.hostname()
; hostname = mail.example.com

; If true, no further log plugins will be called after this one
last = false
```

### Per-plugin routing

You can override any `[main]` setting for a specific Haraka plugin by adding a section named after the plugin:

```ini
[plugins.karma]
url = udp://graylog-karma.example.com:12201
last = true

[plugins.rspamd]
enabled = false
```

Plugin names match the value of `plugin.name` in Haraka (e.g. `karma`, `dkim`, `rcpt_to.in_host_list`).

## URL scheme

The `url` setting controls both the transport endpoint and the preferred address family:

| Scheme    | Behaviour                                                            |
|-----------|----------------------------------------------------------------------|
| `udp://`  | Resolves hostname, dual-stack, uses system address family preference |
| `udp4://` | Forces IPv4                                                          |
| `udp6://` | Forces IPv6                                                          |

## API for other plugins

When the plugin is loaded, it exposes `server.notes.loggelf` which other plugins can use to send structured GELF messages directly.

### `message(callerPlugin, msg)`

Send a raw GELF message. Returns `cfg.last` (boolean) indicating whether further log plugins should be skipped.

```javascript
exports.hook_queue = function (next, connection) {
    const gelf = connection.server.notes.loggelf;

    gelf?.message(this, {
        short_message: 'Mail queued',
        level: 6, // INFO
        _recipient: connection.transaction.rcpt_to.toString(),
        _sender: connection.transaction.mail_from.toString(),
    });

    next(OK);
};
```

### `log(callerPlugin, connection, level, shortMessage, extra)`

Send a log message with automatic `_logger` and `_transaction` fields populated from the plugin and connection objects.

```javascript
gelf?.log(this, connection, 6, 'Mail queued', { _queue: 'outbound' });
```

### Convenience log methods

All methods accept `(callerPlugin, connection, shortMessage, extraFields)`:

```javascript
const gelf = connection.server.notes.loggelf;

gelf?.emergency(this, connection, 'System failure');
gelf?.alert(this, connection, 'Disk almost full');
gelf?.critical(this, connection, 'Database unreachable');
gelf?.error(this, connection, 'Delivery failed', { _recipient: 'user@example.com' });
gelf?.warning(this, connection, 'Rate limit approached');
gelf?.notice(this, connection, 'New connection', { _ip: connection.remote.ip });
gelf?.info(this, connection, 'Message accepted');
gelf?.debug(this, connection, 'Processing step', { _detail: 'some value' });
```

`connection` may be `null` when not in a connection context, in which case `_transaction` will be omitted.

### `getSender(callerPlugin)`

Returns a scoped sender object bound to the plugin's configuration. Useful when sending many messages from the same plugin to avoid repeated config lookups.

```javascript
exports.hook_data_post = function (next, connection) {
    const sender = connection.server.notes.loggelf?.getSender(this);

    sender?.message({
        short_message: 'Data received',
        level: 6,
        _size: connection.transaction.data_bytes,
    });

    next();
};
```

The sender's `message(msg)` method returns a Promise that resolves to `cfg.last`.

### Additional fields

Any extra fields in `msg` are included as GELF additional fields (prefixed with `_`). Fields already prefixed with `_` are passed through as-is. Reserved GELF field names (`host`, `version`, `short_message`, etc.) are never overwritten by additional fields.

| Type      | Behaviour                              |
|-----------|----------------------------------------|
| `string`  | Passed as-is                           |
| `number`  | Passed as-is                           |
| `boolean` | Passed as-is                           |
| `Date`    | Converted to ISO 8601 string           |
| Other     | JSON round-tripped                     |

## GELF message format

Messages are sent as GELF 1.1 JSON over UDP. The following standard GELF fields are supported:

| Field           | Source                                            |
|-----------------|---------------------------------------------------|
| `version`       | Always `"1.1"`                                    |
| `host`          | `msg.host`, `cfg.hostname`, or `os.hostname()`    |
| `short_message` | Required                                          |
| `full_message`  | Optional                                          |
| `timestamp`     | `msg.timestamp` (Date) or current time            |
| `level`         | Syslog severity (0–7)                             |
| `facility`      | Optional                                          |
| `file`          | Optional                                          |
| `line`          | Optional                                          |

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
