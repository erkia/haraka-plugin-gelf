'use strict';

const os = require('node:os');
const dns  = require('node:dns');
const dgram = require('node:dgram');
const zlib = require('node:zlib');
const gzip = require('node:util').promisify(zlib.gzip);
const crypto = require('node:crypto');

const GELF_VERSION = '1.1';
const CHUNK_MAGIC_0 = 0x1e;
const CHUNK_MAGIC_1 = 0x0f;
const GELF_RESERVED = new Set([
    'id', 'version', 'host', 'short_message', 'full_message',
    'timestamp', 'level', 'facility', 'line', 'file',
]);

const LogLevel = Object.freeze({
    EMERG:  0,
    ALERT:  1,
    CRIT:   2,
    ERROR:  3,
    WARN:   4,
    NOTICE: 5,
    INFO:   6,
    DEBUG:  7,
});

function toBool(value, defaultValue)
{
    if (typeof value === 'boolean') {
        return value;
    } else if (typeof value === 'string') {
        if (/^(true|yes|1)$/i.test(value)) {
            return true;
        } else if (/^(false|no|0)$/i.test(value)) {
            return false;
        }
    }

    return defaultValue;
}

function resolveConfig(plugin, name, main, ovr = {})
{
    const out = { ...main };

    if ('enabled' in ovr)           out.enabled = toBool(ovr.enabled, true);
    if ('log_hook_enabled' in ovr)  out.log_hook_enabled = toBool(ovr.log_hook_enabled, true);
    if ('url' in ovr)               out.url = ovr.url;
    if ('compress' in ovr)          out.compress = toBool(ovr.compress, true);
    if ('last' in ovr)              out.last = toBool(ovr.last, false);
    if ('max_chunk_size' in ovr)    out.max_chunk_size = Number(ovr.max_chunk_size);
    if ('hostname' in ovr)          out.hostname = ovr.hostname;

    // Have some sane minimum limit and theoretical maximum limit for max_chunk_size.
    // Actual meaningful values depend on network MTU and Graylog components.
    if (!Number.isInteger(out.max_chunk_size) || out.max_chunk_size < 64 || out.max_chunk_size > 65475) {
        plugin.logerror(`${name}: invalid max_chunk_size: ${out.max_chunk_size}`);
        out.enabled = false;
    }

    try {
        const { protocol } = new URL(out.url);
        if (protocol !== "udp" && protocol !== "udp4" && protocol !== "udp6") {
            throw new Error('protocol not supported');
        }
    } catch (err) {
        plugin.logerror(`${name}: invalid url: ${out.url} (${err.message})`);
        out.enabled = false;
    }

    return out;
}

function stringify(value)
{
    if (typeof value === 'string') {
        return value;
    } else if (value === undefined || value === null) {
        return '';
    } else {
        return String(value);
    }
}

function jsonify(value)
{
    try {
        return JSON.parse(JSON.stringify(value));
    } catch {
        return stringify(value);
    }
}

function sanitizeValue(value)
{
    if (value instanceof Error) {
        return {
            name: value.name,
            message: value.message,
            stack: value.stack,
        };
    }

    if (value instanceof Date) {
        return value.toISOString();
    }

    if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') {
        return value;
    }

    return jsonify(value);
}

function normalizeInteger(value, defaultValue)
{
    value = Number(value);
    if (!Number.isInteger(value)) {
        value = defaultValue;
    }
    return value;
}

function normalizeTimestamp(value)
{
    if (value instanceof Date) {
        return value.getTime() / 1000;
    } else if (typeof value === 'number' && Number.isFinite(value)) {
        return value;
    } else if (typeof value === 'string' && value.trim() !== '') {
        const n = Number(value);
        if (Number.isFinite(n)) {
            return n;
        }
    }

    return Date.now() / 1000;
}

function createMessage(cfg, msg)
{
    const out = {
        version: GELF_VERSION,
        host: stringify(msg.host || cfg.hostname),
        short_message: stringify(msg.short_message),
        full_message: (msg.full_message !== undefined ? stringify(msg.full_message) : undefined),
        timestamp: normalizeTimestamp(msg.timestamp),
        level: normalizeInteger(msg.level, LogLevel.INFO),
        facility: (msg.facility !== undefined ? stringify(msg.facility) : undefined),
        file: (msg.file !== undefined ? stringify(msg.file) : undefined),
        line: normalizeInteger(msg.line, undefined),
    };


    // Convert all remaining custom fields to GELF additional fields (_foo).
    for (const [key, value] of Object.entries(msg)) {
        const additional_key = key.startsWith('_') ? key : `_${key}`;
        if (GELF_RESERVED.has(additional_key.slice(1)) || value === null || value === undefined) {
            continue;
        }
        out[additional_key] = sanitizeValue(value);
    }

    return out;
}

async function sendGelf(socket, cfg, message)
{
    const payload = createMessage(cfg, message);

    let buffer = Buffer.from(JSON.stringify(payload), 'utf8');

    if (cfg.compress) {
        buffer = await gzip(buffer);
    }

    if (buffer.length <= cfg.max_chunk_size) {
        socket.send(buffer, (err) => {
            if (err) {
                // Do not use Haraka logger to avoid log loop
                console.error(`GELF UDP send failed: ${err.message}`);
            }
        });
        return;
    }

    // GELF chunked UDP header:
    // 2 bytes magic + 8 bytes message id + 1 byte seq + 1 byte seq count
    const messageId = crypto.randomBytes(8);
    const headerSize = 12;
    const chunkDataSize = cfg.max_chunk_size - headerSize;

    if (chunkDataSize <= 0) {
        throw new Error('chunkDataSize <= 0');
    }

    const chunks = Math.ceil(buffer.length / chunkDataSize);

    if (chunks > 128) {
        throw new Error(`GELF payload requires ${chunks} chunks, exceeds GELF UDP limit`);
    }

    for (let seq = 0; seq < chunks; seq++) {
        const start = seq * chunkDataSize;
        const end = Math.min(start + chunkDataSize, buffer.length);
        const part = buffer.subarray(start, end);

        const packet = Buffer.allocUnsafe(headerSize + part.length);
        packet[0] = CHUNK_MAGIC_0;
        packet[1] = CHUNK_MAGIC_1;
        messageId.copy(packet, 2);
        packet[10] = seq;
        packet[11] = chunks;
        part.copy(packet, headerSize);

        socket.send(packet, (err) => {
            if (err) {
                // Do not use Haraka logger to avoid log loop
                console.error(`GELF UDP chunk send failed: ${err.message}`);
            }
        });
    }
}

function getConfig(plugin, callerPlugin)
{
    const pluginName = callerPlugin?.name;
    if (pluginName && plugin.cfg.plugins[pluginName]) {
        return plugin.cfg.plugins[pluginName];
    } else {
        return plugin.cfg.main;
    }
}

exports.register = function ()
{
    const plugin = this;

    plugin.load_gelf_config();

    // Initialize once per Haraka process.
    plugin.register_hook('init_master', 'init_gelf_sender');
    plugin.register_hook('init_child', 'init_gelf_sender');
};

exports.load_gelf_config = function ()
{
    const plugin = this;

    const cfg = plugin.config.get(
        'gelf.ini',
        {
            booleans: [
                '+main.enabled',
                '+main.log_hook_enabled',
                '+main.compress',
                '-main.last',
            ],
        },
        () => {
            plugin.load_gelf_config();
        }
    ) || {};

    // Pass through resolveConfig() for validation
    cfg.main = resolveConfig(plugin, 'main', {
        enabled: toBool(cfg.main?.enabled, true),
        log_hook_enabled: toBool(cfg.main?.log_hook_enabled, true),
        url: cfg.main?.url || 'udp://localhost:12201',
        compress: toBool(cfg.main?.compress, true),
        last: toBool(cfg.main?.last, false),
        max_chunk_size: Number(cfg.main?.max_chunk_size || 1420),
        hostname: cfg.main?.hostname || os.hostname(),
    });

    const plugins = {};
    for (const [pluginName, pluginCfg] of Object.entries(cfg.plugins || {})) {
        if (!pluginCfg || typeof pluginCfg !== 'object') {
            continue;
        }
        plugins[pluginName] = resolveConfig(plugin, `plugins.${pluginName}`, cfg.main, pluginCfg);
    }
    cfg.plugins = plugins;

    plugin.cfg = cfg;

    plugin.loginfo("config loaded");
};

exports.init_gelf_sender = function (next, server)
{
    const plugin = this;

    if (!server.notes) {
        server.notes = {};
    }

    if (server.notes.loggelf) {
        // Already initialized
        return next();
    }

    const sockets = new Map();

    const lookup = (hostname, family, cb) =>
    {
        dns.lookup(hostname, { all: true, family: family, order: 'verbatim' }, (err, addresses) =>
        {
            if (err || !addresses || !addresses.length) {
                cb(new Error(`GELF UDP host lookup failed: ${hostname}: ${err?.message}`));
                return;
            }

            const resolvedFamily = addresses[0].family;

            // For udp6 socket but IPv4 host we need to use IPv4-mapped IPv6 address
            const resolvedAddress = (family !== 4 && resolvedFamily === 4 ? `::ffff:${addresses[0].address}` : addresses[0].address);

            cb(null, resolvedAddress, resolvedFamily);
        });
    };

    const resolveUrl = (url) =>
    {
        const { protocol: scheme, hostname, port } = new URL(url);

        const protocol = (scheme ?? 'udp:').slice(0, -1);

        // Find preferred address family by configuration
        let family = 0;
        if (protocol === "udp4") {
            family = 4;
        } else if (protocol === "udp6") {
            family = 6;
        } else if (protocol !== "udp") {
            throw new Error(`Invalid protocol: ${protocol}`);
        }

        // Create udp6 dual-stack socket unless udp4 or udp6 is explicitly requested.
        // On systems, where IPv6 stack is not enabled, udp4 must be used in configuration.
        const socket = dgram.createSocket({
            type:       (family === 4 ? 'udp4' : 'udp6'),
            ipv6Only:   (family === 6),
            lookup:     (lookuphost, _, cb) => lookup(lookuphost, family, cb),
        });
        socket.on('error', (err) => {
            // Do not use Haraka logger to avoid log loop
            console.error(`GELF UDP socket error: ${url}: ${err.message}`);
        });

        return {
            family: family,
            hostname : hostname,
            port: (port ? parseInt(port) : 12201),
            socket: socket,
            send(packet, cb) {
                if (this.socket) {
                    this.socket.send(packet, this.port, this.hostname, cb);
                } else {
                    cb(new Error('GELF UDP socket not opened'));
                }
            },
        };
    };

    const getSocket = (url) =>
    {
        if (sockets.has(url)) {
            return sockets.get(url);
        }

        const socket = resolveUrl(url);

        sockets.set(url, socket);

        return socket;
    };

    server.notes.loggelf = {

        getSender(callerPlugin)
        {
            const pluginCfg = getConfig(plugin, callerPlugin);

            if (!pluginCfg.enabled) {
                return {
                    message(msg) {
                        return pluginCfg.last;
                    },
                };
            }

            try {
                const socket = getSocket(pluginCfg.url);
                return {
                    message(msg) {
                        sendGelf(socket, pluginCfg, msg).catch((err) => {
                            // Do not use Haraka logger to avoid log loop
                            console.error(err.message);
                        });
                        return pluginCfg.last;
                    },
                };
            } catch (err) {
                return {
                    message(msg) {
                        console.error(`GELF UDP socket error: ${pluginCfg.url}: ${err.message}`);
                        return pluginCfg.last;
                    },
                };
            }
        },

        message(callerPlugin, msg)
        {
            const pluginCfg = getConfig(plugin, callerPlugin);

            if (pluginCfg.enabled) {
                (async () => {
                    try {
                        const socket = getSocket(pluginCfg.url);
                        await sendGelf(socket, pluginCfg, msg);
                    } catch (err) {
                        // Do not use Haraka logger to avoid log loop
                        console.error(err.message);
                    }
                })();
            }

            return pluginCfg.last;
        },

        log(callerPlugin, connection, level, shortMessage, extra = {})
        {
            const pluginName = callerPlugin?.name;
            const txid = connection?.transaction?.uuid;

            return this.message(callerPlugin, {
                ...extra,
                level,
                short_message: `[${(txid ?? '-')}] [${pluginName ?? '-'}] ${shortMessage}`,
                _logger: pluginName,
                _connection: connection?.uuid,
                _transaction: txid,
            });
        },

        emergency(callerPlugin, connection, shortMessage, extra = {}) {
            return this.log(callerPlugin, connection, LogLevel.EMERG, shortMessage, extra);
        },

        alert(callerPlugin, connection, shortMessage, extra = {}) {
            return this.log(callerPlugin, connection, LogLevel.ALERT, shortMessage, extra);
        },

        critical(callerPlugin, connection, shortMessage, extra = {}) {
            return this.log(callerPlugin, connection, LogLevel.CRIT, shortMessage, extra);
        },

        error(callerPlugin, connection, shortMessage, extra = {}) {
            return this.log(callerPlugin, connection, LogLevel.ERROR, shortMessage, extra);
        },

        warning(callerPlugin, connection, shortMessage, extra = {}) {
            return this.log(callerPlugin, connection, LogLevel.WARN, shortMessage, extra);
        },

        notice(callerPlugin, connection, shortMessage, extra = {}) {
            return this.log(callerPlugin, connection, LogLevel.NOTICE, shortMessage, extra);
        },

        info(callerPlugin, connection, shortMessage, extra = {}) {
            return this.log(callerPlugin, connection, LogLevel.INFO, shortMessage, extra);
        },

        debug(callerPlugin, connection, shortMessage, extra = {}) {
            return this.log(callerPlugin, connection, LogLevel.DEBUG, shortMessage, extra);
        },

        close() {
            for (const sock of sockets.values()) {
                if (sock.socket) {
                    try {
                        sock.socket.close();
                    } catch (err) {
                        plugin.logerror(`socket.close(): ${err.message}`);
                    } finally {
                        sock.socket = null;
                    }
                }
            }
            sockets.clear();
        },

    };

    plugin.loggelf = server.notes.loggelf;

    plugin.loginfo('GELF UDP sender ready');

    next();
};

exports.hook_log = function (next, logger, log)
{
    const plugin = this;

    if (!plugin.loggelf) {
        return next();
    }

    const msg = {
        level: LogLevel[log.level.toUpperCase()] ?? LogLevel.DEBUG,
        short_message: undefined,
        _connection: undefined,
        _transaction: undefined,
        _logger: undefined,
    };

    // Get UUID and caller plugin name from log message
    const match = log.data.match(/^\[([^\]]+)\] \[([^\]]+)\] \[([^\]]+)\] (.+)$/);
    if (match) {
        if (match[2] !== '-') {
            // UUID is
            //       connection UUID (i.e. 481ED4FE-1B62-49CF-B581-F7B6641256BB)
            //   or transaction UUID (i.e. 481ED4FE-1B62-49CF-B581-F7B6641256BB.1)
            const uuidmatch = match[2].match(/^([^\.]+)(\.([0-9]+))?$/i);
            if (uuidmatch) {
                msg._connection = uuidmatch[1];
                if (uuidmatch[3]) {
                    msg._transaction = uuidmatch[0];
                }
            }
        }
        if (match[3] !== '-') {
            msg._logger = match[3];
        }
        // Remove log level, but keep UUID and plugin name
        msg.short_message = `[${match[2]}] [${match[3]}] ${match[4]}`;
    } else {
        msg.short_message = log.data;
    }

    // Make fake callerPlugin to pass to our functions, which currently only use the name of the plugin.
    const callerPlugin = { name: msg._logger };

    const pluginCfg = getConfig(plugin, callerPlugin);
    if (!pluginCfg.log_hook_enabled) {
        return next();
    }

    const is_last = plugin.loggelf.message(callerPlugin, msg);

    if (is_last) {
        // Skip all following logger plugins
        return next(OK);
    } else {
        return next();
    }
};
