'use strict';

const assert = require('node:assert/strict');
const dgram = require('node:dgram');
const zlib = require('node:zlib');
const { describe, it } = require('node:test');
const fixtures = require('haraka-test-fixtures');

const defaultConfig = {
    enabled: true,
    log_hook_enabled: true,
    url: 'udp://graylog.example.test:12201',
    compress: false,
    max_chunk_size: 1420,
    hostname: 'mx.example.test',
    last: false,
    fields: {
        logger: '${logger}',
        connection: '${connection_uuid}',
        transaction: '${transaction_uuid}',
        environment: 'test',
        missing: '${missing_value}',
    },
};

function waitFor(predicate)
{
    return new Promise((resolve, reject) => {
        const deadline = Date.now() + 250;

        function poll()
        {
            if (predicate()) {
                resolve();
            } else if (Date.now() > deadline) {
                reject(new Error('timed out waiting for async GELF send'));
            } else {
                setImmediate(poll);
            }
        }

        poll();
    });
}

function createPlugin(yaml = defaultConfig)
{
    const plugin = new fixtures.plugin('gelf');

    plugin.config.get = () => structuredClone(yaml);
    plugin.logerror = (msg) => {
        plugin.last_err = msg;
    };
    plugin.loginfo = () => {};
    plugin.load_gelf_config();

    return plugin;
}

function installSocketStub(t, opts = {})
{
    const originalCreateSocket = dgram.createSocket;
    const sockets = [];

    dgram.createSocket = (options) => {
        const socket = {
            options,
            packets: [],
            closed: false,
            on() {},
            send(packet, port, host, cb) {
                this.packets.push({
                    packet: Buffer.from(packet),
                    port,
                    host,
                });
                cb?.(opts.sendError);
            },
            close() {
                this.closed = true;
            },
        };
        sockets.push(socket);
        return socket;
    };

    t.after(() => {
        dgram.createSocket = originalCreateSocket;
    });

    return sockets;
}

function installConsoleErrorStub(t)
{
    const originalConsoleError = console.error;
    const messages = [];

    console.error = (msg) => {
        messages.push(msg);
    };

    t.after(() => {
        console.error = originalConsoleError;
    });

    return messages;
}

function decodePacket(socket, index = 0)
{
    return JSON.parse(socket.packets[index].packet.toString('utf8'));
}

function decodeGzipPacket(socket, index = 0)
{
    return JSON.parse(zlib.gunzipSync(socket.packets[index].packet).toString('utf8'));
}

function assertMissingProperty(object, key)
{
    assert.equal(Object.prototype.hasOwnProperty.call(object, key), false);
}

describe('gelf config', () => {
    it('loads defaults and registers sender hooks', () => {
        const plugin = createPlugin();

        plugin.register();

        assert.equal(plugin.cfg.main.enabled, true);
        assert.equal(plugin.cfg.main.log_hook_enabled, true);
        assert.equal(plugin.cfg.main.url, 'udp://graylog.example.test:12201');
        assert.equal(plugin.cfg.main.compress, false);
        assert.equal(plugin.cfg.main.hostname, 'mx.example.test');
        assert.deepEqual(plugin.hooks.init_master, ['init_gelf_sender']);
        assert.deepEqual(plugin.hooks.init_child, ['init_gelf_sender']);
    });

    it('validates invalid urls by disabling that config', () => {
        const plugin = createPlugin({
            ...defaultConfig,
            url: 'tcp://graylog.example.test:12201',
        });

        assert.equal(plugin.cfg.main.enabled, false);
        assert.match(plugin.last_err, /main: invalid url:/);
    });

    it('validates max_chunk_size lower bound', () => {
        const plugin = createPlugin({
            ...defaultConfig,
            max_chunk_size: 63,
        });

        assert.equal(plugin.cfg.main.enabled, false);
        assert.match(plugin.last_err, /main: invalid max_chunk_size: 63/);
    });

    it('validates max_chunk_size upper bound', () => {
        const plugin = createPlugin({
            ...defaultConfig,
            max_chunk_size: 65476,
        });

        assert.equal(plugin.cfg.main.enabled, false);
        assert.match(plugin.last_err, /main: invalid max_chunk_size: 65476/);
    });

    it('validates max_chunk_size integers', () => {
        const plugin = createPlugin({
            ...defaultConfig,
            max_chunk_size: 1420.5,
        });

        assert.equal(plugin.cfg.main.enabled, false);
        assert.match(plugin.last_err, /main: invalid max_chunk_size: 1420.5/);
    });

    it('accepts max_chunk_size string integers', () => {
        const plugin = createPlugin({
            ...defaultConfig,
            max_chunk_size: '8192',
        });

        assert.equal(plugin.cfg.main.enabled, true);
        assert.equal(plugin.cfg.main.max_chunk_size, 8192);
    });

    it('coerces main config string booleans', () => {
        const plugin = createPlugin({
            ...defaultConfig,
            enabled: 'yes',
            log_hook_enabled: '1',
            compress: 'false',
            last: 'no',
        });

        assert.equal(plugin.cfg.main.enabled, true);
        assert.equal(plugin.cfg.main.log_hook_enabled, true);
        assert.equal(plugin.cfg.main.compress, false);
        assert.equal(plugin.cfg.main.last, false);
    });

    it('coerces per-plugin config string booleans', () => {
        const plugin = createPlugin({
            ...defaultConfig,
            plugins: {
                rspamd: {
                    enabled: '0',
                    log_hook_enabled: 'true',
                    compress: 'no',
                    last: '1',
                },
            },
        });

        assert.equal(plugin.cfg.plugins.rspamd.enabled, false);
        assert.equal(plugin.cfg.plugins.rspamd.log_hook_enabled, true);
        assert.equal(plugin.cfg.plugins.rspamd.compress, false);
        assert.equal(plugin.cfg.plugins.rspamd.last, true);
    });

    it('merges per-plugin fields and allows null to suppress inherited fields', () => {
        const plugin = createPlugin({
            ...defaultConfig,
            plugins: {
                rspamd: {
                    last: true,
                    url: 'udp4://rspamd.example.test:12201',
                    fields: {
                        transaction: null,
                        component: 'rspamd',
                    },
                },
            },
        });

        assert.equal(plugin.cfg.plugins.rspamd.last, true);
        assert.equal(plugin.cfg.plugins.rspamd.url, 'udp4://rspamd.example.test:12201');
        assert.equal(plugin.cfg.plugins.rspamd.fields.logger, '${logger}');
        assert.equal(plugin.cfg.plugins.rspamd.fields.transaction, null);
        assert.equal(plugin.cfg.plugins.rspamd.fields.component, 'rspamd');
    });
});

describe('gelf sender api', () => {
    it('creates server.notes when missing', () => {
        const plugin = createPlugin();
        const server = {};
        let nextCalled = false;

        plugin.init_gelf_sender(() => {
            nextCalled = true;
        }, server);

        assert.equal(nextCalled, true);
        assert.equal(typeof server.notes.loggelf.message, 'function');
        assert.equal(plugin.loggelf, server.notes.loggelf);
    });

    it('exposes server.notes.loggelf and sends raw GELF messages', async (t) => {
        const sockets = installSocketStub(t);
        const plugin = createPlugin();
        const server = { notes: {} };

        plugin.init_gelf_sender(() => {}, server);
        const isLast = server.notes.loggelf.message({ name: 'queue' }, {
            short_message: 'Mail queued',
            level: 6,
            recipient: 'user@example.test',
            accepted: true,
            when: new Date('2026-05-13T12:00:00Z'),
            nested: { ok: true },
        });

        assert.equal(isLast, false);
        await waitFor(() => sockets[0]?.packets.length === 1);

        assert.equal(sockets[0].options.type, 'udp6');
        assert.equal(sockets[0].packets[0].host, 'graylog.example.test');
        assert.equal(sockets[0].packets[0].port, 12201);

        const payload = decodePacket(sockets[0]);
        assert.equal(payload.version, '1.1');
        assert.equal(payload.host, 'mx.example.test');
        assert.equal(payload.short_message, 'Mail queued');
        assert.equal(payload.level, 6);
        assert.equal(payload._recipient, 'user@example.test');
        assert.equal(payload._accepted, 'true');
        assert.equal(payload._when, '2026-05-13T12:00:00.000Z');
        assert.deepEqual(payload._nested, { ok: true });
        assert.equal(payload._logger, 'queue');
        assert.equal(payload._environment, 'test');
        assertMissingProperty(payload, '_missing');
    });

    it('creates udp4 sockets for udp4 urls', async (t) => {
        const sockets = installSocketStub(t);
        const plugin = createPlugin({
            ...defaultConfig,
            url: 'udp4://graylog.example.test:12202',
        });
        const server = { notes: {} };

        plugin.init_gelf_sender(() => {}, server);
        server.notes.loggelf.message({ name: 'queue' }, {
            short_message: 'ipv4',
        });

        await waitFor(() => sockets[0]?.packets.length === 1);

        assert.equal(sockets[0].options.type, 'udp4');
        assert.equal(sockets[0].options.ipv6Only, false);
        assert.equal(sockets[0].packets[0].host, 'graylog.example.test');
        assert.equal(sockets[0].packets[0].port, 12202);
    });

    it('creates ipv6-only udp6 sockets for udp6 urls', async (t) => {
        const sockets = installSocketStub(t);
        const plugin = createPlugin({
            ...defaultConfig,
            url: 'udp6://graylog.example.test:12203',
        });
        const server = { notes: {} };

        plugin.init_gelf_sender(() => {}, server);
        server.notes.loggelf.message({ name: 'queue' }, {
            short_message: 'ipv6',
        });

        await waitFor(() => sockets[0]?.packets.length === 1);

        assert.equal(sockets[0].options.type, 'udp6');
        assert.equal(sockets[0].options.ipv6Only, true);
        assert.equal(sockets[0].packets[0].host, 'graylog.example.test');
        assert.equal(sockets[0].packets[0].port, 12203);
    });

    it('defaults GELF UDP urls without ports to 12201', async (t) => {
        const sockets = installSocketStub(t);
        const plugin = createPlugin({
            ...defaultConfig,
            url: 'udp://graylog.example.test',
        });
        const server = { notes: {} };

        plugin.init_gelf_sender(() => {}, server);
        server.notes.loggelf.message({ name: 'queue' }, {
            short_message: 'default port',
        });

        await waitFor(() => sockets[0]?.packets.length === 1);

        assert.equal(sockets[0].options.type, 'udp6');
        assert.equal(sockets[0].options.ipv6Only, false);
        assert.equal(sockets[0].packets[0].host, 'graylog.example.test');
        assert.equal(sockets[0].packets[0].port, 12201);
    });

    it('returns last but does not create sockets when disabled', (t) => {
        const sockets = installSocketStub(t);
        const plugin = createPlugin({
            ...defaultConfig,
            enabled: false,
            last: true,
        });
        const server = { notes: {} };

        plugin.init_gelf_sender(() => {}, server);
        const isLast = server.notes.loggelf.message({ name: 'queue' }, {
            short_message: 'disabled',
        });

        assert.equal(isLast, true);
        assert.equal(sockets.length, 0);
    });

    it('routes caller plugins through matching per-plugin config', async (t) => {
        const sockets = installSocketStub(t);
        const plugin = createPlugin({
            ...defaultConfig,
            plugins: {
                rspamd: {
                    url: 'udp4://rspamd.example.test:12204',
                    last: true,
                    fields: {
                        component: 'rspamd',
                        environment: 'override',
                    },
                },
            },
        });
        const server = { notes: {} };

        plugin.init_gelf_sender(() => {}, server);
        const isLast = server.notes.loggelf.message({ name: 'rspamd' }, {
            short_message: 'rspamd verdict',
        });

        assert.equal(isLast, true);
        await waitFor(() => sockets[0]?.packets.length === 1);

        assert.equal(sockets.length, 1);
        assert.equal(sockets[0].options.type, 'udp4');
        assert.equal(sockets[0].packets[0].host, 'rspamd.example.test');
        assert.equal(sockets[0].packets[0].port, 12204);

        const payload = decodePacket(sockets[0]);
        assert.equal(payload.short_message, 'rspamd verdict');
        assert.equal(payload._logger, 'rspamd');
        assert.equal(payload._component, 'rspamd');
        assert.equal(payload._environment, 'override');
    });

    it('honors disabled per-plugin config overrides', (t) => {
        const sockets = installSocketStub(t);
        const plugin = createPlugin({
            ...defaultConfig,
            plugins: {
                rspamd: {
                    enabled: false,
                    last: true,
                    url: 'udp4://rspamd.example.test:12204',
                },
            },
        });
        const server = { notes: {} };

        plugin.init_gelf_sender(() => {}, server);
        const isLast = server.notes.loggelf.message({ name: 'rspamd' }, {
            short_message: 'disabled override',
        });

        assert.equal(isLast, true);
        assert.equal(sockets.length, 0);
    });

    it('reuses sockets for repeated sends to the same URL', async (t) => {
        const sockets = installSocketStub(t);
        const plugin = createPlugin();
        const server = { notes: {} };

        plugin.init_gelf_sender(() => {}, server);
        server.notes.loggelf.message({ name: 'queue' }, {
            short_message: 'first',
        });
        server.notes.loggelf.message({ name: 'queue' }, {
            short_message: 'second',
        });

        await waitFor(() => sockets[0]?.packets.length === 2);

        assert.equal(sockets.length, 1);
        assert.equal(decodePacket(sockets[0], 0).short_message, 'first');
        assert.equal(decodePacket(sockets[0], 1).short_message, 'second');
    });

    it('creates separate sockets for different per-plugin URLs', async (t) => {
        const sockets = installSocketStub(t);
        const plugin = createPlugin({
            ...defaultConfig,
            plugins: {
                rspamd: {
                    url: 'udp4://rspamd.example.test:12204',
                },
                karma: {
                    url: 'udp4://karma.example.test:12205',
                },
            },
        });
        const server = { notes: {} };

        plugin.init_gelf_sender(() => {}, server);
        server.notes.loggelf.message({ name: 'rspamd' }, {
            short_message: 'rspamd',
        });
        server.notes.loggelf.message({ name: 'karma' }, {
            short_message: 'karma',
        });

        await waitFor(() => sockets.length === 2 && sockets.every((socket) => socket.packets.length === 1));

        assert.equal(sockets[0].packets[0].host, 'rspamd.example.test');
        assert.equal(sockets[0].packets[0].port, 12204);
        assert.equal(decodePacket(sockets[0]).short_message, 'rspamd');
        assert.equal(sockets[1].packets[0].host, 'karma.example.test');
        assert.equal(sockets[1].packets[0].port, 12205);
        assert.equal(decodePacket(sockets[1]).short_message, 'karma');
    });

    it('chunks large GELF UDP payloads', async (t) => {
        const sockets = installSocketStub(t);
        const plugin = createPlugin({
            ...defaultConfig,
            max_chunk_size: 64,
        });
        const server = { notes: {} };
        const longMessage = 'x'.repeat(220);

        plugin.init_gelf_sender(() => {}, server);
        server.notes.loggelf.message({ name: 'queue' }, {
            short_message: longMessage,
        });

        await waitFor(() => sockets[0]?.packets.length > 1);

        const packets = sockets[0].packets.map(({ packet }) => packet);
        const messageId = packets[0].subarray(2, 10);
        const chunkCount = packets[0][11];

        assert.equal(chunkCount, packets.length);
        assert.equal(chunkCount > 1, true);

        for (let seq = 0; seq < packets.length; seq++) {
            assert.equal(packets[seq][0], 0x1e);
            assert.equal(packets[seq][1], 0x0f);
            assert.deepEqual(packets[seq].subarray(2, 10), messageId);
            assert.equal(packets[seq][10], seq);
            assert.equal(packets[seq][11], chunkCount);
            assert.equal(packets[seq].length <= 64, true);
        }

        const reassembled = Buffer.concat(packets.map((packet) => packet.subarray(12)));
        const payload = JSON.parse(reassembled.toString('utf8'));

        assert.equal(payload.short_message, longMessage);
        assert.equal(payload._logger, 'queue');
    });

    it('gzip compresses GELF UDP payloads when enabled', async (t) => {
        const sockets = installSocketStub(t);
        const plugin = createPlugin({
            ...defaultConfig,
            compress: true,
        });
        const server = { notes: {} };

        plugin.init_gelf_sender(() => {}, server);
        server.notes.loggelf.message({ name: 'queue' }, {
            short_message: 'compressed payload',
            detail: 'kept',
        });

        await waitFor(() => sockets[0]?.packets.length === 1);

        assert.equal(sockets[0].packets[0].packet[0], 0x1f);
        assert.equal(sockets[0].packets[0].packet[1], 0x8b);

        const payload = decodeGzipPacket(sockets[0]);
        assert.equal(payload.short_message, 'compressed payload');
        assert.equal(payload._detail, 'kept');
        assert.equal(payload._logger, 'queue');
    });

    it('logs and suppresses GELF UDP chunk limit failures', async (t) => {
        const sockets = installSocketStub(t);
        const errors = installConsoleErrorStub(t);
        const plugin = createPlugin({
            ...defaultConfig,
            max_chunk_size: 64,
        });
        const server = { notes: {} };
        const tooLongMessage = 'x'.repeat(8000);

        plugin.init_gelf_sender(() => {}, server);
        assert.doesNotThrow(() => {
            server.notes.loggelf.message({ name: 'queue' }, {
                short_message: tooLongMessage,
            });
        });

        await waitFor(() => errors.length === 1);

        assert.equal(sockets.length, 1);
        assert.equal(sockets[0].packets.length, 0);
        assert.match(errors[0], /GELF UDP socket error: udp:\/\/graylog\.example\.test:12201:/);
        assert.match(errors[0], /exceeds GELF UDP limit/);
    });

    it('logs socket send callback errors without using Haraka logging', async (t) => {
        const sockets = installSocketStub(t, {
            sendError: new Error('send failed'),
        });
        const errors = installConsoleErrorStub(t);
        const plugin = createPlugin();
        const server = { notes: {} };
        const harakaErrors = [];

        plugin.logerror = (msg) => {
            harakaErrors.push(msg);
        };

        plugin.init_gelf_sender(() => {}, server);
        server.notes.loggelf.message({ name: 'queue' }, {
            short_message: 'send callback failure',
        });

        await waitFor(() => errors.length === 1);

        assert.equal(sockets[0].packets.length, 1);
        assert.deepEqual(harakaErrors, []);
        assert.equal(errors[0], 'GELF UDP send failed: send failed');
    });

    it('does not duplicate reserved GELF fields as additional fields', async (t) => {
        const sockets = installSocketStub(t);
        const plugin = createPlugin();
        const server = { notes: {} };

        plugin.init_gelf_sender(() => {}, server);
        server.notes.loggelf.message({ name: 'queue' }, {
            id: 'id-1',
            version: '9.9',
            host: 'host.example.test',
            short_message: 'reserved fields',
            full_message: 'full message',
            timestamp: 1778673600,
            level: 4,
            facility: 'mail',
            file: 'queue.js',
            line: 42,
            detail: 'kept',
        });

        await waitFor(() => sockets[0]?.packets.length === 1);

        const payload = decodePacket(sockets[0]);
        assert.equal(payload.version, '1.1');
        assert.equal(payload.host, 'host.example.test');
        assert.equal(payload.short_message, 'reserved fields');
        assert.equal(payload.full_message, 'full message');
        assert.equal(payload.timestamp, 1778673600);
        assert.equal(payload.level, 4);
        assert.equal(payload.facility, 'mail');
        assert.equal(payload.file, 'queue.js');
        assert.equal(payload.line, 42);
        assert.equal(payload._detail, 'kept');

        for (const reserved of [
            'id',
            'version',
            'host',
            'short_message',
            'full_message',
            'timestamp',
            'level',
            'facility',
            'file',
            'line',
        ]) {
            assertMissingProperty(payload, `_${reserved}`);
        }
    });

    it('omits null and undefined additional fields', async (t) => {
        const sockets = installSocketStub(t);
        const plugin = createPlugin();
        const server = { notes: {} };

        plugin.init_gelf_sender(() => {}, server);
        server.notes.loggelf.message({ name: 'queue' }, {
            short_message: 'nullable fields',
            null_field: null,
            undefined_field: undefined,
            kept: 'value',
        });

        await waitFor(() => sockets[0]?.packets.length === 1);

        const payload = decodePacket(sockets[0]);
        assertMissingProperty(payload, '_null_field');
        assertMissingProperty(payload, '_undefined_field');
        assert.equal(payload._kept, 'value');
    });

    it('falls back to stringifying circular additional fields', async (t) => {
        const sockets = installSocketStub(t);
        const plugin = createPlugin();
        const server = { notes: {} };
        const circular = {
            name: 'circular',
        };
        circular.self = circular;

        plugin.init_gelf_sender(() => {}, server);
        server.notes.loggelf.message({ name: 'queue' }, {
            short_message: 'circular field',
            circular,
        });

        await waitFor(() => sockets[0]?.packets.length === 1);

        const payload = decodePacket(sockets[0]);
        assert.equal(payload._circular, '[object Object]');
    });

    it('normalizes standard GELF timestamp fields', async (t) => {
        const sockets = installSocketStub(t);
        const plugin = createPlugin();
        const server = { notes: {} };

        plugin.init_gelf_sender(() => {}, server);
        server.notes.loggelf.message({ name: 'queue' }, {
            short_message: 'date timestamp',
            timestamp: new Date('2026-05-13T12:00:00Z'),
        });
        server.notes.loggelf.message({ name: 'queue' }, {
            short_message: 'string timestamp',
            timestamp: '1778673600.25',
        });
        server.notes.loggelf.message({ name: 'queue' }, {
            short_message: 'number timestamp',
            timestamp: 1778673600.5,
        });

        await waitFor(() => sockets[0]?.packets.length === 3);

        assert.equal(decodePacket(sockets[0], 0).timestamp, 1778673600);
        assert.equal(decodePacket(sockets[0], 1).timestamp, 1778673600.25);
        assert.equal(decodePacket(sockets[0], 2).timestamp, 1778673600.5);
    });

    it('falls back for invalid standard GELF level and line fields', async (t) => {
        const sockets = installSocketStub(t);
        const plugin = createPlugin();
        const server = { notes: {} };

        plugin.init_gelf_sender(() => {}, server);
        server.notes.loggelf.message({ name: 'queue' }, {
            short_message: 'invalid standard fields',
            timestamp: 'not a timestamp',
            level: 'not a level',
            line: 'not a line',
        });

        await waitFor(() => sockets[0]?.packets.length === 1);

        const payload = decodePacket(sockets[0]);
        assert.equal(payload.level, 6);
        assertMissingProperty(payload, 'line');
        assert.equal(typeof payload.timestamp, 'number');
        assert.equal(Number.isFinite(payload.timestamp), true);
    });

    it('stringifies optional standard GELF fields', async (t) => {
        const sockets = installSocketStub(t);
        const plugin = createPlugin();
        const server = { notes: {} };

        plugin.init_gelf_sender(() => {}, server);
        server.notes.loggelf.message({ name: 'queue' }, {
            short_message: 12345,
            full_message: { detail: 'full' },
            facility: 456,
            file: ['queue.js'],
            line: '42',
        });

        await waitFor(() => sockets[0]?.packets.length === 1);

        const payload = decodePacket(sockets[0]);
        assert.equal(payload.short_message, '12345');
        assert.equal(payload.full_message, '[object Object]');
        assert.equal(payload.facility, '456');
        assert.equal(payload.file, 'queue.js');
        assert.equal(payload.line, 42);
    });

    it('interpolates config fields and allows reserved field overrides', async (t) => {
        const sockets = installSocketStub(t);
        const plugin = createPlugin({
            ...defaultConfig,
            fields: {
                host: 'configured-host.example.test',
                facility: '${logger}:${connection_uuid}:${transaction_uuid}',
                route: '${logger}/${connection_uuid}/${transaction_uuid}',
                missing: '${missing_value}',
            },
        });
        const server = { notes: {} };
        const connection = {
            uuid: 'CONN-2',
            transaction: {
                uuid: 'CONN-2.1',
            },
        };

        plugin.init_gelf_sender(() => {}, server);
        server.notes.loggelf.info({ name: 'queue' }, connection, 'field interpolation');

        await waitFor(() => sockets[0]?.packets.length === 1);

        const payload = decodePacket(sockets[0]);
        assert.equal(payload.host, 'configured-host.example.test');
        assert.equal(payload.facility, 'queue:CONN-2:CONN-2.1');
        assert.equal(payload._route, 'queue/CONN-2/CONN-2.1');
        assertMissingProperty(payload, '_missing');
        assertMissingProperty(payload, '_host');
        assertMissingProperty(payload, '_facility');
    });

    it('omits inherited interpolated fields suppressed by per-plugin null', async (t) => {
        const sockets = installSocketStub(t);
        const plugin = createPlugin({
            ...defaultConfig,
            fields: {
                logger: '${logger}',
                connection: '${connection_uuid}',
                transaction: '${transaction_uuid}',
            },
            plugins: {
                rspamd: {
                    fields: {
                        transaction: null,
                    },
                },
            },
        });
        const server = { notes: {} };
        const connection = {
            uuid: 'CONN-3',
            transaction: {
                uuid: 'CONN-3.1',
            },
        };

        plugin.init_gelf_sender(() => {}, server);
        server.notes.loggelf.info({ name: 'rspamd' }, connection, 'null field');

        await waitFor(() => sockets[0]?.packets.length === 1);

        const payload = decodePacket(sockets[0]);
        assert.equal(payload._logger, 'rspamd');
        assert.equal(payload._connection, 'CONN-3');
        assertMissingProperty(payload, '_transaction');
    });

    it('formats structured convenience logs with connection variables', async (t) => {
        const sockets = installSocketStub(t);
        const plugin = createPlugin();
        const server = { notes: {} };
        const connection = {
            uuid: 'CONN-1',
            transaction: {
                uuid: 'CONN-1.1',
            },
        };

        plugin.init_gelf_sender(() => {}, server);
        server.notes.loggelf.error({ name: 'queue' }, connection, 'Delivery failed', {
            queue_id: 'abc123',
        });

        await waitFor(() => sockets[0]?.packets.length === 1);

        const payload = decodePacket(sockets[0]);
        assert.equal(payload.short_message, '[CONN-1.1] [queue] Delivery failed');
        assert.equal(payload.level, 3);
        assert.equal(payload._queue_id, 'abc123');
        assert.equal(payload._connection, 'CONN-1');
        assert.equal(payload._transaction, 'CONN-1.1');
    });

    it('maps convenience logging methods to syslog levels', async (t) => {
        const sockets = installSocketStub(t);
        const plugin = createPlugin();
        const server = { notes: {} };
        const levels = [
            ['emergency', 0],
            ['alert', 1],
            ['critical', 2],
            ['error', 3],
            ['warning', 4],
            ['notice', 5],
            ['info', 6],
            ['debug', 7],
        ];

        plugin.init_gelf_sender(() => {}, server);
        for (const [method] of levels) {
            server.notes.loggelf[method]({ name: 'queue' }, null, `${method} message`);
        }

        await waitFor(() => sockets[0]?.packets.length === levels.length);

        for (let i = 0; i < levels.length; i++) {
            const [method, level] = levels[i];
            const payload = decodePacket(sockets[0], i);

            assert.equal(payload.level, level);
            assert.equal(payload.short_message, `[-] [queue] ${method} message`);
        }
    });

    it('closes opened sockets', (t) => {
        const sockets = installSocketStub(t);
        const plugin = createPlugin();
        const server = { notes: {} };

        plugin.init_gelf_sender(() => {}, server);
        server.notes.loggelf.message({ name: 'queue' }, { short_message: 'test' });
        server.notes.loggelf.close();

        assert.equal(sockets[0].closed, true);
    });
});

describe('hook_log', () => {
    it('does nothing before the sender has been initialized', () => {
        const plugin = createPlugin();
        let nextArg = 'not called';

        plugin.hook_log((arg) => {
            nextArg = arg;
        }, null, {
            level: 'INFO',
            data: '[INFO] [CONN-1] [queue] queued',
        });

        assert.equal(nextArg, undefined);
    });

    it('forwards Haraka log records and honors last=true', async (t) => {
        const sockets = installSocketStub(t);
        const plugin = createPlugin({
            ...defaultConfig,
            last: true,
        });
        const server = { notes: {} };
        let nextArg;

        plugin.init_gelf_sender(() => {}, server);
        plugin.hook_log((arg) => {
            nextArg = arg;
        }, null, {
            level: 'DATA',
            data: '[DATA] [CONN-1.2] [queue] client command',
        });

        await waitFor(() => sockets[0]?.packets.length === 1);

        const payload = decodePacket(sockets[0]);
        assert.equal(payload.short_message, '[CONN-1.2] [queue] client command');
        assert.equal(payload.level, 7);
        assert.equal(payload._logger, 'queue');
        assert.equal(payload._connection, 'CONN-1');
        assert.equal(payload._transaction, 'CONN-1.2');
        assert.equal(nextArg, OK);
    });

    it('forwards non-Haraka formatted log data as the short message', async (t) => {
        const sockets = installSocketStub(t);
        const plugin = createPlugin();
        const server = { notes: {} };
        let nextArg = 'not called';

        plugin.init_gelf_sender(() => {}, server);
        plugin.hook_log((arg) => {
            nextArg = arg;
        }, null, {
            level: 'INFO',
            data: 'raw logger output',
        });

        await waitFor(() => sockets[0]?.packets.length === 1);

        const payload = decodePacket(sockets[0]);
        assert.equal(payload.short_message, 'raw logger output');
        assert.equal(payload.level, 6);
        assertMissingProperty(payload, '_logger');
        assertMissingProperty(payload, '_connection');
        assertMissingProperty(payload, '_transaction');
        assert.equal(nextArg, undefined);
    });

    it('extracts connection UUIDs without transaction suffixes', async (t) => {
        const sockets = installSocketStub(t);
        const plugin = createPlugin();
        const server = { notes: {} };

        plugin.init_gelf_sender(() => {}, server);
        plugin.hook_log(() => {}, null, {
            level: 'INFO',
            data: '[INFO] [CONN-4] [queue] connection log',
        });

        await waitFor(() => sockets[0]?.packets.length === 1);

        const payload = decodePacket(sockets[0]);
        assert.equal(payload.short_message, '[CONN-4] [queue] connection log');
        assert.equal(payload._logger, 'queue');
        assert.equal(payload._connection, 'CONN-4');
        assertMissingProperty(payload, '_transaction');
    });

    it('maps PROTOCOL log level to debug', async (t) => {
        const sockets = installSocketStub(t);
        const plugin = createPlugin();
        const server = { notes: {} };

        plugin.init_gelf_sender(() => {}, server);
        plugin.hook_log(() => {}, null, {
            level: 'PROTOCOL',
            data: '[PROTOCOL] [CONN-5] [queue] protocol log',
        });

        await waitFor(() => sockets[0]?.packets.length === 1);

        assert.equal(decodePacket(sockets[0]).level, 7);
    });

    it('maps unknown log levels to debug', async (t) => {
        const sockets = installSocketStub(t);
        const plugin = createPlugin();
        const server = { notes: {} };

        plugin.init_gelf_sender(() => {}, server);
        plugin.hook_log(() => {}, null, {
            level: 'BOGUS',
            data: '[BOGUS] [CONN-6] [queue] unknown level',
        });

        await waitFor(() => sockets[0]?.packets.length === 1);

        assert.equal(decodePacket(sockets[0]).level, 7);
    });

    it('uses per-plugin log_hook_enabled=true when main log hook is disabled', async (t) => {
        const sockets = installSocketStub(t);
        const plugin = createPlugin({
            ...defaultConfig,
            log_hook_enabled: false,
            plugins: {
                rspamd: {
                    log_hook_enabled: true,
                    fields: {
                        component: 'rspamd',
                    },
                },
            },
        });
        const server = { notes: {} };

        plugin.init_gelf_sender(() => {}, server);
        plugin.hook_log(() => {}, null, {
            level: 'INFO',
            data: '[INFO] [CONN-7] [rspamd] per-plugin enabled',
        });

        await waitFor(() => sockets[0]?.packets.length === 1);

        const payload = decodePacket(sockets[0]);
        assert.equal(payload.short_message, '[CONN-7] [rspamd] per-plugin enabled');
        assert.equal(payload._logger, 'rspamd');
        assert.equal(payload._component, 'rspamd');
    });

    it('uses per-plugin log_hook_enabled=false when main log hook is enabled', (t) => {
        const sockets = installSocketStub(t);
        const plugin = createPlugin({
            ...defaultConfig,
            log_hook_enabled: true,
            plugins: {
                rspamd: {
                    log_hook_enabled: false,
                },
            },
        });
        const server = { notes: {} };
        let nextCalled = false;

        plugin.init_gelf_sender(() => {}, server);
        plugin.hook_log(() => {
            nextCalled = true;
        }, null, {
            level: 'INFO',
            data: '[INFO] [CONN-8] [rspamd] per-plugin disabled',
        });

        assert.equal(nextCalled, true);
        assert.equal(sockets.length, 0);
    });

    it('skips forwarding when log_hook_enabled is false', () => {
        const plugin = createPlugin({
            ...defaultConfig,
            log_hook_enabled: false,
        });
        const server = { notes: {} };
        let nextCalled = false;

        plugin.init_gelf_sender(() => {}, server);
        plugin.loggelf.message = () => {
            throw new Error('should not send');
        };

        plugin.hook_log(() => {
            nextCalled = true;
        }, null, {
            level: 'INFO',
            data: '[INFO] [CONN-1] [queue] queued',
        });

        assert.equal(nextCalled, true);
    });
});
