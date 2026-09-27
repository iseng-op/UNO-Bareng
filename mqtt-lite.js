/*
 * mqtt-lite.js
 * Minimal MQTT 3.1.1 client for browsers over WebSocket.
 * This is intentionally dependency-free so the UNO page does not need
 * a third-party JavaScript CDN just to load the game.
 */
(function (global) {
  'use strict';

  const te = new TextEncoder();
  const td = new TextDecoder();

  function utf8(s) { return te.encode(String(s)); }
  function u16(n) { return new Uint8Array([(n >>> 8) & 255, n & 255]); }
  function concat() {
    let total = 0;
    for (const a of arguments) total += a.length;
    const out = new Uint8Array(total);
    let off = 0;
    for (const a of arguments) { out.set(a, off); off += a.length; }
    return out;
  }
  function strField(s) {
    const b = utf8(s);
    return concat(u16(b.length), b);
  }
  function encodeRemainingLength(n) {
    const out = [];
    do {
      let d = n % 128;
      n = Math.floor(n / 128);
      if (n > 0) d |= 128;
      out.push(d);
    } while (n > 0);
    return new Uint8Array(out);
  }
  function packet(typeFlags, body) {
    return concat(new Uint8Array([typeFlags]), encodeRemainingLength(body.length), body);
  }
  function readRemainingLength(a, off) {
    let multiplier = 1, value = 0, i = off;
    for (; i < a.length && i < off + 4; i++) {
      const d = a[i];
      value += (d & 127) * multiplier;
      if ((d & 128) === 0) return { value, next: i + 1 };
      multiplier *= 128;
    }
    return null;
  }
  function readUtf8(a, off) {
    if (off + 2 > a.length) return null;
    const len = (a[off] << 8) | a[off + 1];
    const start = off + 2, end = start + len;
    if (end > a.length) return null;
    return { value: td.decode(a.slice(start, end)), next: end };
  }

  class Emitter {
    constructor() { this._events = Object.create(null); }
    on(name, fn) { (this._events[name] ||= []).push(fn); return this; }
    once(name, fn) {
      const wrap = (...args) => { this.off(name, wrap); fn(...args); };
      return this.on(name, wrap);
    }
    off(name, fn) {
      const a = this._events[name];
      if (!a) return this;
      this._events[name] = a.filter(x => x !== fn);
      return this;
    }
    emit(name, ...args) {
      const a = (this._events[name] || []).slice();
      for (const fn of a) { try { fn(...args); } catch (e) { setTimeout(() => { throw e; }, 0); } }
      return a.length > 0;
    }
  }

  class Client extends Emitter {
    constructor(url, opts) {
      super();
      this.url = url;
      this.options = Object.assign({
        clientId: 'mqttjs_' + Math.random().toString(16).slice(2, 10),
        clean: true,
        keepalive: 60,
        reconnectPeriod: 1000,
        connectTimeout: 30000,
        protocolVersion: 4
      }, opts || {});
      this.connected = false;
      this.reconnecting = false;
      this._ws = null;
      this._closedByUser = false;
      this._retryTimer = null;
      this._connectTimer = null;
      this._pingTimer = null;
      this._rx = new Uint8Array(0);
      this._nextId = 1;
      this._pendingPub = new Map();
      this._pendingSub = new Map();
      this._subscriptions = new Map();
      this._connect();
    }

    _nextPacketId() {
      const n = this._nextId;
      this._nextId = this._nextId >= 65535 ? 1 : this._nextId + 1;
      return n;
    }

    _connect() {
      if (this._closedByUser) return;
      clearTimeout(this._retryTimer);
      clearTimeout(this._connectTimer);
      if (this._ws) { try { this._ws.close(); } catch (_) {} }

      let ws;
      try {
        ws = new WebSocket(this.url, ['mqtt']);
        ws.binaryType = 'arraybuffer';
      } catch (e) {
        this.emit('error', e);
        this._scheduleReconnect();
        return;
      }
      this._ws = ws;
      this._rx = new Uint8Array(0);
      this._connectTimer = setTimeout(() => {
        if (!this.connected) {
          try { ws.close(); } catch (_) {}
          this.emit('error', new Error('MQTT connection timeout'));
        }
      }, this.options.connectTimeout);

      ws.onopen = () => {
        this._sendConnect();
      };
      ws.onmessage = e => this._feed(e.data);
      ws.onerror = () => {
        // Browser WebSocket intentionally hides many low-level error details.
        this.emit('error', new Error('WebSocket error'));
      };
      ws.onclose = () => {
        const wasConnected = this.connected;
        this.connected = false;
        clearTimeout(this._connectTimer);
        clearInterval(this._pingTimer);
        this.emit('close');
        if (wasConnected) this.emit('offline');
        if (!this._closedByUser) this._scheduleReconnect();
      };
    }

    _scheduleReconnect() {
      if (this._closedByUser || this.options.reconnectPeriod <= 0 || this._retryTimer) return;
      this.reconnecting = true;
      this._retryTimer = setTimeout(() => {
        this._retryTimer = null;
        this.emit('reconnect');
        this._connect();
      }, this.options.reconnectPeriod);
    }

    _sendConnect() {
      const flags = (this.options.clean ? 0x02 : 0x00);
      const vh = concat(
        strField('MQTT'),
        new Uint8Array([4, flags, (this.options.keepalive >>> 8) & 255, this.options.keepalive & 255]),
        strField(this.options.clientId)
      );
      this._send(packet(0x10, vh));
    }

    _send(data) {
      if (this._ws && this._ws.readyState === WebSocket.OPEN) this._ws.send(data);
    }

    _feed(data) {
      const incoming = data instanceof ArrayBuffer ? new Uint8Array(data) : new Uint8Array(data);
      this._rx = concat(this._rx, incoming);
      while (this._rx.length >= 2) {
        const rl = readRemainingLength(this._rx, 1);
        if (!rl) return;
        const headerLen = rl.next;
        if (this._rx.length < headerLen + rl.value) return;
        const typeFlags = this._rx[0];
        const body = this._rx.slice(headerLen, headerLen + rl.value);
        this._rx = this._rx.slice(headerLen + rl.value);
        this._handlePacket(typeFlags, body);
      }
    }

    _handlePacket(tf, b) {
      const type = tf >> 4;
      if (type === 2) { // CONNACK
        clearTimeout(this._connectTimer);
        const rc = b[1];
        if (rc !== 0) {
          this.emit('error', new Error('MQTT CONNACK error: ' + rc));
          try { this._ws.close(); } catch (_) {}
          return;
        }
        this.connected = true;
        this.reconnecting = false;
        this.emit('connect', { sessionPresent: !!(b[0] & 1) });
        // Restore subscriptions after a reconnect. MQTT.js does this too.
        if (this._subscriptions.size) {
          const parts = [u16(this._nextPacketId())];
          for (const [topic, qos] of this._subscriptions) parts.push(strField(topic), new Uint8Array([qos]));
          this._send(packet(0x82, concat.apply(null, parts)));
        }
        clearInterval(this._pingTimer);
        if (this.options.keepalive > 0) {
          this._pingTimer = setInterval(() => {
            if (this.connected) this._send(packet(0xC0, new Uint8Array(0)));
          }, Math.max(1000, this.options.keepalive * 500));
        }
      } else if (type === 3) { // PUBLISH
        const qos = (tf >> 1) & 3;
        let o = 0;
        const topic = readUtf8(b, o);
        if (!topic) return;
        o = topic.next;
        let packetId = null;
        if (qos === 1 || qos === 2) {
          if (o + 2 > b.length) return;
          packetId = (b[o] << 8) | b[o + 1];
          o += 2;
        }
        // MQTT 3.1.1 has no properties in PUBLISH; the remainder is payload.
        const payload = b.slice(o);
        if (qos === 1 && packetId != null) this._send(packet(0x40, new Uint8Array([(packetId >>> 8) & 255, packetId & 255])));
        // mqtt-lite uses Uint8Array internally, but the UNO app expects the
        // MQTT.js-compatible payload.toString() to return UTF-8 text.
        // Uint8Array.prototype.toString() returns comma-separated numbers,
        // which made JSON.parse() fail on every incoming game message.
        const textPayload = td.decode(payload);
        this.emit('message', topic.value, textPayload);
      } else if (type === 4) { // PUBACK
        if (b.length >= 2) {
          const id = (b[0] << 8) | b[1];
          const cb = this._pendingPub.get(id);
          if (cb) { this._pendingPub.delete(id); cb(null); }
        }
      } else if (type === 9) { // SUBACK
        if (b.length >= 2) {
          const id = (b[0] << 8) | b[1];
          const cb = this._pendingSub.get(id);
          if (cb) {
            this._pendingSub.delete(id);
            const code = b[2];
            cb(code != null && code >= 0x80 ? new Error('MQTT SUBACK error: ' + code) : null,
               code == null ? [] : [{ qos: code }]);
          }
        }
      } else if (type === 13) { // PINGRESP
        // Nothing to do.
      }
    }

    publish(topic, message, options, callback) {
      if (typeof options === 'function') { callback = options; options = {}; }
      options ||= {};
      const qos = options.qos || 0;
      const topicBytes = strField(topic);
      const payload = typeof message === 'string' ? utf8(message) : new Uint8Array(message);
      let body;
      let id = null;
      if (qos === 1) {
        id = this._nextPacketId();
        body = concat(topicBytes, u16(id), payload);
      } else {
        body = concat(topicBytes, payload);
      }
      const flags = 0x30 | (options.retain ? 1 : 0) | (qos << 1);
      if (id != null && callback) this._pendingPub.set(id, callback);
      this._send(packet(flags, body));
      if (id == null && callback) setTimeout(() => callback(null), 0);
      return this;
    }

    subscribe(topics, options, callback) {
      if (typeof options === 'function') { callback = options; options = {}; }
      options ||= {};
      const list = Array.isArray(topics) ? topics : [topics];
      const id = this._nextPacketId();
      const parts = [u16(id)];
      for (const t of list) parts.push(strField(typeof t === 'string' ? t : t.topic), new Uint8Array([typeof t === 'object' && t.qos != null ? t.qos : (options.qos || 0)]));
      for (const t of list) {
        const topic = typeof t === 'string' ? t : t.topic;
        const qos = typeof t === 'object' && t.qos != null ? t.qos : (options.qos || 0);
        this._subscriptions.set(topic, qos);
      }
      if (callback) this._pendingSub.set(id, callback);
      this._send(packet(0x82, concat.apply(null, parts)));
      return this;
    }

    end(force, options, callback) {
      if (typeof force === 'function') callback = force;
      this._closedByUser = true;
      clearTimeout(this._retryTimer);
      clearTimeout(this._connectTimer);
      clearInterval(this._pingTimer);
      if (this._ws) { try { this._ws.close(); } catch (_) {} }
      if (callback) setTimeout(callback, 0);
      return this;
    }

    reconnect() {
      this._closedByUser = false;
      this._connect();
      return this;
    }
  }

  global.mqtt = {
    connect: function (url, options) { return new Client(url, options); },
    Client
  };
})(window);
