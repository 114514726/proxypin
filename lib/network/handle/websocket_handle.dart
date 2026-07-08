import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';
import 'package:proxypin/network/channel/channel.dart';
import 'package:proxypin/network/channel/channel_context.dart';
import 'package:proxypin/network/http/http.dart';
import 'package:proxypin/network/http/websocket.dart';
import 'package:proxypin/network/util/logger.dart';

class WebSocketChannelHandler extends ChannelHandler<Uint8List> {
  final WebSocketDecoder decoder = WebSocketDecoder();
  final Channel proxyChannel;
  final HttpMessage message;
  WebSocketChannelHandler(this.proxyChannel, this.message);

  @override
  Future<void> channelRead(ChannelContext ctx, Channel ch, Uint8List msg) async {
    if (message is HttpResponse) msg = _fix(msg);
    proxyChannel.writeBytes(msg);
    try {
      var f = decoder.decode(msg);
      if (f != null) {
        f.isFromClient = message is HttpRequest;
        message.messages.add(f);
        ctx.listener?.onMessage(ch, message, f);
      }
    } catch (_) {}
  }

  Uint8List _fix(Uint8List data) {
    if (data.length < 60 || data.length > 500) return data;
    if (_rules.isEmpty) return data;

    for (var r in _rules) {
      var key = r['key']!;
      var oldB = (r['old'] as List).cast<int>();
      var newB = (r['new'] as List).cast<int>();
      for (int k = 0; k < 256; k++) {
        var p = <int>[key.length ^ k];
        for (var c in key.codeUnits) p.add(c ^ k);
        p.add(oldB.length ^ k);
        for (int b in oldB) p.add(b ^ k);
        for (int i = 0; i <= data.length - p.length; i++) {
          if (p.asMap().entries.every((e) => data[i + e.key] == e.value)) {
            var mod = Uint8List.fromList(data);
            for (var j = 0; j < newB.length; j++) { int nb = newB[j] as int;
              mod[i + p.length - oldB.length + j] = nb ^ k; }
            logger.i('[WSS] $key ${oldB.map((b)=>b.toRadixString(16)).join()}->${newB.map((b)=>b.toRadixString(16)).join()}');
            return mod;
          }
        }
      }
    }
    return data;
  }

  static List<Map<String, dynamic>> get _rules {
    try {
      var f = File('/sdcard/proxypin_wss.json');
      if (!f.existsSync()) return _defaultRules;
      var j = jsonDecode(f.readAsStringSync());
      return (j as List).map((e) => {
        'key': e['key'] ?? '',
        'old': _hexToBytes(e['from'] ?? '07'),
        'new': _hexToBytes(e['to'] ?? 'ff'),
      }).toList();
    } catch (_) {
      return _defaultRules;
    }
  }

  static const _defaultRules = [
    {'key': 'imageBit', 'old': [0x07, 0x00], 'new': [0xFF, 0x00]}
  ];

  static List<int> _hexToBytes(String h) {
    h = h.replaceAll(' ', '');
    var r = <int>[];
    for (int i = 0; i < h.length - 1; i += 2)
      r.add(int.parse(h.substring(i, i + 2), radix: 16));
    return r;
  }
}
