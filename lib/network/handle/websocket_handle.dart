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
  static List<Map>? _rules;
  static int _lastSize = -1;

  WebSocketChannelHandler(this.proxyChannel, this.message);

  @override
  Future<void> channelRead(ChannelContext ctx, Channel ch, Uint8List msg) async {
    if (message is HttpResponse) msg = _apply(msg);
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

  Uint8List _apply(Uint8List data) {
    try {
      var f = File('/sdcard/proxypin_wss.json');
      if (!f.existsSync()) return data;
      var sz = f.lengthSync();
      if (sz != _lastSize) {
        _rules = (jsonDecode(f.readAsStringSync()) as List).cast<Map>();
        _lastSize = sz;
        logger.i('[WSS] 加载 ${_rules!.length} 条规则');
      }
    } catch (e) {
      return data;
    }
    if (_rules == null || _rules!.isEmpty) return data;

    var hex = _hex(data);
    for (var r in _rules!) {
      var key = r['key'] as String?;
      var from = r['from'] as String?;
      var to = r['to'] as String?;
      var dir = r['dir'] as String? ?? 'download';
      if (dir != 'download' && dir != 'both') continue;
      if (key == null || to == null) continue;

      // XOR暴力扫描
      for (int k = 0; k < 256; k++) {
        var nameHex = _hex(Uint8List.fromList([key.length ^ k]));
        var valHex = _hex(Uint8List.fromList(key.codeUnits.map((c) => c ^ k).toList()));
        var pat = nameHex + valHex;
        var idx = hex.indexOf(pat);
        if (idx >= 0) {
          var valPos = (idx ~/ 2) + 1 + key.length + 1;
          if (valPos >= data.length) continue;
          var oldVal = data[valPos] ^ k;
          var newVal = int.tryParse(to, radix: 16) ?? 0xFF;
          var mod = Uint8List.fromList(data);
          mod[valPos] = newVal ^ k;
          logger.i('[WSS] $key: $oldVal→$newVal');
          return mod;
        }
      }
    }
    return data;
  }

  static String _hex(Uint8List b) => b.map((e) => e.toRadixString(16).padLeft(2, '0')).join();
}
