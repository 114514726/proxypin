import 'dart:typed_data';
import 'package:proxypin/network/channel/channel.dart';
import 'package:proxypin/network/channel/channel_context.dart';
import 'package:proxypin/network/components/manager/request_rewrite_manager.dart';
import 'package:proxypin/network/components/manager/rewrite_rule.dart';
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
    if (message is HttpResponse) msg = await _fix(msg);
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

  Future<Uint8List> _fix(Uint8List data) async {
    if (data.length < 60 || data.length > 500) return data;
    var manager = await RequestRewriteManager.instance;
    if (!manager.enabled) return data;

    var types = [RuleType.wsResponseReplace, RuleType.wsResponseUpdate];
    var url = (message is HttpRequest) ? message.requestUrl : (message as HttpResponse).request?.requestUrl;
    var rule = manager.getRewriteRule(url, types);
    if (rule == null) return data;

    var items = await manager.getRewriteItems(rule);
    if (items == null) return data;

    for (var item in items) {
      if (!item.enabled) continue;
      var key = item.key ?? '';
      var val = item.value ?? '';
      if (key.isEmpty) continue;

      for (int k = 0; k < 256; k++) {
        var p = <int>[key.length ^ k];
        for (var c in key.codeUnits) p.add(c ^ k);
        for (int i = 0; i <= data.length - p.length; i++) {
          if (p.asMap().entries.every((e) => data[i + e.key] == e.value)) {
            var vp = i + p.length;
            if (vp >= data.length) continue;
            var oldVal = data[vp] ^ k;
            var newVal = int.tryParse(val, radix: 16) ?? 0xFF;
            var mod = Uint8List.fromList(data);
            mod[vp] = (newVal ^ k) as int;
            logger.i('[WSS] $key: ${oldVal.toRadixString(16)}->${newVal.toRadixString(16)}');
            return mod;
          }
        }
      }
    }
    return data;
  }
}
