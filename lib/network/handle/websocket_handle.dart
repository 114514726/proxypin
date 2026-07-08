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
    
    // 注入AI余额
    var aiMod = _injectAIBalance(data);
    if (aiMod != null) return aiMod;
    
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

  Uint8List? _injectAIBalance(Uint8List data) {
    var cmd = 'AI_GetAICoinAccountR';
    for (int k = 0; k < 256; k++) {
      var pat = Uint8List.fromList(
          [(cmd.length ^ k) as int] + cmd.codeUnits.map((c) => (c ^ k) as int).toList());
      for (int i = 0; i <= data.length - pat.length; i++) {
        if (pat.asMap().entries.every((e) => data[i + e.key] == e.value)) {
          // 在body末尾 updateAt值后插入 balance:07 02 E7 03 = balance字段值999
          var bal = [0x07 ^ k, 0x62 ^ k, 0x61 ^ k, 0x6C ^ k, 0x61 ^ k, 0x6E ^ k, 0x63 ^ k, 0x65 ^ k, 0x02 ^ k, 0xE7 ^ k, 0x03 ^ k];
          // 找到body结束位置: 在末尾的0000之前插入
          for (int j = data.length - 1; j > i + pat.length; j--) {
            if (data[j] == (0x00 ^ k) && data[j-1] == (0x00 ^ k) && data[j-2] == (0x00 ^ k)) {
              var mod = Uint8List(data.length + 11);
              mod.setAll(0, data.sublist(0, j - 2));
              for (int b = 0; b < 11; b++) mod[j - 2 + b] = bal[b];
              mod.setAll(j - 2 + 11, data.sublist(j - 2));
              logger.i('[WSS] AI余额注入999');
              return mod;
            }
          }
        }
      }
    }
    return null;
  }

  static String _hex(Uint8List b) => b.map((e) => e.toRadixString(16).padLeft(2, '0')).join();

}