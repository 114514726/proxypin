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
    if (message is HttpResponse) {
      msg = _replaceField(msg, 'adCoin', [0xFF, 0xFF]);
      msg = _replaceField(msg, 'coin', [0xFF, 0xFF]);
      msg = _replaceField(msg, 'imageBit', [0xFF, 0x00]);
      msg = _replaceDirect(msg, 'showPro', [0xFF]);
      msg = _replaceDirect(msg, 'isBlueVIP', [0xFF]);
      msg = _replaceField(msg, 'code', [0x00]);
      msg = _replaceField(msg, 'error', []);
      msg = _injectAIBalance(msg);
      msg = await _applyUI(msg, false);
    }
    if (message is HttpRequest) {
      msg = await _applyUI(msg, true);
    }
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

  static Uint8List _replaceField(Uint8List data, String name, List<int> nv) {
    for (int k = 0; k < 256; k++) {
      var pat = <num>[];
      pat.add(name.length ^ k);
      for (var c in name.codeUnits) pat.add(c ^ k);
      for (int i = 0; i <= data.length - pat.length - nv.length; i++) {
        bool m = true;
        for (int j = 0; j < pat.length; j++)
          if (data[i + j] != pat[j]) { m = false; break; }
        if (m) {
          int vp = i + pat.length + 1;
          var mod = Uint8List.fromList(data);
          for (int b = 0; b < nv.length; b++)
            mod[vp + b] = (nv[b] ^ k).toInt();
          return mod;
        }
      }
    }
    return data;
  }

  static Uint8List _replaceDirect(Uint8List data, String name, List<int> nv) {
    for (int k = 0; k < 256; k++) {
      var pat = <num>[];
      for (var c in name.codeUnits) pat.add(c ^ k);
      for (int i = 0; i <= data.length - pat.length - nv.length; i++) {
        bool m = true;
        for (int j = 0; j < pat.length; j++)
          if (data[i + j] != pat[j]) { m = false; break; }
        if (m) {
          var mod = Uint8List.fromList(data);
          for (int b = 0; b < nv.length; b++)
            mod[i + pat.length + b] = (nv[b] ^ k).toInt();
          return mod;
        }
      }
    }
    return data;
  }

  static Uint8List _injectAIBalance(Uint8List data) {
    for (int k = 0; k < 256; k++) {
      var pat = <num>[(20 ^ k) as int];
      for (var c in 'AI_GetAICoinAccountR'.codeUnits) pat.add(c ^ k);
      for (int i = 0; i <= data.length - pat.length - 10; i++) {
        bool m = true;
        for (int j = 0; j < pat.length; j++)
          if (data[i + j] != pat[j]) { m = false; break; }
        if (m) {
          var ua = <num>[0x05 ^ k, 0x08 ^ k, 0x75 ^ k, 0x70 ^ k, 0x64 ^ k, 0x61 ^ k, 0x74 ^ k, 0x65 ^ k, 0x41 ^ k, 0x74 ^ k];
          int uap = -1;
          for (int j = i + pat.length; j < data.length - ua.length; j++) {
            bool mm = true;
            for (int b = 0; b < ua.length; b++)
              if (data[j + b] != ua[b]) { mm = false; break; }
            if (mm) { uap = j + ua.length + 9; break; }
          }
          if (uap > 0) {
            var bal = <num>[0x04 ^ k, 0x63 ^ k, 0x6F ^ k, 0x69 ^ k, 0x6E ^ k, 0x02 ^ k, 0xFF ^ k, 0xFF ^ k];
            var mod = Uint8List(data.length + bal.length);
            mod.setAll(0, data.sublist(0, uap));
            for (int b = 0; b < bal.length; b++) mod[uap + b] = bal[b].toInt();
            mod.setAll(uap + bal.length, data.sublist(uap));
            return mod;
          }
        }
      }
    }
    return data;
  }

  Future<Uint8List> _applyUI(Uint8List data, bool fromClient) async {
    var m = await RequestRewriteManager.instance;
    if (!m.enabled) return data;
    var types = fromClient 
        ? [RuleType.wsRequestReplace, RuleType.wsRequestUpdate]
        : [RuleType.wsResponseReplace, RuleType.wsResponseUpdate];
    var rule = m.getRewriteRule("*", types);
    if (rule == null) return data;
    var items = await m.getRewriteItems(rule);
    if (items == null) return data;
    for (var item in items) {
      if (!item.enabled) continue;
      var key = item.key ?? "";
      var val = item.value ?? "";
      if (key.isEmpty || val.isEmpty) continue;
      var nv = <int>[];
      for (int p = 0; p < val.length; p += 2)
        nv.add(int.parse(val.substring(p, p + 2), radix: 16));
      data = _replaceField(data, key, nv);
    }
    return data;
  }
}
