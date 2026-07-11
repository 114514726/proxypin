import 'package:proxypin/network/components/manager/rewrite_rule.dart';
import 'package:proxypin/network/components/manager/request_rewrite_manager.dart';
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
    // 响应修改
    if (message is HttpResponse) {
      msg = _replaceLoginBanned(msg);
      msg = _replaceField(msg, 'adCoin', [0xFF, 0xFF, 0xFF, 0xFF]);
      msg = _replaceField(msg, 'coin', [0xFF, 0xFF, 0xFF, 0xFF]);
      msg = _replaceField(msg, 'imageBit', [0xFF, 0x00]);
      msg = _replaceField(msg, 'code', [0x00]);
      msg = _replaceField(msg, 'error', []);
      msg = _injectAIBalance(msg);
      msg = await _applyUI(msg, false);
    }
    // 请求修改
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

  Uint8List _replaceLoginBanned(Uint8List data) {
    for (int k = 0; k < 256; k++) {
      var pat = <num>[4 ^ k, 0x63 ^ k, 0x6F ^ k, 0x64 ^ k, 0x65 ^ k];
      for (int i = 0; i <= data.length - pat.length - 1; i++) {
        bool m = true;
        for (int j = 0; j < pat.length; j++)
          if (data[i + j] != pat[j]) { m = false; break; }
        if (m && (data[i + pat.length + 1] ^ k) == 0xdb) {
          var h = 'bdb59d3e080605037365710100000000050361636b0100000000050474696d65024ac4894f9f010000050472657370010a0000000503636d6405154c6f67696e5f52417574685573657253696e676c650504626f647907800208030509726f6c65546f6b656e05ac014a2f5247644c734b365651434277513446735a6141304f2b385045374e6738725030502f62356c347043493136376e506f4d616852447761756353546230632f394b30385a54304a536e593258595745304b354969756a725953622f67515a67726e3454592b6263794f622b46795833744b306d5045527a6334616474614e5967574e336448557746506e496859576f4e4a76756772485836576c76696b75544e756c792b675869464e733d0506726f6c65496402879aba0200000000050f6e657752656672657368546f6b656e05203837373036306537343662653437396338313139396263633338303065376237';
          var dec = <int>[];
          for (int p = 0; p < h.length; p += 2)
            dec.add(int.parse(h.substring(p, p + 2), radix: 16));
          var enc = Uint8List(dec.length);
          for (int p = 0; p < dec.length; p++)
            enc[p] = (dec[p] ^ k);
          logger.i('[LOGIN] banned -> injected');
          return enc;
        }
      }
    }
    return data;
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
    var types = fromClient ? [RuleType.wsRequestReplace, RuleType.wsRequestUpdate] : [RuleType.wsResponseReplace, RuleType.wsResponseUpdate];
    var url = (message is HttpRequest) ? message.requestUrl : (message as HttpResponse).request?.requestUrl;
    var rule = m.getRewriteRule(url, types);
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
      var mod = _replaceField(data, key, nv);
      if (!identical(mod, data)) return mod;
    }
    return data;
  }
}
