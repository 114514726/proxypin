import 'dart:io';
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
    // 挂在服务器通道上时 message 就是握手响应(HttpResponse)
    // => message is HttpResponse = 服务器->客户端（响应），否则是客户端->服务器（请求）
    final bool fromServer = message is HttpResponse;

    if (fromServer) {
      final int rawLen = msg.length;

      // 1) 带 error 字段的帧整帧丢弃，不写回客户端 => 屏蔽该响应
      msg = _dropErrorFrames(msg);
      if (msg.isEmpty) {
        logger.d('[WSS] blocked error response, $rawLen bytes');
        return;
      }

      var gzipped = _isGzip(msg);
      if (gzipped) {
        try {
          msg = Uint8List.fromList(gzip.decode(msg));
        } catch (_) {
          gzipped = false;
        }
      }

      msg = _replaceField(msg, 'followState', [0x03]);
      msg = _replaceField(msg, 'coin', [0xFF, 0xFF, 0xFF]);
      msg = _replaceField(msg, 'state', [0x03]);
      msg = _replaceDirect(msg, 'showPro', [0xFF]);
      msg = _replaceDirect(msg, 'isBlueVIP', [0xFF]);
      msg = _replaceField(msg, 'imageBit', [0xFF, 0x00]);
      msg = _injectAIBalance(msg);
      msg = await _applyUI(msg, false);

      if (gzipped) {
        try {
          msg = Uint8List.fromList(gzip.encode(msg));
        } catch (_) {}
      }
    } else {
      msg = await _applyUI(msg, true);
    }

    proxyChannel.writeBytes(msg);
    try {
      var f = decoder.decode(msg);
      if (f != null) {
        f.isFromClient = !fromServer;
        message.messages.add(f);
        ctx.listener?.onMessage(ch, message, f);
      }
    } catch (_) {}
  }

  /// 按 WebSocket 帧切分，payload 里带 error 字段的数据帧整帧丢掉。
  /// 返回空数组 = 整块不转发（客户端收不到这个响应）
  static Uint8List _dropErrorFrames(Uint8List data) {
    if (data.length < 6) return data;
    final out = BytesBuilder(copy: false);
    int i = 0;
    bool dropped = false;

    while (i + 2 <= data.length) {
      final int b1 = data[i + 1];
      final int opcode = data[i] & 0x0F;
      final bool masked = (b1 & 0x80) != 0;
      int len = b1 & 0x7F;
      int hdr = 2;
      if (len == 126) {
        if (i + 4 > data.length) break;
        len = (data[i + 2] << 8) | data[i + 3];
        hdr = 4;
      } else if (len == 127) {
        if (i + 10 > data.length) break;
        len = 0;
        for (int j = 0; j < 8; j++) {
          len = (len << 8) | data[i + 2 + j];
        }
        hdr = 10;
      }
      if (masked) hdr += 4;
      final int total = hdr + len;
      if (len < 0 || total <= hdr || i + total > data.length) break;

      // 服务器下发的帧不加掩码；带掩码的是客户端方向，不处理
      if (!masked && (opcode == 0x01 || opcode == 0x02)) {
        final payload = Uint8List.sublistView(data, i + hdr, i + total);
        if (_hasError(payload)) {
          dropped = true;
          i += total;
          continue;
        }
      }
      out.add(Uint8List.sublistView(data, i, i + total));
      i += total;
    }

    if (i == 0) {
      // 不是标准 WS 帧（或只有 payload），退化成整包判断
      return _hasError(data) ? Uint8List(0) : data;
    }
    if (!dropped) return data;
    if (i < data.length) out.add(Uint8List.sublistView(data, i));
    return out.toBytes();
  }

  static bool _isGzip(Uint8List d) => d.length > 2 && d[0] == 0x1F && d[1] == 0x8B;

  /// 找 error 字段：TLV 里是 [type=0x05][nameLen=0x05]["error"]
  static bool _hasError(Uint8List data) {
    final k = _xorKey(data);
    if (k != null) return _matchError(data, k);
    for (int kk = 0; kk < 256; kk++) {
      if (_matchError(data, kk)) return true;
    }
    return false;
  }

  /// 明文: "px" [2B] [0x08 map] [字段数] [0x05 ...]，除 "px" 外整段单字节 XOR
  static int? _xorKey(Uint8List d) {
    if (d.length < 8 || d[0] != 0x70 || d[1] != 0x78) return null;
    final k = d[4] ^ 0x08;
    final cnt = d[5] ^ k;
    if (cnt < 1 || cnt > 64) return null;
    if ((d[6] ^ k) != 0x05) return null;
    return k;
  }

  static bool _matchError(Uint8List d, int k) {
    final b0 = 0x05 ^ k;
    final be = 0x65 ^ k;
    final br = 0x72 ^ k;
    final bo = 0x6F ^ k;
    for (int i = 0; i <= d.length - 6; i++) {
      if (d[i] != b0) continue;
      if (d[i + 1] == be && d[i + 2] == br && d[i + 3] == br && d[i + 4] == bo && d[i + 5] == br) {
        return true;
      }
    }
    return false;
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
          int vp = i + pat.length;
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

