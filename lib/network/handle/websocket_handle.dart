import 'dart:convert';
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
  Future<void> channelRead(ChannelContext channelContext, Channel channel, Uint8List msg) async {
    if (message is HttpResponse) {
      msg = await _applyRewrites(msg, false);
    }
    proxyChannel.writeBytes(msg);
    WebSocketFrame? frame;
    try { frame = decoder.decode(msg); } catch (_) {}
    if (frame == null) return;
    frame.isFromClient = message is HttpRequest;
    message.messages.add(frame);
    channelContext.listener?.onMessage(channel, message, frame);
  }

  Future<Uint8List> _applyRewrites(Uint8List data, bool fromClient) async {
    final manager = await RequestRewriteManager.instance;
    if (!manager.enabled) return data;
    final types = fromClient
        ? [RuleType.wsRequestReplace, RuleType.wsRequestUpdate]
        : [RuleType.wsResponseReplace, RuleType.wsResponseUpdate];
    final rule = manager.getRewriteRule(
        (message is HttpRequest) ? message.requestUrl : (message as HttpResponse).request?.requestUrl, types);
    if (rule == null) return data;
    final items = await manager.getRewriteItems(rule);
    if (items == null) return data; for (final item in items) {
      if (!item.enabled) continue;
      final fieldName = item.key ?? '';
      // _toHex: name_len^K, fieldName^K, val_len^K
      for (int k = 0; k < 256; k++) {
        final prefix = _toHex(Uint8List.fromList([fieldName.length ^ k]));
        final nameHex = _toHex(Uint8List.fromList(fieldName.codeUnits.map((c) => c ^ k).toList()));
        final pat = prefix + nameHex;
        final hex = _toHex(data);
        final idx = hex.indexOf(pat);
        if (idx >= 0) {
          final valLenPos = (idx ~/ 2) + 1 + fieldName.length;
          if (valLenPos >= data.length) continue;
          final valLen = data[valLenPos]; // XOR'd val_len
          final valPos = valLenPos + 1;
          if (valPos >= data.length) continue;
          final oldVal = data[valPos] ^ k;
          final newVal = int.tryParse(item.value ?? 'ff', radix: 16) ?? 0xFF;
          var mod = Uint8List.fromList(data);
          mod[valPos] = newVal ^ k;
          logger.i('[WSS] ${fieldName}: 0x${oldVal.toRadixString(16)}→0x${newVal.toRadixString(16)} k=${k.toRadixString(16)}');
          return mod;
        }
      }
    }
    return data;
  }

  static String _toHex(Uint8List b) => b.map((e) => e.toRadixString(16).padLeft(2, '0')).join();
}
