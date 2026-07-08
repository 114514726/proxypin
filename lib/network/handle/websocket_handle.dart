import 'dart:typed_data';

import 'package:proxypin/network/channel/channel.dart';
import 'package:proxypin/network/channel/channel_context.dart';
import 'package:proxypin/network/http/http.dart';
import 'package:proxypin/network/http/websocket.dart';
import 'package:proxypin/network/util/logger.dart';

/// websocket处理器
class WebSocketChannelHandler extends ChannelHandler<Uint8List> {
  final WebSocketDecoder decoder = WebSocketDecoder();

  final Channel proxyChannel;
  final HttpMessage message;

  WebSocketChannelHandler(this.proxyChannel, this.message);

  @override
  Future<void> channelRead(ChannelContext channelContext, Channel channel, Uint8List msg) async {
    // ====== 新增：WSS重写 — 修改imageBit ======
    if (message is HttpResponse) {
      msg = _rewriteImageBit(msg);
    }
    // ==========================================
    
    proxyChannel.writeBytes(msg);
    WebSocketFrame? frame;
    try {
      frame = decoder.decode(msg);
    } catch (e, stackTrace) {
      log.e("websocket decode error", error: e, stackTrace: stackTrace);
    }
    if (frame == null) {
      return;
    }
    frame.isFromClient = message is HttpRequest;

    message.messages.add(frame);
    channelContext.listener?.onMessage(channel, message, frame);
    logger.d(
        "[${channelContext.clientChannel?.id}] websocket channelRead ${frame.payloadLength} ${frame.fin} ${frame.payloadDataAsString}");
  }

  /// 修改imageBit: 0x0007 → 0x00FF
  Uint8List _rewriteImageBit(Uint8List data) {
    if (data.length < 60 || data.length > 500) return data;
    
    for (int key = 0; key < 256; key++) {
      // imageBit XOR模式: 08^K,69^K,6D^K,61^K,67^K,65^K,42^K,69^K,74^K,02^K
      List<int> pat = [
        0x08^key, 0x69^key, 0x6D^key, 0x61^key, 0x67^key, 0x65^key,
        0x42^key, 0x69^key, 0x74^key, 0x02^key
      ];
      
      for (int i = 0; i < data.length - 11; i++) {
        bool match = true;
        for (int j = 0; j < 10; j++) {
          if (data[i + j] != pat[j]) { match = false; break; }
        }
        if (match && (data[i + 10] ^ key) == 0x07) {
          var modified = Uint8List.fromList(data);
          modified[i + 10] = 0xFF ^ key;
          logger.i("[WSS Rewrite] imageBit 0x07→0xFF key=0x${key.toRadixString(16)}");
          return modified;
        }
      }
    }
    return data;
  }
}
