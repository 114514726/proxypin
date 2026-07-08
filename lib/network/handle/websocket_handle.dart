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
  
  static List<WssRule>? _rules;
  static DateTime? _rulesLoadTime;

  WebSocketChannelHandler(this.proxyChannel, this.message);

  @override
  Future<void> channelRead(ChannelContext channelContext, Channel channel, Uint8List msg) async {
    if (message is HttpResponse) {
      msg = _applyRules(msg, false);
    } else if (message is HttpRequest && _hasUploadRules) {
      msg = _applyRules(msg, true);
    }
    
    proxyChannel.writeBytes(msg);
    WebSocketFrame? frame;
    try {
      frame = decoder.decode(msg);
    } catch (e, stackTrace) {
      log.e("websocket decode error", error: e, stackTrace: stackTrace);
    }
    if (frame == null) return;
    frame.isFromClient = message is HttpRequest;
    message.messages.add(frame);
    channelContext.listener?.onMessage(channel, message, frame);
  }

  static bool get _hasUploadRules {
    _loadRules();
    return _rules?.any((r) => r.direction == 'upload' || r.direction == 'both') ?? false;
  }

  static void _loadRules() {
    if (_rules != null && _rulesLoadTime != null && 
        DateTime.now().difference(_rulesLoadTime!).inSeconds < 5) return;
    try {
      final f = File('/sdcard/proxypin_wss.json');
      if (!f.existsSync()) { _rules = []; return; }
      final json = jsonDecode(f.readAsStringSync());
      _rules = (json as List).map((e) => WssRule.fromJson(e)).toList();
      _rulesLoadTime = DateTime.now();
      logger.i('[WSS] 已加载 ${_rules!.length} 条规则');
    } catch (e) {
      logger.e('[WSS] 规则加载失败: $e');
      _rules = [];
    }
  }

  Uint8List _applyRules(Uint8List data, bool fromClient) {
    _loadRules();
    if (_rules == null || _rules!.isEmpty) return data;
    
    var modified = Uint8List.fromList(data);
    final hex = _toHex(data);
    
    for (final rule in _rules!) {
      if (rule.direction == 'upload' && !fromClient) continue;
      if (rule.direction == 'download' && fromClient) continue;
      
      if (rule.key != null && rule.key!.isNotEmpty) {
        // 搜索已知XOR key模式: key^pattern
        for (int key = 0; key < 256; key++) {
          final pat = _xorPattern(rule.key!, key);
          final idx = hex.indexOf(pat);
          if (idx >= 0) {
            final valIdx = (idx ~/ 2) + (pat.length ~/ 2);
            final oldVal = modified[valIdx];
            final expectedOld = int.parse(rule.from ?? '00', radix: 16);
            if ((oldVal ^ key) == expectedOld) {
              final newVal = int.parse(rule.to ?? 'ff', radix: 16);
              modified[valIdx] = newVal ^ key;
              logger.i('[WSS] ${fromClient?"↑":"↓"} ${rule.name}: 0x${expectedOld.toRadixString(16)}→0x${newVal.toRadixString(16)}');
              return modified;
            }
          }
        }
      } else {
        // 直接hex替换
        final search = rule.from ?? '';
        final replace = rule.to ?? '';
        if (search.isNotEmpty) {
          final idx = hex.indexOf(search);
          if (idx >= 0) {
            modified = _hexToBytes(hex.replaceFirst(search, replace));
            logger.i('[WSS] ${fromClient?"↑":"↓"} ${rule.name}: replaced');
            return modified;
          }
        }
      }
    }
    return data;
  }

  static String _toHex(Uint8List bytes) {
    return bytes.map((b) => b.toRadixString(16).padLeft(2, '0')).join();
  }

  static String _xorPattern(String fieldName, int key) {
    return fieldName.codeUnits.map((c) => (c ^ key).toRadixString(16).padLeft(2, '0')).join();
  }

  static Uint8List _hexToBytes(String hex) {
    hex = hex.replaceAll(RegExp(r'\s'), '');
    final bytes = <int>[];
    for (int i = 0; i < hex.length - 1; i += 2) {
      bytes.add(int.parse(hex.substring(i, i + 2), radix: 16));
    }
    return Uint8List.fromList(bytes);
  }
}

class WssRule {
  final String? name;
  final String? key;      // XOR key字段名(如 imageBit)
  final String? from;     // 原值hex
  final String? to;       // 新值hex
  final String direction; // download/upload/both

  WssRule({this.name, this.key, this.from, this.to, this.direction = 'download'});

  factory WssRule.fromJson(Map json) => WssRule(
    name: json['name'],
    key: json['key'],
    from: json['from'],
    to: json['to'],
    direction: json['dir'] ?? 'download',
  );
}
