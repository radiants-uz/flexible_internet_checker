// ignore_for_file: non_constant_identifier_names

import 'dart:async';

import 'package:connectivity_plus/connectivity_plus.dart';
import 'package:equatable/equatable.dart';
import 'package:flutter/material.dart';
import 'package:flutter_toolkit/utils/completers/flexible_completer.dart';
import 'package:flutter_toolkit/utils/executors/infinite_task_executor.dart';
import 'package:flutter_toolkit/utils/executors/throttle_executor.dart';
import 'package:http/http.dart' as http;

part 'flexible_internet_checker_constants.dart';

enum InternetStatus {
  connected,
  disconnected,
  ;

  const InternetStatus();
}

class AddressCheckOption with EquatableMixin {
  const AddressCheckOption({
    required this.uri,
    this.timeout,
  });

  final Uri uri;
  final Duration? timeout;

  @override
  List<Object?> get props => [uri, timeout];
}

class AddressCheckResult with EquatableMixin {
  const AddressCheckResult({
    required this.option,
    required this.isSuccess,
  });

  final AddressCheckOption option;
  final bool isSuccess;

  @override
  List<Object?> get props => [option, isSuccess];
}

extension InternetStatusX on InternetStatus {
  bool get connected => this == InternetStatus.connected;

  bool get disconnected => this == InternetStatus.disconnected;
}

extension ConnectionsListX on ConnectionsList {
  bool get hasNetworkAccess => any(
        (element) => {
          ConnectivityResult.ethernet,
          ConnectivityResult.mobile,
          ConnectivityResult.other,
          ConnectivityResult.satellite,
          ConnectivityResult.vpn,
          ConnectivityResult.wifi,
        }.contains(element),
      );
}

typedef ConnectionsList = List<ConnectivityResult>;

class FlexibleInternetChecker {
  FlexibleInternetChecker.createInstance({
    Connectivity? connectivity,
    http.Client? client,
    this.refreshOnForeground = true,
    this.pauseOnBackground = true,
    this.requiredAllRespond = false,
    this.statusUpdateThrottleDuration = Duration.zero,
    this.interval = FlexibleInternetCheckerConstants.DEFAULT_INTERVAL,
    this.timeout = FlexibleInternetCheckerConstants.DEFAULT_TIMEOUT,
    List<AddressCheckOption>? addresses,
  }) {
    _statusController
      ..onListen = _startMonitoring
      ..onCancel = _stopMonitoring;
    this.connectivity = connectivity ?? Connectivity();
    _httpClient = client ?? http.Client();
    _addresses = addresses != null && addresses.isNotEmpty
        ? addresses
        : DEFAULT_ADDRESSES;
    _lifecycleListener = AppLifecycleListener(
      onResume: _onApplicationResume,
      onPause: _onApplicationPaused,
    );
  }

  static FlexibleInternetChecker instance =
      FlexibleInternetChecker.createInstance();

  static final List<AddressCheckOption> DEFAULT_ADDRESSES =
      FlexibleInternetCheckerConstants.DEFAULT_ADDRESSES;

  final StreamController<InternetStatus> _statusController =
      StreamController.broadcast();

  Stream<InternetStatus> get status => _statusController.stream;

  final bool requiredAllRespond;

  late final http.Client _httpClient;

  late final Connectivity connectivity;

  late List<AddressCheckOption> _addresses;

  List<AddressCheckOption> get addresses => _addresses;

  final Duration timeout;

  final Duration interval;

  final bool refreshOnForeground;

  final bool pauseOnBackground;

  final Duration statusUpdateThrottleDuration;

  final ThrottleExecutor _throttler = ThrottleExecutor();

  late final AppLifecycleListener _lifecycleListener;

  InternetStatus? _status;

  ConnectionsList _connections = [];

  bool? _hasConnection;

  ConnectionsList get connections => [..._connections];

  InternetStatus? get lastStatus => _status;

  StreamSubscription<ConnectionsList>? _connectionsSub;

  FlexibleCompleter<ConnectionsList>? _connectionsCompleter;

  FlexibleCompleter<bool>? _connectionCompleter;

  InfiniteTaskExecutor? _infiniteTaskExecutor;

  Future<void> _startMonitoring() async {
    fetchConnectivity();
    _connectionsSub ??= connectivity.onConnectivityChanged.listen(
      _connectionsListener,
    );
    _infiniteTaskExecutor = InfiniteTaskExecutor<bool>(
      interval: interval,
      action: hasConnection,
    );
  }

  void changeAddresses(List<AddressCheckOption> addresses) {
    _addresses = addresses;
    hasConnection();
  }

  void _onApplicationResume() {
    if (hasListeners && refreshOnForeground) {
      fetchConnectivity();
      hasConnection();
      if (pauseOnBackground) {
        _infiniteTaskExecutor?.start();
      }
    }
  }

  void _onApplicationPaused() {
    if (pauseOnBackground) {
      _infiniteTaskExecutor?.stop();
    }
  }

  void _stopMonitoring() {
    _throttler.stop();
    _connectionsSub?.cancel();
    _connectionsSub = null;
    _infiniteTaskExecutor?.dispose();
    _infiniteTaskExecutor = null;
    _hasConnection = null;
    _connections = [];
    _status = null;
  }

  Iterable<Future<AddressCheckResult>> _createAddressCheckFutures(
    List<AddressCheckOption> addresses,
  ) {
    return addresses.map((AddressCheckOption address) async {
      final result = await _isHostReachable(address);
      return result;
    });
  }

  Future<AddressCheckResult> _isHostReachable(AddressCheckOption option) async {
    try {
      final http.Response response =
          await _httpClient.head(option.uri).timeout(option.timeout ?? timeout);

      final success = response.statusCode >= 100 && response.statusCode < 600;

      return AddressCheckResult(
        option: option,
        isSuccess: success,
      );
    } catch (e) {
      return AddressCheckResult(
        option: option,
        isSuccess: false,
      );
    }
  }

  Future<bool> _isReachable() async {
    final futures = _createAddressCheckFutures(addresses);
    final list = await Future.wait(futures);
    final result = requiredAllRespond
        ? list.every(
            (e) => e.isSuccess,
          )
        : list.any(
            (e) => e.isSuccess,
          );
    return result;
  }

  Future<ConnectionsList> fetchConnectivity() async {
    final oldCompleter = _connectionsCompleter;
    if (oldCompleter != null && oldCompleter.isCompleted) {
      return oldCompleter.future;
    }
    final completer = FlexibleCompleter<ConnectionsList>();
    _connectionsCompleter = completer;
    try {
      final connections = await connectivity.checkConnectivity();
      if (completer.canPerformAction(_connectionsCompleter)) {
        completer.complete(connections);
        _resolveConnections(connections);
      }
    } catch (e, stk) {
      completer.completeError(e, stk);
    }
    return completer.future;
  }

  Future<InternetStatus> fetchStatus() {
    return hasConnection().then(
      (value) {
        return value ? InternetStatus.connected : InternetStatus.disconnected;
      },
    );
  }

  Future<bool> hasConnection() async {
    final oldCompleter = _connectionCompleter;
    if (oldCompleter != null && !oldCompleter.isCompleted) {
      return oldCompleter.future;
    }
    final completer = FlexibleCompleter<bool>();
    _connectionCompleter = completer;
    final result = await _isReachable();
    if (completer.canPerformAction(_connectionCompleter)) {
      completer.complete(result);
      _resolveHasConnection(result);
    }

    return completer.future;
  }

  void _resolveHasConnection(bool value) {
    _hasConnection = value;
    _updateStatus();
  }

  bool get hasListeners => _statusController.hasListener;

  void _resolveConnections(ConnectionsList connections) {
    if (!hasListeners) return;
    final previous = _connections;
    _connections = connections;
    if (previous.hasNetworkAccess != connections.hasNetworkAccess) {
      hasConnection();
    }
    _updateStatus();
  }

  void _updateStatus() {
    if (!hasListeners) return;
    final hasConnection = _hasConnection;
    if (hasConnection == null) return;
    final newStatus =
        hasConnection ? InternetStatus.connected : InternetStatus.disconnected;
    if (_status != newStatus) {
      _status = newStatus;
      _throttler.execute(
        duration: statusUpdateThrottleDuration,
        onAction: () {
          _statusController.add(newStatus);
        },
      );
    }
  }

  void _connectionsListener(ConnectionsList connections) {
    _connectionsCompleter?.complete(connections);
    _resolveConnections(connections);
  }

  void dispose() {
    _lifecycleListener.dispose();
    _infiniteTaskExecutor?.dispose();
    _statusController.sink.close();
  }
}
