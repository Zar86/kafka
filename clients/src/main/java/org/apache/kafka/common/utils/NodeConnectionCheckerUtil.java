package org.apache.kafka.common.utils;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.NetworkException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.TimeUnit;

public class NodeConnectionCheckerUtil {

    private static final long SOCKET_TTL = TimeUnit.MINUTES.toMillis(3);
    private static final int CONNECT_TIMEOUT = (int) TimeUnit.SECONDS.toMillis(5);

    private static final ForkJoinPool.ForkJoinWorkerThreadFactory factory = pool -> {
        final ForkJoinWorkerThread worker = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool);
        worker.setName("kafka-check-node-availability-" + worker.getPoolIndex());
        return worker;
    };
    private static final ForkJoinPool commonPool = new ForkJoinPool(128, factory, null, false);

    private static final ConcurrentMap<Node, SocketWrapper> connectedNodes = new ConcurrentHashMap<>();

    /**
     * @param nodes - current nodes from metadata cache
     * @throws IOException - throws if node unavailable
     */
    public static void checkNodesAvailability(final Collection<Node> nodes) throws Exception {
        restoreConnectionsTtl();
        final List<CompletableFuture<Void>> checkFutures = new ArrayList<>(nodes.size());
        for (final Node node : nodes) {
            checkFutures.add(CompletableFuture.runAsync(() -> checkNodeAvailability(node), commonPool));
        }
        for (final CompletableFuture<Void> future : checkFutures) {
            future.get(CONNECT_TIMEOUT, TimeUnit.MILLISECONDS);
        }
    }

    private static void checkNodeAvailability(final Node node) {
        Socket nodeSocket = null;
        try {
            nodeSocket = connectedNodes.computeIfAbsent(node, NodeConnectionCheckerUtil::openSocket).socket;
            sendPing(node, nodeSocket);
        } catch (final Exception exc) {
            closeSocket(nodeSocket);
            removeBrokenNodeConnection(node);
            throw exc;
        }
    }

    private static void restoreConnectionsTtl() {
        for (final Map.Entry<Node, SocketWrapper> entry : connectedNodes.entrySet()) {
            final Node node = entry.getKey();
            final SocketWrapper socketWrapper = entry.getValue();
            if (socketWrapper.socket.isClosed() || System.currentTimeMillis() > socketWrapper.ttl) {
                removeBrokenNodeConnection(node);
            }
        }
    }

    private static SocketWrapper openSocket(final Node node) throws NetworkException {
        final Socket socket = new Socket();
        try {
            socket.connect(new InetSocketAddress(node.host(), node.port()), CONNECT_TIMEOUT);
            socket.setKeepAlive(true);
            socket.setTcpNoDelay(true);
            socket.setReuseAddress(true);
            return new SocketWrapper(socket, SOCKET_TTL);
        } catch (final Exception e) {
            closeSocket(socket);
            throw new NetworkException(String.format("Unavailable or broken connection to Node[%s];", node), e);
        }
    }

    private static void sendPing(final Node node, final Socket socket) {
        try {
            socket.sendUrgentData(1);
        } catch (final IOException e) {
            connectedNodes.compute(node, (k, v) -> {
                if (Objects.nonNull(v)) {
                    closeSocket(v);
                    return openSocket(node);
                }
                return null;
            });
        }
    }

    private static void removeBrokenNodeConnection(final Node node) {
        final SocketWrapper removed = connectedNodes.remove(node);
        closeSocket(removed);
    }

    private static void closeSocket(final SocketWrapper socketWrapper) {
        if (Objects.isNull(socketWrapper)) {
            return;
        }
        closeSocket(socketWrapper.socket);
    }

    private static void closeSocket(final Socket socket) throws NetworkException {
        if (Objects.isNull(socket)) {
            return;
        }
        try {
            socket.close();
        } catch (final IOException e) {
            throw new NetworkException(e);
        }
    }

    private static final class SocketWrapper {
        final Socket socket;
        final Long ttl;

        private SocketWrapper(final Socket socket, final int ttl, final TimeUnit ttlTimeUnit) {
            this.socket = socket;
            this.ttl = System.currentTimeMillis() + ttlTimeUnit.toMillis(ttl);
        }

        private SocketWrapper(final Socket socket, final long ttlMs) {
            this.socket = socket;
            this.ttl = System.currentTimeMillis() + ttlMs;
        }
    }

}
