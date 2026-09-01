package com.cex.market.ws;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.stream.ChunkedWriteHandler;
import io.netty.handler.timeout.IdleStateHandler;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

/**
 * 行情 WebSocket Netty 服务，提供握手、心跳和订阅协议接入。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class NettyWebSocketServer {

    private static final int MAX_HTTP_CONTENT_LENGTH = 65_536;

    private final MarketWebSocketFrameHandler frameHandler;

    @Value("${netty.ws.port:9001}")
    private int port;

    @Value("${netty.ws.heartbeat-timeout-seconds:90}")
    private int heartbeatTimeoutSeconds;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;

    /**
     * 启动独立的 Netty WebSocket 监听端口。
     *
     * @throws InterruptedException 绑定端口时线程被中断
     */
    @PostConstruct
    public void start() throws InterruptedException {
        if (heartbeatTimeoutSeconds <= 0) {
            throw new IllegalStateException("WebSocket 心跳超时时间必须大于零");
        }
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup();
        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel channel) {
                        channel.pipeline().addLast(
                                new HttpServerCodec(),
                                new HttpObjectAggregator(MAX_HTTP_CONTENT_LENGTH),
                                new ChunkedWriteHandler(),
                                new IdleStateHandler(heartbeatTimeoutSeconds, 0, 0, TimeUnit.SECONDS),
                                new WebSocketServerProtocolHandler("/ws"),
                                frameHandler);
                    }
                });
        serverChannel = bootstrap.bind(port).sync().channel();
        log.info("行情 WebSocket 服务器已启动: port={}, heartbeatTimeoutSeconds={}", port, heartbeatTimeoutSeconds);
    }

    @PreDestroy
    public void stop() {
        if (serverChannel != null) {
            serverChannel.close();
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }
    }
}
