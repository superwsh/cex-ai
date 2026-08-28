package com.cex.market.ws;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.stream.ChunkedWriteHandler;
import io.netty.util.concurrent.GlobalEventExecutor;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * 行情 WebSocket 服务器（Netty 实现，高并发推送）
 *
 * 链路：Kafka 消费 TradeEvent -> 聚合行情 -> broadcast() 推送给所有已订阅客户端
 * 订阅协议（如 {"type":"subscribe","channel":"tick.BTCUSDT"}）后续在业务层实现
 */
@Slf4j
@Component
public class NettyWebSocketServer {

    @Value("${netty.ws.port:9001}")
    private int port;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;

    /** 当前所有已连接客户端 */
    private static final ChannelGroup CHANNELS = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

    @PostConstruct
    public void start() throws InterruptedException {
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup();
        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(
                                new HttpServerCodec(),
                                new HttpObjectAggregator(65536),
                                new ChunkedWriteHandler(),
                                new WebSocketServerProtocolHandler("/ws"),
                                new SimpleChannelInboundHandler<TextWebSocketFrame>() {
                                    @Override
                                    protected void channelRead0(ChannelHandlerContext ctx, TextWebSocketFrame frame) {
                                        // 骨架：回显心跳/订阅消息，订阅路由后续实现
                                        ctx.writeAndFlush(new TextWebSocketFrame(frame.text()));
                                    }

                                    @Override
                                    public void handlerAdded(ChannelHandlerContext ctx) {
                                        CHANNELS.add(ctx.channel());
                                    }

                                    @Override
                                    public void handlerRemoved(ChannelHandlerContext ctx) {
                                        CHANNELS.remove(ctx.channel());
                                    }

                                    @Override
                                    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                        log.error("WebSocket 连接异常: {}", ctx.channel().remoteAddress(), cause);
                                        ctx.close();
                                    }
                                });
                    }
                });
        serverChannel = bootstrap.bind(port).sync().channel();
        log.info("行情 WebSocket 服务器已启动，端口: {}", port);
    }

    /**
     * 向所有连接客户端广播行情消息（由 Kafka 消费线程调用）
     */
    public void broadcast(String message) {
        CHANNELS.writeAndFlush(new TextWebSocketFrame(message));
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
