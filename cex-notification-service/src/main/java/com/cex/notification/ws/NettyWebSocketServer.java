package com.cex.notification.ws;

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
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * 通知 WebSocket 服务器（Netty 实现，定向推送）
 *
 * 链路：Kafka 消费成交回报/资产变动 -> 按 userId 定位连接 -> 定向推送
 * 客户端鉴权（token -> userId 映射）后续在业务层实现
 */
@Slf4j
@Component
public class NettyWebSocketServer {

    @Value("${netty.ws.port:9002}")
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
                                        // 骨架：回显心跳消息，鉴权/订阅协议后续实现
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
        log.info("通知 WebSocket 服务器已启动，端口: {}", port);
    }

    /**
     * 消费成交事件示例：通知服务收到成交后定向推送用户
     * （TODO: 按 buyOrderId/sellOrderId 关联 userId 后调用 sendToUser）
     */
    @KafkaListener(topics = "cex.trade.event", groupId = "cex-notification")
    public void onTrade(String tradeJson) {
        log.info("收到成交事件: {}", tradeJson);
        broadcast(tradeJson);
    }

    /**
     * 向所有连接客户端广播（简化实现；正式实现按 userId 路由定向推送）
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
