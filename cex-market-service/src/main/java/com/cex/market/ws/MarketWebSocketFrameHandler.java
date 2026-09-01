package com.cex.market.ws;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.timeout.IdleStateEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/** 维护 WebSocket 生命周期，并将文本帧交给行情协议处理器。 */
@Slf4j
@Component
@ChannelHandler.Sharable
@RequiredArgsConstructor
public class MarketWebSocketFrameHandler extends SimpleChannelInboundHandler<TextWebSocketFrame> {

    private final MarketWebSocketCommandHandler commandHandler;
    private final MarketWebSocketSessionManager sessionManager;

    /**
     * 处理客户端文本协议帧。
     *
     * @param context Netty 连接上下文
     * @param frame 客户端文本帧
     */
    @Override
    protected void channelRead0(ChannelHandlerContext context, TextWebSocketFrame frame) {
        commandHandler.handle(context.channel(), frame.text());
    }

    /**
     * 在 WebSocket 握手完成时登记订阅会话。
     *
     * @param context Netty 连接上下文
     * @param event Netty 用户事件
     */
    @Override
    public void userEventTriggered(ChannelHandlerContext context, Object event) throws Exception {
        if (event instanceof WebSocketServerProtocolHandler.HandshakeComplete) {
            sessionManager.register(context.channel());
            log.info("行情 WebSocket 已连接: channelId={}, remoteAddress={}", context.channel().id(),
                    context.channel().remoteAddress());
            return;
        }
        if (event == IdleStateEvent.READER_IDLE_STATE_EVENT) {
            log.info("行情 WebSocket 心跳超时，关闭连接: channelId={}, remoteAddress={}", context.channel().id(),
                    context.channel().remoteAddress());
            context.close();
            return;
        }
        context.fireUserEventTriggered(event);
    }

    /**
     * 在连接断开时释放其订阅集合。
     *
     * @param context Netty 连接上下文
     */
    @Override
    public void channelInactive(ChannelHandlerContext context) throws Exception {
        sessionManager.unregister(context.channel());
        log.info("行情 WebSocket 已断开: channelId={}, remoteAddress={}", context.channel().id(),
                context.channel().remoteAddress());
        context.fireChannelInactive();
    }

    /**
     * 记录连接级异常并关闭连接，防止异常连接泄漏订阅状态。
     *
     * @param context Netty 连接上下文
     * @param cause 异常原因
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext context, Throwable cause) {
        log.warn("行情 WebSocket 连接异常: channelId={}, remoteAddress={}", context.channel().id(),
                context.channel().remoteAddress(), cause);
        context.close();
    }
}
