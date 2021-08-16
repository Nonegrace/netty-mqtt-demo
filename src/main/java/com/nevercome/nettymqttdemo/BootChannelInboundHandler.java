package com.nevercome.nettymqttdemo;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ChannelHandler.Sharable
public class BootChannelInboundHandler extends ChannelInboundHandlerAdapter {
    private Logger log = LoggerFactory.getLogger(this.getClass());

    public BootChannelInboundHandler() {
        super();
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        super.channelRegistered(ctx);
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        super.channelUnregistered(ctx);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
    }

    /**
     * 从客户端收到新的数据时，这个方法会在收到消息时被调用
     *
     * @param ctx
     * @param msg
     * @throws Exception
     */
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg != null) {
            MqttMessage mqttMessage = (MqttMessage) msg;
            log.debug("MQTT Message: " + mqttMessage.toString());
            MqttFixedHeader mqttFixedHeader = mqttMessage.fixedHeader();
            Channel channel = ctx.channel();

            if (mqttFixedHeader.messageType().equals(MqttMessageType.CONNECT)) {
                // 在一个网络连接上，客户端只能发送一次CONNECT报文。
                // 服务端必须将客户端发送的第二个CONNECT报文当作协议违规处理并断开客户端的连接
                // TODO 建议connect消息单独处理，用来对客户端进行认证管理等 这里直接返回一个CONNACK消息
                BootMqttMsgBack.connack(channel, mqttMessage);
            }

            switch (mqttFixedHeader.messageType()) {
                case PUBLISH: // 客户端发布消息
                    // PUBACK 报文是对 QoS1 等级的 PUBLISH 报文的响应
                    log.debug("Publish Ack: Hello World");
                    BootMqttMsgBack.puback(channel, mqttMessage);
                case PUBREL: // 发布释放
                    // PUBREL 报文是对 PUBREC 报文的响应
                    //	TODO
                    BootMqttMsgBack.pubcomp(channel, mqttMessage);
                    break;
                case SUBSCRIBE: // 客户端订阅主题
                    // 客户端向服务端发送 SUBSCRIBE 报文用于创建一个或多个订阅，每个订阅注册客户端关心的一个或多个主题。
                    // 为了将应用消息转发给与那些订阅匹配的主题，服务端发送 PUBLISH 报文给客户端。
                    // SUBSCRIBE 报文也（为每个订阅）指定了最大的 QoS 等级，服务端根据这个发送应用消息给客户端
                    // TODO
                    BootMqttMsgBack.suback(channel, mqttMessage);
                    break;
                case UNSUBSCRIBE: // 客户端取消订阅
                    //	客户端发送 UNSUBSCRIBE 报文给服务端，用于取消订阅主题
                    //	TODO
                    BootMqttMsgBack.unsuback(channel, mqttMessage);
                    break;
                case PINGREQ: // 客户端发起心跳
                    // 客户端发送 PINGREQ 报文给服务端的
                    // 在没有任何其它控制报文从客户端发给服务的时，告知服务端客户端还活着
                    // 请求服务端发送 响应确认它还活着，使用网络以确认网络连接没有断开
                    BootMqttMsgBack.pingresp(channel, mqttMessage);
                    break;
                case DISCONNECT: // 客户端主动断开连接
                    //	DISCONNECT 报文是客户端发给服务端的最后一个控制报文，服务端必须验证所有的保留位都被设置为 0
                    //	TODO
                    break;
                default:
                    break;
            }
        }
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        super.channelReadComplete(ctx);
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        super.userEventTriggered(ctx, evt);
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
        super.channelWritabilityChanged(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        super.exceptionCaught(ctx, cause);
    }
}