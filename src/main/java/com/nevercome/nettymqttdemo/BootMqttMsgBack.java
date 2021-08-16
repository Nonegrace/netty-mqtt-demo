package com.nevercome.nettymqttdemo;

import io.netty.channel.Channel;
import io.netty.handler.codec.mqtt.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class BootMqttMsgBack {

    private static Logger log = LoggerFactory.getLogger(BootMqttMsgBack.class);

    /**
     * 确认连接请求
     *
     * @param channel
     * @param mqttMessage
     */
    public static void connack(Channel channel, MqttMessage mqttMessage) {
        MqttConnectMessage mqttConnectMessage = (MqttConnectMessage) mqttMessage;
        MqttFixedHeader mqttFixedHeaderInfo = mqttConnectMessage.fixedHeader();
        MqttConnectVariableHeader mqttConnectVariableHeaderInfo = mqttConnectMessage.variableHeader();

        // 构建返回报文，可变报头
        MqttConnAckVariableHeader mqttConnAckVariableHeaderBack = new
                MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_ACCEPTED,
                mqttConnectVariableHeaderInfo.isCleanSession());
        // 构建返回报文，固定报头
        MqttFixedHeader mqttFixedHeaderBack = new
                MqttFixedHeader(MqttMessageType.CONNACK, mqttFixedHeaderInfo.isDup(),
                MqttQoS.AT_MOST_ONCE, mqttFixedHeaderInfo.isRetain(), 0x02);
        // 构建 CONNACK 消息体
        MqttConnAckMessage connAck = new MqttConnAckMessage(mqttFixedHeaderBack, mqttConnAckVariableHeaderBack);
        log.info("Connect Back: " + connAck.toString());
        channel.writeAndFlush(connAck);
    }

    /**
     * 根据 qos 发布确认
     *
     * @param channel
     * @param mqttMessage
     */
    public static void puback(Channel channel, MqttMessage mqttMessage) {
        MqttPublishMessage mqttPublishMessage = (MqttPublishMessage) mqttMessage;
        MqttFixedHeader mqttFixedHeaderInfo = mqttPublishMessage.fixedHeader();
        MqttQoS qos = (MqttQoS) mqttFixedHeaderInfo.qosLevel();
        byte[] headBytes = new byte[mqttPublishMessage.payload().readableBytes()];
        mqttPublishMessage.payload().readBytes(headBytes);
        String data = new String(headBytes);
        log.debug("Publish Data: " + data);

        switch (qos) {
            case AT_MOST_ONCE:
                break;
            case AT_LEAST_ONCE:
                // 构建返回报文 可变报头
                MqttMessageIdVariableHeader mqttMessageIdVariableHeaderBack = MqttMessageIdVariableHeader
                        .from(mqttPublishMessage.variableHeader().packetId());
                // 构建返回报文，固定报头
                MqttFixedHeader mqttFixedHeaderBack = new
                        MqttFixedHeader(MqttMessageType.PUBACK, mqttFixedHeaderInfo.isDup(),
                        MqttQoS.AT_MOST_ONCE, mqttFixedHeaderInfo.isRetain(), 0x02);
                // 构建 PUBACK 消息体
                MqttPubAckMessage pubAck = new MqttPubAckMessage(mqttFixedHeaderBack, mqttMessageIdVariableHeaderBack);
                log.info("Publish Back: " + pubAck.toString());
                channel.writeAndFlush(pubAck);
                break;
        }
    }

    /**
     * 发布完成 qos2
     *
     * @param channel
     * @param mqttMessage
     */
    public static void pubcomp(Channel channel, MqttMessage mqttMessage) {
        MqttPublishMessage mqttPublishMessage = (MqttPublishMessage) mqttMessage;
        // 构建返回报文，固定报头
        MqttFixedHeader mqttFixedHeaderBack = new
                MqttFixedHeader(MqttMessageType.PUBCOMP, false,
                MqttQoS.AT_MOST_ONCE, false, 0x02);
        // 构建返回报文，可变报头
        MqttMessageIdVariableHeader mqttMessageIdVariableHeaderBack = MqttMessageIdVariableHeader
                .from(mqttPublishMessage.variableHeader().packetId());
        MqttMessage mqttMessageBack = new MqttMessage(mqttFixedHeaderBack, mqttMessageIdVariableHeaderBack);
        log.info("Publish Complete Back: " + mqttMessageBack.toString());
        channel.writeAndFlush(mqttMessageBack);
    }

    /**
     * 订阅确认
     *
     * @param channel
     * @param mqttMessage
     */
    public static void suback(Channel channel, MqttMessage mqttMessage) {
        MqttSubscribeMessage mqttSubscribeMessage = (MqttSubscribeMessage) mqttMessage;
        MqttMessageIdVariableHeader messageIdVariableHeader = mqttSubscribeMessage.variableHeader();
        // 构建返回报文，可变报头
        MqttMessageIdVariableHeader variableHeaderBack = MqttMessageIdVariableHeader.from(messageIdVariableHeader.messageId());
        Set<String> topics = mqttSubscribeMessage.payload().topicSubscriptions()
                .stream().map(MqttTopicSubscription::topicName).collect(Collectors.toSet());
        log.debug("Topics: " + topics);

        List<Integer> grantedQoSLevels = new ArrayList<>(topics.size());
        for (int i = 0; i < topics.size(); i++) {
            grantedQoSLevels.add(mqttSubscribeMessage.payload().topicSubscriptions().get(i).qualityOfService().value());
        }
        // 构建返回报文，有效负载
        MqttSubAckPayload payloadBack = new MqttSubAckPayload(grantedQoSLevels);
        // 构建返回报文，固定报头
        MqttFixedHeader mqttFixedHeaderBack = new
                MqttFixedHeader(MqttMessageType.SUBACK, false,
                MqttQoS.AT_MOST_ONCE, false, 2 + topics.size());
        // 构建返回报文，订阅确认
        MqttSubAckMessage subAck = new MqttSubAckMessage(mqttFixedHeaderBack, variableHeaderBack, payloadBack);
        log.info("Subscribe Back: " + subAck.toString());
        channel.writeAndFlush(subAck);
    }

    /**
     * 取消订阅确认
     *
     * @param channel
     * @param mqttMessage
     */
    public static void unsuback(Channel channel, MqttMessage mqttMessage) {
        MqttMessageIdVariableHeader messageIdVariableHeader = (MqttMessageIdVariableHeader) mqttMessage.variableHeader();
        // 构建返回报文，可变报头
        MqttMessageIdVariableHeader variableHeaderBack = MqttMessageIdVariableHeader.from(messageIdVariableHeader.messageId());
        // 构建返回报文，固定报头
        MqttFixedHeader mqttFixedHeaderBack = new
                MqttFixedHeader(MqttMessageType.UNSUBACK, false,
                MqttQoS.AT_MOST_ONCE, false, 2);
        // 构建返回报文，取消订阅确认
        MqttUnsubAckMessage unSubAck = new MqttUnsubAckMessage(mqttFixedHeaderBack, variableHeaderBack);
        log.info("UnSubscribe Back: " + unSubAck.toString());
        channel.writeAndFlush(unSubAck);
    }

    /**
     * 心跳响应
     *
     * @param channel
     * @param mqttMessage
     */
    public static void pingresp(Channel channel, MqttMessage mqttMessage) {
        // 心跳响应报文，11010000 00000000，固定报文
        MqttFixedHeader fixedHeader = new
                MqttFixedHeader(MqttMessageType.PINGRESP, false,
                MqttQoS.AT_MOST_ONCE, false, 0);
        MqttMessage mqttMessageBack = new MqttMessage(fixedHeader);
        log.info("Ping Back: " + mqttMessageBack.toString());
        channel.writeAndFlush(mqttMessageBack);
    }

}
