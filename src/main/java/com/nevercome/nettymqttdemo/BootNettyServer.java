package com.nevercome.nettymqttdemo;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.timeout.IdleStateHandler;

public class BootNettyServer {
    private int port = 1883;

    private NioEventLoopGroup bossGroup;

    private NioEventLoopGroup workGroup;

    public void startUp() {
        try {
            bossGroup = new NioEventLoopGroup();
            workGroup = new NioEventLoopGroup();

            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workGroup);
            bootstrap.channel(NioServerSocketChannel.class);

            bootstrap.option(ChannelOption.SO_REUSEADDR, true)
                    .option(ChannelOption.SO_BACKLOG, 1024)
                    .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                    .option(ChannelOption.SO_RCVBUF, 10485760);

            bootstrap.childOption(ChannelOption.TCP_NODELAY, true)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);

            bootstrap.childHandler(new ChannelInitializer<>() {
                @Override
                protected void initChannel(Channel ch) throws Exception {
                    ChannelPipeline channelPipeline = ch.pipeline();
                    channelPipeline.addLast(new IdleStateHandler(600, 600, 1200));
                    channelPipeline.addLast("encoder", MqttEncoder.INSTANCE);
                    channelPipeline.addLast("decoder", new MqttDecoder());
                    channelPipeline.addLast(new BootChannelInboundHandler());
                }
            });

            ChannelFuture f = bootstrap.bind(port).sync();
            f.channel().closeFuture().sync();

            System.out.println("server up successfully");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void shutdown() throws InterruptedException {
        if (workGroup != null && bossGroup != null) {
            bossGroup.shutdownGracefully();
            workGroup.shutdownGracefully();
            System.out.println("server shutdown successfully");
        }
    }

}
