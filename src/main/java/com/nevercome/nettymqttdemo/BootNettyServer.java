package com.nevercome.nettymqttdemo;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.timeout.IdleStateHandler;

import javax.net.ssl.*;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;

public class BootNettyServer {
    private int port = 2883;

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

            bootstrap.childHandler(initSSL());

            ChannelFuture f = bootstrap.bind(port).sync();
            f.channel().closeFuture().sync();

            System.out.println("server up successfully");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private ChannelInitializer initSSL() throws NoSuchAlgorithmException {
        SSLContext sslContext = SSLContext.getInstance("TLSv1.3");
        String keyStorePassword = "keystore";
        try {
            KeyStore ks = KeyStore.getInstance("JKS");
            ks.load(getClass().getClassLoader().getResourceAsStream("cert/server.keystore"), keyStorePassword.toCharArray());

            // Set up key manager factory to use our key store
            KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(ks, keyStorePassword.toCharArray());

            // truststore
            KeyStore ts = KeyStore.getInstance("JKS");
            ts.load(getClass().getClassLoader().getResourceAsStream("cert/servertruststore.keystore"), keyStorePassword.toCharArray());

            // set up trust manager factory to use our trust store
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(ts);
            TrustManager[] trustManagers = new TrustManager[tmf.getTrustManagers().length];
            for (int i = 0; i < tmf.getTrustManagers().length; i++) {
                TrustManager tm = tmf.getTrustManagers()[i];
                if (tm instanceof X509TrustManager) {
                    trustManagers[i] = (X509TrustManager) tm;
                }
            }

            // Initialize the SSLContext to work with our key managers.
            sslContext.init(kmf.getKeyManagers(), trustManagers, null);

        } catch (Exception e) {
            throw new Error("Failed to initialize the server-side SSLContext", e);
        }
        SSLEngine sslEngine = sslContext.createSSLEngine();
        sslEngine.setUseClientMode(false);
        sslEngine.setNeedClientAuth(true);
        sslEngine.setEnableSessionCreation(true);

        ChannelInitializer ch = new ChannelInitializer<>() {
            @Override
            protected void initChannel(Channel ch) throws Exception {
                ChannelPipeline channelPipeline = ch.pipeline();
                channelPipeline.addLast(new SslHandler(sslEngine));
                channelPipeline.addLast(new IdleStateHandler(600, 600, 1200));
                channelPipeline.addLast("encoder", MqttEncoder.INSTANCE);
                channelPipeline.addLast("decoder", new MqttDecoder());
                channelPipeline.addLast(new BootChannelInboundHandler());
            }
        };
        return ch;
    }

    public void shutdown() throws InterruptedException {
        if (workGroup != null && bossGroup != null) {
            bossGroup.shutdownGracefully();
            workGroup.shutdownGracefully();
            System.out.println("server shutdown successfully");
        }
    }

}
