package com.i.server.handler;

import com.alibaba.fastjson.JSON;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.CharsetUtil;

/**
 * @author ：liqiang.
 * @date ：Created in 21:04 2018/12/15
 */
@Sharable
public class EchoServerHandler extends ChannelInboundHandlerAdapter {
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        // ByteBuf in = (ByteBuf) msg;
        // System.out.println("Server received: " +
        // in.toString(CharsetUtil.UTF_8));
        // ctx.write(in);
        System.out.println("Server received");
        if (msg instanceof FullHttpRequest) {
            System.out.println("FullHttpRequest received");
            FullHttpRequest req = (FullHttpRequest) msg;

            try {

                // 1.获取URI
                String uri = req.uri();

                // 2.获取请求体
                ByteBuf buf = req.content();
                String content = buf.toString(CharsetUtil.UTF_8);

                // 3.获取请求方法
                HttpMethod method = req.method();

                // 4.获取请求头
                HttpHeaders headers = req.headers();

                // 5.根据method，确定不同的逻辑
                if (method.equals(HttpMethod.GET)) {

                    // TODO
                }

                if (method.equals(HttpMethod.POST)) {
                    // 接收用户输入，并将输入返回给用户
                    Content c = new Content();
                    c.setUri(uri);
                    c.setContent(content);

                    response(ctx, c);
                }

                if (method.equals(HttpMethod.PUT)) {
                    // TODO
                }

                if (method.equals(HttpMethod.DELETE)) {
                    // TODO
                }
            } finally {
                req.release();
            }
        }
        System.out.println("FullHttpRequest end");
    }

    class Content {
        String uri;
        String content;

        public String getUri() {
            return uri;
        }

        public void setUri(String uri) {
            this.uri = uri;
        }

        public String getContent() {
            return content;
        }

        public void setContent(String content) {
            this.content = content;
        }
    }

    private void response(ChannelHandlerContext ctx, Content c) {

        // 1.设置响应
        FullHttpResponse resp = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK,
                Unpooled.copiedBuffer(JSON.toJSONString(c), CharsetUtil.UTF_8));
        resp.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/html; charset=UTF-8");

        // 2.发送
        // 注意必须在使用完之后，close channel
        ctx.writeAndFlush(resp).addListener(ChannelFutureListener.CLOSE);
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.writeAndFlush(ChannelFutureListener.CLOSE);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }

}
