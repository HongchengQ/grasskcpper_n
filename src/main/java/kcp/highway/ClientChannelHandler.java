package kcp.highway;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.socket.DatagramPacket;
import io.netty.util.HashedWheelTimer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import kcp.highway.threadPool.IMessageExecutor;
import kcp.highway.threadPool.IMessageExecutorPool;

import java.util.concurrent.TimeUnit;

/**
 * Created by JinMiao
 * 2019-06-26.
 */
public class ClientChannelHandler extends ChannelInboundHandlerAdapter {
    static final Logger logger = LoggerFactory.getLogger(ClientChannelHandler.class);

    private final ClientConvChannelManager channelManager;

    private final ChannelConfig channelConfig;

    private final IMessageExecutorPool iMessageExecutorPool;

    private final KcpListener kcpListener;

    private final HashedWheelTimer hashedWheelTimer;

    // Handle handshake
    public void handleEnet(ByteBuf data, Ukcp ukcp, User user) {
        if (data == null || data.readableBytes() != 20) {
            return;
        }
        // Get
        int code = data.readInt();
        long data1 = data.readUnsignedIntLE();
        long data2 = data.readUnsignedIntLE();
        long convId = data1 << 32 | data2;
        data.readInt();
        data.readUnsignedInt();
        switch (code) {
            case 325 -> { // Handshake Resp
                IMessageExecutor iMessageExecutor = iMessageExecutorPool.getIMessageExecutor();

                KcpOutput kcpOutput = new KcpOutPutImp();
                Ukcp newUkcp = new Ukcp(kcpOutput, kcpListener, iMessageExecutor, channelConfig, channelManager);
                newUkcp.user(user);
                newUkcp.setConv(convId);
                channelManager.New(user.getRemoteAddress(), newUkcp, null);
                hashedWheelTimer.newTimeout(new ScheduleTask(iMessageExecutor, newUkcp, hashedWheelTimer),
                        newUkcp.getInterval(),
                        TimeUnit.MILLISECONDS);
                iMessageExecutor.execute(() -> {
                    try {
                        kcpListener.onConnected(newUkcp);
                    } catch (Throwable throwable) {
                        kcpListener.handleException(throwable, newUkcp);
                    }
                });
            }
            case 404 -> { // Disconnect
                if (ukcp != null) {
                    ukcp.close(false);
                }
            }
        }
    }

    public ClientChannelHandler(IChannelManager channelManager, ChannelConfig channelConfig, IMessageExecutorPool iMessageExecutorPool, HashedWheelTimer hashedWheelTimer, KcpListener kcpListener) {
        this.channelManager = (ClientConvChannelManager) channelManager;
        this.channelConfig = channelConfig;
        this.iMessageExecutorPool = iMessageExecutorPool;
        this.hashedWheelTimer = hashedWheelTimer;
        this.kcpListener = kcpListener;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        logger.error("", cause);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object object) {
        DatagramPacket msg = (DatagramPacket) object;
        ByteBuf byteBuf = msg.content();
        User user = new User(ctx.channel(), msg.sender(), msg.recipient());
        Ukcp ukcp = channelManager.get(msg);

        if (byteBuf.readableBytes() == 20) {
            // receive handshake
            handleEnet(byteBuf, ukcp, user);
            return;
        }

        if (ukcp != null) {
            ukcp.read(byteBuf);
        }
    }
}