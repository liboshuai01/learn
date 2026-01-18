package cn.liboshuai.flink;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.MessageToByteEncoder;

public abstract class NettyMessage {
    public static final int MAGIC_NUMBER = 0xBADC0FFE; // Flink 经典魔数
    public static final int FRAME_HEADER_LENGTH = 4 + 4 + 1; // Length + Magic + ID;

    // 消息类型 ID
    public static final byte ID_PARTITION_REQUEST = 1;
    public static final byte ID_BUFFER_RESPONSE = 2;

    // --- 消息子类定义

    /**
     * 客户端向服务端请求分区数据（握手）
     */
    public static class PartitionRequest extends NettyMessage {
        public final int partitionId;
        public final int credit; // 模拟信用分

        public PartitionRequest(int partitionId, int credit) {
            this.partitionId = partitionId;
            this.credit = credit;
        }
    }

    /**
     * 服务端向客户端发送数据
     */
    public static class BufferResponse extends NettyMessage {
        public final NetworkBuffer buffer;

        public BufferResponse(NetworkBuffer buffer) {
            this.buffer = buffer;
        }
    }

    /**
     * 编码器：将对象转换为 ByteBuf
     */
    public static class MessageEncoder extends MessageToByteEncoder<NettyMessage> {

        @Override
        protected void encode(ChannelHandlerContext ctx, NettyMessage msg, ByteBuf out) throws Exception {
            // 1. 预留 4 字节写 Frame Length
            int startIndex = out.writerIndex();
            out.writeInt(0);

            // 2. 写 Magic Number
            out.writeInt(MAGIC_NUMBER);

            // 3. 写 Body
            if (msg instanceof PartitionRequest) {
                out.writeByte(ID_PARTITION_REQUEST);
                PartitionRequest req = (PartitionRequest) msg;
                out.writeInt(req.partitionId);
                out.writeInt(req.credit);
            } else if (msg instanceof BufferResponse) {
                out.writeByte(ID_BUFFER_RESPONSE);
                BufferResponse resp = (BufferResponse) msg;
                byte[] data = resp.buffer.getBytes();
                out.writeInt(data.length);
                out.writeBytes(data);
            } else {
                throw new IllegalArgumentException("未知的消息类型：" + msg.getClass());
            }

            // 4. 回填 Frame Length (当前 writeIndex - startIndex - 4字节长度字段本身）
            int endIndex = out.writerIndex();
            out.setIndex(startIndex, endIndex - startIndex - 4);
        }
    }

    public static class MessageDecoder extends LengthFieldBasedFrameDecoder {

        public MessageDecoder() {
            super(Integer.MAX_VALUE, 0, 4, 0, 4);
        }

        @Override
        protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
            ByteBuf frame = (ByteBuf) super.decode(ctx, in);
            if (frame == null) {
                return null;
            }
            try {
                // 验证魔数
                int magic = frame.readInt();
                if (magic != MAGIC_NUMBER) {
                    throw new IllegalStateException("魔数错误，网络流可能已损坏。");
                }

                // 读取消息 ID
                byte msgId = frame.readByte();
                switch (msgId) {
                    case ID_PARTITION_REQUEST:
                        int parId = frame.readInt();
                        int credit = frame.readInt();
                        return new PartitionRequest(parId, credit);

                    case ID_BUFFER_RESPONSE:
                        int dataLen = frame.readInt();
                        byte[] data = new byte[dataLen];
                        frame.readBytes(data);
                        return new BufferResponse(new NetworkBuffer(data));

                    default:
                        throw new IllegalStateException("收到未知消息 ID: " + msgId);
                }
            } finally {
                frame.release();
            }
        }
    }

}
