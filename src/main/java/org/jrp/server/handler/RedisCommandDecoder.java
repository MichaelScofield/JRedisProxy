package org.jrp.server.handler;

import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ReplayingDecoder;
import org.jrp.cmd.Command;
import org.jrp.cmd.InvalidCommand;
import org.jrp.config.ProxyConfig;
import org.jrp.exception.RedisCodecException;
import org.jrp.monitor.ClientStat;
import org.jrp.monitor.metrics.RedisproxyMetrics;
import org.jrp.monitor.metrics.RedisproxyStatusMetrics;
import org.jrp.reply.ErrorReply;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

// TODO ReplayingDecoder is not efficient, use Netty's RedisDecoder.
public class RedisCommandDecoder extends ReplayingDecoder<Void> {

    private static final byte CR = '\r';
    private static final byte LF = '\n';

    private final int maxQueuedCommands;

    private byte[][] bulkStrings;
    private int currBulkStringIndex;

    public RedisCommandDecoder() {
        this(ProxyConfig.DEFAULT_MAX_QUEUED_COMMANDS);
    }

    public RedisCommandDecoder(int maxQueuedCommands) {
        this.maxQueuedCommands = maxQueuedCommands;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        if (bulkStrings != null) {
            Command command = decodeBulkStrings(in);
            pushHandlerQueue(ctx, command, out);
        } else if (in.getByte(in.readerIndex()) == '*') {
            decodeArray(in);
            decode(ctx, in, out);
        } else {
            Command command = decodeInline(in);
            pushHandlerQueue(ctx, command, out);
        }
    }

    @VisibleForTesting
    Command decodeInline(ByteBuf in) {
        int lastReaderIndex = in.readerIndex();

        int len = in.bytesBefore(CR);
        byte[] line = new byte[len];
        in.readBytes(line);
        in.skipBytes(2);

        int currReaderIndex = in.readerIndex();
        checkpoint(lastReaderIndex, currReaderIndex);

        List<byte[]> tokens = new ArrayList<>();
        int i = 0, j = -1;
        for (; i < line.length; i++) {
            byte b = line[i];
            if (b != ' ') {
                if (j == -1) {
                    j = i;
                }
            } else {
                if (j != -1) {
                    tokens.add(Arrays.copyOfRange(line, j, i));
                    j = -1;
                }
            }
        }
        if (j != -1) {
            tokens.add(Arrays.copyOfRange(line, j, i));
        }
        return new Command(tokens.toArray(byte[][]::new));
    }

    @VisibleForTesting
    void decodeArray(ByteBuf in) throws RedisCodecException {
        int lastReaderIndex = in.readerIndex();
        in.skipBytes(1);

        int bulkStringsNum = readNum(in);
        if (bulkStringsNum > 1048576 || bulkStringsNum < 0) {
            throw new IllegalArgumentException(String.format(
                    "Array length %s is not valid, must in range [0, 1048576)", bulkStringsNum));
        }
        bulkStrings = new byte[bulkStringsNum][];
        currBulkStringIndex = 0;

        int currReaderIndex = in.readerIndex();
        checkpoint(lastReaderIndex, currReaderIndex);
    }

    @VisibleForTesting
    Command decodeBulkStrings(ByteBuf in) {
        try {
            while (currBulkStringIndex < bulkStrings.length) {
                decodeBulkString(in);
                currBulkStringIndex += 1;
            }
            Command command = new Command(bulkStrings);
            reset();
            return command;
        } catch (Exception e) {
            reset();
            return new InvalidCommand(e);
        }
    }

    private void reset() {
        bulkStrings = null;
        currBulkStringIndex = 0;
    }

    private void decodeBulkString(ByteBuf in) throws RedisCodecException {
        int lastReaderIndex = in.readerIndex();
        byte b = in.readByte();
        if (b != '$') {
            in.skipBytes(in.readableBytes());
            throw new RedisCodecException(String.format(
                    "Expecting '$' as bulk string start, got '%s'", (char) b));
        }

        int len = readNum(in);
        if (len > 1048576 || len < 0) {
            in.skipBytes(len + 2);
            throw new RedisCodecException(String.format(
                    "Bulk string size %s is not valid, must in range [0, 1048576)", len));
        }
        int bytesBefore = in.bytesBefore(CR);
        if (bytesBefore != len) {
            in.skipBytes(in.readableBytes());
            throw new RedisCodecException(String.format(
                    "Expecting %s bytes before \\r, actual %d", len, bytesBefore));
        }

        byte[] rawBulkString = new byte[len];
        in.readBytes(rawBulkString);
        bulkStrings[currBulkStringIndex] = rawBulkString;
        in.skipBytes(2);

        int currReaderIndex = in.readerIndex();
        checkpoint(lastReaderIndex, currReaderIndex);
    }

    private void pushHandlerQueue(ChannelHandlerContext ctx, Command command, List<Object> out) {
        RedisproxyMetrics.getCurrent().recv.incr();

        if (RedisproxyStatusMetrics.INSTANCE.queued.get() > maxQueuedCommands) {
            ctx.channel().writeAndFlush(ErrorReply.BUSY_ERROR);
        } else {
            RedisproxyStatusMetrics.INSTANCE.queued.incr();

            out.add(command);
        }

        Channel channel = ctx.channel();
        ClientStat.recordActivity(channel);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        try {
            super.channelRead(ctx, msg);
        } catch (Exception e) {
            RedisproxyMetrics.getCurrent().errProtocol.incr();

            throw e;
        }
    }

    private void checkpoint(int lastReaderIndex, int currReaderIndex) {
        // TODO use Netty's traffic handler to record bytesIn
        RedisproxyMetrics.getCurrent().bytesIn.incrBy(currReaderIndex - lastReaderIndex);
        super.checkpoint();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        ClientStat.active(ctx.channel());
        RedisproxyStatusMetrics.INSTANCE.activeConn.incr();
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        ClientStat.inactive(ctx.channel());
        RedisproxyStatusMetrics.INSTANCE.activeConn.decr();
        super.channelInactive(ctx);
    }

    @VisibleForTesting
    static int readNum(ByteBuf byteBuf) throws RedisCodecException {
        byte b = byteBuf.readByte();
        boolean isNegative = b == '-';
        if (isNegative) {
            b = byteBuf.readByte();
        }

        int acc = 0;
        do {
            if (b == CR) {
                if (byteBuf.readableBytes() < 1) {
                    throw new RedisCodecException(
                            "expect '\\n' after '\\r' as RESP delimiter, actual EOF");
                }
                if ((b = byteBuf.readByte()) != LF) {
                    throw new RedisCodecException(String.format(
                            "expect '\\n' after '\\r' as RESP delimiter, actual %s", b));
                }
                break;
            }

            int v = b - '0';
            if (v >= 0 && v < 10) {
                acc *= 10;
                acc += v;
            } else {
                throw new RedisCodecException(String.format(
                        "expect got number to be parsed, actual got '%s'", (char) b));
            }

            b = byteBuf.readByte();
        } while (true);
        return isNegative ? -acc : acc;
    }

    @VisibleForTesting
    byte[][] getBulkStrings() {
        return bulkStrings;
    }

    @VisibleForTesting
    void setBulkStrings(byte[][] bulkStrings) {
        this.bulkStrings = bulkStrings;
    }

    @VisibleForTesting
    int getCurrBulkStringIndex() {
        return currBulkStringIndex;
    }
}
