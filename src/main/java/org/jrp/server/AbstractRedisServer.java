package org.jrp.server;

import io.lettuce.core.Range;
import io.lettuce.core.Range.Boundary;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jrp.cmd.CommandProcessors;
import org.jrp.cmd.RedisKeyword;
import org.jrp.config.ProxyConfig;
import org.jrp.exception.RedisException;
import org.jrp.monitor.ClientStat;
import org.jrp.reply.*;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;

import static org.jrp.exception.RedisException.NOT_IMPLEMENTED_ERROR;
import static org.jrp.reply.SimpleStringReply.OK;
import static org.jrp.utils.BytesUtils.*;

public abstract class AbstractRedisServer implements RedisServer {

    private static final Logger LOGGER = LogManager.getLogger(AbstractRedisServer.class);

    final ProxyConfig proxyConfig;

    public AbstractRedisServer(ProxyConfig proxyConfig) {
        this.proxyConfig = proxyConfig;
    }

    @Override
    public ProxyConfig getProxyConfig() {
        return proxyConfig;
    }

    @Override
    public final Reply select(byte[] index) {
        int db = toInt(index);
        ClientStat stat = ClientStat.getStat(RedisServerContext.getChannel());
        stat.setDb(db);
        return OK;
    }

    @Override
    public final Reply client(byte[][] args) {
        ClientStat stat = ClientStat.getStat(RedisServerContext.getChannel());
        switch (RedisKeyword.convert(args[0])) {
            case LIST:
                LOGGER.warn("start handling \"CLIENT LIST\" command from {}",
                        RedisServerContext.getCommand().getClientAddress());
                return new AsyncReply<>(() -> BulkReply.bulkReply(ClientStat.list()));
            case GETNAME:
                return BulkReply.bulkReply(stat.getName());
            case SETNAME:
                String clientName = string(args[1]);
                stat.setName(clientName);
                return SimpleStringReply.OK;
            case ID:
                return IntegerReply.integer(stat.id);
            case INFO:
                return BulkReply.bulkReply(stat.dump());
            case KILL:
            case PAUSE:
            case UNPAUSE:
            case REPLY:
            case UNBLOCK:
            case CACHING:
            case GETREDIR:
            case TRACKING:
            case TRACKINGINFO:
            default:
                return ErrorReply.NOT_IMPL;
        }
    }

    @Override
    public final Reply config(byte[][] args) {
        return switch (RedisKeyword.convert(args[0])) {
            case GET -> configGet(args);
            case SET -> configSet(args);
            case RESETSTAT -> configResetstat();
            case REWRITE -> configRewrite();
            default -> ErrorReply.SYNTAX_ERROR;
        };
    }

    private Reply configGet(byte[][] args) throws RedisException {
        String parameter = string(args[1]);
        if (parameter.startsWith("proxy.")) {
            String proxyParameter = StringUtils.removeStart(parameter, "proxy.");
            String value;
            try {
                value = BeanUtils.getProperty(getProxyConfig(), proxyParameter);
            } catch (IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
                throw new RedisException(e);
            }
            return MultiBulkReply.from(Arrays.asList(parameter, value));
        } else {
            return doConfigGet(parameter);
        }
    }

    protected Reply doConfigGet(String parameter) {
        return ErrorReply.NOT_IMPL;
    }

    private Reply configSet(byte[][] args) throws RedisException {
        String parameter = string(args[1]);
        String value = string(args[2]);
        if (parameter.startsWith("proxy.")) {
            String proxyParameter = StringUtils.removeStart(parameter, "proxy.");
            if (proxyParameter.equals("timeout")) {
                int proxyTimeout = Integer.parseInt(value);
                ClientStat stat = ClientStat.getStat(RedisServerContext.getChannel());
                stat.setProxyTimeout(proxyTimeout);
            } else {
                try {
                    BeanUtils.setProperty(getProxyConfig(), parameter, value);
                } catch (IllegalAccessException | InvocationTargetException e) {
                    throw new RedisException(e);
                }
            }
            return SimpleStringReply.OK;
        } else {
            return doConfigSet(parameter, value);
        }
    }

    protected Reply doConfigSet(String parameter, String value) {
        return ErrorReply.NOT_IMPL;
    }

    protected Reply configResetstat() {
        return ErrorReply.NOT_IMPL;
    }

    protected Reply configRewrite() {
        return ErrorReply.NOT_IMPL;
    }

    @Override
    public Reply info(byte[] section) {
        String sb = "# Proxy Config\n" + getProxyConfig() + "\n" + doInfo(section) + "\n";
        return BulkReply.bulkReply(sb);
    }

    protected String doInfo(byte[] section) {
        return null;
    }

    @Override
    public final Reply command(byte[] subcommand, byte[][] options) {
        if (subcommand == null) {
            return ErrorReply.NOT_IMPL;
        }
        return switch (RedisKeyword.convert(subcommand)) {
            case COUNT -> IntegerReply.integer(CommandProcessors.count());
            case DOCS, GETKEYS, GETKEYSANDFLAGS, INFO, LIST -> throw NOT_IMPLEMENTED_ERROR;
            default -> ErrorReply.SYNTAX_ERROR;
        };
    }

    Range<Double> newDoubleRange(byte[] min, byte[] max) {
        return Range.from(newDoubleBoundary(min), newDoubleBoundary(max));
    }

    Range<byte[]> newBytesRange(byte[] min, byte[] max) {
        return Range.from(newBytesBoundary(min), newBytesBoundary(max));
    }

    private Boundary<Double> newDoubleBoundary(byte[] bytes) {
        Boundary<Double> boundary = Boundary.unbounded();
        if (bytes != null && bytes.length > 0) {
            boundary = bytes[0] == '(' ?
                    Boundary.excluding(toDouble(bytes, 1, bytes.length - 1)) :
                    Boundary.including(toDouble(bytes));
        }
        return boundary;
    }

    private Boundary<byte[]> newBytesBoundary(byte[] bytes) {
        Boundary<byte[]> boundary = Boundary.unbounded();
        if (bytes != null && bytes.length > 0) {
            if (bytes[0] == '(') {
                boundary = Boundary.excluding(Arrays.copyOfRange(bytes, 1, bytes.length));
            } else if (bytes[0] == '[') {
                boundary = Boundary.including(Arrays.copyOfRange(bytes, 1, bytes.length));
            } else {
                boundary = Boundary.including(bytes);
            }
        }
        return boundary;
    }
}
