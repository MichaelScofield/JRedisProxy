package org.jrp.reply;

import io.netty.buffer.ByteBuf;

public interface Reply {

    byte[] CRLF = new byte[]{'\r', '\n'};

    void write(ByteBuf out);
}
