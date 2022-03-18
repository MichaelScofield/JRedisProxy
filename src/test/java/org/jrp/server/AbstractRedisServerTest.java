package org.jrp.server;

import org.apache.commons.lang3.RandomStringUtils;
import org.jrp.cmd.CommandProcessors;
import org.jrp.exception.RedisException;
import org.jrp.reply.BulkReply;
import org.jrp.reply.IntegerReply;
import org.jrp.reply.Reply;
import org.jrp.reply.SimpleStringReply;
import org.junit.jupiter.api.Test;

import static org.jrp.utils.BytesUtils.bytes;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AbstractRedisServerTest {

    AbstractRedisServer server = new Dummy();

    String getRandomString() {
        return RandomStringUtils.randomAlphabetic(10);
    }

    @Test
    public void testEcho() {
        String m = getRandomString();
        BulkReply reply = server.echo(bytes(m));
        assertEquals(m, reply.toString());
    }

    @Test
    public void testPing() {
        Reply reply1 = server.ping(null);
        assertTrue(reply1 instanceof SimpleStringReply);
        assertEquals("PONG", reply1.toString());

        String m = getRandomString();
        Reply reply2 = server.ping(bytes(m));
        assertTrue(reply2 instanceof BulkReply);
        assertEquals(m, reply2.toString());
    }

    @Test
    public void testCommandCount() throws RedisException {
        Reply count = server.command(bytes("COUNT"), null);
        assertTrue(count instanceof IntegerReply);
        assertEquals(CommandProcessors.count(), ((IntegerReply) count).integer());
    }

    static class Dummy extends AbstractRedisServer {
        public Dummy() {
            super(null);
        }
    }
}
