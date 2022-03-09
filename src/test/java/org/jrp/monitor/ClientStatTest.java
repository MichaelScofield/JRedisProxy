package org.jrp.monitor;

import io.netty.channel.DefaultChannelId;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class ClientStatTest {

    @Test
    public void testClientActive() {
        EmbeddedChannel channel = new EmbeddedChannel(DefaultChannelId.newInstance());
        assertNull(ClientStat.getStat(channel));
        ClientStat.active(channel);
        assertNotNull(ClientStat.getStat(channel));
    }

    @Test
    public void testClientInactive() {
        EmbeddedChannel channel = new EmbeddedChannel(DefaultChannelId.newInstance());
        ClientStat.getClientStats().put(channel.id(), new ClientStat(channel));
        assertNotNull(ClientStat.getStat(channel));
        ClientStat.inactive(channel);
        assertNull(ClientStat.getStat(channel));
    }

    @Test
    public void testRecordActivity() {
        EmbeddedChannel channel = new EmbeddedChannel(DefaultChannelId.newInstance());
        ClientStat.getClientStats().put(channel.id(), new ClientStat(channel));
        ClientStat stat = ClientStat.getStat(channel);
        long lastActive = stat.getLastActive();
        ClientStat.recordActivity(channel);
        assertTrue(stat.getLastActive() > lastActive);
    }

    @Test
    public void testListClientStats() {
        ClientStat.getClientStats().clear();

        for (int i = 0; i < 10; i++) {
            EmbeddedChannel channel = new EmbeddedChannel(DefaultChannelId.newInstance());
            ClientStat stat = new ClientStat(channel);
            ClientStat.getClientStats().put(channel.id(), stat);
        }
        String[] clientList = ClientStat.list().split("\\n");
        assertEquals(10, clientList.length);
        for (String dump : clientList) {
            assertTrue(dump.matches("id=\\d+ " +
                    "addr=embedded " +
                    "laddr=null " +
                    "fd=0 " +
                    "name=null " +
                    "age=\\d+ " +
                    "idle=\\d+ " +
                    "flags=null " +
                    "db=0 " +
                    "sub=0 " +
                    "psub=0 " +
                    "multi=0 " +
                    "qbuf=0 " +
                    "qbuf-free=0 " +
                    "argv-mem=0 " +
                    "obl=0 " +
                    "oll=0 " +
                    "omem=0 " +
                    "tot-mem=0 " +
                    "events= {2}" +
                    "cmd=null " +
                    "user=null " +
                    "redir=0"));
        }
    }

    @Test
    public void testDumpClientStat() {
        EmbeddedChannel channel = new EmbeddedChannel(DefaultChannelId.newInstance());
        ClientStat stat = new ClientStat(channel);
        stat.setLaddr("127.0.0.1:16379");
        stat.setFd(1);
        stat.setName("testName");
        stat.setFlags("testFlags");
        stat.setDb(2);
        stat.setSub(3);
        stat.setPsub(4);
        stat.setMulti(5);
        stat.setQbuf(6);
        stat.setQbufFree(7);
        stat.setArgvMem(8);
        stat.setObl(9);
        stat.setOll(10);
        stat.setOmem(11);
        stat.setTotalMem(12);
        stat.setEvents('r');
        stat.setCmd("ping");
        stat.setUser("root");
        stat.setRedir(-1);
        assertTrue(stat.dump().matches("id=\\d+ " +
                "addr=embedded " +
                "laddr=127\\.0\\.0\\.1:16379 " +
                "fd=1 " +
                "name=testName " +
                "age=\\d+ " +
                "idle=\\d+ " +
                "flags=testFlags " +
                "db=2 " +
                "sub=3 " +
                "psub=4 " +
                "multi=5 " +
                "qbuf=6 " +
                "qbuf-free=7 " +
                "argv-mem=8 " +
                "obl=9 " +
                "oll=10 " +
                "omem=11 " +
                "tot-mem=12 " +
                "events=r " +
                "cmd=ping " +
                "user=root " +
                "redir=-1"));
    }
}
