package org.jrp;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.util.Objects;

public class BootstrapTest {

    @Test
    @Disabled
    public void testBootstrap() throws Exception {
        URL resource = BootstrapTest.class.getResource("/redisproxy-async.yaml");
        String confFile = Objects.requireNonNull(resource).getPath();
        Bootstrap.main(new String[]{"-c", confFile});
    }
}
