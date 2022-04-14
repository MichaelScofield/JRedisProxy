package org.jrp.cmd;

import org.jrp.utils.BytesUtils;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertSame;

public class RedisKeywordTest {

    @Test
    public void testConvert() {
        List<String> keywords = Arrays.stream(RedisKeyword.values())
                .map(Enum::name)
                .collect(Collectors.toList());
        Collections.shuffle(keywords);

        for (String keyword : keywords) {
            assertSame(RedisKeyword.valueOf(keyword), RedisKeyword.convert(BytesUtils.bytes(keyword)));
        }
    }
}
