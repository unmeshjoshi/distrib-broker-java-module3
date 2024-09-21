package com.dist.simplekafka;

import com.dist.common.TestUtils;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class LogTest {

    @Test
    public void appendsMessages() throws IOException {
        Log log = new Log(TestUtils.tempFile());
        long offset = log.append("key".getBytes(), "value".getBytes());
        System.out.println("offset = " + offset);

        long offset2 = log.append("key1".getBytes(), "value1".getBytes());
        System.out.println("offset2 = " + offset2);

        long offset3 = log.append("key2".getBytes(), "value2".getBytes());

        Log.Message message = log.readSingleMessage(offset);
        assertEquals("value", new String(message.value));

        Log.Message message2 = log.readSingleMessage(offset3);
        assertEquals("value2", new String(message2.value));

        Log.Message message3 = log.readSingleMessage(offset3);
        assertEquals("value2", new String(message3.value));
    }

}