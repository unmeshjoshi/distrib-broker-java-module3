package com.dist.simplekafka;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Log {
    private static final int MessageSizeLength = 4; //we can store a
    // MessageSet instead of a single message.
    private static final int KeySizeLength = 4;
    private static final int KeyOffset = KeySizeLength;
    private static final int ValueSizeLength = 4;
    private final AtomicLong nextOffset =
            new AtomicLong(1); //initialize from the file at startup.

    private final Map<Long, Long> offsetIndex = new ConcurrentHashMap<>();
    private final Lock lock = new ReentrantLock(); //lock for log.append.
    // reads don't need locks


    private final FileChannel channel;

    public Log(File file) throws IOException {
        channel = openChannel(file);
    }

    public List<Message> read(long startOffset, long maxOffset) throws IOException {
        List<Message> messages = new ArrayList<>();
        for (long i = startOffset; i <= maxOffset; i++) {
            messages.add(readSingleMessage(i)); //This can be optimized to
            // read all the messages in one read.
        }
        return messages;
    }

    public long lastOffset() {
        return nextOffset.get() - 1;
    }

    static class Message {
        public final byte[] key;
        public final byte[] value;

        public Message(byte[] key, byte[] value) {
            this.key = key;
            this.value = value;
        }

    }

    private FileChannel openChannel(File file) throws IOException {
        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
        return randomAccessFile.getChannel();
    }

    public long append(byte[] key, byte[] value) throws IOException {
        lock.lock();
        try {
            long position = channel.position();
            long offset = nextOffset.getAndIncrement();
            writeToFile(key, value);
            offsetIndex.put(offset, position);
            return offset;
        } finally {
            lock.unlock();
        }
    }


    public Message readSingleMessage(long offset) throws IOException {
        Long filePosition = offsetIndex.get(offset);
        int messageSize = readLength(filePosition);
        ByteBuffer message = readMessage(filePosition, messageSize);

        byte[] keyBytes = readKey(message);
        byte[] messageBytes = readMessageValue(message);

        return new Message(keyBytes, messageBytes);
    }

    private byte[] readMessageValue(ByteBuffer message) {
        int messageSize = message.getInt();
        byte[] messageBytes = new byte[messageSize];
        message.get(messageBytes);
        return messageBytes;
    }

    private byte[] readKey(ByteBuffer message) {
        int keySize = message.getInt();
        byte[] key = new byte[keySize];
        message.get(key);
        return key;
    }

    private ByteBuffer readMessage(long fileLocation, int recordSize) throws IOException {
        ByteBuffer records = ByteBuffer.allocate(recordSize);
        channel.read(records, fileLocation + MessageSizeLength);
        records.flip();
        return records;
    }

    private int readLength(long fileLocation) throws IOException {
        ByteBuffer length = ByteBuffer.allocate(MessageSizeLength);
        channel.read(length, fileLocation);
        length.flip();
        return length.getInt();
    }

    //   1. 4 byte total message length
    //   2. 4 byte key length, containing length K
    //   3. K byte key
    //   4. 4 byte payload length, containing length V
    //   5. V byte payload
    private int writeToFile(byte[] key, byte[] value) throws IOException {
        int messageSize = KeySizeLength +
                key.length +
                ValueSizeLength +
                value.length;
        ByteBuffer buffer =
                ByteBuffer.allocate(MessageSizeLength + messageSize);

        buffer.putInt(messageSize);
        buffer.putInt(key.length);
        buffer.put(key);
        buffer.putInt(value.length);
        buffer.put(value);
        buffer.flip();
        int written = 0;
        while (written < sizeInBytes(buffer))
            written += channel.write(buffer);
        return written;
    }

    private int sizeInBytes(ByteBuffer buffer) {
        return buffer.limit();
    }
}
