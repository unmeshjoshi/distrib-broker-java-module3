package com.dist.simplekafka;

import com.dist.common.Config;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

//Copied and modified from Cassandra 1.0
public class SequenceFile {
    private final Config config;
    private final Map<String, Long> keyIndexes = new HashMap<>();
    private final Map<Long, Long> offsetIndexes = new HashMap<>();
    private final AtomicInteger offset = new AtomicInteger(0);

    public SequenceFile(Config config) {
        this.config = config;
    }

    public Set<Long> getAllOffSetsFrom(long fromOffset, long toOffset) {
        return offsetIndexes.keySet().stream()
                .filter(i -> i >= fromOffset && i <= toOffset)
                .collect(Collectors.toSet());
    }

    public int lastOffset() {
        return offset.get();
    }

    public Map<Long, Long> getOffsetIndexes() {
        return offsetIndexes;
    }

    Long getFilePosition(Long currentOffset) {
        return getOffsetIndexes().get(currentOffset);
    }

    public abstract class AbstractWriter {
        protected String fileName;

        public AbstractWriter(String fileName) {
            this.fileName = fileName;
        }

        public long lastModified() {
            File file = new File(fileName);
            return file.lastModified();
        }
    }

    public class Writer extends AbstractWriter {
        private long lastWritePosition = 0;
        protected RandomAccessFile file;

        public Writer(String fileName) throws IOException {
            super(fileName);
            this.file = init(fileName);
        }

        public long getCurrentPosition(){
            try {
                return file.getFilePointer();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public void seek(long position) throws IOException {
            file.seek(position);
        }

        public int append(String key, String message) throws IOException {
            return append(key, message.getBytes());
        }

        public int append(String key, byte[] buffer) throws IOException {
            if (key == null) throw new IllegalArgumentException("Key cannot be NULL.");
            long filePosition = lastWritePosition;
            file.seek(filePosition);
            file.writeUTF(key);
            int length = buffer.length;
            file.writeInt(length);
            file.write(buffer, 0, length);
            file.getFD().sync();
            this.lastWritePosition = file.getFilePointer();
            keyIndexes.put(key, filePosition);
            int currentOffset = offset.incrementAndGet();
            offsetIndexes.put((long) currentOffset, filePosition);
            System.out.println("storing offset " + currentOffset + " for key " + key + " in " + config.getBrokerId());
            return currentOffset;
        }

        public void append(String key, long value) throws IOException {
            if (key == null) throw new IllegalArgumentException("Key cannot be NULL.");
            file.seek(file.getFilePointer());
            file.writeUTF(key);
            file.writeLong(value);
        }

        public long getFileSize() throws IOException {
            return file.length();
        }

        protected RandomAccessFile init(String filename) throws IOException {
            File file = new File(filename);
            if (!file.exists()) file.createNewFile();
            return new RandomAccessFile(file, "rw");
        }
    }

    public class Reader {
        protected RandomAccessFile file;

        public Reader(String fileName) throws IOException {
            this.file = init(fileName);
        }

        public void seekToOffset(long offset) throws IOException {
            file.seek(offset);
        }

        public void seekToKeyPosition(String key) throws IOException {
            Long index = keyIndexes.get(key);
            if (index != null) {
                file.seek(index);
            }
        }

        public long next(DataOutputStream bufOut) throws IOException {
            long bytesRead = -1L;
            if (isEOF()) return bytesRead;
            long startPosition = file.getFilePointer();
            String key = file.readUTF();
            if (key != null) {
                bufOut.writeUTF(key);
                int dataSize = file.readInt();
                bufOut.writeInt(dataSize);
                byte[] data = new byte[dataSize];
                file.readFully(data);
                bufOut.write(data);
                long endPosition = file.getFilePointer();
                bytesRead = endPosition - startPosition;
            }
            return bytesRead;
        }

        public boolean isEOF() throws IOException {
            return getCurrentPosition() == getEOF();
        }

        public long getEOF() throws IOException {
            return file.length();
        }

        public long getCurrentPosition() throws IOException {
            return file.getFilePointer();
        }

        protected RandomAccessFile init(String filename) throws IOException {
            File file = new File(filename);
            if (!file.exists()) file.createNewFile();
            return new RandomAccessFile(file, "rw");
        }
    }
}
