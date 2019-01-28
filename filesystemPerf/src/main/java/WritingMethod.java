import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class WritingMethod {

    public static int oneShotNoFsync(ByteBuffer buf, FileChannel channel) throws IOException {
        int numBytes = channel.write(buf);
        return numBytes;
    }

    public static int oneShotFsync(ByteBuffer buf, FileChannel channel) throws IOException {
        int numBytes = channel.write(buf);
        channel.force(true);
        return numBytes;
    }

    public static int chunkedFsync(ByteBuffer buf, int chunkSize, FileChannel channel) throws IOException {
        int numBytes = 0;

        while(buf.hasRemaining()) {
            ByteBuffer sliced = buf.slice();
            int stride = Math.min(sliced.remaining(), chunkSize);
            sliced.limit(stride);
            numBytes += channel.write(sliced);
            channel.force(true);
            buf.position(buf.position() + stride);
        }

        return numBytes;
    }

    public static int multiFileWriteFsyncPerWrite(ByteBuffer buf, int chunkSize, FileChannel[] channels) throws IOException {
        int numBytes = 0;

        int fileIdx = 0;

        while(buf.hasRemaining()) {
            ByteBuffer sliced = buf.slice();
            int stride = Math.min(sliced.remaining(), chunkSize);
            sliced.limit(stride);
            numBytes += channels[fileIdx % channels.length].write(sliced);
            channels[fileIdx % channels.length].force(true);
            buf.position(buf.position() + stride);
            fileIdx++;
        }

        return numBytes;
    }

    public static int multiFileWritePostFsync(ByteBuffer buf, int chunkSize, FileChannel[] channels) throws IOException {
        int numBytes = 0;

        int fileIdx = 0;

        while(buf.hasRemaining()) {
            ByteBuffer sliced = buf.slice();
            int stride = Math.min(sliced.remaining(), chunkSize);
            sliced.limit(stride);
            numBytes += channels[fileIdx % channels.length].write(sliced);
            //channels[fileIdx % channels.length].force(true);
            buf.position(buf.position() + stride);
            fileIdx++;
        }

        for (FileChannel channel : channels) {
            channel.force(true);
        }

        return numBytes;
    }
}
