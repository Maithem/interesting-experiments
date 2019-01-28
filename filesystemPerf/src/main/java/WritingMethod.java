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
}
