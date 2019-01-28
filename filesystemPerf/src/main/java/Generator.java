import java.nio.ByteBuffer;
import java.util.Random;

public class Generator implements IGenerator {

    final Random rand = new Random();

    public ByteBuffer generateDataInMB(int numMbs) {
        byte[] buf = new byte[numMbs * 1_000_000];
        rand.nextBytes(buf);
        return ByteBuffer.wrap(buf);
    }



    public ByteBuffer generateDataInGb(int numGb) {
        byte[] buf = new byte[numGb * 1_000_000_000];
        rand.nextBytes(buf);
        return ByteBuffer.wrap(buf);
    }
}
