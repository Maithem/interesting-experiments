import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class Driver {

    public static void main(String[] args) throws Exception {
        String path = "/tmp/data";
        FileManager fileManager = new FileManager(path);
        FileChannel channel = fileManager.getNewFile();
        IGenerator generator = new Generator();

        ByteBuffer buf10mb = generator.generateDataInMB(10);
        ByteBuffer buf50mb = generator.generateDataInMB(50);
        ByteBuffer buf100mb = generator.generateDataInMB(100);

        time(() -> WritingMethod.oneShotFsync(buf10mb.slice(), channel), "oneShotFsync 10mb");
        time(() -> WritingMethod.oneShotFsync(buf50mb.slice(), channel), "oneShotFsync 50mb");
        time(() -> WritingMethod.oneShotFsync(buf100mb.slice(), channel), "oneShotFsync 100mb");

        time(() -> WritingMethod.oneShotNoFsync(buf10mb.slice(), channel), "oneShotNoFsync 10mb");
        time(() -> WritingMethod.oneShotNoFsync(buf50mb.slice(), channel), "oneShotNoFsync 50mb");
        time(() -> WritingMethod.oneShotNoFsync(buf100mb.slice(), channel), "oneShotNoFsync 100mb");

        fileManager.close();
    }

    @FunctionalInterface
    public interface CheckedFunction<R> {
        R apply() throws IOException;
    }

    public static void time(CheckedFunction func, String description) throws IOException{
        long ts = System.nanoTime();
        Object ret = func.apply();
        long ts2 = System.nanoTime();
        System.out.println("desc: " + description + " duration(ms): " + ((ts2 - ts)/(1000000.0)) + " ret: " + ret);
    }
}
