import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class Driver {

    public static void main(String[] args) throws Exception {
        String path = "/tmp/data";
        FileManager fileManager = new FileManager(path);
        FileChannel channel = fileManager.getNewFile();
        FileChannel channel2 = fileManager.getNewFile();
        FileChannel channel3 = fileManager.getNewFile();
        FileChannel channel4 = fileManager.getNewFile();

        IGenerator generator = new Generator();

        ByteBuffer buf10mb = generator.generateDataInMB(10);
        ByteBuffer buf20mb = generator.generateDataInMB(20);
        ByteBuffer buf30mb = generator.generateDataInMB(30);
        ByteBuffer buf50mb = generator.generateDataInMB(50);
        ByteBuffer buf100mb = generator.generateDataInMB(100);
        ByteBuffer buf500mb = generator.generateDataInMB(500);

        time(() -> WritingMethod.oneShotFsync(buf10mb.slice(), channel), "oneShotFsync 10mb");
        time(() -> WritingMethod.oneShotFsync(buf20mb.slice(), channel), "oneShotFsync 20mb");
        time(() -> WritingMethod.oneShotFsync(buf30mb.slice(), channel), "oneShotFsync 30mb");
        time(() -> WritingMethod.oneShotFsync(buf50mb.slice(), channel), "oneShotFsync 50mb");
        time(() -> WritingMethod.oneShotFsync(buf100mb.slice(), channel), "oneShotFsync 100mb");

        time(() -> WritingMethod.oneShotNoFsync(buf10mb.slice(), channel), "oneShotNoFsync 10mb");
        time(() -> WritingMethod.oneShotNoFsync(buf50mb.slice(), channel), "oneShotNoFsync 50mb");
        time(() -> WritingMethod.oneShotNoFsync(buf100mb.slice(), channel), "oneShotNoFsync 100mb");

        time(() -> WritingMethod.chunkedFsync(buf100mb.slice(), 10_000_000, channel), "chunkedFsync 100mb/10mb");

        FileChannel[] channels = {channel2, channel3, channel4};
        time(() -> WritingMethod.multiFileWriteFsyncPerWrite(buf100mb.slice(), 10_000_000, channels), "multiFileWriteFsyncPerWrite 100mb/10mb/3files");
        time(() -> WritingMethod.multiFileWritePostFsync(buf100mb.slice(), 10_000_000, channels), "multiFileWritePostFsync 100mb/10mb/3files");

        time(() -> WritingMethod.multiFileWriteFsyncPerWrite(buf500mb.slice(), 10_000_000, channels), "multiFileWriteFsyncPerWrite 500mb/10mb/3files");
        time(() -> WritingMethod.multiFileWritePostFsync(buf500mb.slice(), 10_000_000, channels), "multiFileWritePostFsync 500mb/10mb/3files");

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
