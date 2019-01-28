import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

public class FileManager {

    final Random rand = new Random();
    final Set<FileChannel> files = new HashSet();
    final String dirPath;

    public FileManager(String dirPath) {
        this.dirPath = dirPath;
    }

    public FileChannel getNewFile() throws IOException {
        Path path = Paths.get(dirPath, String.valueOf(Math.abs(rand.nextInt())) + ".data");
        FileChannel dest = FileChannel.open(path, EnumSet.of(StandardOpenOption.READ,
                StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.SPARSE));
        files.add(dest);
        return dest;
    }

    public void close() {
        for (FileChannel channel : files) {
            try {
                channel.close();
            } catch (IOException ioe) {
                System.out.println("close: " + ioe);
            }
        }

        File dir = new File(dirPath);
        try {
            FileUtils.cleanDirectory(dir);
        } catch (IOException ioe) {
            System.out.println("close: " + ioe);
        }
    }
}
