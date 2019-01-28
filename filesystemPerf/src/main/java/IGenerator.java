import java.nio.ByteBuffer;

public interface IGenerator {

    /**
     * Generate a random buffer.
     * @param numMbs number of megabytes to generate
     * @return buffer
     */
    ByteBuffer generateDataInMB(int numMbs);


    /**
     * Generate a random buffer
     * @param numGb number of gigabytes to generate
     * @return buffer
     */
    ByteBuffer generateDataInGb(int numGb);

}
