import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.ConcurrentSkipListMap;

public class WriteAheadLog {

    // Example: wal_1.log, wal_2.log ...
    public static String getWalFileName(int segmentId) {
        return "wal_" + segmentId + ".log";
    }

    // Open a new WAL segment for incoming writes
    public static void openWalSegment(StorageEngine engine, int segmentId) throws IOException {
        engine.currentWalDos = new DataOutputStream(
                new FileOutputStream(
                        getWalFileName(segmentId),
                        true));
    }

    public static void appendToWAL(StorageEngine engine, long key, String value) throws IOException {
        // Append directly to active WAL stream
        SSTable.writeEntry(key, value, engine.currentWalDos);
        engine.currentWalDos.flush();
    }

    public static void recoverFromWAL(StorageEngine engine, ConcurrentSkipListMap<Long, String> memTable)
            throws IOException {
        int recoveredOperations = 0;

        // Replay every WAL segment that exists
        // Scan all files in project directory
        File[] files = new File(".").listFiles();

        if (files != null) {

            // Replay WAL segments in numeric order
            Arrays.sort(files, Comparator.comparingInt(file -> {

                String name = file.getName();

                if (!name.startsWith("wal_") ||
                        !name.endsWith(".log")) {
                    return Integer.MAX_VALUE;
                }

                return Integer.parseInt(
                        name.substring(4, name.length() - 4));
            }));

            for (File walFile : files) {

                String name = walFile.getName();

                if (!name.startsWith("wal_") ||
                        !name.endsWith(".log")) {
                    continue;
                }

                DataInputStream dis = new DataInputStream(new FileInputStream(walFile));

                Entry entry;

                while ((entry = SSTable.readNextEntry(dis)) != null) {

                    memTable.put(entry.key, entry.value);
                    recoveredOperations++;
                }

                dis.close();
            }
        }

        System.out.println(
                "Recovered " + recoveredOperations + " operations from WAL segments");
    }

    // Delete WAL segment after its Memtable is safely flushed
    public static void deleteWalSegment(StorageEngine engine, int walSegmentId) {
        File walFile = new File(
                getWalFileName(walSegmentId));

        if (walFile.exists()) {
            walFile.delete();
            System.out.println(
                    "Deleted WAL segment: "
                            + walFile.getName());
        }
    }

    public static int getMaxWalSegment() {
        int maxWalSegment = 0;

        File[] files = new File(".").listFiles();

        if (files != null) {

            for (File file : files) {

                String name = file.getName();

                if (!name.startsWith("wal_") ||
                        !name.endsWith(".log")) {
                    continue;
                }

                // Extract number from wal_7.log -> 7
                String number = name.substring(4, name.length() - 4);

                maxWalSegment = Math.max(maxWalSegment, Integer.parseInt(number));
            }
        }
        return maxWalSegment;
    }
}
