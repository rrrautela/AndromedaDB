import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.io.DataOutputStream;

public class StorageEngine {

    public static final String TOMBSTONE = "__DELETED__";

    // Stores small RAM metadata for every SSTable
    public final ArrayList<SparseIndexEntry> sparseIndex = new ArrayList<>();

    public int sstableCounter = 1;
    public int compactionCounter = 1;

    // Prevent duplicate compaction jobs from being queued
    public final AtomicBoolean compactionRunning = new AtomicBoolean(false);

    // Current WAL segment receiving writes
    public int currentWalSegment = 1;

    // Active WAL stream kept open for fast appends
    public DataOutputStream currentWalDos;

    // Background SSTable flushes
    public final ExecutorService flushExecutor = Executors.newSingleThreadExecutor();

    // Background compactions
    public final ExecutorService compactionExecutor = Executors.newSingleThreadExecutor();

    public StorageEngine() {
    }

    public void openWalSegment(int segmentId) throws IOException {
        WriteAheadLog.openWalSegment(this, segmentId);
    }

    public void appendToWAL(long key, String value) throws IOException {
        WriteAheadLog.appendToWAL(this, key, value);
    }

    public void recoverFromWAL(ConcurrentSkipListMap<Long, String> memTable) throws IOException {
        WriteAheadLog.recoverFromWAL(this, memTable);
    }

    public void deleteWalSegment(int walSegmentId) {
        WriteAheadLog.deleteWalSegment(this, walSegmentId);
    }

    public int getMaxWalSegment() {
        return WriteAheadLog.getMaxWalSegment();
    }

    public String getWalFileName(int segmentId) {
        return WriteAheadLog.getWalFileName(segmentId);
    }

    public void flushMemTable(ConcurrentSkipListMap<Long, String> memTable, int walSegmentId) throws IOException {
        SSTable.flushMemTable(this, memTable, walSegmentId);
    }

    public String get(long targetKey, ConcurrentSkipListMap<Long, String> memTable) throws IOException {

        // STEP 1:
        // Check Memtable (RAM) first
        // Newest writes usually exist here
        if (memTable.containsKey(targetKey)) {

            System.out.println("Found in Memtable");

            if (memTable.get(targetKey).equals(TOMBSTONE))
                return null; // key is in map but its deleted so it should not have any value

            return memTable.get(targetKey);
        }

        // STEP 2:
        // Search SSTables from newest -> oldest
        // Newer SSTables contain newer values
        for (int i = sparseIndex.size() - 1; i >= 0; i--) {

            // Current SSTable metadata entry
            SparseIndexEntry entry = sparseIndex.get(i);

            // Skip SSTables whose key range cannot possibly contain target key
            if (targetKey < entry.firstKey || targetKey > entry.lastKey) {
                System.out.println("Skipping " + entry.fileName);
                continue;
            }

            // Bloom Filter says key definitely does not exist
            if (!entry.bloomFilter.mightContain(targetKey)) {
                System.out.println("Bloom Filter skipped " + entry.fileName);
                continue;
            }

            // SSTable selected for scan
            String fileName = entry.fileName;

            // Open SSTable for sequential scan
            FileInputStream fis = new FileInputStream(fileName);

            // Wrapper stream for primitive reads
            DataInputStream dis = new DataInputStream(fis);

            System.out.println("Scanning " + fileName);

            // Sequentially scan entire SSTable
            while (dis.available() > 0) {

                // Read 8-byte key
                long key = dis.readLong();

                // Read value size metadata
                int valueLength = dis.readInt();

                // Create byte array for value
                byte[] valueBytes = new byte[valueLength];

                // Read exact value bytes
                dis.readFully(valueBytes);

                // Convert bytes -> String
                String value = new String(valueBytes);

                // Key found
                if (key == targetKey) {

                    dis.close();

                    System.out.println("Found in " + fileName);

                    // Newest matching value found
                    if (value.equals(TOMBSTONE))
                        return null;

                    return value;
                }
            }

            // Close SSTable stream
            dis.close();
        }

        // Key does not exist anywhere
        return null;
    }

    public void rangeQuery(long startKey, long endKey) throws IOException {

        // Open binary database file for sequential scan
        FileInputStream fis = new FileInputStream("binary.db");

        // Wrapper stream for reading primitive data types
        DataInputStream dis = new DataInputStream(fis);

        // Scan entire append-only log sequentially
        while (dis.available() > 0) {

            // Read fixed-size 8-byte key
            long key = dis.readLong();

            // Read value length metadata
            int valueLength = dis.readInt();

            // Create byte array for value bytes
            byte[] valueBytes = new byte[valueLength];

            // Read exact value bytes
            dis.readFully(valueBytes);

            // Convert raw bytes back into String
            String value = new String(valueBytes);

            // Check if key falls inside requested range
            if (key >= startKey && key <= endKey) {

                System.out.println("Key: " + key);
                System.out.println("Value: " + value);
                System.out.println("----------------");
            }
        }

        // Close outer stream (also closes inner stream)
        dis.close();
    }

    public void bootstrapSparseIndex() throws IOException {
        SSTable.bootstrapSparseIndex(this);
    }

    public SparseIndexEntry buildSparseIndexEntry(String fileName) throws IOException {
        return SSTable.buildSparseIndexEntry(this, fileName);
    }

    public void compact(String olderFile, String newerFile, String outputFile) throws IOException {
        Compactor.compact(this, olderFile, newerFile, outputFile);
    }

    public void compactAllSSTables() throws IOException {
        Compactor.compactAllSSTables(this);
    }

    public void delete(long key, ConcurrentSkipListMap<Long, String> memTable) throws IOException {
        appendToWAL(key, TOMBSTONE);
        memTable.put(key, TOMBSTONE);
    }

    public void printFile(String fileName) throws IOException {
        // to print all entries of a file
        System.out.println("\n===== " + fileName + " =====");
        DataInputStream dis = new DataInputStream(new FileInputStream(fileName));
        Entry entry;

        while ((entry = SSTable.readNextEntry(dis)) != null) {
            System.out.println(
                    entry.key + " -> " + entry.value);
        }
        dis.close();

        System.out.println("====================\n");
    }

    public void shutdown() {
        flushExecutor.shutdown();
        compactionExecutor.shutdown();
        try {
            flushExecutor.awaitTermination(10, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        try {
            compactionExecutor.awaitTermination(10, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
