import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.concurrent.ConcurrentSkipListMap;

public class SSTable {

    public static void writeEntry(long key, String value, DataOutputStream dos) throws IOException {
        dos.writeLong(key);
        byte[] valueBytes = value.getBytes();
        dos.writeInt(valueBytes.length);
        dos.write(valueBytes);
    }

    // Reads the next SSTable entry from disk
    // Returns null when end-of-file is reached
    public static Entry readNextEntry(DataInputStream dis) throws IOException {

        if (dis.available() <= 0) {
            return null;
        }

        long key = dis.readLong();

        int valueLength = dis.readInt();

        byte[] valueBytes = new byte[valueLength];

        dis.readFully(valueBytes);

        String value = new String(valueBytes);

        return new Entry(key, value);
    }

    public static void flushMemTable(StorageEngine engine, ConcurrentSkipListMap<Long, String> memTable,
            int walSegmentId) throws IOException {

        // Shows which thread is performing the flush
        System.out.println(
                "[" + Thread.currentThread().getName() + "] Writing SSTable with "
                        + memTable.size() + " entries");

        // Generate unique SSTable filename for every flush
        // Example: data_1.db, data_2.db, data_3.db
        String fileName = "data_" + engine.sstableCounter + ".db";

        // Create brand new immutable SSTable file
        // No append mode now:
        // every Memtable flush creates its own separate disk file
        FileOutputStream fos = new FileOutputStream(fileName);

        // Move counter forward for next SSTable creation
        engine.sstableCounter++;

        // Wrapper stream for primitive writes
        DataOutputStream dos = new DataOutputStream(fos);

        BloomFilter bloomFilter = new BloomFilter(1000);
        // ConcurrentSkipListMap is already sorted by key
        for (Long key : memTable.keySet()) {
            // Serialize sorted entry into SSTable
            writeEntry(key, memTable.get(key), dos);
            bloomFilter.add(key);
        }

        // Close streams
        dos.close();

        // Small RAM metadata describing SSTable boundaries
        synchronized (engine.sparseIndex) {
            engine.sparseIndex.add(
                    new SparseIndexEntry(
                            memTable.firstKey(),
                            memTable.lastKey(),
                            fileName,
                            bloomFilter));
        }
        // This memtable will become unreachable after flush finishes.
        // Let the Garbage Collector reclaim it naturally.

        // This Memtable's WAL segment is now durable
        WriteAheadLog.deleteWalSegment(engine, walSegmentId);

        System.out.println(
                "[" + Thread.currentThread().getName() + "] Memtable flushed to " + fileName);

        // Trigger background compaction when many SSTables exist

        // Only one thread can successfully switch false -> true
        boolean shouldCompact = false;

        synchronized (engine.sparseIndex) {
            if (engine.sparseIndex.size() >= 4
                    && engine.compactionRunning.compareAndSet(false, true)) {
                shouldCompact = true;
            }
        }

        if (shouldCompact) {
            engine.compactionExecutor.submit(() -> {

                System.out.println(
                        "[" + Thread.currentThread().getName()
                                + "] Starting background compaction");

                try {
                    Compactor.compactAllSSTables(engine);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                } finally {
                    // Allow future compactions
                    engine.compactionRunning.set(false);
                }
            });
        }
    }

    public static void bootstrapSparseIndex(StorageEngine engine) throws IOException {

        // Start scanning SSTables from data_1.db
        int fileNumber = 1;

        while (true) {

            // Build SSTable filename
            String fileName = "data_" + fileNumber + ".db";

            // Filesystem path reference for SSTable
            File file = new File(fileName);

            // Stop when next SSTable file does not exist
            if (!file.exists()) {
                break;
            }

            // Open SSTable for sequential scan
            FileInputStream fis = new FileInputStream(fileName);
            // Wrapper stream for primitive reads
            DataInputStream dis = new DataInputStream(fis);

            // Empty file safety check
            if (dis.available() <= 0) {
                dis.close();
                fileNumber++;
                continue;
            }

            // bloom filter built during bootstrap
            BloomFilter bloomFilter = new BloomFilter(1000);

            // First key of SSTable
            long firstKey = dis.readLong();
            // insert first key to bloom filter
            bloomFilter.add(firstKey);
            // Read value size metadata
            int valueLength = dis.readInt();
            // Allocate byte array for value
            byte[] valueBytes = new byte[valueLength];
            // Read exact value bytes
            dis.readFully(valueBytes);
            // Initialize SSTable upper boundary
            long lastKey = firstKey;

            // Continue scanning till end to discover last key
            while (dis.available() > 0) {

                // Read next key in SSTable
                long key = dis.readLong();
                // insert key into bloom filter
                bloomFilter.add(key);
                valueLength = dis.readInt();
                valueBytes = new byte[valueLength];
                dis.readFully(valueBytes);
                // Keep updating largest key seen
                lastKey = key;
            }

            // Restore sparse metadata into RAM
            engine.sparseIndex.add(new SparseIndexEntry(firstKey, lastKey, fileName, bloomFilter));

            // Close SSTable stream
            dis.close();

            fileNumber++;
        }

        // Keeps future SSTable numbering correct
        engine.sstableCounter = fileNumber;

        System.out.println("Sparse index bootstrapped successfully");
    }

    // Rebuild SparseIndex metadata from an SSTable
    public static SparseIndexEntry buildSparseIndexEntry(StorageEngine engine, String fileName) throws IOException {

        DataInputStream dis = new DataInputStream(new FileInputStream(fileName));

        Entry firstEntry = readNextEntry(dis);

        if (firstEntry == null) {
            dis.close();
            return null;
        }

        long firstKey = firstEntry.key;
        long lastKey = firstKey;

        BloomFilter bloomFilter = new BloomFilter(1000);

        // First key also belongs in Bloom Filter
        bloomFilter.add(firstKey);

        Entry entry;

        while ((entry = readNextEntry(dis)) != null) {
            bloomFilter.add(entry.key);
            lastKey = entry.key;
        }

        dis.close();

        return new SparseIndexEntry(
                firstKey,
                lastKey,
                fileName,
                bloomFilter);
    }
}
