import java.io.*;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Random;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicBoolean;

public class Main {

    // Probabilistic structure used to avoid unnecessary SSTable scans
    static class BloomFilter {

        // Compact binary bitmap storing Bloom Filter hash footprints efficiently in RAM
        BitSet bitSet;
        // Total size of bit array
        int size;

        public BloomFilter(int size) {
            this.size = size;
            // Java BitSet internally manages bits efficiently
            this.bitSet = new BitSet(size);
        }

        // Insert key into Bloom Filter
        public void add(long key) {

            int hash1 = hash1(key);
            int hash2 = hash2(key);
            int hash3 = hash3(key);

            // Mark all hash positions as 1
            bitSet.set(hash1);
            bitSet.set(hash2);
            bitSet.set(hash3);
        }

        // Check if key might exist
        public boolean mightContain(long key) {
            int hash1 = hash1(key);
            int hash2 = hash2(key);
            int hash3 = hash3(key);

            // If ANY required bit is missing, key definitely does not exist
            // (all need to be 1, even one of them is 0 -> not present key)
            return bitSet.get(hash1) && bitSet.get(hash2) && bitSet.get(hash3);
        }

        // First hash function
        private int hash1(long key) {
            return Math.abs(Long.hashCode(key)) % size;
        }

        // Second hash function
        private int hash2(long key) {
            return Math.abs(Long.hashCode(key * 31)) % size;
        }

        // Third hash function
        private int hash3(long key) {
            return Math.abs(Long.hashCode(key * 97)) % size;
        }
    }

    // Represents one SSTable record currently pointed to by a stream
    static class Entry {
        Long key;
        String value;

        public Entry(Long key, String value) {
            this.key = key;
            this.value = value;
        }
    }

    static class SparseIndexEntry {

        long firstKey;
        long lastKey;
        String fileName;
        BloomFilter bloomFilter;

        public SparseIndexEntry(long firstKey, long lastKey, String fileName, BloomFilter bloomFilter) {

            this.firstKey = firstKey;
            this.lastKey = lastKey;
            this.fileName = fileName;
            this.bloomFilter = bloomFilter;
        }
    }

    // Stores small RAM metadata for every SSTable
    static ArrayList<SparseIndexEntry> sparseIndex = new ArrayList<>();

    static int sstableCounter = 1;

    static int compactionCounter = 1;

    // Prevent duplicate compaction jobs from being queued
    static AtomicBoolean compactionRunning =
            new AtomicBoolean(false);

    // Current WAL segment receiving writes
    static int currentWalSegment = 1;

    // Active WAL stream kept open for fast appends
    static DataOutputStream currentWalDos;

    static final String TOMBSTONE =  "__DELETED__";

    // Example: wal_1.log, wal_2.log ...
    static String getWalFileName(int segmentId) {
        return "wal_" + segmentId + ".log";
    }


    // Open a new WAL segment for incoming writes
    public static void openWalSegment(int segmentId) throws IOException {

        currentWalDos = new DataOutputStream(
                new FileOutputStream(
                        getWalFileName(segmentId),
                        true
                )
        );
    }



    // Background SSTable flushes
    static ExecutorService flushExecutor =
            Executors.newSingleThreadExecutor();

    // Background compactions
    static ExecutorService compactionExecutor =
            Executors.newSingleThreadExecutor();

    public static void writeEntry(long key, String value, DataOutputStream dos) throws IOException {
        dos.writeLong(key);
        byte[] valueBytes = value.getBytes();
        dos.writeInt(valueBytes.length);
        dos.write(valueBytes);
    }

    // Reads the next SSTable entry from disk
    // Returns null when end-of-file is reached
    public static Entry readNextEntry(DataInputStream dis) throws IOException {

        if(dis.available() <= 0)
            return null;

        long key = dis.readLong();

        int valueLength = dis.readInt();

        byte[] valueBytes = new byte[valueLength];

        dis.readFully(valueBytes);

        String value = new String(valueBytes);

        return new Entry(key, value);
    }

    public static void rangeQuery(long startKey, long endKey) throws IOException {

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

    public static void flushMemTable(ConcurrentSkipListMap<Long, String> memTable,
                                    int walSegmentId) throws IOException {

        // Shows which thread is performing the flush
        System.out.println(
                "[" + Thread.currentThread().getName() + "] Writing SSTable with "
                        + memTable.size() + " entries"
        );

        // Generate unique SSTable filename for every flush
        // Example: data_1.db, data_2.db, data_3.db
        String fileName = "data_" + sstableCounter + ".db";

        // Create brand new immutable SSTable file
        // No append mode now:
        // every Memtable flush creates its own separate disk file
        FileOutputStream fos = new FileOutputStream(fileName);

        // Move counter forward for next SSTable creation
        sstableCounter++;


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
        synchronized (sparseIndex) {
            sparseIndex.add(
                    new SparseIndexEntry(
                            memTable.firstKey(),
                            memTable.lastKey(),
                            fileName,
                            bloomFilter
                    )
            );
        }
        // This memtable will become unreachable after flush finishes.
        // Let the Garbage Collector reclaim it naturally.

        // This Memtable's WAL segment is now durable
        deleteWalSegment(walSegmentId);

        System.out.println(
                "[" + Thread.currentThread().getName() + "] Memtable flushed to " + fileName
        );

        // Trigger background compaction when many SSTables exist

        // Only one thread can successfully switch false -> true
        boolean shouldCompact = false;

        synchronized (sparseIndex) {
            if (sparseIndex.size() >= 4
                    && compactionRunning.compareAndSet(false, true)) {
                shouldCompact = true;
            }
        }

        if (shouldCompact) {
                compactionExecutor.submit(() -> {


                System.out.println(
                        "[" + Thread.currentThread().getName()
                                + "] Starting background compaction"
                );

                try {

                    compactAllSSTables();

                } catch (IOException e) {

                    throw new RuntimeException(e);

                } finally {

                    // Allow future compactions
                    compactionRunning.set(false);
                }
            });
        }
    }

    public static String get(long targetKey, ConcurrentSkipListMap<Long, String> memTable) throws IOException {

        // STEP 1:
        // Check Memtable (RAM) first
        // Newest writes usually exist here
        if (memTable.containsKey(targetKey)) {

            System.out.println("Found in Memtable");

            if(memTable.get(targetKey).equals(TOMBSTONE))
                return null; //key is in map but its deleet dso it should nt hav enay value

            return memTable.get(targetKey);
        }

        // STEP 2:
        // Search SSTables from newest -> oldest
        // Newer SSTables contain newer values

        // Use sparseIndex.size() because it represents actual active SSTables.
        // sstableCounter only stores the next file ID, so later it may not match real SSTable count.
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
                    if(value.equals(TOMBSTONE))
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

    public static void bootstrapSparseIndex() throws IOException {

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

            //bloom filter built during bootstrap
            BloomFilter bloomFilter = new BloomFilter(1000);

            // First key of SSTable
            long firstKey = dis.readLong();
            //insert first key to bloom filter
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
                //insert key into bloom filter
                bloomFilter.add(key);
                valueLength = dis.readInt();
                valueBytes = new byte[valueLength];
                dis.readFully(valueBytes);
                // Keep updating largest key seen
                lastKey = key;
            }

            // Restore sparse metadata into RAM
            sparseIndex.add(new SparseIndexEntry(firstKey, lastKey, fileName, bloomFilter));

            // Close SSTable stream
            dis.close();

            fileNumber++;
        }

        // Keeps future SSTable numbering correct
        sstableCounter = fileNumber;

        System.out.println("Sparse index bootstrapped successfully");
    }

    public static void appendToWAL(long key, String value) throws IOException {

        // Append directly to active WAL stream
        writeEntry(key, value, currentWalDos);
        currentWalDos.flush();
    }

    public static void recoverFromWAL(ConcurrentSkipListMap<Long, String> memTable) throws IOException {
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
                        name.substring(4, name.length() - 4)
                );
            }));

            for (File walFile : files) {

                String name = walFile.getName();

                if (!name.startsWith("wal_") ||
                        !name.endsWith(".log")) {
                    continue;
                }

                DataInputStream dis =
                        new DataInputStream(new FileInputStream(walFile));

                Entry entry;

                while ((entry = readNextEntry(dis)) != null) {

                    memTable.put(entry.key, entry.value);
                    recoveredOperations++;
                }

                dis.close();
            }
        }

        System.out.println(
                "Recovered " + recoveredOperations + " operations from WAL segments"
        );
    }

    // Delete WAL segment after its Memtable is safely flushed
    public static void deleteWalSegment(int walSegmentId) {

        File walFile = new File(
                getWalFileName(walSegmentId)
        );

        if (walFile.exists()) {
            walFile.delete();
            System.out.println(
                    "Deleted WAL segment: "
                            + walFile.getName()
            );
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
                String number =
                        name.substring(4, name.length() - 4);

                maxWalSegment =
                        Math.max(maxWalSegment, Integer.parseInt(number));
            }
        }
        return maxWalSegment;
    }

    // Rebuild SparseIndex metadata from an SSTable
    public static SparseIndexEntry buildSparseIndexEntry(String fileName) throws IOException {

        DataInputStream dis =
                new DataInputStream(new FileInputStream(fileName));

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
                bloomFilter
        );
    }

    public static void compact(String olderFile, String newerFile, String outputFile) throws IOException {

        // Open older SSTable
        DataInputStream oldDis =
                new DataInputStream(new FileInputStream(olderFile));

        // Open newer SSTable
        DataInputStream newDis =
                new DataInputStream(new FileInputStream(newerFile));

        // Output SSTable produced after compaction
        DataOutputStream dos =
                new DataOutputStream(new FileOutputStream(outputFile));

        // Initial pointers (equivalent to i = 0, j = 0)
        Entry oldEntry = readNextEntry(oldDis);
        Entry newEntry = readNextEntry(newDis);

        // Merge phase
        while(oldEntry != null && newEntry != null){

            // Older key comes first
            if(oldEntry.key < newEntry.key){

                if(!oldEntry.value.equals(TOMBSTONE))
                    writeEntry(oldEntry.key, oldEntry.value, dos);

                // i++
                oldEntry = readNextEntry(oldDis);
            }

            // Newer key comes first
            else if(newEntry.key < oldEntry.key){

                if(!newEntry.value.equals(TOMBSTONE))
                    writeEntry(newEntry.key, newEntry.value, dos);

                // j++
                newEntry = readNextEntry(newDis);
            }

            // Same key exists in both SSTables
            else{

                // Newer SSTable wins
                if(!newEntry.value.equals(TOMBSTONE))
                    writeEntry(newEntry.key, newEntry.value, dos);

                // i++, j++
                oldEntry = readNextEntry(oldDis);
                newEntry = readNextEntry(newDis);
            }
        }

        // Drain remaining entries from older SSTable
        while(oldEntry != null){

            if(!oldEntry.value.equals(TOMBSTONE))
                writeEntry(oldEntry.key, oldEntry.value, dos);

            oldEntry = readNextEntry(oldDis);
        }

        // Drain remaining entries from newer SSTable
        while(newEntry != null){

            if(!newEntry.value.equals(TOMBSTONE))
                writeEntry(newEntry.key, newEntry.value, dos);

            newEntry = readNextEntry(newDis);
        }

        // Cleanup
        oldDis.close();
        newDis.close();
        dos.close();

        // Remove old SSTables and register compacted SSTable atomically
        synchronized (sparseIndex) {

            sparseIndex.removeIf(
                    entry ->
                            entry.fileName.equals(olderFile)
                                    || entry.fileName.equals(newerFile)
            );

            // Build metadata for compacted SSTable
            SparseIndexEntry compactedEntry =
                    buildSparseIndexEntry(outputFile);

            // Register compacted SSTable
            if (compactedEntry != null) {
                sparseIndex.add(compactedEntry);
            }
        }

        // Old SSTables no longer needed
        new File(olderFile).delete();
        new File(newerFile).delete();

        System.out.println(
                "Compaction completed: "
                        + outputFile
        );
    }

    // Merge all active SSTables into one final SSTable
    public static void compactAllSSTables() throws IOException {

        // Need at least 2 SSTables to compact
        // Need at least 2 SSTables to compact
        synchronized (sparseIndex) {

            if (sparseIndex.size() < 2) {
                System.out.println("Not enough SSTables to compact");
                return;
            }
        }

        // Keep compacting until only one SSTable remains
        while (true) {

            synchronized (sparseIndex) {

                if (sparseIndex.size() <= 1) {
                    break;
                }
            }
            // Oldest SSTable
            String olderFile;
            //newest SSTable
            String newerFile;

            synchronized (sparseIndex) {

                olderFile =
                        sparseIndex.get(0).fileName;

                newerFile =
                        sparseIndex.get(1).fileName;
            }

            // Temporary output SSTable
            String outputFile =
                    "compacted_" + compactionCounter++ + ".db";

            System.out.println(
                    "Compacting "
                            + olderFile
                            + " + "
                            + newerFile
            );

            // Reuse existing compaction logic
            compact(
                    olderFile,
                    newerFile,
                    outputFile
            );
        }

        synchronized (sparseIndex) {

            System.out.println(
                    "Final SSTable: "
                            + sparseIndex.get(0).fileName
            );
        }
    }

    public static void delete(long key, ConcurrentSkipListMap<Long, String> memTable) throws IOException{
        appendToWAL(key, TOMBSTONE);
        memTable.put(key, TOMBSTONE);
    }

    public static void printFile(String fileName) throws IOException {
//        to print all entrie sof  a file
        System.out.println("\n===== " + fileName + " =====");
        DataInputStream dis = new DataInputStream(new FileInputStream(fileName));
        Entry entry;

        while((entry = readNextEntry(dis)) != null){
            System.out.println(
                    entry.key + " -> " + entry.value
            );
        }
        dis.close();

        System.out.println("====================\n");
    }

    public static void main(String[] args) throws IOException {

        bootstrapSparseIndex();

        // Memtable = sorted in-memory buffer
        ConcurrentSkipListMap<Long, String> memTable = new ConcurrentSkipListMap<>();
        recoverFromWAL(memTable);

        // Find highest WAL segment already present on disk
        int maxWalSegment = getMaxWalSegment();

        // New writes continue in the next WAL segment
        currentWalSegment = maxWalSegment + 1;

        // Open active WAL segment
        openWalSegment(currentWalSegment);


        System.out.println(
                "Current WAL segment: " + currentWalSegment
        );

        Random rand = new Random();

        String chars = "abcdefghijklmnopqrstuvwxyz";

        long start = System.currentTimeMillis();


        // Insert 50k records into Memtable
        int totalWrites = 500000;

        for (long i = 1; i <= totalWrites; i++) {

            StringBuilder sb = new StringBuilder();

            // Generate random 10-character value
            for (int j = 0; j < 10; j++) {

                int randomIndex = rand.nextInt(chars.length());

                sb.append(chars.charAt(randomIndex));
            }

            // Store inside Memtable instead of disk
            // Random key between 1 and 100000
//            Long keyy = (long) rand.nextInt(250000);
            Long keyy  = i;
            appendToWAL(keyy, sb.toString());
            memTable.put(keyy, sb.toString());

            // Memtable full -> freeze current table
            if (memTable.size() >= 1000) {

                // Finish current WAL segment
                currentWalDos.flush();
                currentWalDos.close();

                // WAL segment belonging to this frozen Memtable
                int oldWalSegment = currentWalSegment;

                // New writes go to a new WAL segment
                currentWalSegment++;

                // New writes go into a fresh WAL segment
                openWalSegment(currentWalSegment);

                ConcurrentSkipListMap<Long, String> oldMemTable = memTable;

                // New active Memtable for incoming writes
                memTable = new ConcurrentSkipListMap<>();

                // Flush old Memtable in background
                flushExecutor.submit(() -> {
                    // Background worker picked up a flush job
                    System.out.println(
                            "[" + Thread.currentThread().getName() + "] Starting scheduled flush"
                    );
                    try {
                        flushMemTable(oldMemTable, oldWalSegment);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
            }
        }

        long end = System.currentTimeMillis();

        // Raw insert throughput measurement
        System.out.println(
                "\nInsert Time = "
                        + (end - start)
                        + " ms"
        );

        System.out.println();
        System.out.println(
                "Writes/sec = "
                        + (totalWrites * 1000.0) / (end - start)
        );
        System.out.println();

        // Flush remaining entries still in RAM
        if (!memTable.isEmpty()) {

            // Close final active WAL segment
            currentWalDos.flush();
            currentWalDos.close();

            int oldWalSegment = currentWalSegment;

            ConcurrentSkipListMap<Long, String> oldMemTable = memTable;

            flushExecutor.submit(() -> {
                // Background worker picked up the final flush
                System.out.println(
                        "[" + Thread.currentThread().getName() + "] Starting final flush"
                );
                try {
                    flushMemTable(oldMemTable, oldWalSegment);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        // No more background jobs will be submitted

        // Main thread now waits for all background flushes
        System.out.println("[main] Finished inserts. Waiting for background flushes...");



        flushExecutor.shutdown();

        try {
            flushExecutor.awaitTermination(
                    10,
                    TimeUnit.MINUTES
            );
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        // Wait for background compactions
        compactionExecutor.shutdown();

        try {
            compactionExecutor.awaitTermination(
                    10,
                    TimeUnit.MINUTES
            );
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        // Safe to read SSTables now
        System.out.println("[main] All background flushes completed.");


        System.out.println(
                "Final SSTables = "
                        + sparseIndex.size()
        );

        System.out.println(
                "Final File = "
                        + sparseIndex.get(0).fileName
        );

        //let's try finding it in all SSTables
        System.out.println(get(100, memTable));
        System.out.println(get(1000000, memTable));


        //testing for compaction
//        printFile("data_1.db");
//
//        printFile("data_2.db");


//        //let's do compaction now
//        compactAllSSTables();
//
//        // Show final remaining SSTable
//        System.out.println(
//                "Final SSTable = "
//                        + sparseIndex.get(0).fileName
//        );
//
//        System.out.println(
//                "Active SSTables = "
//                        + sparseIndex.size()
//        );

//        printFile(
//                sparseIndex.get(0).fileName
//        );



    }


}
