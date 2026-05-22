import java.io.*;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Random;
import java.util.TreeMap;

public class Main {

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

    // Probabilistic structure used to avoid unnecessary SSTable scans

    // Stores small RAM metadata for every SSTable
    static ArrayList<SparseIndexEntry> sparseIndex = new ArrayList<>();

    static int sstableCounter = 1;

    static final String WAL_FILE = "wal.log";

    public static void writeEntry(long key, String value, DataOutputStream dos) throws IOException {
        dos.writeLong(key);
        byte[] valueBytes = value.getBytes();
        dos.writeInt(valueBytes.length);
        dos.write(valueBytes);
    }

    public static void readEntry() throws IOException {

        // Open binary database file for reading
        FileInputStream fis = new FileInputStream("binary.db");

        // Wrapper stream to easily read primitive data types
        // (Decorator Pattern: DataInputStream wraps FileInputStream)
        DataInputStream dis = new DataInputStream(fis);

        while (dis.available() > 0) {
            // Read fixed-size 8-byte long key
            long key = dis.readLong();

            // Read next 4 bytes -> tells how many bytes value occupies
            int valueLength = dis.readInt();

            // Create byte array to hold upcoming value bytes
            byte[] valueBytes = new byte[valueLength];

            // Read exact number of bytes into array
            dis.readFully(valueBytes);

            // Convert raw bytes back into readable String
            String value = new String(valueBytes);

            // Print reconstructed key-value pair
            System.out.println("Key: " + key);
            System.out.println("Value: " + value);
            System.out.println("----------------");
        }

        // Closing outer wrapper stream also closes inner stream
        dis.close();
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

    public static void flushMemTable(TreeMap<Long, String> memTable) throws IOException {

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
        // TreeMap is already sorted by key
        for (Long key : memTable.keySet()) {
            // Serialize sorted entry into SSTable
            writeEntry(key, memTable.get(key), dos);
            bloomFilter.add(key);
        }

        // Close streams
        dos.close();

        // Small RAM metadata describing SSTable boundaries
        sparseIndex.add(new SparseIndexEntry(memTable.firstKey(), memTable.lastKey(), fileName, bloomFilter));
        // Clear RAM after successful flush
        memTable.clear();
        // WAL no longer needed after durable flush
        clearWAL();

        System.out.println("Memtable flushed to " + fileName);
    }

    public static String get(long targetKey, TreeMap<Long, String> memTable) throws IOException {

        // STEP 1:
        // Check Memtable (RAM) first
        // Newest writes usually exist here
        if (memTable.containsKey(targetKey)) {

            System.out.println("Found in Memtable");
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

        // true = append mode
        FileOutputStream fos = new FileOutputStream(WAL_FILE, true);

        // Wrapper stream for primitive writes
        DataOutputStream dos = new DataOutputStream(fos);

        // Append operation to WAL
        writeEntry(key, value, dos);

        // Push buffered bytes toward OS
        dos.flush();

        dos.close();
    }

    public static void recoverFromWAL(TreeMap<Long, String> memTable) throws IOException {

        // WAL file path reference
        File walFile = new File(WAL_FILE);

        // No WAL exists yet
        if (!walFile.exists()) {
            System.out.println("No WAL found");
            return;
        }

        // Open WAL for replay
        FileInputStream fis = new FileInputStream(WAL_FILE);
        // Wrapper stream for primitive reads
        DataInputStream dis = new DataInputStream(fis);

        int recoveredOperations = 0;

        // Replay operations sequentially
        while (dis.available() > 0) {

            // Read logged key
            long key = dis.readLong();
            // Read value size metadata
            int valueLength = dis.readInt();
            // Allocate byte array for value
            byte[] valueBytes = new byte[valueLength];
            // Read exact value bytes
            dis.readFully(valueBytes);
            // Convert bytes back into String
            String value = new String(valueBytes);

            // Replay operation back into Memtable
            memTable.put(key, value);
            recoveredOperations++;
        }

        // Close WAL stream
        dis.close();

        System.out.println("Recovered " + recoveredOperations + " operations from WAL");
    }

    public static void clearWAL() throws IOException {

        // Opening without append mode truncates file to 0 bytes
        FileOutputStream fos = new FileOutputStream(WAL_FILE);

        // Close stream after clearing contents
        fos.close();

        // WAL reset complete
        System.out.println("WAL cleared");
    }

    public static void main(String[] args) throws IOException {

        bootstrapSparseIndex();

        // Memtable = sorted in-memory buffer
        TreeMap<Long, String> memTable = new TreeMap<>();

        recoverFromWAL(memTable);

        Random rand = new Random();

        String chars = "abcdefghijklmnopqrstuvwxyz";

        long start = System.currentTimeMillis();

        // Insert 5000 records into Memtable (RAM only)
//        for (long i = 1; i <= 5000; i++) {
//
//            StringBuilder sb = new StringBuilder();
//
//            // Generate random 10-character value
//            for (int j = 0; j < 10; j++) {
//
//                int randomIndex = rand.nextInt(chars.length());
//
//                sb.append(chars.charAt(randomIndex));
//            }
//
//            // Store inside Memtable instead of disk
//            // Random key between 1 and 5000
//            Long keyy = (long) (rand.nextInt(5000) + 1);
//            appendToWAL(keyy, sb.toString());
//            memTable.put(keyy, sb.toString());
//
//            // Flush when Memtable reaches threshold
//            if (memTable.size() >= 10) {
//                flushMemTable(memTable);
//            }
//        }

        long end = System.currentTimeMillis();

        // Flush remaining entries still in RAM
        if (!memTable.isEmpty()) {
            flushMemTable(memTable);
        }

        //let's try finding it in all SSTables
        System.out.println(get(100, memTable));
        System.out.println(get(1000000, memTable));


        //memtable is empty now so no need for thi code:
//        System.out.println("Memtable write complete");
//        System.out.println("Time Taken: " + (end - start) + " ms");
//
//        System.out.println();
//
//        // Print all sorted key-value pairs
//        for (Long key : memTable.keySet()) {
//
//            System.out.println("Key: " + key);
//            System.out.println("Value: " + memTable.get(key));
//            System.out.println("----------------");
//        }
    }
}
