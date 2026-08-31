import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ConcurrentSkipListMap;

public class Main {

    public static void main(String[] args) throws IOException {

        StorageEngine engine = new StorageEngine();

        engine.bootstrapSparseIndex();

        // Memtable = sorted in-memory buffer
        ConcurrentSkipListMap<Long, String> memTable = new ConcurrentSkipListMap<>();
        engine.recoverFromWAL(memTable);

        // Find highest WAL segment already present on disk
        int maxWalSegment = engine.getMaxWalSegment();

        // New writes continue in the next WAL segment
        engine.currentWalSegment = maxWalSegment + 1;

        // Open active WAL segment
        engine.openWalSegment(engine.currentWalSegment);

        System.out.println(
                "Current WAL segment: " + engine.currentWalSegment);

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
            Long keyy = i;
            engine.appendToWAL(keyy, sb.toString());
            memTable.put(keyy, sb.toString());

            // Memtable full -> freeze current table
            if (memTable.size() >= 1000) {

                // Finish current WAL segment
                engine.currentWalDos.flush();
                engine.currentWalDos.close();

                // WAL segment belonging to this frozen Memtable
                int oldWalSegment = engine.currentWalSegment;

                // New writes go to a new WAL segment
                engine.currentWalSegment++;

                // New writes go into a fresh WAL segment
                engine.openWalSegment(engine.currentWalSegment);

                ConcurrentSkipListMap<Long, String> oldMemTable = memTable;

                // New active Memtable for incoming writes
                memTable = new ConcurrentSkipListMap<>();

                // Flush old Memtable in background
                engine.flushExecutor.submit(() -> {
                    // Background worker picked up a flush job
                    System.out.println(
                            "[" + Thread.currentThread().getName() + "] Starting scheduled flush");
                    try {
                        engine.flushMemTable(oldMemTable, oldWalSegment);
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
                        + " ms");

        System.out.println();
        System.out.println(
                "Writes/sec = "
                        + (totalWrites * 1000.0) / (end - start));
        System.out.println();

        // Flush remaining entries still in RAM
        if (!memTable.isEmpty()) {

            // Close final active WAL segment
            engine.currentWalDos.flush();
            engine.currentWalDos.close();

            int oldWalSegment = engine.currentWalSegment;

            ConcurrentSkipListMap<Long, String> oldMemTable = memTable;

            engine.flushExecutor.submit(() -> {
                // Background worker picked up the final flush
                System.out.println(
                        "[" + Thread.currentThread().getName() + "] Starting final flush");
                try {
                    engine.flushMemTable(oldMemTable, oldWalSegment);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        // No more background jobs will be submitted

        // Main thread now waits for all background flushes
        System.out.println("[main] Finished inserts. Waiting for background flushes...");

        engine.shutdown();

        // Safe to read SSTables now
        System.out.println("[main] All background flushes completed.");

        System.out.println(
                "Final SSTables = "
                        + engine.sparseIndex.size());

        System.out.println(
                "Final File = "
                        + engine.sparseIndex.get(0).fileName);

        // let's try finding it in all SSTables
        System.out.println(engine.get(100, memTable));
        System.out.println(engine.get(1000000, memTable));
    }
}
