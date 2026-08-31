import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class Compactor {

    public static void compact(StorageEngine engine, String olderFile, String newerFile, String outputFile)
            throws IOException {

        // Open older SSTable
        DataInputStream oldDis = new DataInputStream(new FileInputStream(olderFile));

        // Open newer SSTable
        DataInputStream newDis = new DataInputStream(new FileInputStream(newerFile));

        // Output SSTable produced after compaction
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(outputFile));

        // Initial pointers (equivalent to i = 0, j = 0)
        Entry oldEntry = SSTable.readNextEntry(oldDis);
        Entry newEntry = SSTable.readNextEntry(newDis);

        // Merge phase
        while (oldEntry != null && newEntry != null) {

            // Older key comes first
            if (oldEntry.key < newEntry.key) {

                if (!oldEntry.value.equals(StorageEngine.TOMBSTONE)) {
                    SSTable.writeEntry(oldEntry.key, oldEntry.value, dos);
                }

                // i++
                oldEntry = SSTable.readNextEntry(oldDis);
            }

            // Newer key comes first
            else if (newEntry.key < oldEntry.key) {

                if (!newEntry.value.equals(StorageEngine.TOMBSTONE)) {
                    SSTable.writeEntry(newEntry.key, newEntry.value, dos);
                }

                // j++
                newEntry = SSTable.readNextEntry(newDis);
            }

            // Same key exists in both SSTables
            else {

                // Newer SSTable wins
                if (!newEntry.value.equals(StorageEngine.TOMBSTONE)) {
                    SSTable.writeEntry(newEntry.key, newEntry.value, dos);
                }

                // i++, j++
                oldEntry = SSTable.readNextEntry(oldDis);
                newEntry = SSTable.readNextEntry(newDis);
            }
        }

        // Drain remaining entries from older SSTable
        while (oldEntry != null) {

            if (!oldEntry.value.equals(StorageEngine.TOMBSTONE)) {
                SSTable.writeEntry(oldEntry.key, oldEntry.value, dos);
            }

            oldEntry = SSTable.readNextEntry(oldDis);
        }

        // Drain remaining entries from newer SSTable
        while (newEntry != null) {

            if (!newEntry.value.equals(StorageEngine.TOMBSTONE)) {
                SSTable.writeEntry(newEntry.key, newEntry.value, dos);
            }

            newEntry = SSTable.readNextEntry(newDis);
        }

        // Cleanup
        oldDis.close();
        newDis.close();
        dos.close();

        // Remove old SSTables and register compacted SSTable atomically
        synchronized (engine.sparseIndex) {

            engine.sparseIndex.removeIf(
                    entry -> entry.fileName.equals(olderFile)
                            || entry.fileName.equals(newerFile));

            // Build metadata for compacted SSTable
            SparseIndexEntry compactedEntry = SSTable.buildSparseIndexEntry(engine, outputFile);

            // Register compacted SSTable
            if (compactedEntry != null) {
                engine.sparseIndex.add(compactedEntry);
            }
        }

        // Old SSTables no longer needed
        new File(olderFile).delete();
        new File(newerFile).delete();

        System.out.println(
                "Compaction completed: "
                        + outputFile);
    }

    // Merge all active SSTables into one final SSTable
    public static void compactAllSSTables(StorageEngine engine) throws IOException {

        // Need at least 2 SSTables to compact
        synchronized (engine.sparseIndex) {
            if (engine.sparseIndex.size() < 2) {
                System.out.println("Not enough SSTables to compact");
                return;
            }
        }

        // Keep compacting until only one SSTable remains
        while (true) {

            synchronized (engine.sparseIndex) {
                if (engine.sparseIndex.size() <= 1) {
                    break;
                }
            }

            // Oldest SSTable
            String olderFile;
            // newest SSTable
            String newerFile;

            synchronized (engine.sparseIndex) {
                olderFile = engine.sparseIndex.get(0).fileName;
                newerFile = engine.sparseIndex.get(1).fileName;
            }

            // Temporary output SSTable
            String outputFile = "compacted_" + engine.compactionCounter++ + ".db";

            System.out.println(
                    "Compacting "
                            + olderFile
                            + " + "
                            + newerFile);

            // Reuse existing compaction logic
            compact(engine, olderFile, newerFile, outputFile);
        }

        synchronized (engine.sparseIndex) {
            System.out.println(
                    "Final SSTable: "
                            + engine.sparseIndex.get(0).fileName);
        }
    }
}
