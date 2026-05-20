import java.io.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Random;

public class Main {

    public static void readTest() throws IOException {

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


    public static void writeTest(long key, String value, DataOutputStream dos) throws IOException {

        // Write fixed-size 8-byte long key
        dos.writeLong(key);

        // Convert variable-length String into bytes
        byte[] valueBytes = value.getBytes();

        // Store value size first
        dos.writeInt(valueBytes.length);

        // Store actual value bytes
        dos.write(valueBytes);
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
    public static void main(String[] args) throws IOException {

        Random rand = new Random(); // used to generate random keys and values

        // In-memory index: key → byte position in file
        //HashMap<Integer, Long> indexMap = new HashMap<>();
        //RandomAccessFile raf = new RandomAccessFile("database.txt", "rw");

        //bootstrapIndex(raf, indexMap);

        // Close file after updates
        //raf.close();


        String chars = "abcdefghijklmnopqrstuvwxyz";


// Open binary file once for efficient sequential appends
        FileOutputStream fos = new FileOutputStream("binary.db", true);

// Wrapper stream for writing primitive data types
// (Decorator Pattern: DataOutputStream wraps FileOutputStream)
        DataOutputStream dos = new DataOutputStream(fos);


// Start measuring write performance
        long start = System.currentTimeMillis();
        long end;


// Insert 5000 records into append-only binary log
        for (long i = 1; i <= 5000; i++) {

            StringBuilder sb = new StringBuilder();

            // Generate random 10-character string value
            for (int j = 0; j < 10; j++) {

                int randomIndex = rand.nextInt(chars.length());

                sb.append(chars.charAt(randomIndex));
            }

            // Append record:
            // [8-byte key][4-byte valueLength][valueBytes]
            writeTest(i, sb.toString(), dos);
        }


// Close streams once after all writes finish
        dos.close();

        end = System.currentTimeMillis();

        System.out.println("Writing process is done");
        System.out.println("Time Taken: " + (end - start) + " ms");

        System.out.println();


// Measure sequential binary replay read performance
        start = System.currentTimeMillis();

        readTest();

        end = System.currentTimeMillis();

        System.out.println("Reading process is done");
        System.out.println("Time Taken: " + (end - start) + " ms");


        start = System.currentTimeMillis();

        rangeQuery(4900, 4910);

        end = System.currentTimeMillis();

        System.out.println("Range Query Time: " + (end - start) + " ms");


    }
}

