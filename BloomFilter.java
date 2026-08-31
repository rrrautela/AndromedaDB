import java.util.BitSet;

public class BloomFilter {

    // Compact binary bitmap storing Bloom Filter hash footprints efficiently in RAM
    private final BitSet bitSet;
    // Total size of bit array
    private final int size;

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
