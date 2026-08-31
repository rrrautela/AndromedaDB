public class SparseIndexEntry {

    public long firstKey;
    public long lastKey;
    public String fileName;
    public BloomFilter bloomFilter;

    public SparseIndexEntry(long firstKey, long lastKey, String fileName, BloomFilter bloomFilter) {
        this.firstKey = firstKey;
        this.lastKey = lastKey;
        this.fileName = fileName;
        this.bloomFilter = bloomFilter;
    }
}
