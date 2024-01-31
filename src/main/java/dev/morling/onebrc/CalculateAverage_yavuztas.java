/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package dev.morling.onebrc;

import sun.misc.Unsafe;

import java.lang.foreign.Arena;
import java.lang.reflect.Field;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;
import java.util.function.Consumer;

public class CalculateAverage_yavuztas {

    private static final Path FILE = Path.of("./measurements.txt");

    private static final Unsafe UNSAFE = unsafe();

    // I compared all three: MappedByteBuffer, MemorySegment and Unsafe.
    // Accessing the memory using Unsafe is still the fastest in my experience.
    // However, I would never use it in production, single programming error will crash your app.
    private static Unsafe unsafe() {
        try {
            final Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            return (Unsafe) f.get(null);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Extract bytes from a long
     */
    private static long partial(long word, int length) {
        final long mask = (~0L) << (length << 3);
        return word & (~mask);
    }

    // Only one object, both for measurements and keys, less object creation in hotpots is always faster
    private static final class Record {

        private final long start; // memory address of the underlying data
        private final int length;
        private final long word1;
        private final long word2;
        private final long wordLast;
        private final int hash;
        private Record next; // linked list to resolve hash collisions

        private int min = 999; // calculations over int is faster than double, we convert to double in the end only once
        private int max = -999;
        private long sum;
        private int count;

        public Record(long start, int length, long word1, long word2, long wordLast, int hash) {
            this.start = start;
            this.length = length;
            this.word1 = word1;
            this.word2 = word2;
            this.wordLast = wordLast;
            this.hash = hash;
        }

        public Record(long start, int length, long word1, long word2, long wordLast, int hash, int temp) {
            this.start = start;
            this.length = length;
            this.word1 = word1;
            this.word2 = word2;
            this.wordLast = wordLast;
            this.hash = hash;
            this.min = temp;
            this.max = temp;
            this.sum = temp;
            this.count = 1;
        }

        @Override
        public boolean equals(Object o) {
            final Record record = (Record) o;
            return equals(record.start, record.word1, record.word2, record.wordLast, record.length);
        }

        private static boolean notEquals(long address1, long address2, int step) {
            return UNSAFE.getLong(address1 + step) != UNSAFE.getLong(address2 + step);
        }

        private static boolean equalsComparingLongs(long start1, long start2, int length) {
            // first shortcuts
            if (length < 24)
                return true;
            if (length < 32)
                return !notEquals(start1, start2, 16);

            int step = 24; // starting from 3rd long
            length -= step;
            while (length >= 8) { // scan longs
                if (notEquals(start1, start2, step)) {
                    return false;
                }
                length -= 8;
                step += 8; // 8 bytes
            }
            return true;
        }

        private boolean equals(long start, long word1, long word2, long last, int length) {
            if (this.word1 != word1)
                return false;
            if (this.word2 != word2)
                return false;

            // equals check is done by comparing longs instead of byte by byte check, this is faster
            return equalsComparingLongs(this.start, start, length) && this.wordLast == last;
        }

        @Override
        public String toString() {
            final byte[] bytes = new byte[this.length];
            UNSAFE.copyMemory(null, this.start, bytes, Unsafe.ARRAY_BYTE_BASE_OFFSET, this.length);
            return new String(bytes, StandardCharsets.UTF_8);
        }

        private void collect(int temp) {
            if (temp < this.min)
                this.min = temp;
            if (temp > this.max)
                this.max = temp;
            this.sum += temp;
            this.count++;
        }

        private void merge(Record that) {
            if (that.min < this.min)
                this.min = that.min;
            if (that.max > this.max)
                this.max = that.max;
            this.sum += that.sum;
            this.count += that.count;
        }

        private String measurements() {
            // here is only executed once for each unique key, so StringBuilder creation doesn't harm
            final StringBuilder sb = new StringBuilder(14);
            sb.append(round(this.min)).append("/");
            sb.append(round(1.0 * this.sum / this.count)).append("/");
            sb.append(round(this.max));
            return sb.toString();
        }
    }

    private static class Region {
        long start;
        long limit;
        long position;

        public Region(long start, long limit) {
            this.start = start;
            this.limit = limit;
            this.position = start;
        }

        long position() {
            return this.position;
        }

        void seek(int pos) {
            this.position += pos;
        }

        boolean hasRemaining() {
            return this.position < this.limit;
        }

        long getWord() {
            return UNSAFE.getLong(this.position);
        }
    }

    // One actor for one thread, no synchronization
    // Region processor for a single region
    private static class RegionProcessor extends Thread {

        // Bigger bucket size less collisions, but you have to find a sweet spot otherwise it is becoming slower.
        // Also works good enough for 10K stations
        static final int SIZE = 1 << 14; // 16kb - enough for 10K
        static final int BITMASK = SIZE - 1;
        static final int MAX_INNER_LOOP_SIZE = 11;

        final Region region;

        Record[] aggregations;

        public RegionProcessor(Region region) {
            this.region = region;
        }

        @Override
        public void run() {
            // local vars is faster than field access, so we carried record array here
            final Record[] records = new Record[SIZE];

            // process single region
            final Region region = this.region;

            while (region.hasRemaining()) {
                final long word = region.getWord();
                final long hasSemicolon = hasSemicolon(word);
                final Record record = findRecord(records, region, word, hasSemicolon);
                final int temp = getTemp(region);
                record.collect(temp);
            }

            this.aggregations = records; // to expose records after the job is done
        }

        static Record findRecord(Record[] records, Region region, long word, long hasSemicolon) {
            final long pos = region.position();
            if (hasSemicolon != 0) {
                final int spos = semicolonPos(hasSemicolon);
                region.seek(spos); // seek to semicolon
                word = partial(word, spos);
                return getOrCreateRecord(records, completeHash(word), pos, spos, word, 0, 0);
            }
            else {
                long next = getWord(pos + 8);
                if ((hasSemicolon = hasSemicolon(next)) != 0) {
                    final int spos = semicolonPos(hasSemicolon);
                    region.seek(spos + 8); // seek to semicolon
                    next = partial(next, spos);
                    return getOrCreateRecord(records, completeHash(word, next), pos, spos + 8, word, 0, 0);
                }
                else {
                    long last = 0; // last word
                    int length = 16;
                    long hash = appendHash(word, next);
                    // Let the compiler know the loop size ahead
                    // Then it's automatically unrolled
                    // Max key length is 13 longs, 2 we've read before, 11 left
                    for (int i = 0; i < MAX_INNER_LOOP_SIZE; i++) {
                        if ((hasSemicolon = hasSemicolon(last = getWord(pos + length))) != 0) {
                            break;
                        }
                        hash = appendHash(hash, last);
                        length += 8;
                    }
                    final int spos = semicolonPos(hasSemicolon);
                    region.seek(spos + length); // seek to semicolon
                    last = partial(last, spos);
                    return getOrCreateRecord(records, completeHash(hash, last), pos, spos + length, word, next, last);
                }
            }
        }

        static int getTemp(Region region) { // region pos + semicolonPos
            final long numberWord = getWord(region.position() + 1);
            final int decimalPos = decimalPos(numberWord);
            region.seek((decimalPos >>> 3) + 4);
            return convertIntoNumber(decimalPos, numberWord);
        }

        static int processRest(Record[] records, long word1, long word2, long s, long pointer) {
            final int pos;
            long word = 0;
            int length = 16;
            long hash = appendHash(word1, word2);
            // Let the compiler know the loop size ahead
            // Then it's automatically unrolled
            // Max key length is 13 longs, 2 we've read before, 11 left
            for (int i = 0; i < MAX_INNER_LOOP_SIZE; i++) {
                if ((s = hasSemicolon((word = getWord(pointer + length)))) != 0) {
                    break;
                }
                hash = appendHash(hash, word);
                length += 8;
            }

            pos = semicolonPos(s);
            length += pos;
            // read temparature

            final long numberWord = getWord(pointer + length + 1);
            final int decimalPos = decimalPos(numberWord);
            final int temp = convertIntoNumber(decimalPos, numberWord);

            word = partial(word, pos); // last word
            putAndCollect(records, completeHash(hash, word), temp, pointer, length, word1, word2, word);

            return length + (decimalPos >>> 3) + 4; // seek to the line end
        }

        static int processWord2(Record[] records, long s, long pointer, long word2, long word1) {
            final int pos;
            pos = semicolonPos(s);
            // read temparature
            final int length = pos + 8;
            final long numberWord = getWord(pointer + length + 1);
            final int decimalPos = decimalPos(numberWord);
            final int temp = convertIntoNumber(decimalPos, numberWord);

            final long word = partial(word2, pos); // last word
            putAndCollect(records, completeHash(word1, word), temp, pointer, length, word1, word, 0);

            return length + (decimalPos >>> 3) + 4; // seek to the line end
        }

        static int processWord1(Record[] records, long s, long pointer, long word1) {
            final int pos;
            pos = semicolonPos(s);
            // read temparature
            final long numberWord = getWord(pointer + pos + 1);
            final int decimalPos = decimalPos(numberWord);
            final int temp = convertIntoNumber(decimalPos, numberWord);

            final long word = partial(word1, pos); // last word
            putAndCollect(records, completeHash(word), temp, pointer, pos, word, 0, 0);

            return pos + (decimalPos >>> 3) + 4;
        }

        private static boolean hasNoRecord(Record[] records, int index) {
            return records[index] == null;
        }

        private static int hashBucket(int hash) {
            hash = hash ^ (hash >>> 16); // naive bit spreading but surprisingly decreases collision :)
            return hash & BITMASK; // fast modulo, to find bucket
        }

        static Record getOrCreateRecord(Record[] records, int hash, long start, int length, long word1, long word2, long wordLast) {
            final int bucket = hashBucket(hash);
            if (hasNoRecord(records, bucket)) {
                return records[bucket] = new Record(start, length, word1, word2, wordLast, hash);
            }

            Record existing = records[bucket];
            if (existing.equals(start, word1, word2, wordLast, length)) {
                return existing;
            }

            // collision++;
            // find possible slot by scanning the slot linked list
            while (existing.next != null) {
                if (existing.next.equals(start, word1, word2, wordLast, length)) {
                    return existing.next;
                }
                existing = existing.next; // go on to next
                // collision++;
            }
            return existing.next = new Record(start, length, word1, word2, wordLast, hash);
        }

        static void putAndCollect(Record[] records, int hash, int temp, long start, int length, long word1, long word2, long wordLast) {
            final int bucket = hashBucket(hash);
            if (hasNoRecord(records, bucket)) {
                records[bucket] = new Record(start, length, word1, word2, wordLast, hash, temp);
                return;
            }

            Record existing = records[bucket];
            if (existing.equals(start, word1, word2, wordLast, length)) {
                existing.collect(temp);
                return;
            }

            // collision++;
            // find possible slot by scanning the slot linked list
            while (existing.next != null) {
                if (existing.next.equals(start, word1, word2, wordLast, length)) {
                    existing.next.collect(temp);
                    return;
                }
                existing = existing.next; // go on to next
                // collision++;
            }
            existing.next = new Record(start, length, word1, word2, wordLast, hash, temp);
        }

        private static void putOrMerge(Record[] records, Record key) {
            final int bucket = hashBucket(key.hash);
            if (hasNoRecord(records, bucket)) {
                key.next = null;
                records[bucket] = key;
                return;
            }

            Record existing = records[bucket];
            if (existing.equals(key)) {
                existing.merge(key);
                return;
            }

            // collision++;
            // find possible slot by scanning the slot linked list
            while (existing.next != null) {
                if (existing.next.equals(key)) {
                    existing.next.merge(key);
                    return;
                }
                existing = existing.next; // go on to next
                // collision++;
            }
            key.next = null;
            existing.next = key;
        }

        private static void forEach(Record[] records, Consumer<Record> consumer) {
            int pos = 0;
            Record key;
            while (pos < SIZE) {
                if ((key = records[pos++]) == null) {
                    continue;
                }
                Record next = key.next;
                consumer.accept(key);
                while (next != null) { // also traverse the records in the collision list
                    final Record tmp = next.next;
                    consumer.accept(next);
                    next = tmp;
                }
            }
        }

        private static void merge(Record[] records, Record[] other) {
            forEach(other, key -> putOrMerge(records, key));
        }

        static long getWord(long address) {
            return UNSAFE.getLong(address);
        }

        // hasvalue & haszero
        // adapted from https://graphics.stanford.edu/~seander/bithacks.html#ZeroInWord
        static long hasSemicolon(long word) {
            // semicolon pattern
            final long hasVal = word ^ 0x3B3B3B3B3B3B3B3BL; // hasvalue
            return ((hasVal - 0x0101010101010101L) & ~hasVal & 0x8080808080808080L); // haszero
        }

        static int semicolonPos(long hasVal) {
            return Long.numberOfTrailingZeros(hasVal) >>> 3;
        }

        static int decimalPos(long numberWord) {
            return Long.numberOfTrailingZeros(~numberWord & 0x10101000);
        }

        // Hashes are calculated by a Mersenne Prime (1 << 7) -1
        // This is faster than multiplication in some machines
        private static long appendHash(long hash, long word) {
            return (hash << 7) - hash + word;
        }

        private static long appendHash(long hash, long word1, long word2) {
            hash = (hash << 7) - hash + word1;
            return (hash << 7) - hash + word2;
        }

        static int completeHash(long partial) {
            return (int) (partial ^ (partial >>> 25));
        }

        static int completeHash(long hash, long partial) {
            hash = (hash << 7) - hash + partial;
            return (int) (hash ^ (hash >>> 25));
        }

        private static int completeHash(long hash, long word1, long word2) {
            hash = (hash << 7) - hash + word1;
            hash = (hash << 7) - hash + word2;
            return (int) hash ^ (int) (hash >>> 25);
        }

        // Credits to @merrykitty. Magical solution to parse temparature values branchless!
        // Taken as without modification, comments belong to @merrykitty
        private static int convertIntoNumber(int decimalSepPos, long numberWord) {
            final int shift = 28 - decimalSepPos;
            // signed is -1 if negative, 0 otherwise
            final long signed = (~numberWord << 59) >> 63;
            final long designMask = ~(signed & 0xFF);
            // Align the number to a specific position and transform the ascii code
            // to actual digit value in each byte
            final long digits = ((numberWord & designMask) << shift) & 0x0F000F0F00L;
            // Now digits is in the form 0xUU00TTHH00 (UU: units digit, TT: tens digit, HH: hundreds digit)
            // 0xUU00TTHH00 * (100 * 0x1000000 + 10 * 0x10000 + 1) =
            // 0x000000UU00TTHH00 +
            // 0x00UU00TTHH000000 * 10 +
            // 0xUU00TTHH00000000 * 100
            // Now TT * 100 has 2 trailing zeroes and HH * 100 + TT * 10 + UU < 0x400
            // This results in our value lies in the bit 32 to 41 of this product
            // That was close :)
            final long absValue = ((digits * 0x640a0001) >>> 32) & 0x3FF;
            final long value = (absValue ^ signed) - signed;
            return (int) value;
        }

    }

    private static final class MultiRegionProcessor extends RegionProcessor {

        private final Region region2;
        private final Region region3;

        public MultiRegionProcessor(Region region, Region region2, Region region3) {
            super(region);
            this.region2 = region2;
            this.region3 = region3;
        }

        @Override
        public void run() {
            // local vars is faster than field access, so we carried record array here
            final Record[] records = new Record[SIZE];

            long pointer1 = this.region.start;
            long pointer2 = this.region2.start;
            long pointer3 = this.region3.start;

            final long limit1 = this.region.limit;
            final long limit2 = this.region2.limit;
            final long limit3 = this.region3.limit;

            while (true) {
                // region boundary checks
                if (pointer1 >= limit1)
                    break;
                if (pointer2 >= limit2)
                    break;
                if (pointer3 >= limit3)
                    break;

                pointer1 += processRegion(pointer1, records);
                pointer2 += processRegion(pointer2, records);
                pointer3 += processRegion(pointer3, records);
            }
            // process leftovers
            while (pointer1 < limit1) {
                pointer1 += processRegion(pointer1, records);
            }
            while (pointer2 < limit2) {
                pointer2 += processRegion(pointer2, records);
            }
            while (pointer3 < limit3) {
                pointer3 += processRegion(pointer3, records);
            }

            this.aggregations = records; // to expose records after the job is done
        }

        private static int processRegion(long pointer, Record[] records) {
            long semicolon; // semicolon check word
            final long word1 = getWord(pointer);
            if ((semicolon = hasSemicolon(word1)) != 0) {
                return processWord1(records, semicolon, pointer, word1);
            }
            else {
                final long word2 = getWord(pointer + 8);
                if ((semicolon = hasSemicolon(word2)) != 0) {
                    return processWord2(records, semicolon, pointer, word2, word1);
                }
                else {
                    return processRest(records, word1, word2, semicolon, pointer);
                }
            }
        }

    }

    private static double round(double value) {
        return Math.round(value) / 10.0;
    }

    /**
     * Scans the given buffer to the left
     */
    private static long findClosestLineEnd(long start, int size) {
        long position = start + size;
        while (UNSAFE.getByte(--position) != '\n') {
            // read until a linebreak
            size--;
        }
        return size;
    }

    private static boolean isWorkerProcess(String[] args) {
        return Arrays.asList(args).contains("--worker");
    }

    private static void runAsWorker() throws Exception {
        final ProcessHandle.Info info = ProcessHandle.current().info();
        final List<String> commands = new ArrayList<>();
        info.command().ifPresent(commands::add);
        info.arguments().ifPresent(args -> commands.addAll(Arrays.asList(args)));
        commands.add("--worker");

        new ProcessBuilder()
                .command(commands)
                .start()
                .getInputStream()
                .transferTo(System.out);
    }

    public static void main(String[] args) throws Exception {

        // Based on @thomaswue's idea, to cut unmapping delay.
        // Strangely, unmapping delay doesn't occur on macOS/M1 however in Linux/AMD it's substantial - ~200ms
        if (!isWorkerProcess(args)) {
            runAsWorker();
            return;
        }

        var concurrency = 2 * Runtime.getRuntime().availableProcessors();

        final long fileSize = Files.size(FILE);
        int regionPerThread = 1;
        int regionCount = regionPerThread * concurrency;
        long regionSize = fileSize / regionCount;

        // handling extreme cases
        while (regionSize > Integer.MAX_VALUE) {
            concurrency *= 2;
            regionCount *= 2;
            regionSize /= 2;
        }
        if (fileSize <= 1 << 20) { // small file (1mb), no need concurrency
            concurrency = 1;
            regionPerThread = 1;
            regionCount = 1;
            regionSize = fileSize;
        }

        long startPos = 0;
        final FileChannel channel = (FileChannel) Files.newByteChannel(FILE, StandardOpenOption.READ);
        // get the memory address, this is the only thing we need for Unsafe
        final long memoryAddress = channel.map(FileChannel.MapMode.READ_ONLY, startPos, fileSize, Arena.global()).address();

        final Region[] regions = new Region[regionCount];
        for (int i = 0; i < regionCount; i++) {
            // calculate boundaries
            long maxSize = (startPos + regionSize > fileSize) ? fileSize - startPos : regionSize;
            // shift position to back until we find a linebreak
            maxSize = findClosestLineEnd(memoryAddress + startPos, (int) maxSize);
            regions[i] = new Region(memoryAddress + startPos, memoryAddress + startPos + maxSize);

            startPos += maxSize;
        }

        final RegionProcessor[] actors = new RegionProcessor[concurrency];
        for (int i = 0; i < concurrency; i++) {
            final RegionProcessor actor;
            if (regionPerThread == 1) {
                // single region per processor
                actor = new RegionProcessor(regions[i]);
            }
            else {
                // 3 regions per processor
                actor = new MultiRegionProcessor(regions[3 * i], regions[3 * i + 1], regions[3 * i + 2]);
            }
            actor.start(); // start imeediately
            actors[i] = actor;
        }

        for (RegionProcessor actor : actors) {
            actor.join(); // blocks until we get the result from thread
        }

        final Record[] output = new Record[RegionProcessor.SIZE];
        for (RegionProcessor actor : actors) {
            RegionProcessor.merge(output, actor.aggregations); // merge all
        }

        // sort and print the result
        final TreeMap<String, String> sorted = new TreeMap<>();
        RegionProcessor.forEach(output,
                key -> sorted.put(key.toString(), key.measurements()));
        System.out.println(sorted);
        System.out.close(); // closing the stream will trigger the main process to pick up the output early

    }

}
