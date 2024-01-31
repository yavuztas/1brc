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
import java.util.concurrent.atomic.AtomicLong;
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

        private int min; // calculations over int is faster than double, we convert to double in the end only once
        private int max;
        private long sum;
        private int count;

        public Record(long start, int length, long word1, long word2, long wordLast, int hash) {
            this.start = start;
            this.length = length;
            this.word1 = word1;
            this.word2 = word2;
            this.wordLast = wordLast;
            this.hash = hash;
            this.min = 999;
            this.max = -999;
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

    // Inspired by @spullara - customized hashmap on purpose
    // The main difference is we hold only one array instead of two, fewer objects is faster
    private static final class RecordMap {

        // Bigger bucket size less collisions, but you have to find a sweet spot otherwise it is becoming slower.
        // Also works good enough for 10K stations
        private static final int SIZE = 1 << 14; // 16kb - enough for 10K
        private static final int BITMASK = SIZE - 1;
        private final Record[] keys = new Record[SIZE];

        // int collision;

        private boolean hasNoRecord(int index) {
            return this.keys[index] == null;
        }

        private Record getRecord(int index) {
            return this.keys[index];
        }

        private static int hashBucket(int hash) {
            hash = hash ^ (hash >>> 16); // naive bit spreading but surprisingly decreases collision :)
            return hash & BITMASK; // fast modulo, to find bucket
        }

        private void putAndCollect(int hash, int temp, long start, int length, long word1, long word2, long wordLast) {
            final int bucket = hashBucket(hash);
            if (hasNoRecord(bucket)) {
                this.keys[bucket] = new Record(start, length, word1, word2, wordLast, hash, temp);
                return;
            }

            Record existing = getRecord(bucket);
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

        private Record putOrGet(int hash, long start, int length, long word1, long word2, long wordLast) {
            final int bucket = hashBucket(hash);
            if (hasNoRecord(bucket)) {
                return this.keys[bucket] = new Record(start, length, word1, word2, wordLast, hash);
            }

            Record existing = getRecord(bucket);
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

        private void putOrMerge(Record key) {
            final int bucket = hashBucket(key.hash);
            if (hasNoRecord(bucket)) {
                key.next = null;
                this.keys[bucket] = key;
                return;
            }

            Record existing = getRecord(bucket);
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

        private void forEach(Consumer<Record> consumer) {
            int pos = 0;
            Record key;
            while (pos < SIZE) {
                if ((key = this.keys[pos++]) == null) {
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

        private void merge(RecordMap other) {
            other.forEach(this::putOrMerge);
        }

    }

    // One actor for one thread, no synchronization
    private static final class RegionActor extends Thread {

        private static final int SEGMENT_SIZE = 1 << 21; // 2mb

        private final long fileStart;
        private final long fileEnd;
        private long startPos; // start of region memory address
        private int size;

        private final RecordMap map = new RecordMap();

        public RegionActor(long fileStart, long fileEnd, long startPos, int size) {
            this.fileStart = fileStart;
            this.fileEnd = fileEnd;
            this.startPos = startPos;
            this.size = size;
        }

        private static long getWord(long address) {
            return UNSAFE.getLong(address);
        }

        // hasvalue & haszero
        // adapted from https://graphics.stanford.edu/~seander/bithacks.html#ZeroInWord
        private static long hasSemicolon(long word) {
            // semicolon pattern
            final long hasVal = word ^ 0x3B3B3B3B3B3B3B3BL; // hasvalue
            return ((hasVal - 0x0101010101010101L) & ~hasVal & 0x8080808080808080L); // haszero
        }

        private static int semicolonPos(long hasVal) {
            return Long.numberOfTrailingZeros(hasVal) >>> 3;
        }

        private static int decimalPos(long numberWord) {
            return Long.numberOfTrailingZeros(~numberWord & 0x10101000);
        }

        private static final int MAX_INNER_LOOP_SIZE = 11;

        // @Override
        public void run2() {
            long pointer = this.startPos;
            final long size = pointer + this.size;
            while (pointer < size) { // line start
                long hash = 0; // reset hash
                long s; // semicolon check word
                final int pos; // semicolon position
                long word1 = getWord(pointer);
                if ((s = hasSemicolon(word1)) != 0) {
                    pos = semicolonPos(s);
                    // read temparature
                    final long numberWord = getWord(pointer + pos + 1);
                    final int decimalPos = decimalPos(numberWord);
                    final int temp = convertIntoNumber(decimalPos, numberWord);

                    word1 = partial(word1, pos); // last word
                    this.map.putAndCollect(completeHash(hash, word1), temp, pointer, pos, word1, 0, 0);

                    pointer += pos + (decimalPos >>> 3) + 4;
                }
                else {
                    long word2 = getWord(pointer + 8);
                    if ((s = hasSemicolon(word2)) != 0) {
                        pos = semicolonPos(s);
                        // read temparature
                        final int length = pos + 8;
                        final long numberWord = getWord(pointer + length + 1);
                        final int decimalPos = decimalPos(numberWord);
                        final int temp = convertIntoNumber(decimalPos, numberWord);

                        word2 = partial(word2, pos); // last word
                        this.map.putAndCollect(completeHash(hash, word1, word2), temp, pointer, length, word1, word2, 0);

                        pointer += length + (decimalPos >>> 3) + 4; // seek to the line end
                    }
                    else {
                        long word = 0;
                        int length = 16;
                        hash = appendHash(hash, word1, word2);
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
                        this.map.putAndCollect(completeHash(hash, word), temp, pointer, length, word1, word2, word);

                        pointer += length + (decimalPos >>> 3) + 4; // seek to the line end
                    }
                }
            }
        }

        private static final AtomicLong counter = new AtomicLong(0);

        @Override
        public void run() {

            while (true) {
                // steal a segment
                long segmentIndex = counter.getAndIncrement();
                this.startPos = this.fileStart + segmentIndex * SEGMENT_SIZE;

                if (this.startPos >= this.fileEnd) {
                    return;
                }

                if (this.startPos == this.fileStart) {
                    final long limit = Math.min(this.fileEnd, this.startPos + SEGMENT_SIZE);
                    this.size = findClosestLineEnd(this.startPos, (int) (limit - this.startPos));
                }
                else {
                    this.startPos += findClosestLineEnd(this.startPos - SEGMENT_SIZE, SEGMENT_SIZE) - SEGMENT_SIZE;
                    final long limit = Math.min(this.fileEnd, this.startPos + SEGMENT_SIZE);
                    this.size = findClosestLineEnd(this.startPos, (int) (limit - this.startPos));
                }

                // calculate bounderies to split parallel instructions
                long pointer1 = this.startPos;
                final long limit1;
                long pointer2;
                final long limit2 = pointer1 + this.size;

                final int newPos = findClosestLineEnd(pointer1, this.size / 2);
                if (newPos == 0) {
                    limit1 = pointer1 + this.size;
                }
                else {
                    limit1 = pointer1 + newPos;
                }
                pointer2 = limit1;

                while (true) { // line start
                    if (pointer1 >= limit1) {
                        break;
                    }
                    if (pointer2 >= limit2) {
                        break;
                    }

                    final long word1 = getWord(pointer1);
                    final long word2 = getWord(pointer2);
                    final long semicolon1 = hasSemicolon(word1);
                    final long semicolon2 = hasSemicolon(word2);
                    final Record record1 = findRecord(this.map, pointer1, word1, semicolon1);
                    final Record record2 = findRecord(this.map, pointer2, word2, semicolon2);

                    // read temparature
                    pointer1 += collectTemp(record1, pointer1);
                    pointer2 += collectTemp(record2, pointer2);
                }

                // process leftovers
                while (pointer1 < limit1) {
                    final long word = getWord(pointer1);
                    final long semicolon = hasSemicolon(word);
                    final Record record = findRecord(this.map, pointer1, word, semicolon);
                    // read temparature
                    pointer1 += collectTemp(record, pointer1);
                }
                while (pointer2 < limit2) {
                    final long word = getWord(pointer2);
                    final long semicolon = hasSemicolon(word);
                    final Record record = findRecord(this.map, pointer2, word, semicolon);
                    // read temparature
                    pointer2 += collectTemp(record, pointer2);
                }

            }

        }

        private static Record findRecord(RecordMap map, long pointer, long word, long hasSemicolon) {
            if (hasSemicolon != 0) {
                final int spos = semicolonPos(hasSemicolon);
                word = partial(word, spos);
                return map.putOrGet(completeHash(0, word), pointer, spos, word, 0, 0);
            }
            else {
                long next = getWord(pointer + 8);
                if ((hasSemicolon = hasSemicolon(next)) != 0) {
                    final int spos = semicolonPos(hasSemicolon);
                    next = partial(next, spos);
                    return map.putOrGet(completeHash(word, next), pointer, spos + 8, word, 0, 0);
                }
                else {
                    long last = 0; // last word
                    int length = 16;
                    long hash = appendHash(word, next);
                    // Let the compiler know the loop size ahead
                    // Then it's automatically unrolled
                    // Max key length is 13 longs, 2 we've read before, 11 left
                    for (int i = 0; i < MAX_INNER_LOOP_SIZE; i++) {
                        if ((hasSemicolon = hasSemicolon(last = getWord(pointer + length))) != 0) {
                            break;
                        }
                        hash = appendHash(hash, last);
                        length += 8;
                    }
                    final int spos = semicolonPos(hasSemicolon);
                    last = partial(last, spos);
                    return map.putOrGet(completeHash(hash, last), pointer, spos + length, word, next, last);
                }
            }
        }

        private static int collectTemp(Record record, long pointer) {
            final long numberWord = getWord(pointer + record.length + 1);
            final int decimalPos = decimalPos(numberWord);
            record.collect(convertIntoNumber(decimalPos, numberWord));
            return record.length + (decimalPos >>> 3) + 4; // return pointer increment
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

        private static int completeHash(long hash, long partial) {
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

        /**
         * blocks until the map is fully collected
         */
        private RecordMap get() throws InterruptedException {
            join();
            return this.map;
        }
    }

    private static double round(double value) {
        return Math.round(value) / 10.0;
    }

    /**
     * Scans the given buffer to the left
     */
    private static int findClosestLineEnd(long start, int size) {
        long position = start + size;
        while (UNSAFE.getByte(--position) != '\n') {
            // read until a linebreak
            size--;
            if (size == 0) // no newline found
                break;
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

        // Dased on @thomaswue's idea, to cut unmapping delay.
        // Strangely, unmapping delay doesn't occur on macOS/M1 however in Linux/AMD it's substantial - ~200ms
        if (!isWorkerProcess(args)) {
            runAsWorker();
            return;
        }

        var concurrency = 2 * Runtime.getRuntime().availableProcessors();
        final long fileSize = Files.size(FILE);
        long regionSize = fileSize / concurrency;

        // handling extreme cases
        while (regionSize > Integer.MAX_VALUE) {
            concurrency *= 2;
            regionSize /= 2;
        }
        if (fileSize <= 1 << 20) { // small file (1mb), no need concurrency
            concurrency = 1;
            regionSize = fileSize;
        }

        long startPos = 0;
        final FileChannel channel = (FileChannel) Files.newByteChannel(FILE, StandardOpenOption.READ);
        // get the memory address, this is the only thing we need for Unsafe
        final long memoryAddress = channel.map(FileChannel.MapMode.READ_ONLY, startPos, fileSize, Arena.global()).address();

        final RegionActor[] actors = new RegionActor[concurrency];
        for (int i = 0; i < concurrency; i++) {
            // calculate boundaries
            long maxSize = (startPos + regionSize > fileSize) ? fileSize - startPos : regionSize;
            // shift position to back until we find a linebreak
            maxSize = findClosestLineEnd(memoryAddress + startPos, (int) maxSize);

            final RegionActor region = (actors[i] = new RegionActor(memoryAddress, memoryAddress + fileSize, memoryAddress + startPos, (int) maxSize));
            region.start(); // start processing

            startPos += maxSize;
        }

        final RecordMap output = new RecordMap(); // output to merge all records
        for (RegionActor actor : actors) {
            final RecordMap partial = actor.get(); // blocks until get the result
            output.merge(partial);
            // System.out.println("collisions: " + partial.collision);
        }

        // sort and print the result
        final TreeMap<String, String> sorted = new TreeMap<>();
        output.forEach(key -> {
            sorted.put(key.toString(), key.measurements());
        });
        System.out.println(sorted);
        System.out.close(); // closing the stream will trigger the main process to pick up the output early
    }

}
