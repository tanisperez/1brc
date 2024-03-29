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

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

/**
 * Tests on my MacBook Pro 2020 with i5 and 16 GB of RAM.
 *
 * 1. baseline with zulu 21 -> 3:53
 * 2. my implementation with zulu 21 -> 2:02
 * 3. baseline with GraalVM 21.0.2 -> 3:33
 * 4. my implementation with GraalVM 21.0.2 -> 2:09
 * 5. native image -> 5:18
 * 6. Multithread implementation with zulu 21 -> 1:14
 * 7. Multithread implementation with GraalVM 21.0.2 -> 1:04
 * 8. Improved multithread implementation with GraalVM 21.0.2 -> 0:51
 * 9. Custom parseInt with GraalVM 21.0.2 -> 0:44
 * 10. Inlining POW_10X with GraalVM 21.0.2 -> 0:40.858
 */
public class CalculateAverage_tanisperez {
    private static final String FILE = "./measurements-1br.txt";

    private static final int MAX_STATIONS = 10_000;

    public static void main(String[] args) throws Exception {
        int numberOfCores = Runtime.getRuntime().availableProcessors();

        try (RandomAccessFile file = new RandomAccessFile(FILE, "r")) {
            List<MappedByteBuffer> chunks = splitFileInChunks(file, numberOfCores);
            Map<Integer, Map<Station, MeasurementAggregator>> resultsPerCore = processChunksInParallel(numberOfCores, chunks);
            Map<Station, MeasurementAggregator> accumulatedMeasures = groupResults(resultsPerCore);
            Map<String, ResultRow> results = getSortedResults(accumulatedMeasures);

            System.out.println(results);
        }
    }

    private static List<MappedByteBuffer> splitFileInChunks(RandomAccessFile file, int numberOfCores) throws IOException {
        FileChannel fileChannel = file.getChannel();

        long fileSize = fileChannel.size();
        long chunkSize = fileSize / numberOfCores;

        List<MappedByteBuffer> chunks = new ArrayList<>((int) (fileSize / chunkSize) + 1);

        long chunkStart = 0L;
        while (chunkStart < fileSize) {
            long currentPosition = chunkStart + chunkSize;
            if (currentPosition < fileSize) {
                file.seek(currentPosition);
                while (file.read() != '\n') {
                    currentPosition++;
                }
                currentPosition++; // Skip the \n position
            }
            else {
                currentPosition = fileSize;
            }

            MappedByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, chunkStart, currentPosition - chunkStart);
            buffer.order(ByteOrder.nativeOrder());
            chunks.add(buffer);

            chunkStart = currentPosition;
        }

        return chunks;
    }

    private static Map<Integer, Map<Station, MeasurementAggregator>> processChunksInParallel(int numberOfCores, List<MappedByteBuffer> chunks) throws InterruptedException {
        Thread[] workers = new Thread[numberOfCores];
        Map<Integer, Map<Station, MeasurementAggregator>> results = new HashMap<>(numberOfCores);
        for (int i = 0; i < numberOfCores; i++) {
            Map<Station, MeasurementAggregator> workerResults = new HashMap<>(MAX_STATIONS);
            results.put(Integer.valueOf(i), workerResults);

            workers[i] = new Thread(new ChunkWorker(chunks.get(i), workerResults));
            workers[i].start();
        }

        for (int i = 0; i < numberOfCores; i++) {
            workers[i].join();
        }
        return results;
    }

    private static Map<Station, MeasurementAggregator> groupResults(Map<Integer, Map<Station, MeasurementAggregator>> results) {
        Map<Station, MeasurementAggregator> accumulatedMeassures = new HashMap<>(MAX_STATIONS);
        for (Map<Station, MeasurementAggregator> meassures : results.values()) {
            for (Map.Entry<Station, MeasurementAggregator> entry : meassures.entrySet()) {
                accumulatedMeassures.merge(entry.getKey(), entry.getValue(), MeasurementAggregator::merge);
            }
        }
        return accumulatedMeassures;
    }

    private static Map<String, ResultRow> getSortedResults(Map<Station, MeasurementAggregator> accumulatedMeassures) {
        Map<String, ResultRow> sortedResults = new TreeMap<>();
        for (Map.Entry<Station, MeasurementAggregator> entry : accumulatedMeassures.entrySet()) {
            sortedResults.put(entry.getKey().toString(), entry.getValue().toResultRow());
        }
        return sortedResults;
    }

    /**
     * Station class to be used as key in the Maps. It stores the name of the station
     * using an array of bytes, instead of converting the bytes to a String.
     * <p>
     * This class implements a lazy cached hashCode which is used in the equals Method.
     */
    private static final class Station {
        final byte[] buffer;
        int hashCode = 0;

        private Station(byte[] buffer) {
            this.buffer = buffer;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            Station station = (Station) o;
            return this.hashCode() == station.hashCode();
        }

        @Override
        public int hashCode() {
            if (this.hashCode == 0) {
                this.hashCode = Arrays.hashCode(this.buffer);
            }
            return this.hashCode;
        }

        @Override
        public String toString() {
            return new String(this.buffer);
        }
    }

    /**
     * Measurement class to represent each line of the file.
     * <p>
     * The value of the measurement will be a non-null double between -99.9 (inclusive) and 99.9 (inclusive),
     * always with one fractional digit.
     * <p>
     * To improve the performance of the operations, the station measures will be processed as int values multiplied by 10.
     * For example:
     * <ul>
     *     <li>14.5: 145</li>
     *     <li>-0.3: -3</li>
     *     <li>-27.8: -278</li>
     * </ul>
     *
     * @param station The station name.
     * @param value The measurement of the station as int.
     */
    private record Measurement(Station station, int value) {
        // static array with the immediate results of 10EX powers.
        private static final int[] POW_10X = new int[] {
            1,   // 10E0
            10,  // 10E1
            100, // 10E2
            1000 // 10E3
        };

        /**
         * This method will be called from the ChunkWorker every line is processed.
         * <p>
         * Instead of creating a new array of bytes and copying every byte, the whole byte buffer is passed
         * with the calculated line length.
         *
         * @param line Array of bytes representing a line in the file. It could be up to 255 bytes.
         * @param length The calculated line length, it should always be less than 255 bytes.
         * @return The Measurement class representing a station name and the measured temperature.
         */
        public static Measurement from(final byte[] line, final int length) {
            // find the position of the character ; in the array of bytes
            int separatorPosition = 0;
            while (line[separatorPosition] != ';') {
                separatorPosition++;
            }
            // create an array of bytes with the length of the station name
            byte[] stationBuffer = new byte[separatorPosition];
            System.arraycopy(line, 0, stationBuffer, 0, separatorPosition);
            // Create the Station object by copying the bytes from the line to the stationBuffer
            Station station = new Station(stationBuffer);

            // create an array of bytes with the size of the measured temperature
            // it can be 2, 3 or 4 bytes to store the number multiplied by 10 and the negative sign.
            byte[] value = new byte[length - separatorPosition - 2];
            for (int index = 0, i = separatorPosition + 1; i < length; i++) {
                final byte character = line[i];
                if (character != '.') { // We skip the . character to "multiply by 10"
                    value[index++] = character;
                }
            }

            // Instead of converting this array of bytes to String and the call the Integer.parseInt method
            int intValue = fastAsciiBytesToInt(value);
            return new Measurement(station, intValue);
        }

        /**
         * Function to convert an array of ascii bytes, with an optional negative sign, to an int number.
         * <p>
         * Algorithm explanation:
         * - Input: the number 210 will be passed as [50, 49, 48]
         * - We will subtract 48 from every byte, because 48 is where the character '0' starts in the ASCII table.
         * - By doing this, the number 210 will be converted from [50, 49, 48] to [2, 1, 0].
         * - Then, using the index of the array, we will multiply every number with a power of 10.
         * - The digit 2 will be multiplied by 100, the digit 1 will be multiplied by 10 and the digit 0 will be multiplied by 1.
         * - The result will be: (2*100) + (1*10) + (0*1) = 210.
         * <p>
         * If the buffer starts with the character '-', at the end of the function, the result will be negated.
         * <p>
         * TODO: I think this algorithm could be improved by avoiding integer multiplication using some kind of bit shift
         * operations.
         *
         * @param buffer Array of bytes representing the number.
         * @return The int representation of the array of bytes.
         */
        public static int fastAsciiBytesToInt(byte[] buffer) {
            boolean isNegative = buffer[0] == '-';
            int startPosition = isNegative ? 1 : 0;

            int result = 0;
            for (int i = startPosition; i < buffer.length; i++) {
                result += (buffer[i] - 48) * (POW_10X[buffer.length - i - 1]);
            }

            return isNegative ? -result: result;
        }
    }

    private record ResultRow(double min, double mean, double max) {

        public String toString() {
            return round(min) + "/" + round(mean) + "/" + round(max);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    };

    private static class MeasurementAggregator {
        private long min;
        private long max;
        private long sum;
        private long count;

        private MeasurementAggregator() {
        }

        private MeasurementAggregator(int value) {
            this.min = value;
            this.max = value;
            this.sum = value;
            this.count = 1;
        }

        static MeasurementAggregator merge(MeasurementAggregator agg1, MeasurementAggregator agg2) {
            MeasurementAggregator result = new MeasurementAggregator();
            result.min = Math.min(agg1.min, agg2.min);
            result.max = Math.max(agg1.max, agg2.max);
            result.sum = agg1.sum + agg2.sum;
            result.count = agg1.count + agg2.count;
            return result;
        }

        ResultRow toResultRow() {
            final double min = this.min / 10.0;
            final double max = this.max / 10.0;
            final double sum = this.sum / 10.0;
            return new ResultRow(min, (Math.round(sum * 10.0) / 10.0) / this.count, max);
        }
    }

    private static final class ChunkWorker implements Runnable {
        private final MappedByteBuffer mappedByteBuffer;
        private final Map<Station, MeasurementAggregator> results;

        private ChunkWorker(MappedByteBuffer mappedByteBuffer, Map<Station, MeasurementAggregator> results) {
            this.mappedByteBuffer = mappedByteBuffer;
            this.results = results;
        }

        @Override
        public void run() {
            byte[] buffer = new byte[255];
            while (mappedByteBuffer.position() < mappedByteBuffer.limit()) {
                int bufferLength = 0;

                byte byteRead = mappedByteBuffer.get();
                while (byteRead != '\n') {
                    buffer[bufferLength++] = byteRead;
                    byteRead = mappedByteBuffer.get();
                }

                final Measurement measurement = Measurement.from(buffer, bufferLength);

                results.merge(measurement.station, new MeasurementAggregator(measurement.value), MeasurementAggregator::merge);
            }
        }
    }

}
