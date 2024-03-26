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

import java.io.BufferedReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.nio.file.Files.newBufferedReader;

public class CalculateAverage_tanisperez {
    private static final String FILE = "./measurements.txt";

    private record Measurement(String station, int value) {
        public static Measurement from(final String line) {
            int separatorPosition = 0;
            while (line.charAt(separatorPosition) != ';') {
                separatorPosition++;
            }
            String station = line.substring(0, separatorPosition);

            char[] value = new char[line.length() - separatorPosition - 2];
            for (int index = 0, i = separatorPosition + 1; i < line.length(); i++) {
                final char character = line.charAt(i);
                if (character != '.') {
                    value[index++] = line.charAt(i);
                }
            }

            final String measure = new String(value);
            return new Measurement(station, Integer.parseInt(measure));
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

        public MeasurementAggregator(int sum) {
            this.min = sum;
            this.max = sum;
            this.sum = sum;
            this.count = 1;
        }

        MeasurementAggregator merge(MeasurementAggregator other) {
            this.max = Math.max(other.max, this.max);
            this.min = Math.min(other.min, this.min);
            this.sum += other.sum;
            this.count += other.count;
            return this;
        }
    }

    private static final class Producer implements Runnable {
        private int numberOfCores;
        private AtomicBoolean eof;
        private Map<Integer, Queue<String>> queues;

        private Producer(int numberOfCores, AtomicBoolean eof, Map<Integer, Queue<String>> queues) {
            this.numberOfCores = numberOfCores;
            this.eof = eof;
            this.queues = queues;
        }

        @Override
        public void run() {
            try (BufferedReader reader = newBufferedReader(Paths.get(FILE), StandardCharsets.UTF_8)) {
                for (;;) {
                    String line = reader.readLine();
                    if (line == null) {
                        eof.getAndSet(true);
                        break;
                    }
                    final char firstLetter = line.charAt(0);
                    final Integer cpuCoreAssignation = getStationCoreAssignation(firstLetter);

                    queues.get(cpuCoreAssignation).add(line);
                }
            }
            catch (final Exception exception) {
                exception.printStackTrace();
            }
        }

        private Integer getStationCoreAssignation(char firstLetter) {
            // Improve with a custom hash with bit shiffting by the numberOfCores
            return Integer.valueOf(Math.abs(Character.hashCode(firstLetter) % numberOfCores));
        }
    }

    private static final class Consumer implements Runnable {
        private AtomicBoolean eof;
        private Queue<String> queue;
        private Map<String, MeasurementAggregator> results;

        private Consumer(AtomicBoolean eof, Queue<String> queue, Map<String, MeasurementAggregator> results) {
            this.eof = eof;
            this.queue = queue;
            this.results = results;
        }

        @Override
        public void run() {
            while (!queue.isEmpty() || !eof.get()) {
                final String line = queue.poll();

                if (line != null) {
                    final Measurement measurement = Measurement.from(line);
                    MeasurementAggregator measurementAggregator = new MeasurementAggregator(measurement.value);
                    results.merge(measurement.station, measurementAggregator, MeasurementAggregator::merge);
                }
            }
        }
    }

    private static Map<Integer, Queue<String>> createQueues(int numberOfCores) {
        Map<Integer, Queue<String>> queues = new HashMap<>();
        for (int i = 0; i < numberOfCores; i++) {
            queues.put(i, new LinkedTransferQueue<>());
        }
        return queues;
    }

    private static Map<String, ResultRow> getSortedResults(Map<String, MeasurementAggregator> results) {
        final Map<String, ResultRow> sortedResults = new TreeMap<>();
        for (final Map.Entry<String, MeasurementAggregator> entry : results.entrySet()) {
            final double min = entry.getValue().min / 10.0;
            final double max = entry.getValue().max / 10.0;
            final double sum = entry.getValue().sum / 10.0;
            ResultRow resultRow = new ResultRow(min, (Math.round(sum * 10.0) / 10.0) / entry.getValue().count, max);

            sortedResults.put(entry.getKey(), resultRow);
        }
        return sortedResults;
    }

    public static void main(String[] args) throws Exception {
        // My MacBook Pro has 4 physical cores and 4 logical cores. With 8 threads the performance is worse...
        final int numberOfCores = Runtime.getRuntime().availableProcessors() / 2;

        final AtomicBoolean eof = new AtomicBoolean(false);

        Map<Integer, Queue<String>> queues = createQueues(numberOfCores);
        Map<String, MeasurementAggregator> results = new ConcurrentHashMap<>(10_000);

        Thread producer = new Thread(new Producer(numberOfCores, eof, queues));
        producer.start();

        Thread[] consumers = new Thread[numberOfCores];
        for (int i = 0; i < numberOfCores; i++) {
            consumers[i] = new Thread(new Consumer(eof, queues.get(Integer.valueOf(i)), results));
            consumers[i].start();
        }

        producer.join();
        for (int i = 0; i < numberOfCores; i++) {
            consumers[i].join();
        }

        final Map<String, ResultRow> sortedResults = getSortedResults(results);
        System.out.println(sortedResults);
    }

}
