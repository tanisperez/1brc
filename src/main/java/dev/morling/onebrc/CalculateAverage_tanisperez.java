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
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static java.nio.file.Files.newBufferedReader;

public class CalculateAverage_tanisperez {

    private static final String FILE = "./measurements.txt";

    private record Measurement(String station, double value) {

        public static Measurement from(final String line) {
            boolean readingValue = false;
            final StringBuilder station = new StringBuilder(line.length());
            final StringBuilder value = new StringBuilder(line.length());
            for (int i = 0; i < line.length(); i++) {
                final char character = line.charAt(i);
                if (character == ';') {
                    readingValue = true;
                    continue;
                }
                if (readingValue) {
                    value.append(character);
                } else {
                    station.append(character);
                }
            }
            return new Measurement(station.toString(), Double.parseDouble(value.toString()));
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
        private double min;
        private double max;
        private double sum;
        private long count;

        public MeasurementAggregator(double sum) {
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
        private Map<Integer, Queue<Measurement>> queues;
        private Map<String, Integer> stationCoreAssignation = new HashMap<>();

        private Producer(int numberOfCores, AtomicBoolean eof, Map<Integer, Queue<Measurement>> queues) {
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
                    final Measurement measurement = Measurement.from(line);
                    final Integer cpuCoreAssignation = getStationCoreAssignation(measurement.station);

                    queues.get(cpuCoreAssignation).add(measurement);
                }
            }
            catch (final Exception exception) {
                exception.printStackTrace();
            }

            System.out.println("Producer finished");
        }

        private Integer getStationCoreAssignation(String station) {
            final Integer cpuAssignation = stationCoreAssignation.get(station);
            if (cpuAssignation != null) {
                return cpuAssignation;
            }

            final Integer index = Integer.valueOf(Math.abs(station.hashCode() % numberOfCores));
            stationCoreAssignation.put(station, index);
            return index;
        }
    }

    private static final class Consumer implements Runnable {
        private AtomicBoolean eof;
        private Queue<Measurement> queue;
        private Map<String, MeasurementAggregator> results;

        private Consumer(AtomicBoolean eof, Queue<Measurement> queue, Map<String, MeasurementAggregator> results) {
            this.eof = eof;
            this.queue = queue;
            this.results = results;
        }

        @Override
        public void run() {
            while (!queue.isEmpty() || !eof.get()) {
                final Measurement measurement = queue.poll();

                if (measurement != null) {
                    MeasurementAggregator measurementAggregator = new MeasurementAggregator(measurement.value);
                    results.merge(measurement.station, measurementAggregator, MeasurementAggregator::merge);
                }
            }
            System.out.println("Consumer " + Thread.currentThread().getName() + " finished");
        }
    }

    private static Map<Integer, Queue<Measurement>> createQueues(int numberOfCores) {
        Map<Integer, Queue<Measurement>> queues = new HashMap<>();
        for (int i = 0; i < numberOfCores; i++) {
            queues.put(i, new LinkedTransferQueue<>());
        }
        return queues;
    }

    public static void main(String[] args) throws Exception {
        final int numberOfCores = Runtime.getRuntime().availableProcessors();

        final AtomicBoolean eof = new AtomicBoolean(false);

        Map<Integer, Queue<Measurement>> queues = createQueues(numberOfCores);
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

        Map<String, ResultRow> accumulatedResults = results.entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey,
                        resultRow -> new ResultRow(
                                resultRow.getValue().min,
                                (Math.round(resultRow.getValue().sum * 10.0) / 10.0) / resultRow.getValue().count,
                                resultRow.getValue().max)));

        TreeMap<String, ResultRow> sortedResults = new TreeMap<>();
        sortedResults.putAll(accumulatedResults);

        System.out.println(sortedResults);
    }

}
