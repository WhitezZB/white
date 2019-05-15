package org.star.white.simd;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.Random;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;

/**
 * Created by acetylzhang@tencent.com on 2019-04-19.
 */
@State(Scope.Thread)
@OutputTimeUnit(NANOSECONDS)
@BenchmarkMode(AverageTime)
@Fork(value = 1, jvmArgsAppend = {
        "-XX:+UseSuperWord",
        "-XX:+UnlockDiagnosticVMOptions",
        "-XX:CompileCommand=print,*BenchmarkSIMDBlog.array1"})
@Warmup(iterations = 5)
@Measurement(iterations = 10)
public class BenchmarkSIMDBlog {
    public static final int SIZE = 1024;

    @State(Scope.Thread)
    public static class Context {
        public final int[] values = new int[SIZE];
        public final int[] results = new int[SIZE];

        public final long[] longValues = new long[SIZE/2];
        public final long[] longResults = new long[SIZE/2];


        @Setup
        public void setup() {
            Random random = new Random();
            for (int i = 0; i < SIZE; i++) {
                values[i] = random.nextInt(Integer.MAX_VALUE / 32);
            }

            for (int i = 0; i < SIZE / 2; i++) {
                longValues[i] = random.nextLong();
            }
        }
    }

    @Benchmark
    public int[] increment(Context context) {
        for (int i = 0; i < SIZE; i++) {
            context.results[i] = context.values[i] + 1;
        }
        return context.results;
    }

    @Benchmark
    public long[] incrementLong(Context context) {
        for (int i = 0; i < SIZE/2; i++) {
            context.longResults[i] = context.longValues[i] + 1;
        }
        return context.longResults;
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(BenchmarkSIMDBlog.class.getSimpleName())
                .build();

        new Runner(opt).run();
    }
}