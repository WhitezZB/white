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
        "-XX:CompileCommand=print,*BenchmarkSIMDLongBlog.bitshift"})
@Warmup(iterations = 5)
@Measurement(iterations = 10)
public class BenchmarkSIMDLongBlog
{
    public static final int SIZE = 1024;

    @State(Scope.Thread)
    public static class Context
    {
        public final long[] values = new long[SIZE];
        public final long[] temporary = new long[SIZE];
        public final int[] results = new int[SIZE];

        @Setup
        public void setup()
        {
            Random random = new Random();
            for (int i = 0; i < SIZE; i++) {
                values[i] = random.nextLong() % (Long.MAX_VALUE / 32L);
            }
        }
    }

    @Benchmark
    @CompilerControl(CompilerControl.Mode.DONT_INLINE) //makes looking at assembly easier
    public void increment(Context context)
    {
        for (int i = 0; i < SIZE; i++) {
            context.temporary[i] = context.values[i] + 1;
        }
    }

    @Benchmark
    @CompilerControl(CompilerControl.Mode.DONT_INLINE) //makes looking at assembly easier
    public void bitshift(Context context)
    {
        for (int i = 0; i < SIZE; i++) {
            context.temporary[i] = context.values[i] / 2;
        }
    }

    @Benchmark
    @CompilerControl(CompilerControl.Mode.DONT_INLINE) //makes looking at assembly easier
    public int[] hashLongLoop(Context context)
    {
        for (int i = 0; i < SIZE; i++) {
            context.results[i] = getHashPosition(context.values[i], 1048575);
        }

        return context.results;
    }

    @Benchmark
    @CompilerControl(CompilerControl.Mode.DONT_INLINE) //makes looking at assembly easier
    public int[] hashLongLoopSplit(Context context)
    {
        for (int i = 0; i < SIZE; i++) {
            context.temporary[i] = getHashPositionMangle(context.values[i]);
        }
        for (int i = 0; i < SIZE; i++) {
            context.temporary[i] = getHashPositionMul1(context.values[i]);
        }
        for (int i = 0; i < SIZE; i++) {
            context.temporary[i] = getHashPositionMangle(context.values[i]);
        }
        for (int i = 0; i < SIZE; i++) {
            context.temporary[i] = getHashPositionMul2(context.values[i]);
        }
        for (int i = 0; i < SIZE; i++) {
            context.temporary[i] = getHashPositionMangle(context.values[i]);
        }
        for (int i = 0; i < SIZE; i++) {
            context.results[i] = getHashPositionCast(context.values[i]);
        }
        for (int i = 0; i < SIZE; i++) {
            context.results[i] = getHashPositionMask(context.results[i], 1048575);
        }

        return context.results;
    }

    private static int getHashPosition(long rawHash, int mask)
    {
        rawHash ^= rawHash >>> 33;
        rawHash *= 0xff51afd7ed558ccdL;
        rawHash ^= rawHash >>> 33;
        rawHash *= 0xc4ceb9fe1a85ec53L;
        rawHash ^= rawHash >>> 33;

        return (int) (rawHash & mask);
    }

    private static long getHashPositionMangle(long rawHash)
    {
        return rawHash ^ (rawHash >>> 33);
    }

    private static long getHashPositionMul1(long rawHash)
    {
        return rawHash * 0xff51afd7ed558ccdL;
    }

    private static long getHashPositionMul2(long rawHash)
    {
        return rawHash * 0xc4ceb9fe1a85ec53L;
    }

    private static int getHashPositionCast(long rawHash)
    {
        return (int) rawHash;
    }

    private static int getHashPositionMask(int rawHash, int mask)
    {
        return rawHash & mask;
    }

    @Benchmark
    @CompilerControl(CompilerControl.Mode.DONT_INLINE) //makes looking at assembly easier
    public long[] hashLoopTwoInstructions(Context context)
    {
        for (int i = 0; i < SIZE; i++) {
            context.temporary[i] = getHashPositionTwoInstructions(context.values[i]);
        }
        return context.temporary;
    }

    private static long getHashPositionTwoInstructions(long rawHash)
    {
        rawHash ^= rawHash >>> 33;
        return rawHash * 0xff51afd7ed558ccdL;
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(BenchmarkSIMDLongBlog.class.getSimpleName())
                .build();

        new Runner(opt).run();
    }
}
