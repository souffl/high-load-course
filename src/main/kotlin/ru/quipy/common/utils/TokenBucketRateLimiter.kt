package ru.quipy.common.utils

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import kotlin.math.ceil

class TokenBucketRateLimiter(
    private val rate: Int,
    private val bucketMaxCapacity: Int,
    window: Long,
    timeUnit: TimeUnit = TimeUnit.MINUTES,
    private val nanoTime: () -> Long = System::nanoTime,
) : RateLimiter {
    private val windowNanos = timeUnit.toNanos(window)

    init {
        require(rate > 0 && bucketMaxCapacity > 0 && window > 0 && windowNanos > 0)
    }

    private val bucket = AtomicReference(BucketState(bucketMaxCapacity.toDouble(), nanoTime()))

    override fun tick(): Boolean {
        while (true) {
            val state = bucket.get()
            val now = nanoTime()
            val tokens = availableTokens(state, now)
            if (tokens < 1.0) return false
            if (bucket.compareAndSet(state, BucketState(tokens - 1.0, now))) return true
        }
    }

    /** Estimates token availability assuming queued requests consume tokens as they arrive. */
    fun estimateDelayMillis(permitsAhead: Int): Long {
        require(permitsAhead > 0)
        val state = bucket.get()
        val missingTokens = (permitsAhead - availableTokens(state, nanoTime())).coerceAtLeast(0.0)
        // Round up so a positive fractional millisecond never becomes a zero wait.
        return ceil(missingTokens * windowNanos / rate / TimeUnit.MILLISECONDS.toNanos(1)).toLong()
    }

    private fun availableTokens(state: BucketState, now: Long): Double {
        val elapsedNanos = (now - state.refilledAtNanos).coerceAtLeast(0L)
        return minOf(
            bucketMaxCapacity.toDouble(),
            state.tokens + elapsedNanos.toDouble() * rate / windowNanos,
        )
    }

    private data class BucketState(val tokens: Double, val refilledAtNanos: Long)
}
