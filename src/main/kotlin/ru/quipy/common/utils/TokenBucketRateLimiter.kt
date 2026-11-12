package ru.quipy.common.utils

import java.util.concurrent.TimeUnit

class TokenBucketRateLimiter(
    private val ratePerSecond: Int,
    private val capacity: Int = ratePerSecond,
) : RateLimiter {
    init {
        require(ratePerSecond > 0 && capacity > 0)
    }

    private val lock = Any()
    private var tokens = capacity.toDouble()
    private var lastRefillAtNanos = System.nanoTime()

    override fun tick(): Boolean = synchronized(lock) {
        refill(System.nanoTime())
        if (tokens < 1.0) return@synchronized false
        tokens -= 1.0
        true
    }

    @Throws(InterruptedException::class)
    fun acquire() {
        while (true) {
            val waitNanos = synchronized(lock) {
                refill(System.nanoTime())
                if (tokens >= 1.0) {
                    tokens -= 1.0
                    return
                }
                maxOf(
                    1L,
                    ((1.0 - tokens) * TimeUnit.SECONDS.toNanos(1) / ratePerSecond).toLong(),
                )
            }
            TimeUnit.NANOSECONDS.sleep(waitNanos)
        }
    }

    private fun refill(nowNanos: Long) {
        val elapsedNanos = nowNanos - lastRefillAtNanos
        if (elapsedNanos <= 0) return
        tokens = minOf(
            capacity.toDouble(),
            tokens + elapsedNanos.toDouble() * ratePerSecond / TimeUnit.SECONDS.toNanos(1),
        )
        lastRefillAtNanos = nowNanos
    }
}
