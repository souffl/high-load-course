package ru.quipy.apigateway

import io.github.resilience4j.ratelimiter.RateLimiter
import io.github.resilience4j.ratelimiter.RateLimiterConfig
import jakarta.annotation.PreDestroy
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.payments.logic.PaymentExternalSystemAdapter
import java.time.Duration
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

@Component
class PaymentRequestQueue(
    paymentAccounts: List<PaymentExternalSystemAdapter>,
    @param:Value("\${payment.queue.processing-budget-millis:2300}")
    private val processingBudgetMillis: Long,
    @param:Value("\${payment.deadline-safety-margin-millis:200}")
    private val safetyMarginMillis: Long,
    @param:Value("\${payment.queue.max-wait-millis:500}")
    private val maxWaitMillis: Long,
) {
    init {
        require(paymentAccounts.isNotEmpty()) { "At least one payment account is required." }
        require(processingBudgetMillis > 0 && safetyMarginMillis >= 0 && maxWaitMillis > 0)
        require(paymentAccounts.all { it.rateLimitPerSecond() > 0 && it.parallelism() > 0 })
    }

    private val rateLimitPerSecond = paymentAccounts.minOf { it.rateLimitPerSecond() }
    private val workerCount = paymentAccounts.minOf { it.parallelism() }
    // This queue holds requests waiting for an ingress rate permit. Its capacity must
    // match the account start rate, not the processing budget used by the deadline.
    private val queueCapacity = maxOf(
        1L, rateLimitPerSecond.toLong() * maxWaitMillis / 1_000
    ).toInt()
    // Reserve spaced starts using the ready-made limiter, including short bursts within the wait budget.
    private val admissionLimiter = RateLimiter.of(
        "payment-admission",
        RateLimiterConfig.custom()
            .limitForPeriod(1)
            .limitRefreshPeriod(Duration.ofNanos((TimeUnit.SECONDS.toNanos(1) + rateLimitPerSecond - 1) / rateLimitPerSecond))
            .timeoutDuration(Duration.ofMillis(maxWaitMillis))
            .build()
    )
    private val executor = ThreadPoolExecutor(
        workerCount, workerCount, 0L, TimeUnit.MILLISECONDS,
        ArrayBlockingQueue<Runnable>(queueCapacity),
        NamedThreadFactory("payment-request-executor"),
        ThreadPoolExecutor.AbortPolicy(),
    )

    init {
        LoggerFactory.getLogger(PaymentRequestQueue::class.java).info(
            "Payment executor: workers={}, queueCapacity={}, rateLimitPerSecond={}, processingBudgetMillis={}, maxWaitMillis={}",
            workerCount, queueCapacity, rateLimitPerSecond, processingBudgetMillis, maxWaitMillis
        )
    }

    fun submit(deadline: Long, action: (Long) -> Unit): Long? = synchronized(executor) {
        val receivedAt = System.currentTimeMillis()
        if (executor.isShutdown) return@synchronized null
        if (executor.poolSize >= workerCount && executor.queue.remainingCapacity() == 0) {
            return@synchronized null
        }
        admissionLimiter.changeTimeoutDuration(Duration.ofMillis(maxWaitMillis))
        val permitWaitNanos = admissionLimiter.reservePermission()
        if (permitWaitNanos < 0) return@synchronized null
        val startAtNanos = System.nanoTime() + permitWaitNanos

        try {
            executor.execute {
                val delayNanos = startAtNanos - System.nanoTime()
                if (delayNanos > 0) TimeUnit.NANOSECONDS.sleep(delayNanos)
                action(receivedAt)
            }
        } catch (_: RejectedExecutionException) {
            return@synchronized null
        }
        receivedAt
    }

    @PreDestroy
    fun shutdown() {
        executor.shutdown()
        try {
            if (!executor.awaitTermination(30, TimeUnit.SECONDS)) executor.shutdownNow()
        } catch (_: InterruptedException) {
            executor.shutdownNow()
            Thread.currentThread().interrupt()
        }
    }
}
