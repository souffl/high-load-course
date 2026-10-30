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

    private val averageProcessingMillis = paymentAccounts.sumOf { it.averageProcessingTime().toMillis() }
    private val rateLimitPerSecond = paymentAccounts.minOf { it.rateLimitPerSecond() }
    // Size workers using average service time; use the larger processing budget for admission.
    private val workerCount = minOf(
        paymentAccounts.minOf { it.parallelism() },
        maxOf(1L, (rateLimitPerSecond.toLong() * averageProcessingMillis + 999) / 1_000).toInt()
    )
    private val estimatedThroughput = maxOf(
        1L, minOf(rateLimitPerSecond.toLong(), workerCount * 1_000L / processingBudgetMillis)
    )
    private val queueCapacity = maxOf(1L, estimatedThroughput * maxWaitMillis / 1_000).toInt()
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
            "Payment executor: workers={}, queueCapacity={}, processingBudgetMillis={}, maxWaitMillis={}",
            workerCount, queueCapacity, processingBudgetMillis, maxWaitMillis
        )
    }

    fun submit(deadline: Long, action: (Long) -> Unit): Long? = synchronized(executor) {
        val receivedAt = System.currentTimeMillis()
        if (executor.isShutdown || (executor.poolSize >= workerCount && executor.queue.remainingCapacity() == 0)) {
            return@synchronized null
        }
        // Executor statistics give a conservative queue-length estimate, not a deadline guarantee.
        val tasksAhead = executor.queue.size + if (executor.activeCount >= workerCount) 1 else 0
        val estimatedWaitMillis = (tasksAhead * 1_000L + estimatedThroughput - 1) / estimatedThroughput
        if (estimatedWaitMillis > maxWaitMillis ||
            estimatedWaitMillis + processingBudgetMillis + safetyMarginMillis > deadline - receivedAt
        ) return@synchronized null
        val remainingWaitMillis = minOf(maxWaitMillis, deadline - System.currentTimeMillis() - processingBudgetMillis - safetyMarginMillis)
        if (remainingWaitMillis < 0) return@synchronized null
        // All reservations and per-request timeout changes are serialized by this admission lock.
        admissionLimiter.changeTimeoutDuration(Duration.ofMillis(remainingWaitMillis))
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
