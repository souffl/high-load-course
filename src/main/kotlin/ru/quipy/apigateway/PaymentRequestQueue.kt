package ru.quipy.apigateway

import jakarta.annotation.PreDestroy
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.payments.logic.PaymentExternalSystemAdapter
import java.util.ArrayDeque
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit

@Component
class PaymentRequestQueue(
    paymentAccounts: List<PaymentExternalSystemAdapter>,
    @param:Value("\${payment.deadline-safety-margin-millis:200}")
    private val safetyMarginMillis: Long,
    @param:Value("\${payment.queue.retry-after-millis:1000}")
    private val retryAfterMillis: Long,
    @param:Value("\${payment.queue.target-rate-per-second:3}")
    private val configuredTargetRatePerSecond: Int,
    @param:Value("\${payment.queue.max-pending:0}")
    private val configuredMaxPending: Int,
) {
    init {
        require(paymentAccounts.isNotEmpty()) { "At least one payment account is required." }
        require(
            safetyMarginMillis >= 0 && retryAfterMillis > 0 &&
                configuredTargetRatePerSecond >= 0 && configuredMaxPending >= 0
        )
        require(paymentAccounts.all { it.rateLimitPerSecond() > 0 && it.parallelism() > 0 })
    }

    private val rateLimitPerSecond = paymentAccounts.minOf { it.rateLimitPerSecond() }
    private val dispatchRatePerSecond = configuredTargetRatePerSecond
        .takeIf { it > 0 }
        ?.let { minOf(it, rateLimitPerSecond) }
        ?: rateLimitPerSecond
    private val workerCount = paymentAccounts.minOf { it.parallelism() }
    // Deadline admission is the normal bound. This limit is only a memory fuse for
    // malformed traffic; it must not be sized from account parallelism.
    private val maxPending = configuredMaxPending.takeIf { it > 0 } ?: DEFAULT_MAX_PENDING
    private val dispatchIntervalNanos =
        (TimeUnit.SECONDS.toNanos(1) + dispatchRatePerSecond - 1) / dispatchRatePerSecond

    private val lock = Any()
    private val slots = List(workerCount) { Slot() }
    // FIFO is a reservation queue: a later retry cannot move ahead of a payment
    // that was already admitted and invalidate its calculated start time.
    private val waiting = ArrayDeque<PendingPayment>()
    private val executor = Executors.newFixedThreadPool(workerCount, NamedThreadFactory("payment-request-executor"))
    private val dispatcher = Executors.newSingleThreadScheduledExecutor(NamedThreadFactory("payment-request-dispatcher"))
    private var scheduledDispatch: ScheduledFuture<*>? = null
    private var nextDispatchAtNanos = System.nanoTime()
    private var accepting = true

    init {
        LoggerFactory.getLogger(PaymentRequestQueue::class.java).info(
            "Payment scheduler: workers={}, accountRateLimitPerSecond={}, dispatchRatePerSecond={}, maxPending={}, safetyMarginMillis={}, retryAfterMinMillis={}",
            workerCount, rateLimitPerSecond, dispatchRatePerSecond, maxPending, safetyMarginMillis, retryAfterMillis
        )
    }

    fun submit(deadline: Long, action: (Long) -> Unit): Admission = synchronized(lock) {
        val receivedAt = System.currentTimeMillis()
        if (!accepting || waiting.size >= maxPending || !canStartBeforeDeadline(receivedAt, deadline)) {
            return@synchronized Admission.Rejected(nextRetryAt(receivedAt))
        }

        waiting.addLast(PendingPayment(receivedAt, deadline, action))
        scheduleDispatch()
        Admission.Accepted(receivedAt)
    }

    /**
     * Admission reserves a FIFO dispatch position. Provider execution time is
     * deliberately not guessed here; the actual HTTP call keeps the absolute
     * client deadline as its timeout.
     */
    private fun canStartBeforeDeadline(nowMillis: Long, deadline: Long): Boolean {
        val nowNanos = System.nanoTime()
        val scheduledStartNanos = maxOf(nowNanos, nextDispatchAtNanos) +
            waiting.size.toLong() * dispatchIntervalNanos
        val waitMillis = TimeUnit.NANOSECONDS.toMillis(scheduledStartNanos - nowNanos)
        return waitMillis + safetyMarginMillis < deadline - nowMillis
    }

    private fun nextRetryAt(nowMillis: Long): Long {
        val nowNanos = System.nanoTime()
        val dispatchDelayMillis = TimeUnit.NANOSECONDS.toMillis(maxOf(0L, nextDispatchAtNanos - nowNanos))
        // A retry gets a fresh deadline. Wait until the current bounded backlog has
        // leaked through the paced dispatcher, otherwise the same burst returns at once.
        val backlogDelayMillis = TimeUnit.NANOSECONDS.toMillis(waiting.size.toLong() * dispatchIntervalNanos)
        return nowMillis + maxOf(retryAfterMillis, dispatchDelayMillis, backlogDelayMillis)
    }

    private fun scheduleDispatch() {
        if (scheduledDispatch != null || waiting.isEmpty() || slots.none { it.startedAtNanos == null }) return
        val delayNanos = maxOf(0L, nextDispatchAtNanos - System.nanoTime())
        scheduledDispatch = dispatcher.schedule({
            synchronized(lock) {
                scheduledDispatch = null
                dispatchNext()
            }
        }, delayNanos, TimeUnit.NANOSECONDS)
    }

    private fun dispatchNext() {
        val slot = slots.firstOrNull { it.startedAtNanos == null } ?: return
        val payment = waiting.pollFirst() ?: return
        val nowNanos = System.nanoTime()
        if (nowNanos < nextDispatchAtNanos) {
            waiting.addFirst(payment)
            scheduleDispatch()
            return
        }

        slot.startedAtNanos = nowNanos
        nextDispatchAtNanos = nowNanos + dispatchIntervalNanos
        executor.execute {
            try {
                payment.action(payment.receivedAt)
            } finally {
                synchronized(lock) {
                    slot.startedAtNanos = null
                    scheduleDispatch()
                    shutdownIfDrained()
                }
            }
        }
        scheduleDispatch()
    }

    @PreDestroy
    fun shutdown() = synchronized(lock) {
        accepting = false
        shutdownIfDrained()
    }

    private fun shutdownIfDrained() {
        if (!accepting && waiting.isEmpty() && slots.all { it.startedAtNanos == null }) {
            dispatcher.shutdown()
            executor.shutdown()
        }
    }

    sealed interface Admission {
        data class Accepted(val receivedAt: Long) : Admission
        data class Rejected(val retryAt: Long) : Admission
    }

    private class Slot(var startedAtNanos: Long? = null)

    private data class PendingPayment(
        val receivedAt: Long,
        val deadline: Long,
        val action: (Long) -> Unit,
    )

    private companion object {
        const val DEFAULT_MAX_PENDING = 10_000
    }
}
