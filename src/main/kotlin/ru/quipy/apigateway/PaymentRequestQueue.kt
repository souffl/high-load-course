package ru.quipy.apigateway

import jakarta.annotation.PreDestroy
import org.springframework.stereotype.Component
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.payments.logic.PaymentExternalSystemAdapter
import java.util.ArrayDeque
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit

@Component
class PaymentRequestQueue(paymentAccounts: List<PaymentExternalSystemAdapter>) {
    private val averageProcessingMillis = paymentAccounts.sumOf { it.averageProcessingTime().toMillis() }
    // Reserve 25% per processing operation and another 20% before the deadline.
    private val processingBudgetMillis = averageProcessingMillis + averageProcessingMillis / 4
    private val safetyMarginMillis = maxOf(1L, averageProcessingMillis / 5)
    private val rateLimitPerSecond = paymentAccounts.minOf { it.rateLimitPerSecond() }
    private val dispatchIntervalNanos = (TimeUnit.SECONDS.toNanos(1) + rateLimitPerSecond - 1) / rateLimitPerSecond
    // Rate * processing time bounds useful concurrency; do not create 10,000 worker threads.
    private val capacity = minOf(
        paymentAccounts.minOf { it.parallelism() },
        maxOf(1L, (rateLimitPerSecond.toLong() * processingBudgetMillis + 999) / 1_000).toInt()
    )

    private val lock = Any()
    private val slots = List(capacity) { Slot() }
    private val waiting = ArrayDeque<PendingPayment>()
    private val executor = Executors.newFixedThreadPool(capacity, NamedThreadFactory("payment-request-executor"))
    private val dispatcher = Executors.newSingleThreadScheduledExecutor(NamedThreadFactory("payment-request-dispatcher"))
    private var scheduledDispatch: ScheduledFuture<*>? = null
    private var nextDispatchAtNanos = System.nanoTime()
    private var accepting = true

    fun submit(deadline: Long, action: (Long) -> Unit): Long? = synchronized(lock) {
        check(accepting) { "Payment request queue is shutting down." }
        val receivedAt = System.currentTimeMillis()
        if (!canFinishBeforeDeadline(receivedAt, deadline)) return@synchronized null

        val payment = PendingPayment(receivedAt, deadline, action)
        waiting.addLast(payment)
        try {
            scheduleDispatch()
        } catch (e: RuntimeException) {
            waiting.remove(payment)
            throw e
        }
        receivedAt
    }

    private fun canFinishBeforeDeadline(now: Long, deadline: Long): Boolean {
        val nowNanos = System.nanoTime()
        val processingNanos = TimeUnit.MILLISECONDS.toNanos(processingBudgetMillis)
        val availableIn = LongArray(capacity) { index ->
            val startedAt = slots[index].startedAtNanos
            if (startedAt == null) {
                0L
            } else {
                val elapsed = nowNanos - startedAt
                // An overdue request still occupies its slot; do not predict it as already free.
                maxOf(processingNanos - elapsed, TimeUnit.MILLISECONDS.toNanos(averageProcessingMillis))
            }
        }

        var nextStartIn = maxOf(0L, nextDispatchAtNanos - nowNanos)
        for (payment in waiting) {
            val slot = availableIn.indices.minBy { availableIn[it] }
            val startIn = maxOf(availableIn[slot], nextStartIn)
            availableIn[slot] = startIn + processingNanos
            nextStartIn = startIn + dispatchIntervalNanos
            if (TimeUnit.NANOSECONDS.toMillis(availableIn[slot]) + safetyMarginMillis > payment.deadline - now) return false
        }

        val finishIn = maxOf(availableIn.min(), nextStartIn) + processingNanos
        return TimeUnit.NANOSECONDS.toMillis(finishIn) + safetyMarginMillis <= deadline - now
    }

    // Called under lock: both admission and execution use FIFO order and the same sending interval.
    private fun scheduleDispatch() {
        if (scheduledDispatch != null || waiting.isEmpty() || slots.none { it.startedAtNanos == null }) return
        val delay = maxOf(0L, nextDispatchAtNanos - System.nanoTime())
        scheduledDispatch = dispatcher.schedule({
            synchronized(lock) {
                scheduledDispatch = null
                dispatchNext()
            }
        }, delay, TimeUnit.NANOSECONDS)
    }

    private fun dispatchNext() {
        val slot = slots.firstOrNull { it.startedAtNanos == null } ?: return
        if (waiting.isEmpty()) return
        val nowNanos = System.nanoTime()
        if (nowNanos < nextDispatchAtNanos) {
            scheduleDispatch()
            return
        }

        val payment = waiting.removeFirst()
        slot.startedAtNanos = nowNanos
        nextDispatchAtNanos = nowNanos + dispatchIntervalNanos
        try {
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
        } catch (e: RuntimeException) {
            slot.startedAtNanos = null
            waiting.addFirst(payment)
            throw e
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

    private class Slot(var startedAtNanos: Long? = null)

    private class PendingPayment(
        val receivedAt: Long,
        val deadline: Long,
        val action: (Long) -> Unit
    )
}
