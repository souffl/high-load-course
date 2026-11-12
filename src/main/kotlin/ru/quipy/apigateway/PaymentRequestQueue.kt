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
    @param:Value("\${payment.queue.retry-after-millis:1000}")
    private val retryAfterMillis: Long,
    @param:Value("\${payment.queue.target-rate-per-second:0}")
    private val configuredTargetRatePerSecond: Int,
    @param:Value("\${payment.queue.burst-capacity:0}")
    private val configuredBurstCapacity: Int,
    @param:Value("\${payment.queue.max-pending:0}")
    private val configuredMaxPending: Int,
) {
    init {
        require(paymentAccounts.isNotEmpty()) { "At least one payment account is required." }
        require(
            retryAfterMillis > 0 &&
                configuredTargetRatePerSecond >= 0 && configuredBurstCapacity >= 0 &&
                configuredMaxPending >= 0
        )
        require(paymentAccounts.all { it.rateLimitPerSecond() > 0 && it.parallelism() > 0 })
    }

    private val rateLimitPerSecond = paymentAccounts.minOf { it.rateLimitPerSecond() }
    private val dispatchRatePerSecond = configuredTargetRatePerSecond
        .takeIf { it > 0 }
        ?.let { minOf(it, rateLimitPerSecond) }
        ?: rateLimitPerSecond
    private val workerCount = paymentAccounts.minOf { it.parallelism() }
    private val logger = LoggerFactory.getLogger(PaymentRequestQueue::class.java)
    private val tokenCapacity = configuredBurstCapacity.takeIf { it > 0 }
        ?.let { minOf(it, dispatchRatePerSecond) }
        ?: dispatchRatePerSecond
    private val maxPending = configuredMaxPending.takeIf { it > 0 } ?: DEFAULT_MAX_PENDING

    private val lock = Any()
    private val slots = List(workerCount) { Slot() }
    private val waiting = ArrayDeque<PendingPayment>()
    private val executor = Executors.newFixedThreadPool(workerCount, NamedThreadFactory("payment-request-executor"))
    private val dispatcher = Executors.newSingleThreadScheduledExecutor(NamedThreadFactory("payment-request-dispatcher"))
    private var scheduledDispatch: ScheduledFuture<*>? = null
    private var tokens = tokenCapacity.toDouble()
    private var lastTokenRefillAtNanos = System.nanoTime()
    private var accepting = true

    init {
        LoggerFactory.getLogger(PaymentRequestQueue::class.java).info(
            "Payment scheduler: workers={}, accountRateLimitPerSecond={}, dispatchRatePerSecond={}, tokenCapacity={}, maxPending={}, retryAfterMinMillis={}",
            workerCount, rateLimitPerSecond, dispatchRatePerSecond, tokenCapacity, maxPending, retryAfterMillis
        )
    }

    fun submit(deadline: Long, action: (Long) -> Unit): Admission = synchronized(lock) {
        val receivedAt = System.currentTimeMillis()
        if (!accepting || waiting.size >= maxPending) {
            logger.info(
                "Payment admission rejected: waiting={}, busyWorkers={}, accepting={}",
                waiting.size, slots.count { it.startedAtNanos != null }, accepting,
            )
            return@synchronized Admission.Rejected(receivedAt + retryAfterMillis)
        }

        waiting.addLast(PendingPayment(receivedAt, action))
        scheduleDispatch()
        Admission.Accepted(receivedAt)
    }

    private fun scheduleDispatch() {
        if (scheduledDispatch != null || waiting.isEmpty() || slots.none { it.startedAtNanos == null }) return
        refillTokens(System.nanoTime())
        val delayNanos = nanosUntilNextToken()
        scheduledDispatch = dispatcher.schedule({
            synchronized(lock) {
                scheduledDispatch = null
                dispatchNext()
            }
        }, delayNanos, TimeUnit.NANOSECONDS)
    }

    private fun dispatchNext() {
        val slot = slots.firstOrNull { it.startedAtNanos == null } ?: return
        val nowNanos = System.nanoTime()
        refillTokens(nowNanos)
        if (tokens < 1.0) {
            scheduleDispatch()
            return
        }
        val payment = waiting.pollFirst() ?: return

        tokens -= 1.0
        execute(slot, payment, nowNanos)
        scheduleDispatch()
    }

    private fun execute(slot: Slot, payment: PendingPayment, startedAtNanos: Long) {
        slot.startedAtNanos = startedAtNanos
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
    }

    private fun refillTokens(nowNanos: Long) {
        val elapsedNanos = nowNanos - lastTokenRefillAtNanos
        if (elapsedNanos <= 0) return
        tokens = minOf(
            tokenCapacity.toDouble(),
            tokens + elapsedNanos.toDouble() * dispatchRatePerSecond / TimeUnit.SECONDS.toNanos(1)
        )
        lastTokenRefillAtNanos = nowNanos
    }

    private fun nanosUntilNextToken(): Long {
        if (tokens >= 1.0) return 0
        return maxOf(1L, ((1.0 - tokens) * TimeUnit.SECONDS.toNanos(1) / dispatchRatePerSecond).toLong())
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
        val action: (Long) -> Unit,
    )

    private companion object {
        const val DEFAULT_MAX_PENDING = 10_000
    }
}
