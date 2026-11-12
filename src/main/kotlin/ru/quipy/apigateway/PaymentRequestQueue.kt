package ru.quipy.apigateway

import jakarta.annotation.PreDestroy
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.payments.logic.PaymentExternalSystemAdapter
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

@Component
class PaymentRequestQueue(
    paymentAccounts: List<PaymentExternalSystemAdapter>,
    @Value("\${payment.queue.max-pending:0}")
    configuredMaxPending: Int,
) {
    init {
        require(paymentAccounts.isNotEmpty()) { "At least one payment account is required." }
        require(paymentAccounts.all { it.rateLimitPerSecond() > 0 && it.parallelism() > 0 })
    }

    private val accountRatePerSecond = paymentAccounts.minOf { it.rateLimitPerSecond() }
    private val dispatchRatePerSecond = accountRatePerSecond
    private val workerCount = paymentAccounts.minOf { it.parallelism() }
    private val tokenCapacity = dispatchRatePerSecond
    private val maxPending = configuredMaxPending.takeIf { it > 0 } ?: DEFAULT_MAX_PENDING

    private val logger = LoggerFactory.getLogger(PaymentRequestQueue::class.java)
    private val accepting = AtomicBoolean(true)
    private val waiting = LinkedBlockingQueue<PendingPayment>(maxPending)
    private val workerSlots = Semaphore(workerCount)
    private val executor = Executors.newFixedThreadPool(workerCount, NamedThreadFactory("payment-request-executor"))
    private val dispatcher = Executors.newSingleThreadExecutor(NamedThreadFactory("payment-request-dispatcher"))

    // Accessed only by the single dispatcher thread.
    private var tokens = tokenCapacity.toDouble()
    private var lastTokenRefillAtNanos = System.nanoTime()

    init {
        logger.info(
            "Payment scheduler: workers={}, accountRateLimitPerSecond={}, dispatchRatePerSecond={}, tokenCapacity={}, maxPending={}",
            workerCount, accountRatePerSecond, dispatchRatePerSecond, tokenCapacity, maxPending,
        )
        dispatcher.execute(::dispatchLoop)
    }

    fun submit(deadline: Long, action: (Long) -> Unit): Admission {
        val receivedAt = System.currentTimeMillis()
        if (!accepting.get() || !waiting.offer(PendingPayment(receivedAt, action))) {
            logger.info("Payment admission rejected: waiting={}, accepting={}", waiting.size, accepting.get())
            return Admission.Rejected(receivedAt + RETRY_AFTER_MILLIS)
        }
        return Admission.Accepted(receivedAt)
    }

    private fun dispatchLoop() {
        try {
            while (!Thread.currentThread().isInterrupted) {
                val payment = waiting.take()
                workerSlots.acquire()
                try {
                    acquireToken()
                    executor.execute {
                        try {
                            payment.action(payment.receivedAt)
                        } finally {
                            workerSlots.release()
                        }
                    }
                } catch (_: RejectedExecutionException) {
                    workerSlots.release()
                } catch (e: InterruptedException) {
                    workerSlots.release()
                    throw e
                }
            }
        } catch (_: InterruptedException) {
            Thread.currentThread().interrupt()
        }
    }

    private fun acquireToken() {
        while (true) {
            refillTokens(System.nanoTime())
            if (tokens >= 1.0) {
                tokens -= 1.0
                return
            }
            val waitNanos = maxOf(
                1L,
                ((1.0 - tokens) * TimeUnit.SECONDS.toNanos(1) / dispatchRatePerSecond).toLong(),
            )
            TimeUnit.NANOSECONDS.sleep(waitNanos)
        }
    }

    private fun refillTokens(nowNanos: Long) {
        val elapsedNanos = nowNanos - lastTokenRefillAtNanos
        if (elapsedNanos <= 0) return
        tokens = minOf(
            tokenCapacity.toDouble(),
            tokens + elapsedNanos.toDouble() * dispatchRatePerSecond / TimeUnit.SECONDS.toNanos(1),
        )
        lastTokenRefillAtNanos = nowNanos
    }

    @PreDestroy
    fun shutdown() {
        accepting.set(false)
        dispatcher.shutdownNow()
        executor.shutdown()
    }

    sealed interface Admission {
        data class Accepted(val receivedAt: Long) : Admission
        data class Rejected(val retryAt: Long) : Admission
    }

    private data class PendingPayment(
        val receivedAt: Long,
        val action: (Long) -> Unit,
    )

    private companion object {
        const val DEFAULT_MAX_PENDING = 10_000
        const val RETRY_AFTER_MILLIS = 1_000L
    }
}
