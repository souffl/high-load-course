package ru.quipy.apigateway

import jakarta.annotation.PreDestroy
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.common.utils.TokenBucketRateLimiter
import ru.quipy.payments.logic.PaymentExternalSystemAdapter
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

@Component
class PaymentRequestQueue(
    paymentAccounts: List<PaymentExternalSystemAdapter>,
    @Value("\${payment.queue.max-pending:0}")
    configuredMaxPending: Int,
    @Value("\${payment.queue.deadline-admission-enabled:true}")
    private val deadlineAdmissionEnabled: Boolean,
    @Value("\${payment.queue.deadline-admission-processing-budget-millis:0}")
    configuredAdmissionProcessingBudgetMillis: Long,
    @Value("\${payment.deadline-safety-margin-millis:300}")
    private val deadlineSafetyMarginMillis: Long,
) {
    init {
        require(paymentAccounts.isNotEmpty()) { "At least one payment account is required." }
        require(paymentAccounts.all { it.rateLimitPerSecond() > 0 && it.parallelism() > 0 })
        require(configuredAdmissionProcessingBudgetMillis >= 0 && deadlineSafetyMarginMillis >= 0)
    }

    private val accountRatePerSecond = paymentAccounts.minOf { it.rateLimitPerSecond() }
    private val dispatchRatePerSecond = accountRatePerSecond
    private val workerCount = paymentAccounts.minOf { it.parallelism() }
    private val tokenCapacity = dispatchRatePerSecond
    private val processingBudgetMillis = configuredAdmissionProcessingBudgetMillis.takeIf { it > 0 }
        ?: paymentAccounts.minOf { it.averageProcessingTime().toMillis() }
    private val maxPending = configuredMaxPending.takeIf { it > 0 } ?: DEFAULT_MAX_PENDING

    private val logger = LoggerFactory.getLogger(PaymentRequestQueue::class.java)
    private val accepting = AtomicBoolean(true)
    private val pendingStarts = AtomicInteger()
    private val waiting = LinkedBlockingQueue<PendingPayment>(maxPending)
    private val workerSlots = Semaphore(workerCount)
    private val executor = Executors.newFixedThreadPool(workerCount, NamedThreadFactory("payment-request-executor"))
    private val dispatcher = Executors.newSingleThreadExecutor(NamedThreadFactory("payment-request-dispatcher"))
    private val tokenBucket = TokenBucketRateLimiter(dispatchRatePerSecond, tokenCapacity)

    init {
        logger.info(
            "Payment scheduler: workers={}, accountRateLimitPerSecond={}, dispatchRatePerSecond={}, tokenCapacity={}, maxPending={}, deadlineAdmissionEnabled={}, admissionProcessingBudgetMillis={}",
            workerCount, accountRatePerSecond, dispatchRatePerSecond, tokenCapacity, maxPending,
            deadlineAdmissionEnabled, processingBudgetMillis,
        )
        dispatcher.execute(::dispatchLoop)
    }

    fun submit(deadline: Long, action: (Long) -> Unit): Admission {
        val receivedAt = System.currentTimeMillis()
        if (!accepting.get()) return rejected(receivedAt, deadline)

        if (!enqueue(receivedAt, deadline, action)) {
            return rejected(receivedAt, deadline)
        }
        return Admission.Accepted(receivedAt)
    }

    private fun enqueue(receivedAt: Long, deadline: Long, action: (Long) -> Unit): Boolean {
        while (true) {
            val startsAhead = pendingStarts.get()
            if (!canStartBeforeDeadline(receivedAt, deadline, startsAhead)) return false
            if (pendingStarts.compareAndSet(startsAhead, startsAhead + 1)) break
        }

        if (waiting.offer(PendingPayment(receivedAt, action))) return true
        pendingStarts.decrementAndGet()
        return false
    }

    private fun rejected(receivedAt: Long, deadline: Long): Admission.Rejected {
            logger.info(
                "Payment admission rejected: waiting={}, pendingStarts={}, accepting={}, deadlineInMillis={}",
                waiting.size, pendingStarts.get(), accepting.get(), deadline - receivedAt,
            )
        return Admission.Rejected(receivedAt + RETRY_AFTER_MILLIS)
    }

    private fun canStartBeforeDeadline(receivedAt: Long, deadline: Long, startsAhead: Int): Boolean {
        if (!deadlineAdmissionEnabled) return true
        val queuedStartDelayMillis = TimeUnit.NANOSECONDS.toMillis(
            tokenBucket.estimateDelayNanos(startsAhead + 1) + TimeUnit.MILLISECONDS.toNanos(1) - 1
        )
        return queuedStartDelayMillis + processingBudgetMillis + deadlineSafetyMarginMillis < deadline - receivedAt
    }

    private fun dispatchLoop() {
        try {
            while (!Thread.currentThread().isInterrupted) {
                val payment = waiting.take()
                workerSlots.acquire()
                try {
                    tokenBucket.acquire()
                    pendingStarts.decrementAndGet()
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
