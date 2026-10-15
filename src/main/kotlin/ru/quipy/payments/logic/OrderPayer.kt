package ru.quipy.payments.logic

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Service
import org.springframework.web.server.ResponseStatusException
import ru.quipy.common.utils.CallerBlockingRejectedExecutionHandler
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.time.Duration
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

@Service
class OrderPayer(paymentAccounts: List<PaymentExternalSystemAdapter>) {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    private lateinit var paymentService: PaymentService

    private val capacity = paymentAccounts.minOf { it.parallelism() }
    private val ongoingWindow = OngoingWindow(capacity)
    private val processingBudget = Duration.ofMillis(
        paymentAccounts.sumOf { it.averageProcessingTime().toMillis() }
    ).plusSeconds(1)

    private val paymentExecutor = ThreadPoolExecutor(
        16,
        16,
        0L,
        TimeUnit.MILLISECONDS,
        LinkedBlockingQueue(8_000),
        NamedThreadFactory("payment-submission-executor"),
        CallerBlockingRejectedExecutionHandler()
    )

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        val createdAt = System.currentTimeMillis()
        val waitMillis = deadline - createdAt - processingBudget.toMillis()
        if (waitMillis <= 0 || !ongoingWindow.tryAcquire(Duration.ofMillis(waitMillis))) {
            throw ResponseStatusException(HttpStatus.TOO_MANY_REQUESTS, "Not enough time to accept payment before deadline.")
        }
        try {
            if (deadline - System.currentTimeMillis() <= processingBudget.toMillis()) {
                throw ResponseStatusException(HttpStatus.TOO_MANY_REQUESTS, "Not enough time left to process payment.")
            }
            paymentExecutor.execute {
                try {
                    val createdEvent = paymentESService.create {
                        it.create(paymentId, orderId, amount)
                    }
                    logger.trace("Payment ${createdEvent.paymentId} for order $orderId created.")
                    paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
                } finally {
                    ongoingWindow.release()
                }
            }
        } catch (e: RuntimeException) {
            ongoingWindow.release()
            throw e
        }
        return createdAt
    }
}
