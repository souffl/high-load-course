package ru.quipy.apigateway

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import ru.quipy.orders.repository.OrderRepository
import ru.quipy.payments.logic.OrderPayer
import java.util.*
import java.util.concurrent.ConcurrentHashMap

@RestController
class APIController(
    @Value("\${payment.queue.retry-after-base-millis:100}")
    private val retryAfterBaseMillis: Long,
    @Value("\${payment.queue.retry-after-max-millis:1000}")
    private val retryAfterMaxMillis: Long,
) {

    val logger: Logger = LoggerFactory.getLogger(APIController::class.java)

    @Autowired
    private lateinit var orderRepository: OrderRepository

    @Autowired
    private lateinit var orderPayer: OrderPayer

    @Autowired
    private lateinit var paymentRequestQueue: PaymentRequestQueue

    private val rejectedAttempts = ConcurrentHashMap<UUID, RetryState>()

    init {
        require(retryAfterBaseMillis > 0 && retryAfterMaxMillis >= retryAfterBaseMillis) {
            "Retry-After bounds must be positive and ordered"
        }
    }

    @PostMapping("/users")
    fun createUser(@RequestBody req: CreateUserRequest): User {
        return User(UUID.randomUUID(), req.name)
    }

    data class CreateUserRequest(val name: String, val password: String)

    data class User(val id: UUID, val name: String)

    @PostMapping("/orders")
    fun createOrder(@RequestParam userId: UUID, @RequestParam price: Int): Order {
        val order = Order(
            UUID.randomUUID(),
            userId,
            System.currentTimeMillis(),
            OrderStatus.COLLECTING,
            price,
        )
        return orderRepository.save(order)
    }

    data class Order(
        val id: UUID,
        val userId: UUID,
        val timeCreated: Long,
        val status: OrderStatus,
        val price: Int,
    )

    enum class OrderStatus {
        COLLECTING,
        PAYMENT_IN_PROGRESS,
        PAID,
    }

    @PostMapping("/orders/{orderId}/payment")
    fun payOrder(@PathVariable orderId: UUID, @RequestParam deadline: Long): ResponseEntity<PaymentSubmissionDto> {
        val paymentId = UUID.randomUUID()
        val order = orderRepository.findById(orderId)?.let {
            orderRepository.save(it.copy(status = OrderStatus.PAYMENT_IN_PROGRESS))
            it
        } ?: throw IllegalArgumentException("No such order $orderId")

        val admission = paymentRequestQueue.submit(deadline) { receivedAt ->
            orderPayer.processPayment(orderId, order.price, paymentId, deadline, receivedAt)
        }
        return when (admission) {
            is PaymentRequestQueue.Admission.Accepted -> {
                rejectedAttempts.remove(orderId)
                ResponseEntity.ok(PaymentSubmissionDto(admission.receivedAt, paymentId))
            }
            is PaymentRequestQueue.Admission.Rejected -> {
                val retryAfterMillis = nextRetryAfterMillis(orderId)
                logger.info(
                    "Payment admission rejected: orderId={}, retryAfterMillis={}",
                    orderId, retryAfterMillis,
                )
                ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                    .header(HttpHeaders.RETRY_AFTER, (System.currentTimeMillis() + retryAfterMillis).toString())
                    .build()
            }
        }
    }

    private fun nextRetryAfterMillis(orderId: UUID): Long {
        val attempt = rejectedAttempts.compute(orderId) { _, previous ->
            RetryState((previous?.attempt ?: 0) + 1)
        }!!.attempt
        val multiplier = 1L shl (attempt - 1).coerceAtMost(MAX_RETRY_EXPONENT)
        return (retryAfterBaseMillis * multiplier).coerceAtMost(retryAfterMaxMillis)
    }

    class PaymentSubmissionDto(
        val timestamp: Long,
        val transactionId: UUID
    )

    private data class RetryState(val attempt: Int)

    private companion object {
        const val MAX_RETRY_EXPONENT = 20
    }
}
