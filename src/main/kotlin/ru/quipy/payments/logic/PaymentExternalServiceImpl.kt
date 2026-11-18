package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.InterruptedIOException
import java.time.Duration
import java.util.*
import java.util.concurrent.TimeUnit


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    private val temporaryErrorRetryBaseMillis: Long,
    private val temporaryErrorRetryMaxAttempts: Int,
) : PaymentExternalSystemAdapter {
    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
        const val MAX_RETRY_EXPONENT = 20
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val rateLimiter = SlidingWindowRateLimiter(
        rate = rateLimitPerSec.toLong(),
        window = Duration.ofSeconds(1),
    )
    private val client = OkHttpClient.Builder().build()

    override fun performPaymentAsync(
        paymentId: UUID,
        amount: Int,
        paymentStartedAt: Long,
        deadline: Long
    ) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()

        try {
            // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
            // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
            paymentESService.update(paymentId) {
                it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
            }
            logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

            val request = Request.Builder().run {
                url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                post(emptyBody)
            }.build()

            for (attempt in 1..temporaryErrorRetryMaxAttempts) {
                rateLimiter.tickBlocking()
                val callBudget = deadline - now()
                if (callBudget <= 0) {
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "call budget <= 0")
                    }
                    return
                }

                val body = client.newCall(request).run {
                    timeout().timeout(callBudget, TimeUnit.MILLISECONDS)
                    execute().use { response ->
                        try {
                            mapper.readValue(
                                response.body?.string(),
                                ExternalSysResponse::class.java
                            )
                        } catch (e: Exception) {
                            logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                            ExternalSysResponse(
                                transactionId.toString(),
                                paymentId.toString(),
                                false,
                                e.message
                            )
                        }
                    }
                }

                if (body.result || !isTemporaryError(body.message) || attempt >= temporaryErrorRetryMaxAttempts) {
                    logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")
                    paymentESService.update(paymentId) {
                        it.logProcessing(body.result, now(), transactionId, reason = body.message)
                    }
                    return
                }

                val backoffMillis = retryBackoffMillis(attempt)
                if (now() + backoffMillis >= deadline) {
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = body.message)
                    }
                    return
                }
                logger.info(
                    "[$accountName] Temporary error for payment $paymentId; retry {}/{} in {} ms",
                    attempt,
                    temporaryErrorRetryMaxAttempts,
                    backoffMillis
                )
                TimeUnit.MILLISECONDS.sleep(backoffMillis)
            }
        } catch (e: Exception) {
            when (e) {
                is InterruptedIOException -> {
                    logger.error(
                        "[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId",
                        e
                    )
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                    }
                }

                else -> {
                    logger.error(
                        "[$accountName] Payment failed for txId: $transactionId, payment: $paymentId",
                        e
                    )

                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = e.message)
                    }
                }
            }
        }
    }

    override fun price() = properties.price

    override fun parallelism() = parallelRequests

    override fun rateLimitPerSecond() = rateLimitPerSec

    override fun averageProcessingTime() = properties.averageProcessingTime

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

    private fun isTemporaryError(message: String?) =
        message.equals("Temporary error", ignoreCase = true)

    private fun retryBackoffMillis(attempt: Int): Long {
        val multiplier = 1L shl (attempt - 1).coerceAtMost(MAX_RETRY_EXPONENT)
        return temporaryErrorRetryBaseMillis * multiplier
    }

}

public fun now() = System.currentTimeMillis()
