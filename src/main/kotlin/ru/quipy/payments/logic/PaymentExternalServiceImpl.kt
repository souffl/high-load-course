package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.TimeUnit

// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val rateLimiter = SlidingWindowRateLimiter(
        rate = rateLimitPerSec.toLong(),
        window = Duration.ofSeconds(1),
    )
    private val ongoingWindow = OngoingWindow(parallelRequests)

    private val client = OkHttpClient.Builder().build()

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val processingBudget = requestAverageProcessingTime.toMillis()
        val acquireBudget = deadline - now() - processingBudget

        if (acquireBudget <= 0) {
            failWithoutCalling(paymentId, paymentStartedAt, "Not enough time left before deadline.")
            return
        }

        if (!ongoingWindow.tryAcquire(Duration.ofMillis(acquireBudget))) {
            failWithoutCalling(paymentId, paymentStartedAt, "Timeout waiting for parallel slot.")
            return
        }

        var submittedTransactionId: UUID? = null
        try {
            if (now() + processingBudget >= deadline) {
                failWithoutCalling(paymentId, paymentStartedAt, "Deadline is too close after acquiring slot.")
                return
            }

            if (!tickRateLimiterUntil(deadline - processingBudget)) {
                failWithoutCalling(paymentId, paymentStartedAt, "Timeout waiting for rate limiter.")
                return
            }

            val transactionId = UUID.randomUUID()
            submittedTransactionId = transactionId

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

            val call = client.newCall(request)
            val callBudget = deadline - now()

            if (callBudget <= 0) {
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Deadline exceeded before external call.")
                }
                return
            }

            call.timeout().timeout(callBudget, TimeUnit.MILLISECONDS)
            call.execute().use { response ->
                val body = try {
                    mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                    ExternalSysResponse(transactionId.toString(), paymentId.toString(),false, e.message)
                }

                logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                paymentESService.update(paymentId) {
                    it.logProcessing(body.result, now(), transactionId, reason = body.message)
                }
            }
        } catch (e: Exception) {
            val transactionId = submittedTransactionId
            if (transactionId == null) {
                logger.error("[$accountName] Payment failed before submission: $paymentId", e)
                return
            }

            when (e) {
                is SocketTimeoutException -> {
                    logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                    }
                }

                else -> {
                    logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)

                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = e.message)
                    }
                }
            }
        } finally {
            ongoingWindow.release()
        }
    }

    private fun tickRateLimiterUntil(mustAcquireBefore: Long): Boolean {
        while (now() < mustAcquireBefore) {
            if (rateLimiter.tick()) {
                return true
            }
            Thread.sleep(10)
        }
        return false
    }

    private fun failWithoutCalling(paymentId: UUID, paymentStartedAt: Long, reason: String) {
        val transactionId = UUID.randomUUID()
        logger.warn("[$accountName] Skip payment $paymentId: $reason")

        paymentESService.update(paymentId) {
            it.logSubmission(false, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }
        paymentESService.update(paymentId) {
            it.logProcessing(false, now(), transactionId, reason = reason)
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()
