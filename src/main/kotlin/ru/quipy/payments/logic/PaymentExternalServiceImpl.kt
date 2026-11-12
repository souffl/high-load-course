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
import java.io.IOException
import java.time.Duration
import java.util.*
import java.util.concurrent.TimeUnit


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    private val deadlineSafetyMarginMillis: Long = 200,
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
        const val MAX_ATTEMPTS = 2
        const val RETRY_DELAY_MILLIS = 100L
        const val RETRY_RESERVE_MILLIS = 1_200L
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

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        val transactionId = UUID.randomUUID()
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        val request = Request.Builder().run {
            url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
            post(emptyBody)
        }.build()

        var lastFailure = "Payment was not processed."
        for (attempt in 0 until MAX_ATTEMPTS) {
            try {
                rateLimiter.tickBlocking()
                val remainingBudget = deadline - now() - deadlineSafetyMarginMillis
                val callBudget = if (attempt == 0) {
                    remainingBudget - RETRY_RESERVE_MILLIS
                } else {
                    remainingBudget
                }
                if (callBudget <= 0) {
                    lastFailure = "call budget <= 0"
                    break
                }

                client.newCall(request).apply {
                    timeout().timeout(callBudget, TimeUnit.MILLISECONDS)
                }.execute().use { response ->
                    val responseBody = response.body?.string()
                    if (response.code == 429 || response.code in 500..599) {
                        lastFailure = "HTTP ${response.code}: ${responseBody.orEmpty()}"
                    } else {
                        val body = try {
                            mapper.readValue(responseBody, ExternalSysResponse::class.java)
                        } catch (e: Exception) {
                            paymentESService.update(paymentId) {
                                it.logProcessing(false, now(), transactionId, reason = e.message)
                            }
                            return
                        }
                        paymentESService.update(paymentId) {
                            it.logProcessing(body.result, now(), transactionId, reason = body.message)
                        }
                        return
                    }
                }
            } catch (e: InterruptedIOException) {
                if (Thread.currentThread().isInterrupted) break
                lastFailure = "Request timeout."
            } catch (e: IOException) {
                lastFailure = e.message ?: "Network error"
            } catch (e: Exception) {
                lastFailure = e.message ?: "Unexpected payment error"
                break
            }

            if (attempt + 1 < MAX_ATTEMPTS && now() + RETRY_DELAY_MILLIS + deadlineSafetyMarginMillis < deadline) {
                try {
                    TimeUnit.MILLISECONDS.sleep(RETRY_DELAY_MILLIS)
                } catch (_: InterruptedException) {
                    Thread.currentThread().interrupt()
                    break
                }
            } else {
                break
            }
        }

        paymentESService.update(paymentId) {
            it.logProcessing(false, now(), transactionId, reason = lastFailure)
        }
    }

    override fun price() = properties.price

    override fun parallelism() = parallelRequests

    override fun rateLimitPerSecond() = rateLimitPerSec

    override fun averageProcessingTime() = properties.averageProcessingTime

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()
