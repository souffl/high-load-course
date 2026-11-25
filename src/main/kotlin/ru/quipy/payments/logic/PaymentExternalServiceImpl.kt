package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tags
import io.micrometer.core.instrument.Timer
import okhttp3.Call
import okhttp3.EventListener
import okhttp3.HttpUrl.Companion.toHttpUrl
import okhttp3.OkHttpClient
import okhttp3.Protocol
import okhttp3.Request
import okhttp3.RequestBody
import okhttp3.Response
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.IOException
import java.io.InterruptedIOException
import java.net.InetSocketAddress
import java.net.Proxy
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
    private val temporaryErrorRetryBaseMillis: Long,
    private val maxAttempts: Int,
    private val meterRegistry: MeterRegistry,
    connectTimeoutMillis: Long,
    writeTimeoutMillis: Long,
    readTimeoutMillis: Long,
    private val providerAttemptTimeout: Duration?,
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
    private val metricTags = Tags.of("service", serviceName, "account", accountName)
    private val retries = meterRegistry.counter("payment.provider.retries", metricTags)
    private val phaseTimers = Phase.entries.associateWith { phase ->
        Outcome.entries.associateWith { outcome ->
            Timer.builder("payment.provider.duration")
                .tags(metricTags)
                .tags("phase", phase.tag, "outcome", outcome.tag)
                .publishPercentileHistogram()
                .minimumExpectedValue(Duration.ofNanos(100_000))
                .maximumExpectedValue(Duration.ofSeconds(15))
                .register(meterRegistry)
        }
    }
    private val client = OkHttpClient.Builder()
        .connectTimeout(connectTimeoutMillis, TimeUnit.MILLISECONDS)
        .writeTimeout(writeTimeoutMillis, TimeUnit.MILLISECONDS)
        .readTimeout(readTimeoutMillis, TimeUnit.MILLISECONDS)
        .retryOnConnectionFailure(providerAttemptTimeout == null)
        .eventListenerFactory { newMetricsListener() }
        .build()
        .also(::registerTimeouts)

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
                val requestUrl = "http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount"
                    .toHttpUrl().newBuilder()
                providerAttemptTimeout?.let { requestUrl.addQueryParameter("timeout", it.toString()) }
                url(requestUrl.build())
                post(emptyBody)
            }.build()

            for (attempt in 1..maxAttempts) {
                rateLimiter.tickBlocking()
                val callBudget = deadline - now()
                if (callBudget <= 0) {
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "call budget <= 0")
                    }
                    return
                }

                val call = client.newCall(request)
                call.timeout().timeout(callBudget, TimeUnit.MILLISECONDS)
                if (attempt > 1) retries.increment()
                val (responseCode, responseBody) = call.execute().use { response ->
                    response.code to response.body?.string()
                }
                val body = try {
                    mapper.readValue(responseBody, ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: $responseCode, reason: ${e.message}")
                    ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                }

                val providerTimedOut = providerAttemptTimeout != null && responseCode == 408 && body.message == "Timeout" &&
                    body.transactionId == transactionId.toString() && body.paymentId == paymentId.toString()
                if (body.result || !(providerTimedOut || isTemporaryError(body.message)) || attempt == maxAttempts) {
                    logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")
                    paymentESService.update(paymentId) {
                        it.logProcessing(body.result, now(), transactionId, reason = body.message)
                    }
                    return
                }

                val backoffMillis = if (providerTimedOut) 0L else retryBackoffMillis(attempt)
                if (now() + backoffMillis >= deadline) {
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = body.message)
                    }
                    return
                }
                logger.info(
                    "[$accountName] {} for payment $paymentId; retry {}/{} in {} ms",
                    body.message, attempt, maxAttempts, backoffMillis
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

    private fun registerTimeouts(client: OkHttpClient) {
        mapOf(
            Phase.CONNECT to client.connectTimeoutMillis,
            Phase.WRITE to client.writeTimeoutMillis,
            Phase.READ to client.readTimeoutMillis,
        ).forEach { (phase, timeoutMillis) ->
            Gauge.builder("payment.provider.timeout.limit", timeoutMillis) { it / 1_000.0 }
                .tags(metricTags.and("phase", phase.tag))
                .baseUnit("seconds")
                .strongReference(true)
                .register(meterRegistry)
        }
    }

    private fun newMetricsListener(): EventListener = object : EventListener() {
        private lateinit var attemptTimer: Timer.Sample
        private lateinit var phaseTimer: Timer.Sample
        private var phase: Phase? = null
        private var responseOutcome = Outcome.COMPLETED

        override fun callStart(call: Call) {
            attemptTimer = Timer.start(meterRegistry)
        }

        override fun connectStart(call: Call, inetSocketAddress: InetSocketAddress, proxy: Proxy) =
            start(Phase.CONNECT)

        override fun connectEnd(call: Call, inetSocketAddress: InetSocketAddress, proxy: Proxy, protocol: Protocol?) =
            finish(Outcome.COMPLETED)

        override fun requestHeadersStart(call: Call) = start(Phase.WRITE)

        override fun requestBodyEnd(call: Call, byteCount: Long) = startRead()

        override fun responseHeadersStart(call: Call) {
            // A follow-up request may have no body.
            if (phase == Phase.WRITE) startRead()
        }

        override fun responseHeadersEnd(call: Call, response: Response) {
            responseOutcome = if (response.code == 408) Outcome.PROVIDER_TIMEOUT else Outcome.COMPLETED
        }

        override fun responseBodyEnd(call: Call, byteCount: Long) = finish(responseOutcome)

        override fun callEnd(call: Call) = recordDuration(Phase.ATTEMPT, responseOutcome, attemptTimer)

        override fun callFailed(call: Call, ioe: IOException) {
            val outcome = if (ioe is SocketTimeoutException || (ioe is InterruptedIOException && ioe.message == "timeout"))
                Outcome.TIMEOUT else Outcome.IO_ERROR
            finish(outcome)
            recordDuration(Phase.ATTEMPT, outcome, attemptTimer)
        }

        private fun startRead() {
            finish(Outcome.COMPLETED)
            start(Phase.READ)
        }

        private fun start(next: Phase) {
            phaseTimer = Timer.start(meterRegistry)
            phase = next
        }

        private fun finish(outcome: Outcome) {
            val activePhase = phase ?: return
            recordDuration(activePhase, outcome, phaseTimer)
            phase = null
        }
    }

    private fun recordDuration(phase: Phase, outcome: Outcome, sample: Timer.Sample) {
        sample.stop(phaseTimers.getValue(phase).getValue(outcome))
    }

    private enum class Phase(val tag: String) {
        CONNECT("connect"), WRITE("write"), READ("read"), ATTEMPT("attempt"),
    }

    private enum class Outcome(val tag: String) {
        COMPLETED("completed"), TIMEOUT("timeout"), PROVIDER_TIMEOUT("provider_timeout"), IO_ERROR("io_error"),
    }
}

public fun now() = System.currentTimeMillis()
