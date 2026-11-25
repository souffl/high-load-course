package ru.quipy.payments.config

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.core.env.Environment
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.logic.*
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.*


@Configuration
class PaymentAccountsConfig {
    companion object {
        private val javaClient = HttpClient.newBuilder().build()
        private val mapper = ObjectMapper().registerKotlinModule().registerModules(JavaTimeModule())
    }

    @Value("\${payment.hostPort}")
    lateinit var paymentProviderHostPort: String

    @Value("\${payment.service-name}")
    lateinit var serviceName: String

    @Value("\${payment.token}")
    lateinit var token: String

    @Value("#{'\${payment.accounts}'.split(',')}")
    lateinit var allowedAccounts: List<String>

    @Value("\${payment.provider-temporary-error-retry-base-millis:100}")
    var temporaryErrorRetryBaseMillis: Long = 100

    @Value("\${payment.provider-temporary-error-retry-max-attempts:3}")
    var temporaryErrorRetryMaxAttempts: Int = 3

    @Bean
    fun accountAdapters(
        paymentService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
        meterRegistry: MeterRegistry,
        environment: Environment,
        @Value("\${payment.provider-connect-timeout-millis:250}") connectTimeoutMillis: Long,
        @Value("\${payment.provider-write-timeout-millis:250}") writeTimeoutMillis: Long,
        @Value("\${payment.provider-read-timeout-millis:10000}") readTimeoutMillis: Long,
    ): List<PaymentExternalSystemAdapter> {
        val request = HttpRequest.newBuilder()
            .uri(URI("http://${paymentProviderHostPort}/external/accounts?serviceName=$serviceName&token=$token"))
            .GET()
            .build()

        val resp = javaClient.send(request, HttpResponse.BodyHandlers.ofString())

        println("\nPayment accounts list:")
        return mapper.readValue<List<PaymentAccountProperties>>(
            resp.body(),
            mapper.typeFactory.constructCollectionType(List::class.java, PaymentAccountProperties::class.java)
        )
            .filter { it.accountName in allowedAccounts }
            .map { it.copy(enabled = true) }
            .onEach(::println)
            .map {
                val accountSettings = "payment.provider.accounts.${it.accountName}"
                PaymentExternalSystemAdapterImpl(
                    it,
                    paymentService,
                    paymentProviderHostPort,
                    token,
                    temporaryErrorRetryBaseMillis,
                    environment.getProperty("$accountSettings.max-attempts", Int::class.java, temporaryErrorRetryMaxAttempts),
                    meterRegistry,
                    connectTimeoutMillis,
                    writeTimeoutMillis,
                    readTimeoutMillis,
                    environment.getProperty("$accountSettings.attempt-timeout-millis", Long::class.java, 0L)
                        .takeIf { value -> value > 0 }?.let(Duration::ofMillis),
                )
            }
    }
}
