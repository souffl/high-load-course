package ru.quipy.payments.logic

import org.springframework.stereotype.Service
import java.util.concurrent.Semaphore

@Service
class PaymentAdmissionControl(accounts: List<PaymentExternalSystemAdapter>) {
    // PaymentSystemImpl invokes each account, so the slowest parallel window bounds admission.
    private val capacity = accounts.minOfOrNull { it.parallelism() }
        ?: error("At least one payment account is required.")
    private val slots = Semaphore(capacity.also { require(it > 0) })

    fun tryAcquire(): Boolean = slots.tryAcquire()

    fun release() = slots.release()
}

class PaymentCapacityExceededException : RuntimeException("Payment capacity is exhausted. Retry later.")
