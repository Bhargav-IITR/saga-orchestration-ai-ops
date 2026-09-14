package com.learn.orchestrated.payment.service.service;

import com.learn.orchestrated.payment.service.model.Payment;

/** Outbound port for payment authorization. */
public interface PaymentGateway {
    void authorize(Payment payment);
}
