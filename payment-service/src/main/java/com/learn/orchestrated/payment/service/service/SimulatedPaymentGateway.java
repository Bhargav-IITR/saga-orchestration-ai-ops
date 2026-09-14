package com.learn.orchestrated.payment.service.service;

import com.learn.orchestrated.payment.service.model.Payment;
import com.learn.sagacommons.exception.ValidationException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class SimulatedPaymentGateway implements PaymentGateway {

    @Override
    public void authorize(Payment payment) {
        GatewayResult result = nextResult();
        switch (result) {
            case INSUFFICIENT_FUNDS -> throw new ValidationException(
                    "Payment declined: insufficient funds. Amount: R$%.2f"
                            .formatted(payment.getTotalAmount()));
            case CARD_EXPIRED -> throw new ValidationException(
                    "Payment declined: card expired or invalid credentials.");
            case GATEWAY_TIMEOUT -> throw new ValidationException(
                    "Payment gateway timeout after 30s. Please retry.");
            case DUPLICATE_TRANSACTION -> throw new ValidationException(
                    "Duplicate transaction detected by gateway. TransactionId already processed.");
            case SUCCESS -> log.info("Gateway approved payment for order: {}", payment.getOrderId());
        }
    }

    private GatewayResult nextResult() {
        double random = Math.random();
        if (random < 0.80) return GatewayResult.SUCCESS;
        if (random < 0.87) return GatewayResult.INSUFFICIENT_FUNDS;
        if (random < 0.92) return GatewayResult.CARD_EXPIRED;
        if (random < 0.96) return GatewayResult.GATEWAY_TIMEOUT;
        return GatewayResult.DUPLICATE_TRANSACTION;
    }

    private enum GatewayResult {
        SUCCESS,
        INSUFFICIENT_FUNDS,
        CARD_EXPIRED,
        GATEWAY_TIMEOUT,
        DUPLICATE_TRANSACTION
    }
}
