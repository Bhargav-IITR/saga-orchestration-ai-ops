package com.learn.aisagaagent.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Entity
@Table(name = "operational_escalations")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OperationalEscalation {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    private String source;
    private String remediationId;
    private String orderId;
    private String transactionId;
    @Column(columnDefinition = "TEXT")
    private String reasons;
    @Column(columnDefinition = "TEXT")
    private String payload;
    private LocalDateTime createdAt;
}
