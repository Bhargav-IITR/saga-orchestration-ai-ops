package com.learn.aisagaagent.repository;

import com.learn.aisagaagent.model.OperationalEscalation;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface OperationalEscalationRepository
        extends JpaRepository<OperationalEscalation, Long> {

    List<OperationalEscalation> findAllByOrderByCreatedAtDesc();
}
