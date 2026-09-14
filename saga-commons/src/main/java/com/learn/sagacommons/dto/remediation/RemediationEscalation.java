package com.learn.sagacommons.dto.remediation;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RemediationEscalation {

    private String source;
    private RemediationCommand command;
    @Builder.Default
    private List<String> reasons = new ArrayList<>();
    private LocalDateTime escalatedAt;
}
