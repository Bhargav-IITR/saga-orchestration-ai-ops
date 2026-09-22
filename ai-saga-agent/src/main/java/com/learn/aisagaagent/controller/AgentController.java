package com.learn.aisagaagent.controller;

import com.learn.aisagaagent.model.SagaDiagnostic;
import com.learn.aisagaagent.model.OperationalEscalation;
import com.learn.aisagaagent.repository.OperationalEscalationRepository;
import com.learn.aisagaagent.repository.SagaDiagnosticRepository;
import com.learn.aisagaagent.service.DataAnalystAgentService;
import com.learn.aisagaagent.service.SagaComposerService;
import lombok.AllArgsConstructor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;
import tools.jackson.databind.ObjectMapper;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/agent")
@AllArgsConstructor
public class AgentController {

    private final DataAnalystAgentService dataAnalystAgentService;
    private final SagaDiagnosticRepository diagnosticRepository;
    private final StringRedisTemplate redisTemplate;
    private final SagaComposerService sagaComposerService;
    private final OperationalEscalationRepository escalationRepository;


    @GetMapping("/chat")
    public String getChat(@RequestParam(name = "question") String question) {
        return dataAnalystAgentService.runAgent(question);
    }

    @GetMapping("/diagnostics")
    public List<SagaDiagnostic> getAllDiagnostics() {
        return diagnosticRepository.findAllByOrderByCreatedAtDesc();
    }

    @GetMapping("/escalations")
    public List<OperationalEscalation> getAllEscalations() {
        return escalationRepository.findAllByOrderByCreatedAtDesc();
    }

    @GetMapping("/composer/plans")
    public ResponseEntity<Map<String, Object>> getCurrentPlans() {
        List<String> profiles = List.of(
                "new:high-value", "new:low-value",
                "vip:any", "returning:high-value", "returning:low-value", "default"
        );

        Map<String, Object> plans = new LinkedHashMap<>();
        for (String profile : profiles) {
            String key  = "saga-plan:" + profile;
            String json = redisTemplate.opsForValue().get(key);
            if (json != null) {
                try {
                    plans.put(profile, new ObjectMapper().readValue(json, Map.class));
                } catch (Exception e) {
                    plans.put(profile, json);
                }
            }
        }
        return ResponseEntity.ok(plans);
    }

    @PostMapping("/composer/recompute")
    public ResponseEntity<Map<String, String>> recomputePlans() {
        sagaComposerService.recomputePlans();
        return ResponseEntity.ok(Map.of("status", "recomputed"));
    }
}
