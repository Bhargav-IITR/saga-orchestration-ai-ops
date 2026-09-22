package com.learn.sagacommons.security;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ApiKeyFilterTest {

    private ApiKeyFilter filter;

    @BeforeEach
    void setUp() {
        filter = new ApiKeyFilter();
        ReflectionTestUtils.setField(filter, "configuredApiKey", "secret");
    }

    @Test
    void rejectsProtectedApiWithoutKey() throws Exception {
        var request = new MockHttpServletRequest("GET", "/api/events");
        var response = new MockHttpServletResponse();

        filter.doFilter(request, response, new MockFilterChain());

        assertEquals(401, response.getStatus());
    }

    @Test
    void acceptsProtectedApiWithMatchingKey() throws Exception {
        var request = new MockHttpServletRequest("GET", "/api/events");
        request.addHeader("X-API-Key", "secret");
        var response = new MockHttpServletResponse();

        filter.doFilter(request, response, new MockFilterChain());

        assertEquals(200, response.getStatus());
    }
}
