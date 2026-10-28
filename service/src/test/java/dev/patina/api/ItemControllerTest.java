package dev.patina.api;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import dev.patina.engine.PatinaEngine;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

@WebMvcTest(ItemController.class)
class ItemControllerTest {

    private static final int MAX = 1_048_576;

    @Autowired MockMvc mvc;
    @MockitoBean PatinaEngine engine;

    @Test
    void exactlyOneMiBIsAccepted() throws Exception {
        mvc.perform(put("/items/k").contentType(MediaType.APPLICATION_OCTET_STREAM).content(new byte[MAX]))
                .andExpect(status().isNoContent());

        verify(engine).put(any(), any());
    }

    @Test
    void oneByteOverIsRejectedBeforeReachingRust() throws Exception {
        mvc.perform(put("/items/k").contentType(MediaType.APPLICATION_OCTET_STREAM).content(new byte[MAX + 1]))
                .andExpect(status().isPayloadTooLarge());

        verify(engine, never()).put(any(), any());
    }
}
