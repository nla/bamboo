package bamboo;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

import static org.hamcrest.Matchers.containsString;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
public class WebTest {
    @Autowired
    private MockMvc mockMvc;

    @Test
    public void test() throws Exception {
        mockMvc.perform(get("/")).andExpect(status().isOk());
        mockMvc.perform(get("/series")).andExpect(status().isOk());
        mockMvc.perform(get("/crawls")).andExpect(status().isOk());
        mockMvc.perform(get("/collections")).andExpect(status().isOk());

        // series
        mockMvc.perform(get("/series/new")).andExpect(status().isOk());
        String testSeries = mockMvc.perform(post("/series/new")
                .param("name", "test series")
                .param("description", "test description"))
                .andExpect(status().is3xxRedirection())
                .andReturn().getResponse().getHeader("Location");
        mockMvc.perform(get(testSeries))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString("test description")));
        mockMvc.perform(get(testSeries + "/edit"))
                .andExpect(status().isOk());
        mockMvc.perform(post(testSeries + "/edit")
                .param("description", "new description"))
                .andExpect(status().is3xxRedirection());
        mockMvc.perform(get(testSeries))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString("new description")));

        // collection
        mockMvc.perform(get("/collections/new")).andExpect(status().isOk());
        String testCollection = mockMvc.perform(post("/collections/new")
                .param("name", "test collection")
                .param("description", "test description"))
                .andExpect(status().is3xxRedirection())
                .andReturn().getResponse().getHeader("Location");
        mockMvc.perform(get(testCollection))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString("test description")));
        mockMvc.perform(get(testCollection + "/edit"))
                .andExpect(status().isOk());
        mockMvc.perform(post(testCollection + "/edit")
                .param("description", "new description"))
                .andExpect(status().is3xxRedirection());
        mockMvc.perform(get(testCollection))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString("new description")));

    }
}
