package bamboo.crawl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Optional;

@RestController
public class TestController {
    @Autowired public CrawlRepository repository;

    @GetMapping("/foo")
    public Optional<Crawl> getFoo() {
        return repository.findById(1L);
    }
}
