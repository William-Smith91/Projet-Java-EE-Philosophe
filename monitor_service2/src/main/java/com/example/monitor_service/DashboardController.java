package com.example.monitor_service;

import java.util.List;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.server.ResponseStatusException;

@Controller
public class DashboardController {

    private final EventStore store;
    private final String title;

    public DashboardController(EventStore store, @Value("${monitor.title}") String title) {
        this.store = store;
        this.title = title;
    }

    @GetMapping("/")
    public String dashboard(Model model) {
        model.addAttribute("title", title);
        model.addAttribute("rows", store.summaries());
        return "dashboard";
    }

    @GetMapping("/philosophers/{id}")
    public String detail(@PathVariable String id, Model model) {
        List<EventView> events = store.history(id);
        if (events.isEmpty()) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Philosophe inconnu");
        }
        model.addAttribute("title", title);
        model.addAttribute("id", id);
        model.addAttribute("events", events);
        return "detail";
    }
}