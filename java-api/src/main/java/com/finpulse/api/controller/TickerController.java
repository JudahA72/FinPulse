package com.finpulse.api.controller;

import com.finpulse.api.service.TickerService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
public class TickerController {

    private final TickerService tickerService;

    public TickerController(TickerService tickerService) {
        this.tickerService = tickerService;
    }

    @GetMapping("/tickers")
    public List<String> tickers() {
        return tickerService.getTickers();
    }
}