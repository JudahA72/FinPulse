package com.finpulse.api.service;

import com.finpulse.api.repository.TickerRepository;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class TickerService {

    private final TickerRepository tickerRepository;

    public TickerService(TickerRepository tickerRepository) {
        this.tickerRepository = tickerRepository;
    }

    public List<String> getTickers() {
        return tickerRepository.findAllTickers();
    }
}