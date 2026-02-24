package com.finpulse.api.service;

import com.finpulse.api.model.PriceRow;
import com.finpulse.api.repository.PriceRepository;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class PriceService {

    private final PriceRepository priceRepository;

    public PriceService(PriceRepository priceRepository) {
        this.priceRepository = priceRepository;
    }

    public List<PriceRow> getPrices(String ticker, String start, String end, Integer limit, Integer offset) {
        String t = ticker.toUpperCase();

        int safeLimit = (limit == null) ? 100 : Math.min(Math.max(limit, 1), 500);
        int safeOffset = (offset == null) ? 0 : Math.max(offset, 0);

        // Basic existence check so API returns a clean 404 later
        if (!priceRepository.tickerExists(t)) {
            throw new IllegalArgumentException("TICKER_NOT_FOUND:" + t);
        }

        return priceRepository.findPrices(t, start, end, safeLimit, safeOffset);
    }
}