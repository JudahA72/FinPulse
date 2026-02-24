package com.finpulse.api.controller;

import com.finpulse.api.model.PriceRow;
import com.finpulse.api.service.PriceService;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
public class PriceController {

    private final PriceService priceService;

    public PriceController(PriceService priceService) {
        this.priceService = priceService;
    }

    @GetMapping("/prices/{ticker}")
    public List<PriceRow> prices(
            @PathVariable("ticker") String ticker,                 // <-- explicit
            @RequestParam(value = "start", required = false) String start, // <-- explicit
            @RequestParam(value = "end", required = false) String end,     // <-- explicit
            @RequestParam(value = "limit", required = false) Integer limit, // <-- explicit
            @RequestParam(value = "offset", required = false) Integer offset // <-- explicit
    ) {
        return priceService.getPrices(ticker, start, end, limit, offset);
    }
}