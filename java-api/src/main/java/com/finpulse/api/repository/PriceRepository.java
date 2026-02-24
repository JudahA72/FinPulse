package com.finpulse.api.repository;

import com.finpulse.api.model.PriceRow;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;

@Repository
public class PriceRepository {

    private final JdbcTemplate jdbcTemplate;

    public PriceRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public List<PriceRow> findPrices(
            String ticker,
            String start,
            String end,
            int limit,
            int offset
    ) {
        StringBuilder sql = new StringBuilder(
                "SELECT ticker, date, open, high, low, close, adj_close, volume " +
                "FROM prices WHERE ticker = ?"
        );

        List<Object> params = new ArrayList<>();
        params.add(ticker);

        if (start != null && !start.isBlank()) {
            sql.append(" AND date >= ?");
            params.add(start);
        }

        if (end != null && !end.isBlank()) {
            sql.append(" AND date <= ?");
            params.add(end);
        }

        sql.append(" ORDER BY date ASC LIMIT ? OFFSET ?");
        params.add(limit);
        params.add(offset);

        return jdbcTemplate.query(
                sql.toString(),
                (rs, rowNum) -> new PriceRow(
                        rs.getString("ticker"),
                        rs.getString("date"),
                        rs.getObject("open", Double.class),
                        rs.getObject("high", Double.class),
                        rs.getObject("low", Double.class),
                        rs.getObject("close", Double.class),
                        rs.getObject("adj_close", Double.class),
                        rs.getObject("volume", Long.class)
                ),
                params.toArray()
        );
    }

    public boolean tickerExists(String ticker) {
        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(1) FROM prices WHERE ticker = ?",
                Integer.class,
                ticker
        );
        return count != null && count > 0;
    }
}
