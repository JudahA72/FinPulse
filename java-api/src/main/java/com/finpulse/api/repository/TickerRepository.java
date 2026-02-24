package com.finpulse.api.repository;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public class TickerRepository {

    private final JdbcTemplate jdbcTemplate;

    public TickerRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public List<String> findAllTickers() {
        String sql = "SELECT DISTINCT ticker FROM prices ORDER BY ticker";
        return jdbcTemplate.queryForList(sql, String.class);
    }
}