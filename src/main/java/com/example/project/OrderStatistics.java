package com.example.project;

import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.LocalDate;

@Getter
@NoArgsConstructor
public class OrderStatistics {

    private String amount;

    private LocalDate date;

    @Builder
    public OrderStatistics(String amount, LocalDate date) {
        this.amount = amount;
        this.date = date;
    }
}
