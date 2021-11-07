package ru.curs.homework.util;

import lombok.Builder;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import ru.curs.counting.model.Outcome;

@Data
@Builder
@RequiredArgsConstructor
public class MatchesHistory {
    private final String match;
    private final Outcome outcome;
}
