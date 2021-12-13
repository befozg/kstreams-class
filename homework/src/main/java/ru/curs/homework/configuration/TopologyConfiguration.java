package ru.curs.homework.configuration;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;
import ru.curs.counting.model.*;
import ru.curs.homework.transformer.ScoreTransformer;

import java.time.Duration;

import static ru.curs.counting.model.TopicNames.*;

@Configuration
public class TopologyConfiguration {
    public static final String BETTOR_AMOUNTS = "bettor-amounts";
    public static final String TEAM_AMOUNTS = "team-amounts";
    public static final String POSSIBLE_FRAUDS = "possible-frauds";

    @Autowired
    @Bean
    public Topology createTopology(StreamsBuilder streamsBuilder) {
        /*
        Необходимо создать топологию, которая имеет следующие три выходных топика:
           -- таблица, ключом которой является имя игрока,
           а значением -- сумма ставок, произведённых игроком
           -- таблица, ключом которой является имя команды,
            а значением -- сумма ставок на эту команду (ставки на "ничью" в подсчёте игнорируются)
           -- поток, ключом которого является имя игрока,
           а значениями -- подозрительные ставки.
           Подозрительными считаем ставки, произведённые в пользу команды
           в пределах одной секунды до забития этой командой гола.
         */

        //----------------------------------------------------------------
        //-------------------------raw bids stream------------------------
        //----------------------------------------------------------------
        KStream<String, Bet> rawBets = streamsBuilder.
                stream(BET_TOPIC,
                        Consumed.with(
                                        Serdes.String(),
                                        new JsonSerde<>(Bet.class)
                                )
                                .withTimestampExtractor(
                                        (record, previousTimestamp) -> ((Bet) record.value())
                                                .getTimestamp()
                                )
                );

        //----------------------------------------------------------------
        //---------------------raw goalscorers stream---------------------
        //----------------------------------------------------------------
        KStream<String, EventScore> rawScores = streamsBuilder.stream(EVENT_SCORE_TOPIC,
                Consumed.with(
                                Serdes.String(), new JsonSerde<>(EventScore.class)
                        )
                        .withTimestampExtractor(
                                (record, previousTimestamp) ->
                                        ((EventScore) record.value()).getTimestamp()
                        )
        );

        //----------------------------------------------------------------
        //---------------------1-Bets per Bettor---------------------
        //----------------------------------------------------------------
        KTable<String, Long> betsPerBettorTable
                = rawBets.map((k, v) -> KeyValue.pair(v.getBettor(), v.getAmount()))
                .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(Long.class)))
                .reduce((currentSum, v) -> {
                    Long sum = currentSum + v;
                    return sum;
                });

        //----------------------------------------------------------------
        //---------------------2-Bets per Team---------------------
        //----------------------------------------------------------------
        KTable<String, Long> betsPerTeamTable = rawBets
                .filter((k, v) -> v.getOutcome() != Outcome.D)
                .map((k, v) -> {
                    String[] commands = v.getMatch().split("-");
                    return (v.getOutcome() == Outcome.H) ? KeyValue.pair(commands[0], v.getAmount())
                            : KeyValue.pair(commands[1], v.getAmount());
                })
                .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(Long.class)))
                .reduce((currentSum, v) -> {
                    Long sum = currentSum + v;
                    return sum;
                });

        //----------------------------------------------------------------
        //------------------------3-Possible Frauds-----------------------
        //----------------------------------------------------------------

        KStream<String, Bet> wins = new ScoreTransformer().transformStream(streamsBuilder, rawScores);
        KStream<String, Fraud> possibleFrauds = rawBets.join(wins,
                (bet, winBet) -> Fraud.builder()
                        .bettor(bet.getBettor())
                        .outcome(bet.getOutcome())
                        .amount(bet.getAmount())
                        .match(bet.getMatch())
                        .odds(bet.getOdds())
                        .lag(winBet.getTimestamp() - bet.getTimestamp())
                        .build(),
                JoinWindows.of(Duration.ofSeconds(1)).before(Duration.ZERO),
                StreamJoined.with(Serdes.String(), new JsonSerde<>(Bet.class), new JsonSerde<>(Bet.class))
        ).selectKey((k, v) -> v.getBettor());

        betsPerBettorTable.toStream().to(BETTOR_AMOUNTS, Produced.with(Serdes.String(), Serdes.Long()));
        betsPerTeamTable.toStream().to(TEAM_AMOUNTS, Produced.with(Serdes.String(), Serdes.Long()));
        possibleFrauds.to(POSSIBLE_FRAUDS, Produced.with(Serdes.String(), new JsonSerde<>(Fraud.class)));

        Topology topology = streamsBuilder.build();
        System.out.println("========================================");
        System.out.println(topology.describe());
        System.out.println("========================================");

        return topology;
    }
}
