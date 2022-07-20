package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.Map;

import static com.cs.rfq.decorator.extractors.RfqMetadataFieldNames.*;

public class AverageTradedPrice implements RfqMetadataExtractor{

    private String since;
    public AverageTradedPrice() {
        this.since = DateTime.now().getWeekOfWeekyear() + "-01-01";
    }

    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {

        long todayMs = DateTime.now().withMillisOfDay(0).getMillis();
        long pastWeekMs = DateTime.now().withMillis(todayMs).minusWeeks(1).getMillis();

        String query = String.format("SELECT avg(LastPx) from trade where EntityId='%s' AND TradeDate >= '%s'",
                rfq.getEntityId(),
                since);

        trades.createOrReplaceTempView("trade");
        Dataset<Row> sqlQueryResults = session.sql(query);

        Object averageprice = sqlQueryResults.first().get(0);
        if (averageprice == null) {
            averageprice = 0L;
        }

        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        results.put(RfqMetadataFieldNames.averageTradePrice, averageprice);
        return results;

//        Dataset<Row> filtered = trades
//                //.filter(trades.col("SecurityId").equalTo(rfq.getIsin()))
//                .filter(trades.col("EntityId").equalTo(rfq.getEntityId()));
//
//       // long tradesToday = filtered.filter(trades.col("TradeDate").$greater(new java.sql.Date(todayMs))).count();
//       // long tradesPastWeek = filtered.filter(trades.col("TradeDate").$greater(new java.sql.Date(pastWeekMs))).count();
//
//        Dataset<Row> filteredPeriodToday=filtered.filter(trades.col("TradeDate").$greater(new java.sql.Date(todayMs)));
//        //double    filteredPeriodToday.col("LastPx").mean();
//        Dataset<Row> totalPricePastweek = filteredPeriodToday.groupBy("LastPx").avg("LastPx");
//
//
//        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
//        results.put(averageTradePrice, totalPricePastweek);
//        return results;
    }

    protected void setSince(String since) {
        this.since = since;
    }


}
