package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;

import java.sql.Date;
import java.util.HashMap;
import java.util.Map;

public class TradeSideBias implements RfqMetadataExtractor{
    private final java.sql.Date sincemonth;
    private final java.sql.Date sinceweek;
    private Object TradeSideBiasMTD;
    private Object TradeSideBiasWTD;
    public TradeSideBias()
    {

        long month = new DateTime().minusMonths(1).getMillis();
        this.sincemonth = new java.sql.Date(month);
        long week = new DateTime().minusWeeks(1).getMillis();
        this.sinceweek = new java.sql.Date(week);
    }
    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {
        String buyMonth = String.format("SELECT sum(LastQty) FROM trade WHERE EntityId='%s' AND SecurityId='%s' AND TradeDate >= '%s' AND Side=1",
                rfq.getEntityId(),
                rfq.getIsin(),
                sincemonth);
        String sellMonth = String.format("SELECT sum(LastQty) FROM trade WHERE EntityId='%s' AND SecurityId='%s' AND TradeDate >= '%s' AND Side=2",
                rfq.getEntityId(),
                rfq.getIsin(),
                sincemonth);

        trades.createOrReplaceTempView("trade");
        Object buysideMonth= session.sql(buyMonth).first().get(0);
        Object sellsideMonth = session.sql(sellMonth).first().get(0);
        if(buysideMonth==null&&sellsideMonth==null)
        {
            TradeSideBiasMTD = -1L;
        }
        else if(buysideMonth==null)
        {
            TradeSideBiasMTD = 0L;
        }
        else if(sellsideMonth==null)
        {
            TradeSideBiasMTD = Long.MAX_VALUE;
        }
        else
        {
            TradeSideBiasMTD = (long) buysideMonth/ (long) sellsideMonth;
        }
        String buyWeek = String.format("SELECT sum(LastQty) FROM trade WHERE EntityId='%s' AND SecurityId='%s' AND TradeDate >= '%s' AND Side=1",
                rfq.getEntityId(),
                rfq.getIsin(),
                sinceweek);
        String sellWeek = String.format("SELECT sum(LastQty) FROM trade WHERE EntityId='%s' AND SecurityId='%s' AND TradeDate >= '%s' AND Side=1",
                rfq.getEntityId(),
                rfq.getIsin(),
                sinceweek);
        Object buysideWeek= session.sql(buyWeek).first().get(0);
        Object sellsideWeek = session.sql(sellWeek).first().get(0);
        if(buysideWeek==null&&sellsideWeek==null)
        {
            TradeSideBiasWTD = -1L;
        }
        else if(buysideMonth==null)
        {
            TradeSideBiasWTD = 0L;
        }
        else if(sellsideMonth==null)
        {
            TradeSideBiasWTD = Long.MAX_VALUE;
        }
        else
        {
            TradeSideBiasWTD = (long) buysideWeek/ (long) sellsideWeek;
        }

        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        results.put(RfqMetadataFieldNames.tradeSideBiasMonthtoDate, TradeSideBiasMTD);
        results.put(RfqMetadataFieldNames.tradeSideBiasWeektoDate, TradeSideBiasWTD);
        return results;
    }
}
