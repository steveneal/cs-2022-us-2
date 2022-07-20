package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.Map;

public class TradeSideBias implements RfqMetadataExtractor{
    private String since;
    public TradeSideBias()
    {
        this.since = DateTime.now().getYear()+"-"+DateTime.now().getMonthOfYear()+ "-01";
    }
    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {
        String query = String.format("SELECT sum(LastQty) FROM trade WHERE EntityId='%s' AND SecurityId='%s' AND TradeDate >= '%s' AND Side=1",
                rfq.getEntityId(),
                rfq.getIsin(),
                since);

        trades.createOrReplaceTempView("trade");
        Dataset<Row> sqlQueryResults = session.sql(query);

        Object volume = sqlQueryResults.first().get(0);
        System.out.println(volume);
        if (volume == null) {
            volume = -1L;
        }
        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        results.put(RfqMetadataFieldNames.tradeSideBias, volume);
        return results;

    }

    public void setSince(String s) {
        this.since = since;
    }
}
