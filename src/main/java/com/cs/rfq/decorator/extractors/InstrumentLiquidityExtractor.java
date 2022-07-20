package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

public class InstrumentLiquidityExtractor implements RfqMetadataExtractor {
    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {

        String queryBuy = String.format("SELECT sum(LastQty) from trade where (SecurityId='%s' and Side=1)",
                rfq.getIsin());
        String querySell = String.format("SELECT sum(LastQty) from trade where (SecurityId='%s' and Side=2)",
                rfq.getIsin());

        trades.createOrReplaceTempView("trade");
        Dataset<Row> sqlBuyVolume = session.sql(queryBuy);
        Dataset<Row> sqlSellVolume = session.sql(querySell);

        Object buyVolume = sqlBuyVolume.first().get(0);
        if (buyVolume == null) {
            buyVolume = 0L;
        }

        Object sellVolume = sqlSellVolume.first().get(0);
        if (sellVolume == null) {
            sellVolume = 0L;
        }

        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        results.put(RfqMetadataFieldNames.volumeBoughtForInstrument, buyVolume);
        results.put(RfqMetadataFieldNames.volumeSoldForInstrument, sellVolume);

        return results;
    }
}
