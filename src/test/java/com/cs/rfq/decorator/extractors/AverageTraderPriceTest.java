package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import com.cs.rfq.decorator.TradeDataLoader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AverageTraderPriceTest extends AbstractSparkUnitTest{
    private Rfq rfq;
    Dataset<Row> trades;

    @BeforeEach
    public void setup() {
        rfq = new Rfq();
        rfq.setEntityId(5561279226039690843L);

        String filePath = getClass().getResource("volume-traded-1.json").getPath();
        trades = new TradeDataLoader().loadTrades(session, filePath);
    }

    @Test
    public void checkAverageWhenAllTradesMatch() {

        AverageTradedPrice extractor = new AverageTradedPrice();
        extractor.setSince("2018-06-08");

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object result = meta.get(RfqMetadataFieldNames.averageTradePrice);
        assertEquals(138.4396D, result);
    }

    @Test
    public void checkAverageWhenNoTradesMatch() {

        //all test trade data are for 2018 so this will cause no matches
        AverageTradedPrice extractor = new AverageTradedPrice();
        extractor.setSince("2019-01-01");

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object result = meta.get(RfqMetadataFieldNames.averageTradePrice);

        assertEquals(0L, result);
    }

}
