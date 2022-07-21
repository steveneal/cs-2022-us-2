package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import com.cs.rfq.decorator.TradeDataLoader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.zookeeper.server.SessionTracker;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TotalTradesWithEntityExtractorTest extends AbstractSparkUnitTest {

    private Rfq rfq;
    Dataset<Row> trades;


    @BeforeEach
    public void setup() {
        rfq = new Rfq();
        rfq.setEntityId(5561279226039690843L);
        rfq.setIsin("AT0000A0VRQ6");

        String filePath = "src/test/resources/trades/trades.json";
        trades = new TradeDataLoader().loadTrades(session, filePath);
    }


    @Test
    public void checkTrades(){
        TotalTradesWithEntityExtractor extractor = new TotalTradesWithEntityExtractor();
        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);
        Object result0 = meta.get(RfqMetadataFieldNames.tradesWithEntityPastMonth);
        Object result1 = meta.get(RfqMetadataFieldNames.tradesWithEntityToday);
        Object result2 = meta.get(RfqMetadataFieldNames.tradesWithEntityPastWeek);
        Object result3 = meta.get(RfqMetadataFieldNames.tradesWithEntityPastYear);

        assertEquals(7L, result0);
        assertEquals(0L, result1);
        assertEquals(3L, result2);
        assertEquals(78L, result3);

    }

}
