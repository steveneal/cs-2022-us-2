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

public class TotalTradesForInstrumentExtractorTest extends AbstractSparkUnitTest {

    private Rfq rfq;
    Dataset<Row> trades;


    @BeforeEach
    public void setup() {
        rfq = new Rfq();
        rfq.setTraderId(6915717929522265936L);
        rfq.setIsin("AT0000A001X2");

        String filePath = "src/test/resources/trades/trades.json";
        trades = new TradeDataLoader().loadTrades(session, filePath);
    }


    @Test
    public void checkTradesIn(){
        TotalTradesForInstrumentExtractor extractor = new TotalTradesForInstrumentExtractor();
        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);
        Object result0 = meta.get(RfqMetadataFieldNames.tradesForInstrumentPastMonth);
        Object result1 = meta.get(RfqMetadataFieldNames.tradesForInstrumentToday);
        Object result2 = meta.get(RfqMetadataFieldNames.tradesForInstrumentPastWeek);
        Object result3 = meta.get(RfqMetadataFieldNames.tradesForInstrumentPastYear);

        assertEquals(1L, result0);
        assertEquals(0L, result1);
        assertEquals(0L, result2);
        assertEquals(11L, result3);

    }

}

