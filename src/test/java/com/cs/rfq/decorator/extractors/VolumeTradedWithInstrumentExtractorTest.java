package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import com.cs.rfq.decorator.TradeDataLoader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class VolumeTradedWithInstrumentExtractorTest extends AbstractSparkUnitTest {

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
    public void checkVolumeWhenAllTradesMatch() {

        VolumeTradedWithInstrumentExtractor extractor = new VolumeTradedWithInstrumentExtractor();
        extractor.setSince("2018-01-01");

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object result = meta.get(RfqMetadataFieldNames.volumeTradedYearToDateIn);

        assertEquals(2900000L, result);
    }

    @Test
    public void checkVolumeWhenNoTradesMatch() {

        //all test trade data are for 2018 so this will cause no matches
        VolumeTradedWithInstrumentExtractor extractor = new VolumeTradedWithInstrumentExtractor();
        extractor.setSince("2019-01-01");

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object result = meta.get(RfqMetadataFieldNames.volumeTradedYearToDateIn);

        assertEquals(2900000L, result);
    }

}
