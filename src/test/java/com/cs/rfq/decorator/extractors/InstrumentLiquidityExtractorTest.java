package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import com.cs.rfq.decorator.TradeDataLoader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class InstrumentLiquidityExtractorTest extends AbstractSparkUnitTest{
    private Rfq rfq;
    Dataset<Row> trades;

    @BeforeEach
    public void setup() {
        rfq = new Rfq();
        rfq.setEntityId(5561279226039690843L);
        rfq.setIsin("AT0000A0VRQ6");

        String filePath = getClass().getResource("volume-traded-1.json").getPath();
        trades = new TradeDataLoader().loadTrades(session, filePath);
    }

    @Test
    public void checkVolumeWhenAllTradesMatch() {

        InstrumentLiquidityExtractor extractor = new InstrumentLiquidityExtractor();


        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object result1 = meta.get(RfqMetadataFieldNames.volumeBoughtForInstrument);
        Object result2 = meta.get(RfqMetadataFieldNames.volumeSoldForInstrument);

        assertEquals(850000L, result1);
        assertEquals(500000L, result2);
    }

}
