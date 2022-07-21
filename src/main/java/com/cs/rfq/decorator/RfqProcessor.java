package com.cs.rfq.decorator;

import com.cs.rfq.decorator.extractors.*;
import com.cs.rfq.decorator.publishers.MetadataJsonLogPublisher;
import com.cs.rfq.decorator.publishers.MetadataPublisher;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.sum;

public class RfqProcessor {

    private final static Logger log = LoggerFactory.getLogger(RfqProcessor.class);

    private final SparkSession session;

    private final JavaStreamingContext streamingContext;

    private Dataset<Row> trades;

    private final List<RfqMetadataExtractor> extractors = new ArrayList<>();

    private final MetadataPublisher publisher = new MetadataJsonLogPublisher();

    public RfqProcessor(SparkSession session, JavaStreamingContext streamingContext) {
        this.session = session;
        this.streamingContext = streamingContext;

        //TODO: use the TradeDataLoader to load the trade data archives
        TradeDataLoader loader = new TradeDataLoader();
        String filePath = "src/test/resources/trades/trades.json";
        trades = loader.loadTrades(session, filePath);

        //TODO: take a close look at how these two extractors are implemented
        extractors.add(new TotalTradesWithEntityExtractor());
        extractors.add(new VolumeTradedWithEntityYTDExtractor());
        extractors.add(new InstrumentLiquidityExtractor());
        extractors.add(new TotalTradesForInstrumentExtractor());
        extractors.add(new VolumeTradedWithInstrumentExtractor());
        extractors.add(new AverageTradedPrice());
        extractors.add(new TradeSideBias());
    }

    public void startSocketListener() throws InterruptedException {
        streamingContext.start();
        streamingContext.awaitTermination();
    }

    public void processRfq(Rfq rfq) {
        log.info(String.format("Received Rfq: %s", rfq.toString()));

        //create a blank map for the metadata to be collected
        Map<RfqMetadataFieldNames, Object> metadata = new HashMap<>();

        //TODO: get metadata from each of the extractors
        MetadataJsonLogPublisher metadataPublish = new MetadataJsonLogPublisher();
        for(RfqMetadataExtractor ex: extractors){
            metadataPublish.publishMetadata(ex.extractMetaData(rfq, session, trades));
        }
    }
}
