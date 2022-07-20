package com.cs.rfq.decorator.extractors;

/**
 * Enumeration of all metadata that will be published by this component
 */
public enum RfqMetadataFieldNames {
    tradesWithEntityToday,
    tradesWithEntityPastWeek,
    tradesWithEntityPastYear,
    //add new field for month
    tradesWithEntityPastMonth,
    volumeTradedYearToDate,


    //add new fields for Instrument trades

    tradesForInstrumentToday,
    tradesForInstrumentPastWeek,
    tradesForInstrumentPastMonth,
    tradesForInstrumentPastYear,
    volumeTradedYearToDateIn,


}
