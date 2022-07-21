package com.cs.rfq.decorator.extractors;

/**
 * Enumeration of all metadata that will be published by this component
 */
public enum RfqMetadataFieldNames {
    tradesWithEntityToday,
    tradesWithEntityPastWeek,
    tradesWithEntityPastYear,
    volumeTradedYearToDate,
    volumeBoughtForInstrument,
    volumeSoldForInstrument,
    tradesWithEntityPastMonth,
    tradeSideBiasMonthtoDate,
    tradeSideBiasWeektoDate,
    //add new fields for Instrument trades

    tradesForInstrumentToday,
    tradesForInstrumentPastWeek,
    tradesForInstrumentPastMonth,
    tradesForInstrumentPastYear,
    volumeTradedYearToDateIn,
    averageTradePrice
}
