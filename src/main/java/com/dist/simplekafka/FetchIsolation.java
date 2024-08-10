package com.dist.simplekafka;

public enum FetchIsolation {
    FetchLogEnd,
    FetchHighWatermark,
    FetchTxnCommitted
}
