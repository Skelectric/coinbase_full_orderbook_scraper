#![allow(dead_code)]

use barter_data::{
    builder::Streams,
    model::{MarketEvent, subscription::SubKind},
    ExchangeId,
};
use barter_integration::model::InstrumentKind;
use futures::StreamExt;

#[tokio::main]
async fn websocket() {
    // Initialise a `PublicTrade`, `Candle` & `OrderBook``MarketStream` for
    // `BinanceFuturesUsd`, `Ftx`, `Kraken` & `Coinbase`
    let streams = Streams::builder()
        .subscribe([
            (ExchangeId::Coinbase, "eth", "usd", InstrumentKind::Spot, SubKind::Trade),
         ])
        .init()
        .await
        .unwrap();

    // Join all exchange streams into a StreamMap
    // Note: Use `streams.select(ExchangeId)` to interact with the individual exchange streams!
    let mut joined_stream = streams.join_map::<MarketEvent>().await;

    while let Some((exchange, event)) = joined_stream.next().await {
        println!("Exchange: {}, MarketEvent: {:?}", exchange, event);
        println!("{:?}", event.kind);
    }
}