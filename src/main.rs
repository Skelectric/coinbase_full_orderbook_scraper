use barter::{
    engine::{trader::Trader, Engine, Command},
    event::{Event, EventTx},
    execution::{
        simulated::{Config as ExecutionConfig, SimulatedExecution},
        Fees,
    },
    portfolio::{
        allocator::DefaultAllocator, portfolio::MetaPortfolio,
        repository::in_memory::InMemoryRepository, risk::DefaultRisk,
    },
    statistic::summary::{
        trading::{Config as StatisticConfig, TradingSummary},
        Initialiser,
    },
    strategy::example::{Config as StrategyConfig, RSIStrategy},
};
use barter_data::{
    builder::Streams,
    ExchangeId,
    model::{MarketEvent, subscription::SubKind},
};
use barter_integration::model::{Instrument, InstrumentKind, Market, Side};
use parking_lot::Mutex;
use std::{collections::HashMap, sync::Arc};
use std::pin::Pin;
use barter::data::live::MarketFeed;
use barter_data::model::DataKind;
use barter_data::model::subscription::Subscription;
use tokio::{signal, sync::mpsc};
use uuid::Uuid;
use colored::*;

#[tokio::main]
async fn main() {

    control::set_virtual_terminal(true).unwrap();

    // Create channel to distribute Commands to the Engine & it's Traders (eg/ Command::Terminate)
    let (command_tx, command_rx) = mpsc::channel(20);

    // Create Event channel to listen to all Engine Events in real-time
    let (event_tx, event_rx) = mpsc::unbounded_channel();
    let event_tx = EventTx::new(event_tx);

    // Generate unique identifier to associate an Engine's components
    let engine_id = Uuid::new_v4();

    let spot_eth_usd = Instrument::new("eth", "usd", InstrumentKind::Spot);

    // Create the Market(s) to be traded on (1-to-1 relationship with a Trader)
    let market = Market::new("coinbase", spot_eth_usd.clone());

    let subscription = Subscription::new(ExchangeId::Coinbase, spot_eth_usd,  SubKind::OrderBookL3Delta);

    let market_feed = MarketFeed::init(vec![subscription]).await;

    // Build global shared-state MetaPortfolio (1-to-1 relationship with an Engine)
    let portfolio = Arc::new(Mutex::new(
        MetaPortfolio::builder()
            .engine_id(engine_id)
            .markets(vec![market.clone()])
            .starting_cash(10_000.0)
            .repository(InMemoryRepository::new())
            .allocation_manager(DefaultAllocator {
                default_order_value: 100.0,
            })
            .risk_manager(DefaultRisk {})
            .statistic_config(StatisticConfig {
                starting_equity: 10_000.0,
                trading_days_per_year: 365,
                risk_free_return: 0.0,
            })
            .build_and_init()
            .expect("failed to build & initialise MetaPortfolio"),
    ));

    // Build Trader(s)
    let mut traders = Vec::new();

    // Create channel for each Trader so the Engine can distribute Commands to it
    let (trader_command_tx, trader_command_rx) = mpsc::channel(10);

    traders.push(
        Trader::builder()
            .engine_id(engine_id)
            .market(market.clone())
            .command_rx(trader_command_rx)
            .event_tx(event_tx.clone())
            .portfolio(Arc::clone(&portfolio))
            .data(market_feed.unwrap())
            .strategy(RSIStrategy::new(StrategyConfig { rsi_period: 14 }))
            .execution(SimulatedExecution::new(ExecutionConfig {
                simulated_fees_pct: Fees {
                    exchange: 0.1,
                    slippage: 0.05,
                    network: 0.0,
                },
            }))
            .build()
            .expect("failed to build trader"),
    );

    // Build Engine (1-to-many relationship with Traders)
    // Create HashMap<Market, trader_command_tx> so Engine can route Commands to Traders
    let trader_command_txs = HashMap::from([(market, trader_command_tx)]);

    let engine = Engine::builder()
        .engine_id(engine_id)
        .command_rx(command_rx)
        .portfolio(portfolio)
        .traders(traders)
        .trader_command_txs(trader_command_txs)
        .statistics_summary(TradingSummary::init(StatisticConfig {
            starting_equity: 1000.0,
            trading_days_per_year: 365,
            risk_free_return: 0.0,
        }))
        .build()
        .expect("failed to build engine");

    // Run Engine trading & listen to Events it produces. Also run command listening.
    tokio::spawn(send_commands_to_engine(command_tx));
    tokio::spawn(listen_to_engine_events(event_rx));
    engine.run().await;

}

fn pretty_print_trade(event: MarketEvent) {
    match event.kind {
        DataKind::Trade(trade) => {
            let (side, price, volume) = match trade.side {
                Side::Buy => {(
                    "Buy".green(),
                    format!("{:.2}", trade.price).green(),
                    format!("${:.2}", (trade.quantity * trade.price)).green(),
                )},
                Side::Sell => {(
                    "Sell".red(),
                    format!("{:.2}", trade.price).red(),
                    format!("${:.2}", (trade.quantity * trade.price)).red(),
                )},
            };
            let left_align = format!(
                "{} --- {} {} {}-{} at ${}",
                event.exchange_time,
                side,
                trade.quantity,
                event.instrument.base,
                event.instrument.quote,
                price,
            );
            println!("{:<80} {:>25}",left_align, volume);
        },
        _ => {}
    }
}

// Listen for commands to send to engine, i.e. Command::Terminate
async fn send_commands_to_engine(mut command_tx: mpsc::Sender<Command>) {
    tokio::select! {
        _ = signal::ctrl_c() => {
            let msg = "User initiated termination via Ctrl+C".to_string();
            command_tx.send(Command::Terminate(msg));
        },
    }
}


// Listen to Events that occur in the Engine. These can be used for updating event-sourcing,
// updating dashboard, etc etc.
async fn listen_to_engine_events(mut event_rx: mpsc::UnboundedReceiver<Event>) {
    while let Some(event) = event_rx.recv().await {
        match event {
            Event::Market(event) => {
                // Market Event occurred in Engine
                pretty_print_trade(event);
            }
            Event::Signal(signal) => {
                // Signal Event occurred in Engine
                println!("{signal:?}");
            }
            Event::SignalForceExit(_) => {
                // SignalForceExit Event occurred in Engine
            }
            Event::OrderNew(new_order) => {
                // OrderNew Event occurred in Engine
                println!("{new_order:?}");
            }
            Event::OrderUpdate => {
                // OrderUpdate Event occurred in Engine
            }
            Event::Fill(fill_event) => {
                // Fill Event occurred in Engine
                println!("{fill_event:?}");
            }
            Event::PositionNew(new_position) => {
                // PositionNew Event occurred in Engine
                println!("{new_position:?}");
            }
            Event::PositionUpdate(updated_position) => {
                // PositionUpdate Event occurred in Engine
                println!("{updated_position:?}");
            }
            Event::PositionExit(exited_position) => {
                // PositionExit Event occurred in Engine
                println!("{exited_position:?}");
            }
            Event::Balance(balance_update) => {
                // Balance update Event occurred in Engine
                println!("{balance_update:?}");
            }
        }
    }
}