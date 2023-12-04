use std::fs;
use std::sync::{Arc, Mutex, RwLock};
use tracing::{trace, debug, info, warn, error, Level};
use tracing_subscriber;
use tracing_appender;
use ethers::prelude::*;
use amm::{AMM, lb};
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use tracing_subscriber::{fmt};
use std::thread;
use std::time::{Instant, Duration};
use reqwest;
mod portfolio;
mod executor;
mod cex_feed;
use cex_feed::CexData;
use tokio::sync::watch;
use chrono::prelude::*;

#[derive(PartialEq, Clone, Debug, Serialize, Deserialize)]
pub struct Config {
    pub cex_param: CexFeedType,
    // pub cex_ticker: String,
    // pub secondary_ticker: Option<String>,
    pub wsrpc: String,
    pub archiverpc: String,
    pub heartbeat: String,
    pub executor_address: String,
    pub weth: String,
    pub owner_key: String,
    pub portfolio_config: portfolio::PortfolioConfig,
}
#[derive(PartialEq, Copy, Clone, Debug, Serialize, Deserialize)]
pub struct DisplayBin {
    pub id: u32,
    pub x: f64,
    pub y: f64,
    pub tokens: u128,
}

#[derive(PartialEq, Clone, Debug, Serialize, Deserialize)]
pub enum CexFeedType {
    BinanceBook{symbol1: String},
    BinanceTradeVWAP{symbol1: String, volume_threshold: f64},
    BinanceBookImpl{symbol1: String, symbol2: String},
    BinanceTradeVWAPImpl{symbol1: String, symbol2: String, volume_threshold1: f64, volume_threshold2: f64},
    KucoinBook{symbol1: String}
}

fn generate_cex_feed(param: CexFeedType, tx: watch::Sender<CexData>) {
    match param {
        CexFeedType::BinanceBook{symbol1} => {
            thread::spawn(move || {cex_feed::run_cex_feed(&symbol1, tx)});
        },
        CexFeedType::BinanceTradeVWAP{symbol1, volume_threshold} => {
            thread::spawn(move || {cex_feed::run_cex_feed_trade(&symbol1, tx, volume_threshold)});
        },
        CexFeedType::BinanceBookImpl{symbol1, symbol2} => {
            thread::spawn(move || {cex_feed::run_cex_feed_impl(&symbol1, &symbol2, tx)});
        },
        CexFeedType::BinanceTradeVWAPImpl{symbol1, symbol2, volume_threshold1, volume_threshold2} => {
            thread::spawn(move || {cex_feed::run_cex_feed_impl_trade(&symbol1, &symbol2, tx, volume_threshold1, volume_threshold2)});
        },
        CexFeedType::KucoinBook{symbol1} => {
            thread::spawn(move || {cex_feed::run_cex_feed_kucoin(symbol1, tx)});
        }
    }
}

#[tokio::main]
async fn main() {
    let file_appender = tracing_appender::rolling::daily("./log", "quoter.log");
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);
    tracing_subscriber::fmt()
    .json()
    .with_writer(non_blocking).init();

    let file = fs::File::open("config.json").unwrap();
    let mut config: Config = serde_json::from_reader(file).unwrap();
    let client = Arc::new({
            // connect to the network
            let client = Provider::<Ws>::connect(config.wsrpc).await.unwrap();
            let chain_id = client.get_chainid().await.unwrap();
    
            // this wallet's private key
            let wallet = config.owner_key
                .parse::<LocalWallet>().unwrap()
                .with_chain_id(chain_id.as_u64());
    
            SignerMiddleware::new(client, wallet)
        });
    let archive = Arc::new(Provider::<Http>::try_from(config.archiverpc).unwrap());
    let x_id = Uuid::new_v4();
    let y_id = Uuid::new_v4();

    let amm = lb::LB::new_empty(x_id, y_id);
    let address = config.executor_address.clone();
    let new_client = client.clone();
    let (tx, mut dex_rx) = watch::channel(amm.clone());
    tokio::spawn(async move {
        lb::LB::produce_new(amm, address.as_str(), archive, new_client.clone(), tx).await;
    });
    let (tx, mut cex_rx) = watch::channel(CexData::default());
    // let ticker = config.cex_ticker.clone();
    // if let Some(symbol2) = config.secondary_ticker.clone() {
    //     thread::spawn(move || {cex_feed::run_cex_feed_impl(&ticker, &symbol2, tx)});
    // } else if config.cex_ticker.contains("-") {
    //     let t = ticker.clone();
    //     tokio::spawn(cex_feed::run_cex_feed_kucoin(t, tx));
    // } else {
    //     thread::spawn(move || {cex_feed::run_cex_feed(&ticker, tx)});
    // }

    generate_cex_feed(config.cex_param.clone(), tx);
    tokio::spawn(heartbeat(config.heartbeat));
    assert!(dex_rx.changed().await.is_ok());
    let mut amm = dex_rx.borrow().clone();
    assert!(cex_rx.changed().await.is_ok());
    let mut cex = cex_rx.borrow().clone();

    let mut executor = executor::Executor::new(
        client,
        config.executor_address.parse::<Address>().unwrap(),
        config.weth.parse::<Address>().unwrap(),
        config.portfolio_config,
    ).await;
    let (mut x_amt, mut y_amt) = executor.get_balances().await;
    let mut mypositions = executor.get_liq_tokens(amm.bins.keys().cloned().collect()).await;
    let (x_dec, y_dec) = executor.get_decs().await;
    info!(x_balance = x_amt, y_balance = y_amt, "Executor balances");

    let mut portfolio = portfolio::Portfolio::new(
        &amm,
        x_amt,
        y_amt,
        x_dec,
        y_dec,
        config.portfolio_config,
    );
    
    let mut block_executed = 0;

    // let start = Utc.with_ymd_and_hms(2022, 12, 13, 13, 20, 0).unwrap();
    // let end = Utc.with_ymd_and_hms(2022, 12, 13, 13, 40, 0).unwrap();
    executor.execute(executor::Execute::CheckGas, 0).await;
    executor.execute(executor::Execute::CheckGas, 0).await;
    loop {
        tokio::select! {
            biased;
            res = dex_rx.changed() => {
                assert!(res.is_ok());
                amm = dex_rx.borrow().clone();
                
                cex = cex_rx.borrow().clone();
                info!(dex_block = amm.last_block, "DEX block");
            },
            res = cex_rx.changed() => {
                // assert!(res.is_ok());
                if !res.is_ok() {
                    error!(error=?res, "websocket disconnected");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    let tx;
                    (tx, cex_rx) = watch::channel(CexData::default());
                    // let ticker = config.cex_ticker.clone();
                    // if let Some(symbol2) = config.secondary_ticker.clone() {
                    //     thread::spawn(move || {cex_feed::run_cex_feed_impl(&ticker, &symbol2, tx)});
                    // } else if config.cex_ticker.contains("-") {
                    //     let t = ticker.clone();
                    //     tokio::spawn(cex_feed::run_cex_feed_kucoin(t, tx));
                    // } else {
                    //     thread::spawn(move || {cex_feed::run_cex_feed(&ticker, tx)});
                    // }
                    generate_cex_feed(config.cex_param.clone(), tx);
                    continue;
                }
                cex = cex_rx.borrow().clone();
                info!(cex = ?cex, "CEX data");
            },
            _ = tokio::time::sleep(Duration::from_secs(5)) => {
                let file = fs::File::open("config.json").unwrap();
                match serde_json::from_reader::<_, Config>(file) {
                    Ok(new_config) => {
                        if new_config.portfolio_config != config.portfolio_config {
                            info!(?new_config.portfolio_config, "New config");
                            config.portfolio_config = new_config.portfolio_config;
                            portfolio.config = new_config.portfolio_config;
                            executor.config = new_config.portfolio_config;
                        } else {
                            continue;
                        }
                    },
                    Err(e) => {
                        error!(?e, "Failed to read config");
                        continue;
                    }
                }
                
            }
        };

        // let now = Utc::now();
        // if now >= start && now < end && !portfolio.config.reduce_only {
        //     info!("Setting portfolio to reduce only");
        //     portfolio.config.reduce_only = true;
        // } else if now > end && portfolio.config.reduce_only {
        //     info!("Setting portfolio to normal");
        //     portfolio.config.reduce_only = false;
        // }

        if block_executed > amm.last_block {
            warn!(block_executed = block_executed, dex_block = amm.last_block, "DEX block not updated");
            continue;
        }

        let (action, id) = portfolio.on_state(cex.bid_px, cex.ask_px, &amm);
        if let Some(action) = action {
            if let Some(block) = executor.execute(action.clone(), id).await {
                block_executed = block;
            }
            (x_amt, y_amt) = executor.get_balances().await;
            mypositions = executor.get_liq_tokens((-10..10).map(|x| {(x + amm.active_id as i64) as u32}).collect()).await;
            // mypositions = executor.get_liq_tokens(amm.bins.keys().cloned().collect()).await;
            portfolio.x_balance = x_amt;
            portfolio.y_balance = y_amt;
            portfolio.x_free = x_amt;
            portfolio.y_free = y_amt;
            portfolio.positions = mypositions.iter().map(|(id, tokens)| {
                let total_tokens = amm.supply.get(id).unwrap();
                let bin = amm.bins.get(id).unwrap();
                (*id, portfolio::Bin {
                    id: *id,
                    x: ((bin.x * U256::from(*tokens)) / U256::from(*total_tokens)).as_u128(),
                    y: ((bin.y * U256::from(*tokens)) / U256::from(*total_tokens)).as_u128(),
                    tokens: *tokens,
                })
            }).collect();
        }
        
        info!(curid = id, block = amm.last_block, my_bins = ?portfolio.positions.iter().map(|(id, bin)| DisplayBin{ id: *id, x: bin.x as f64 / 10.0_f64.powi(x_dec as i32), y: bin.y as f64 / 10.0_f64.powi(y_dec as i32), tokens: bin.tokens}).collect::<Vec<DisplayBin>>());
    }
    

}

async fn heartbeat(url: String) {
    let client = reqwest::Client::new();
    loop {
        // Send a GET request to the URL
        let response = client.get(&url).send().await;

        // Wait for 30 seconds before sending the next request
        tokio::time::sleep(Duration::from_secs(30)).await;
    }
}