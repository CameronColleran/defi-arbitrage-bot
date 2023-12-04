use binance::api::*;
use binance::model::*;
use binance::market::*;
use tokio::sync::watch;
use binance::websockets::*;
use std::sync::atomic::{AtomicBool};
use std::collections::VecDeque;

use tokio::sync::watch::Sender;
use ta::indicators::ExponentialMovingAverage;
use tracing::{debug, info, warn, error};
use std::{thread, time};

use kucoin_rs::tokio;
use kucoin_rs::failure;
use kucoin_rs::futures::{TryStreamExt, StreamExt};
use kucoin_rs::kucoin::client::{Kucoin, Credentials, KucoinEnv};
use kucoin_rs::kucoin::model::websocket::{Subscribe, KucoinWebsocketMsg, WSType, WSTopic, WSResp};

#[derive(PartialEq, Copy, Clone, Debug, Default)]
pub struct CexData {
    pub bid_px: f64,
    pub bid_sz: f64,
    pub ask_px: f64,
    pub ask_sz: f64,
}

pub fn run_cex_feed(symbol: &str, tx: Sender<CexData>)  {
    loop {
        let endpoints =vec![
            format!("{}@bookTicker", symbol.to_lowercase())
            ];

        let keep_running = AtomicBool::new(true);
        let mut last = String::new();
        let mut web_socket = WebSockets::new(|event: WebsocketEvent| {
            match event {
                WebsocketEvent::BookTicker(e) => {
                    if last != e.best_bid.clone() + &e.best_ask {
                        last = e.best_bid.clone() + &e.best_ask;
                        tx.send(CexData {
                            bid_px: e.best_bid.parse::<f64>().unwrap(),
                            bid_sz: e.best_bid_qty.parse::<f64>().unwrap(),
                            ask_px: e.best_ask.parse::<f64>().unwrap(),
                            ask_sz: e.best_ask_qty.parse::<f64>().unwrap(),
                        }).unwrap();
                    }
                },
                // WebsocketEvent::Trade(e) => {

                // },
                _ => (),
            }

            Ok(())
        });

        web_socket.connect_multiple_streams(&endpoints).unwrap(); // check error
        if let Err(e) = web_socket.event_loop(&keep_running) {
            error!(error=?e, "websocket error");
            panic!("web_socket error: {:?}", e);
        } else {
            warn!("web_socket event_loop exited. Restarting");
            thread::sleep(time::Duration::from_secs(5));
        }
        web_socket.disconnect().unwrap();
        // Sleep for 1 min
    }

}

pub fn run_cex_feed_trade(symbol: &str, tx: Sender<CexData>, threshold_volume: f64) {
    loop {
        let endpoints = vec![
            format!("{}@trade", symbol.to_lowercase()),
        ];

        let keep_running = AtomicBool::new(true);
        let mut recent_trades: VecDeque<(f64, f64)> = VecDeque::new();
        let mut current_volume = 0.0;
        let mut current_value = 0.0;

        let mut web_socket = WebSockets::new(|event: WebsocketEvent| {
            match event {
                WebsocketEvent::Trade(e) => {
                    let trade_volume = e.qty.parse::<f64>().unwrap();
                    let trade_price = e.price.parse::<f64>().unwrap();

                    // Add new trade to recent_trades
                    current_volume += trade_volume;
                    current_value += trade_volume * trade_price;
                    recent_trades.push_back((trade_volume, trade_price));

                    // Remove trades from the front until the remaining trades' volume is <= threshold_volume
                    while current_volume > threshold_volume && recent_trades.len() > 1 {
                        if let Some((removed_volume, removed_price)) = recent_trades.pop_front() {
                            current_volume -= removed_volume;
                            current_value -= removed_volume * removed_price;
                        }
                    }

                    // Calculate VWAP for the trades within the threshold volume
                    let vwap = current_value / current_volume;

                    tx.send(CexData {
                        bid_px: vwap,
                        bid_sz: std::f64::NAN,
                        ask_px: vwap,
                        ask_sz: std::f64::NAN,
                    }).unwrap();
                },
                _ => (),
            }

            Ok(())
        });

        web_socket.connect_multiple_streams(&endpoints).unwrap(); // check error
        if let Err(e) = web_socket.event_loop(&keep_running) {
            error!(error=?e, "websocket error");
            panic!("web_socket error: {:?}", e);
        } else {
            warn!("web_socket event_loop exited. Restarting");
            thread::sleep(time::Duration::from_secs(5));
        }
        web_socket.disconnect().unwrap();
        // Sleep for 1 min
    }
}

pub async fn run_cex_feed_kucoin(symbol: String, tx: Sender<CexData>)  {
    let api = Kucoin::new(KucoinEnv::Live, None).unwrap();
    let url = api.get_socket_endpoint(WSType::Public).await.unwrap();
    let mut ws = api.websocket();
    let subs = vec![WSTopic::Ticker(vec![symbol])];
    ws.subscribe(url, subs).await.unwrap();

    let mut last = String::new();
    // let msg = ws.try_next().await.unwrap().unwrap();
    // info!(msg = ?msg, "Connecting to kucoin");
    loop {
        let out = ws.try_next().await.unwrap();
        if let Some(KucoinWebsocketMsg::TickerMsg(msg)) = out {
            let e = msg.data;
            if last != e.best_bid.clone() + &e.best_ask {
                last = e.best_bid.clone() + &e.best_ask;
                tx.send(CexData {
                    bid_px: e.best_bid.parse::<f64>().unwrap(),
                    bid_sz: e.best_bid_size.parse::<f64>().unwrap(),
                    ask_px: e.best_ask.parse::<f64>().unwrap(),
                    ask_sz: e.best_ask_size.parse::<f64>().unwrap(),
                }).unwrap();
            }
        } else if let Some(KucoinWebsocketMsg::WelcomeMsg(msg)) = out {
            debug!(msg=?msg, "Kucoin ws message");
        } else {
            match out {
                Some(KucoinWebsocketMsg::WelcomeMsg(msg)) => debug!(msg=?msg, "Kucoin ws message"),
                Some(KucoinWebsocketMsg::PongMsg(msg)) => debug!(msg=?msg, "Kucoin ws message"),
                _ => {
                    error!(out=?out, "websocket error");
                    panic!("web_socket error: {:?}", out);
                }
            }
            
        }
    }

}

pub fn run_cex_feed_impl(symbol1: &str, symbol2: &str, tx: Sender<CexData>)  {
    // symbol1 is of the form TOKEN/USDT
    // symbol2 is of the form ETH/USDT
    // output is symbol1 price/ symbol 2 price = TOKEN/ETH
    loop {
        let endpoints =vec![
            format!("{}@bookTicker", symbol1.to_lowercase()),
            format!("{}@trade", symbol2.to_lowercase()),
            ];

        let keep_running = AtomicBool::new(true);
        let mut last_1 = CexData {
            bid_px: f64::NAN,
            bid_sz: f64::NAN,
            ask_px: f64::NAN,
            ask_sz: f64::NAN,
        };
        let mut last_2 = f64::NAN;
        // CexData {
        //     bid_px: f64::NAN,
        //     bid_sz: f64::NAN,
        //     ask_px: f64::NAN,
        //     ask_sz: f64::NAN,
        // };
        let mut last = CexData {
            bid_px: f64::NAN,
            bid_sz: f64::NAN,
            ask_px: f64::NAN,
            ask_sz: f64::NAN,
        };
        let mut web_socket = WebSockets::new(|event: WebsocketEvent| {
            match event {
                WebsocketEvent::BookTicker(e) => {

                    if e.symbol == symbol1 {
                        let bb = e.best_bid.parse::<f64>().unwrap();
                        let ba = e.best_ask.parse::<f64>().unwrap();
                        if bb != last_1.bid_px || ba != last_1.ask_px {
                            last_1 = CexData {
                                bid_px: bb,
                                bid_sz: e.best_bid_qty.parse::<f64>().unwrap(),
                                ask_px: ba,
                                ask_sz: e.best_ask_qty.parse::<f64>().unwrap(),
                            };
                        } else {
                            return Ok(())
                        }
                    } 
                    // else if e.symbol == symbol2 {
                    //     let bb = e.best_bid.parse::<f64>().unwrap();
                    //     let ba = e.best_ask.parse::<f64>().unwrap();
                    //     if bb != last_2.bid_px || ba != last_2.ask_px {
                    //         last_2 = CexData {
                    //             bid_px: bb,
                    //             bid_sz: e.best_bid_qty.parse::<f64>().unwrap(),
                    //             ask_px: ba,
                    //             ask_sz: e.best_ask_qty.parse::<f64>().unwrap(),
                    //         };
                    //     } else {
                    //         return Ok(())
                    //     }
                    // } else {
                    //     panic!("Unknown symbol");
                    // }
                    
                },
                WebsocketEvent::Trade(e) => {
                    if e.symbol == symbol2 {
                        last_2 = e.price.parse::<f64>().unwrap();
                    } else {
                        panic!("Unknown symbol");
                    }
                },
                _ => (),
            }
            if !last_1.bid_px.is_nan() && !last_2.is_nan() {
                let cur_bid = last_1.bid_px/last_2;
                let cur_ask = last_1.ask_px/last_2;
                if last.bid_px.is_nan() {
                    last = CexData {
                        bid_px: cur_bid,
                        bid_sz: f64::NAN,
                        ask_px: cur_ask,
                        ask_sz: f64::NAN,
                    };
                    tx.send(last.clone()).unwrap();
                } else if (last.bid_px-cur_bid).abs()/cur_bid > 0.0001 || (last.ask_px-cur_ask).abs()/cur_bid > 0.0001 {
                    last = CexData {
                        bid_px: cur_bid,
                        bid_sz: f64::NAN,
                        ask_px: cur_ask,
                        ask_sz: f64::NAN,
                    };
                    tx.send(last.clone()).unwrap();
                }
                
            }

            Ok(())
        });

        web_socket.connect_multiple_streams(&endpoints).unwrap(); // check error
        if let Err(e) = web_socket.event_loop(&keep_running) {
            error!(error=?e, "websocket error");
            panic!("web_socket error: {:?}", e);
        } else {
            warn!("web_socket event_loop exited. Restarting");
            thread::sleep(time::Duration::from_secs(5));
        }
        web_socket.disconnect().unwrap();
        // Sleep for 1 min
    }

}
pub fn run_cex_feed_impl_trade(symbol1: &str, symbol2: &str, tx: Sender<CexData>, threshold_volume1: f64, threshold_volume2: f64) {
    loop {
        let endpoints = vec![
            format!("{}@trade", symbol1.to_lowercase()),
            format!("{}@trade", symbol2.to_lowercase()),
        ];

        let keep_running = AtomicBool::new(true);
        let mut recent_trades1: VecDeque<(f64, f64)> = VecDeque::new();
        let mut recent_trades2: VecDeque<(f64, f64)> = VecDeque::new();
        let mut current_volume1 = 0.0;
        let mut current_volume2 = 0.0;
        let mut current_value1 = 0.0;
        let mut current_value2 = 0.0;

        let mut web_socket = WebSockets::new(|event: WebsocketEvent| {
            match event {
                WebsocketEvent::Trade(e) => {
                    let trade_volume = e.qty.parse::<f64>().unwrap();
                    let trade_price = e.price.parse::<f64>().unwrap();

                    if e.symbol == symbol1 {
                        current_volume1 += trade_volume;
                        current_value1 += trade_volume * trade_price;
                        recent_trades1.push_back((trade_volume, trade_price));

                        while current_volume1 > threshold_volume1 && recent_trades1.len() > 1 {
                            if let Some((removed_volume, removed_price)) = recent_trades1.pop_front() {
                                current_volume1 -= removed_volume;
                                current_value1 -= removed_volume * removed_price;
                            }
                        }
                    } else if e.symbol == symbol2 {
                        current_volume2 += trade_volume;
                        current_value2 += trade_volume * trade_price;
                        recent_trades2.push_back((trade_volume, trade_price));

                        while current_volume2 > threshold_volume2 && recent_trades2.len() > 1 {
                            if let Some((removed_volume, removed_price)) = recent_trades2.pop_front() {
                                current_volume2 -= removed_volume;
                                current_value2 -= removed_volume * removed_price;
                            }
                        }
                    } else {
                        panic!("Unknown symbol");
                    }
                },
                _ => (),
            }

            if !recent_trades1.is_empty() && !recent_trades2.is_empty() {
                let vwap1 = current_value1 / current_volume1;
                let vwap2 = current_value2 / current_volume2;
                let implied_bid = vwap1 / vwap2;
                let implied_ask = implied_bid;

                let cex_data = CexData {
                    bid_px: implied_bid,
                    bid_sz: f64::NAN,
                    ask_px: implied_ask,
                    ask_sz: f64::NAN,
                };
                tx.send(cex_data).unwrap();
            }

            Ok(())
        });

        web_socket.connect_multiple_streams(&endpoints).unwrap(); // check error
        if let Err(e) = web_socket.event_loop(&keep_running) {
            error!(error=?e, "websocket error");
            panic!("web_socket error: {:?}", e);
        } else {
            warn!("web_socket event_loop exited. Restarting");
            thread::sleep(time::Duration::from_secs(5));
        }
        web_socket.disconnect().unwrap();
        // Sleep for 1 min
    }
}
