use ethers::prelude::*;
use ethers::abi::AbiEncode;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use std::collections::HashMap;
use std::{convert::TryFrom, sync::{Arc, Mutex}};
use std::{thread, time, time::{Instant, Duration}};
use amm::{AMM, lb};
use tracing::{trace, debug, info, warn, error};

use crate::portfolio::{Bin, self};
abigen!(
    MM,
    "./src/MM.json",
    event_derives(serde::Deserialize, serde::Serialize)
);

abigen!(
    LBPair,
    "./src/LBPair.json",
    event_derives(serde::Deserialize, serde::Serialize)
);

abigen!(
    ERC20,
    "./src/ERC20.json",
    event_derives(serde::Deserialize, serde::Serialize)
);

abigen!(
    WETH,
    "./src/WETH.json",
    event_derives(serde::Deserialize, serde::Serialize)
);

// const RPC: &str = "wss://mainnet.infura.io/ws/v3/c60b0bb42f8a4c6481ecd229eddaca27";
#[derive(PartialEq, Clone, Debug, Serialize, Deserialize)]
pub enum Execute {
    Make(Vec<(Tick, u128, u128)>),
    Move{ 
        from: Vec<(Tick, u128)>,
        to: Vec<(Tick, u128, u128)>,
    },
    Cancel(Vec<(Tick, u128)>),
    Take{
        amt_in: u128,
        amt_out: u128,
        swap_for_y: bool,
    },
    CancelNTake{
        amt_in: u128,
        amt_out: u128,
        swap_for_y: bool,
        orders: Vec<(Tick, u128)>,
    },
    Claim,
    CheckGas,
}
#[derive(PartialEq, Clone, Debug, Serialize, Deserialize)]
pub enum Tick {
    Delta(i32),
    Exact(u32),
}

#[derive(PartialEq, Clone, Debug, Serialize, Deserialize)]
pub enum Amount {
    Notional(u128),
    Exact(u128, u128),
    All,
}


#[derive(Clone, Debug)]
pub struct Executor <M> {
    pub address: Address,
    pub pair_address: Address,
    pub config: portfolio::PortfolioConfig,
    client: Arc<M>,
    mm: MM<M>,
    pair: LBPair<M>,
    x: ERC20<M>,
    y: ERC20<M>,
    weth: WETH<M>,
    inflight: HashMap<u32, Execute>,
    consecutive_failures: u32,
    bins_touched: HashMap<u32, bool>,
    sent_ts: Vec<Instant>,
}

impl <M: Middleware> Executor <M> {
    pub async fn new(client: Arc<M>, address: Address, weth_address: Address, config: portfolio::PortfolioConfig) -> Self {

        assert!(config.tx_limit_5min > 0, "tx_limit_1min must be greater than 0");

        let mm = MM::new(address, client.clone());
        let pair_address = mm.lb_pair().call().await.unwrap();
        let pair = LBPair::new(pair_address, client.clone());
        let x = ERC20::new(mm.x().call().await.unwrap(), client.clone());
        let y = ERC20::new(mm.y().call().await.unwrap(), client.clone());
        let weth = WETH::new(weth_address, client.clone());
        Self {
            address,
            pair_address,
            config,
            client,
            mm,
            x,
            y,
            weth,
            pair,
            inflight: HashMap::new(),
            consecutive_failures: 0,
            bins_touched: HashMap::new(),
            sent_ts: Vec::new(),
        }
    }

    pub async fn get_balances(&self) -> (u128, u128) {
        let x_bal = self.x.balance_of(self.address).call().await.unwrap();
        let y_bal = self.y.balance_of(self.address).call().await.unwrap();
        (x_bal.as_u128(), y_bal.as_u128())
    }

    pub async fn get_decs(&self) -> (usize, usize) {
        let x_bal = self.x.decimals().call().await.unwrap();
        let y_bal = self.y.decimals().call().await.unwrap();
        (x_bal as usize, y_bal  as usize)
    }

    pub async fn get_liq_tokens(&self, bin_ids: Vec<u32>) -> HashMap<u32, u128> {
        let mut positions = HashMap::new();
        for ids in bin_ids.chunks(50) {
            self.mm.my_bins(
                ids.into_iter().map(|x| U256::from(*x)).collect()
            ).call().await.unwrap().iter().zip(ids.iter())
            .for_each(|(amt, id)| {
                if !amt.is_zero() {
                    positions.insert(*id, amt.as_u128());
                }
            });
        }
        positions
    }

    pub async fn get_supply(&self, bin_ids: Vec<u32>) -> HashMap<u32, u128> {
        let mut positions = HashMap::new();
        for ids in bin_ids.chunks(50) {
            self.mm.supply(
                ids.into_iter().map(|x| U256::from(*x)).collect()
            ).call().await.unwrap().iter().zip(ids.iter())
            .for_each(|(amt, id)| {
                positions.insert(*id, amt.as_u128());
            });
        }
        positions
    }

    pub async fn execute(&mut self, todo: Execute, curid: u32)  -> Option <u64> {
        info!(todo = ?todo, tick = curid);
        let mut call = match todo.clone() {
            Execute::Make(orders) => {
                let mut amount_x = 0;
                let mut amount_y = 0;
                let mut bin_ids = Vec::new();
                let mut distribution_x = Vec::new();
                let mut distribution_y = Vec::new();
                for (tick, x, y) in orders {
                    amount_x += x;
                    amount_y += y;
                    let tick = match tick {
                        Tick::Delta(delta) => ((curid as i32) + delta) as u32,
                        Tick::Exact(tick) => tick,
                    };
                    bin_ids.push(U256::from(tick));
                    self.bins_touched.insert(tick, true);
                    distribution_x.push(U256::from(x) * U256::exp10(18));
                    distribution_y.push(U256::from(y) * U256::exp10(18));
                }
                distribution_x.iter_mut().for_each(|x| if !x.is_zero() {*x /= U256::from(amount_x)});
                distribution_y.iter_mut().for_each(|x| if !x.is_zero()  {*x /= U256::from(amount_y)});
                self.mm.make(curid.into(), amount_x.into(), amount_y.into(), bin_ids, distribution_x, distribution_y)
            },
            Execute::Move{from, to} => {
                let mut amount_x = 0;
                let mut amount_y = 0;
                let mut amounts = Vec::new();
                let mut ids_in = Vec::new();
                let mut ids_out = Vec::new();
                let mut distribution_x = Vec::new();
                let mut distribution_y = Vec::new();
                for (tick, x, y) in to {
                    amount_x += x;
                    amount_y += y;
                    let tick = match tick {
                        Tick::Delta(delta) => ((curid as i32) + delta) as u32,
                        Tick::Exact(tick) => tick,
                    };
                    ids_in.push(U256::from(tick));
                    self.bins_touched.insert(tick, true);
                    distribution_x.push(U256::from(x) * U256::exp10(18));
                    distribution_y.push(U256::from(y) * U256::exp10(18));
                }
                distribution_x.iter_mut().for_each(|x| if !x.is_zero() {*x /= U256::from(amount_x)});
                distribution_y.iter_mut().for_each(|x| if !x.is_zero() {*x /= U256::from(amount_y)});
                let meta = Meta {
                    curid: curid.into(),
                    amount_x: amount_x.into(),
                    amount_y: amount_y.into(),
                };

                for (tick, amount) in from {
                    assert!(amount != 0 && amount != u128::MAX);
                    let tick = match tick {
                        Tick::Exact(tick) => tick,
                        Tick::Delta(delta) => panic!("can't cancel delta ticks"),
                    };
                    ids_out.push(U256::from(tick));
                    amounts.push(U256::from(amount));
                }

                self.mm.move_(meta, ids_in, distribution_x, distribution_y, ids_out, amounts)
            },
            Execute::Cancel(orders) => {
                let mut ids = Vec::new();
                let mut amounts = Vec::new();
                for (tick, amount) in orders {
                    assert!(amount != 0 && amount != u128::MAX);
                    let tick = match tick {
                        Tick::Exact(tick) => tick,
                        Tick::Delta(delta) => panic!("can't cancel delta ticks"),
                    };
                    ids.push(U256::from(tick));
                    amounts.push(U256::from(amount));
                }
                self.mm.cancel(ids, amounts)
            },
            Execute::Take{amt_in, amt_out, swap_for_y} => {
                match swap_for_y {  
                    true => self.mm.take(curid.into(), amt_in.into(), amt_out.into(), swap_for_y),
                    false => self.mm.take(curid.into(), amt_out.into(), amt_in.into(), swap_for_y),
                }
                
            },
            Execute::CancelNTake{amt_in, amt_out, swap_for_y, orders} => {
                let mut ids = Vec::new();
                let mut amounts = Vec::new();
                for (tick, amount) in orders {
                    assert!(amount != 0 && amount != u128::MAX);
                    let tick = match tick {
                        Tick::Exact(tick) => tick,
                        Tick::Delta(delta) => panic!("can't cancel delta ticks"),
                    };
                    ids.push(U256::from(tick));
                    amounts.push(U256::from(amount));
                }
                match swap_for_y {
                    true => self.mm.cancel_n_take(curid.into(), amt_in.into(), amt_out.into(), swap_for_y, ids, amounts),
                    false => self.mm.cancel_n_take(curid.into(), amt_out.into(), amt_in.into(), swap_for_y, ids, amounts),
                }
            },
            Execute::Claim => {
                let mut to_claim = self.get_liq_tokens(
                    self.bins_touched.keys().cloned().collect()
                ).await.iter().filter_map(
                    |(id, tokens)| if *tokens == 0 {None} else {Some(U256::from(*id))}
                ).collect::<Vec<U256>>();
                to_claim.sort();
                to_claim.dedup();
                
                let (total_x, total_y) = self.pair.pending_fees(self.address, to_claim.clone()).call().await.unwrap();
                info!(to_claim = ?to_claim, total_x = ?total_x, total_y = ?total_y, "claiming");
                // let bins = self.bins_touched.keys().cloned().map(|x| U256::from(x)).collect::<Vec<U256>>();
                // let mut to_claim = Vec::new();
                // let mut total_x = U256::zero(); 
                // let mut total_y = U256::zero();
                // for tick in bins {
                //     let (x,y) = self.pair.pending_fees(self.address, vec![tick]).call().await.unwrap();
                //     if !x.is_zero() || !y.is_zero() {
                //         total_x += x;
                //         total_y += y;
                //         to_claim.push(tick);
                //     }
                // }
                if total_x.as_u128() < self.config.token_x_dust && total_y.as_u128() < self.config.token_y_dust {
                    return None;
                }

                self.bins_touched.clear();
                // let calldata = self.pair.collect_fees(self.address, to_claim).encode().unwrap();
                let calldata = LBPairCalls::CollectFees(CollectFeesCall{account: self.address, ids: to_claim}).encode();
                // let calldata = calldata.encode();
                let calldata = vec![mm::Call{target: self.pair_address, call_data: calldata.into()}];
                let call = self.mm.execute(calldata);
                // let call = call.gas_price(self.client.get_gas_price().await.unwrap() * self.config.take_gas_price_scaling / 100);
                let hash = call.send().await.unwrap();
                info!(tx_hash = ?hash, "Submitted fee claim");
                return self.deal_with_tx(*hash).await;
            },
            Execute::CheckGas => {
                let contract_caller = self.client.default_sender().unwrap();
                let gas_balance = self.client.get_balance(contract_caller, None).await.unwrap();
                let to_refill = self.config.min_gas * 2;
                if gas_balance.as_u128() < self.config.min_gas {
                    info!(gas_balance = ?gas_balance, "Topping up gas");
                    if self.weth.balance_of(contract_caller).call().await.unwrap().as_u128() < to_refill {
                        let call = self.weth.transfer_from(self.address, contract_caller, to_refill.into());
                        let hash = call.send().await.unwrap();
                        info!(tx_hash = ?hash, "Submitted weth deposit");
                        self.deal_with_tx(*hash).await;
                    } else {
                        let call = self.weth.withdraw(to_refill.into());
                        let hash = call.send().await.unwrap();
                        info!(tx_hash = ?hash, "Submitted weth withdraw");
                        self.deal_with_tx(*hash).await;
                    }
                }
                return None;
            }
        };
        // panic!("Killing");
        match todo {
            Execute::Make(_) | Execute::Move { .. } => {
                if self.sent_ts.len() > self.config.tx_limit_5min {
                    let earliest = self.sent_ts.first().unwrap();
                    if earliest.elapsed() < Duration::from_secs(300) {
                        warn!(sent_ts = ?self.sent_ts, "On Chain Tx limit reached");
                        return None;
                    } else {
                        self.sent_ts.remove(0);
                    }
                }
            },
            _ => {
                match todo {
                    Execute::Take{..} | Execute::CancelNTake { .. } => {
                        call = call.gas_price(self.client.get_gas_price().await.unwrap() * self.config.take_gas_price_scaling / 100);
                    },
                    _ => {
                        call = call.gas_price(self.client.get_gas_price().await.unwrap());
                    }
                }
                if self.sent_ts.len() > self.config.tx_limit_5min {
                    self.sent_ts = self.sent_ts.as_slice()[self.sent_ts.len()-self.config.tx_limit_5min..].to_vec();
                }
            }
        }
        
        self.sent_ts.push(Instant::now());
        let tx = call.tx.clone();
        let call = call.gas(U256::from(self.config.gas_constant));
        match call
        .send().await {
            Ok(hash) => {
                // record the tx
                info!(tx_hash = ?hash, "Submitted tx");
                return self.deal_with_tx(*hash).await
            }
            Err(err) => {
                let err = err.to_string();
                error!(err = ?err, tx = ?tx, "Failed to submit on chain tx");
            }
        };
        None
    }

    async fn deal_with_tx(&mut self,hash: TxHash) -> Option<u64> {
        let start = Instant::now();
        loop {
            let receipt = self.client
            .get_transaction_receipt(hash)
            .await
            .map_err(ContractError::MiddlewareError::<M>).unwrap();
            
            if let Some(receipt) = receipt {
                // Mined
                if receipt.status == Some(1.into()) {
                    info!(tx_hash = ?hash, block = receipt.block_number.unwrap().as_u64(), "Tx mined successfully");
                    self.consecutive_failures = 0;
                    return Some(receipt.block_number.unwrap().as_u64());
                } else {
                    error!(txhash = ?hash, "Transaction failed");
                    self.consecutive_failures += 1;
                    if self.consecutive_failures > 20 {
                        panic!("Too many consecutive failures");
                    }
                    // thread::sleep(time::Duration::from_secs(30*(self.consecutive_failures*self.consecutive_failures) as u64));
                };
                break;
            }
            if start.elapsed() > Duration::from_secs(60) {
                error!(txhash = ?hash, "Transaction timed out");
                break;
            }
            debug!(tx_hash = ?hash, "Waiting for tx to be mined");
            thread::sleep(time::Duration::from_millis(50));
        }
        None
        
    }
}