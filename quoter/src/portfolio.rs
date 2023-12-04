use ethers::prelude::*;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use std::collections::{HashMap, hash_map};
use std::sync::{Arc, Mutex, RwLock};
use std::cmp::{min, max};
use amm::{AMM, lb};
use tracing::{trace, debug, info, warn, error};
use crate::executor::*;
use std::time::{Instant, Duration};

#[derive(Clone, Debug)]
pub struct Portfolio {
    // #[serde(skip)]
    // amm: Arc<RwLock<lb::LB>>,
    pub token_x: Uuid,
    pub token_y: Uuid,

    pub x_decimals: usize,
    pub y_decimals: usize,

    // Actual smart contract balance
    pub x_balance: u128,
    pub y_balance: u128,

    // Balance portfolio can use
    pub y_free: u128,
    pub x_free: u128,

    // Bins with active liquidity and corresponding token balances
    pub positions: HashMap<u32, Bin>,
    // pub max_bid: f64,
    // pub min_ask: f64,
    // pub pos_ids: Vec<u32>,
    
    pub config: PortfolioConfig,
    bin_step: u16,
    last_fee_claim: Instant,
    last_gas_check: Instant,
    last_rebalance: Instant,
}

#[derive(PartialEq, Copy, Clone, Debug, Serialize, Deserialize)]
pub struct Bin {
    pub id: u32,
    pub x: u128,
    pub y: u128,
    pub tokens: u128,
}

#[derive(PartialEq, Clone, Copy, Debug, Serialize, Deserialize)]
pub struct PortfolioConfig {
    // Represents the minimum balance of token to stay delta neutral
    // Used when a token is borrowed
    pub token_x_delta: Option<u128>,
    pub token_y_delta: Option<u128>,

    // Our definition of a neglible amount of token
    pub token_x_dust: u128,
    pub token_y_dust: u128,

    // Keep this frac of balances within contract. Used to maintain reserves for taker orders
    pub token_x_reserve: f64,
    pub token_y_reserve: f64,

    // Only take orders that are at least this much better than fair
    pub taker_profit_bps: usize,
    // Remove existing liquidity if it is this much worse than fair
    pub maker_loss_bps: usize,

    // Max number of on chain txs in 1 minute
    pub tx_limit_5min: usize,

    // Max percent an asset can make up in a portfolio. Is > 0.5
    // Will stop trading other side if necessary
    pub max_skew: f64,
    // If assets percent larger than threshold, execute on chain rebalance
    // Is > max_skew
    pub taker_scaling_factor: f64,

    // Do not post new orders. Only cancel or close positions
    pub reduce_only: bool,
    // Pause
    pub pause: bool,

    // Minimum balance of gas before topping up
    pub min_gas: u128,

    // Coefficient for skewing price to favor one side
    pub px_skew_factor: f64,
    pub portfolio_skew_factor: f64,
    pub px_scaling_factor: f64,

    // Minutes between rebalances
    pub rebalance_interval: u64,

    pub take_gas_price_scaling: u64,
    pub gas_constant: u64,
    
}

impl Portfolio {
    pub fn new(
        // amm: Arc<RwLock<lb::LB>>,
        amm: &lb::LB,
        x_balance: u128,
        y_balance: u128,
        x_decimals: usize,
        y_decimals: usize,
        mut config: PortfolioConfig,
    ) -> Self {
        assert!(config.token_x_reserve < 1.0);
        assert!(config.token_y_reserve < 1.0);
        assert!(config.max_skew > 0.5, "max_skew must be > 0.5");
        assert!(config.taker_scaling_factor > 0.5, "taker_scaling_factor must be > 0.5");
        assert!(config.taker_profit_bps < 10000, "taker_profit_bps must be < 10000");
        assert!(config.maker_loss_bps < 10000, "maker_loss_bps must be < 10000");
        let token_x;
        let token_y;
        let bin_step;
        {
            // let unlocked = amm.read().unwrap();
            token_x = amm.token_x;
            token_y = amm.token_y;
            bin_step = amm.fee.bin_step;
        }
        
        Self {
            token_x,
            token_y,
            bin_step,
            // amm,
            x_balance,
            y_balance,
            x_decimals,
            y_decimals,
            x_free: x_balance,
            y_free: y_balance,
            positions: HashMap::new(),
            // pos_ids: Vec::new(),
            // max_bid: f64::NAN,
            // min_ask: f64::NAN,
            config,
            last_fee_claim: Instant::now() - Duration::from_secs(60*60),
            last_gas_check: Instant::now() - Duration::from_secs(60*60),
            last_rebalance: Instant::now(),
        }
    }

    pub fn on_state(&mut self, cex_bid: f64, cex_ask: f64, amm: &lb::LB) -> (Option<Execute>, u32) {
        if self.config.pause {
            return (None, 0);
        }

        let max_bid = cex_bid * (10000 + self.config.maker_loss_bps) as f64 / 10000.0;
        let min_ask = cex_ask * (10000 - self.config.maker_loss_bps) as f64 / 10000.0;
        // let max_bid = cex_bid;
        // let min_ask = cex_ask;
        let bid_threshold = cex_bid * (10000 - self.config.taker_profit_bps) as f64 / 10000.0;
        let ask_threshold = cex_ask * (10000 + self.config.taker_profit_bps) as f64 / 10000.0;
        let (active_id, active_bin, cur_bid, cur_ask) = {
            // let amm = self.amm.read().unwrap();
            let active_id = amm.active_id;
            let active_bin = amm.bins.get(&active_id).unwrap().clone();
            let (cur_bid, cur_ask) = amm.get_bid_ask(self.token_y, self.token_x, U256::exp10(self.x_decimals)).unwrap();

            (active_id, active_bin, cur_bid, cur_ask)
        };
        let cur_bid = cur_bid.as_u128() as f64 / 10.0_f64.powi(self.y_decimals as i32);
        let cur_ask = cur_ask.as_u128() as f64 / 10.0_f64.powi(self.y_decimals as i32);
        
        
        let px_128 = lb::Bin::getPriceFromId(active_id.into(), self.bin_step.into());
        let cur_mid = self.get_fpx(active_id);
        let cex_mid = (cex_bid + cex_ask) / 2.0;
        let px_skew = self.config.px_scaling_factor * ((cur_mid / cex_mid) - 1.0) + 0.5; // If > 0.5, we have too much token x and we want to dump. If <0.5 token x is underpriced and we want to buy
        let px_skew = 0.0_f64.max(px_skew).min(1.0);
        let mut x_deployed = 0;
        let mut y_deployed = 0;
        let mut positions = Vec::new();
        for (_, bin) in self.positions.iter() {
            positions.push(bin);
            x_deployed += bin.x;
            y_deployed += bin.y;
        };
        positions.sort_by(|a, b| a.id.cmp(&b.id));

        // X balance in terms of Y. Used for portfolio skew calculation
        let x_amt = x_deployed + self.x_free;
        let y_amt = y_deployed + self.y_free;
        let implied_x_value = Self::x_in_terms_of_y(px_128, x_amt.into()).as_u128();
        let x_skew = implied_x_value  as f64 / (implied_x_value as f64 + y_amt as f64) ;

        let directional_skew = (self.config.px_skew_factor * px_skew) + (self.config.portfolio_skew_factor * x_skew);
        let directional_skew = directional_skew / (self.config.px_skew_factor + self.config.portfolio_skew_factor);

        let total_value = implied_x_value.checked_add(y_amt).unwrap();

        let mut x_deployable_in_y = (min(implied_x_value, (total_value as f64 * directional_skew) as u128) as f64 * (1.0 - self.config.token_x_reserve)) as u128;
        let mut y_deployable = (min(y_amt, (total_value as f64 * (1.0 - directional_skew)) as u128) as f64 * (1.0 - self.config.token_y_reserve)) as u128;
        let mut x_deployable = Self::y_in_terms_of_x(px_128, x_deployable_in_y.into()).as_u128();
        info!(
            active_id = active_id, 
            cur_bid = cur_bid,
            cur_mid = cur_mid, 
            cur_ask = cur_ask,

            cex_bid = cex_bid,
            cex_mid = cex_mid, 
            cex_ask = cex_ask,

            max_bid = max_bid,
            min_ask = min_ask,
            bid_threshold = bid_threshold,
            ask_threshold = ask_threshold,

            px_skew = px_skew,
            y_amt = y_amt, 
            x_amt = x_amt,
            x_value = implied_x_value, 
            x_skew = x_skew,
            directional_skew = directional_skew,
            x_deployable = x_deployable,
            x_deployable_in_y = x_deployable_in_y,
            y_deployable = y_deployable,
        );

        let mut position_wanted = HashMap::new();
        if cur_bid < max_bid && cur_ask > min_ask {
            // Only add to active if the price is good
            let (active_x,active_y) = Self::get_ratio(x_deployable, y_deployable, active_bin.x.as_u128(), active_bin.y.as_u128());
            if let Some(my_active) = self.positions.get(&active_id) {
                info!(
                    active_bin= ?active_bin,
                    my_liquidity = my_active.tokens as f64 / *amm.supply.get(&active_id).unwrap() as f64,
                );
                position_wanted.insert(active_id, (active_x, active_y));
                x_deployable -= active_x;
                y_deployable -= active_y;
            } else if cur_bid < max_bid * 0.9999 && cur_ask > min_ask * 1.0001 {
                // Add additional 1bp threshold if we are not already in the active bin
                position_wanted.insert(active_id, (active_x, active_y));
                x_deployable -= active_x;
                y_deployable -= active_y;
            }
            
            
        } else if cur_ask < bid_threshold && x_skew < self.config.max_skew {
            // We are underpriced, we want to buy
            let scaling = (self.config.taker_scaling_factor * ((bid_threshold / cur_ask) - 1.0)) + 0.95;
            let target = (total_value as f64 * 1.0_f64.min(self.config.max_skew * scaling).max(x_skew)) as u128;
            let mut x_out = match target > implied_x_value {
                true => Self::y_in_terms_of_x(px_128, (target - implied_x_value).into()).as_u128(),
                false => 0,
            };
            if let Some(bin) = self.positions.get(&active_id) {
                // Pull liquidity and send taker order
                x_out = min(x_out, active_bin.x.as_u128() - bin.x);
            } else {
                // Send taker order only
                x_out = min(x_out, active_bin.x.as_u128());
            }
            info!(x_out = x_out, scaling = scaling, "DEX underpriced");
            if let Some(order) = self.make_take(amm, x_out, false) {
                return (Some(order), active_id)
            }
        } else if cur_bid > ask_threshold && x_skew > 1.0 - self.config.max_skew {
            // We are overpriced, we want to dump
            let scaling = (self.config.taker_scaling_factor * ((cur_bid / ask_threshold) - 1.0)) + 0.95;
            let target = (total_value as f64 * 1.0_f64.min(self.config.max_skew * scaling).max(1.0 - x_skew)) as u128;
            let mut y_out = match target > y_amt {
                true => (target - y_amt),
                false => 0,
            };
            if let Some(bin) = self.positions.get(&active_id) {
                // Pull liquidity and send taker order
                y_out = min(y_out, active_bin.y.as_u128() - bin.y);
            } else {
                // Send taker order only
                y_out = min(y_out, active_bin.y.as_u128());
            }
            info!(y_out = y_out, scaling = scaling, "DEX overpriced");
            if let Some(order) = self.make_take(amm, y_out, true) {
                return (Some(order), active_id)
            }
        }
        // Too much delta. Rebalance portfolio.
        if self.last_rebalance.elapsed() > std::time::Duration::from_secs(60 * self.config.rebalance_interval) {
            self.last_rebalance = Instant::now();
            if directional_skew > self.config.max_skew {
                // Too much x, sell some
                // Scaling should always be > 1
                let scaling = directional_skew / self.config.max_skew;
                let mut y_out = ((total_value as f64 * ((1.0 - self.config.max_skew) * scaling).min(0.5)) as u128 - y_amt);
                if let Some(bin) = self.positions.get(&active_id) {
                    // Pull liquidity and send taker order
                    y_out = min(y_out, active_bin.y.as_u128() - bin.y);
                } else {
                    // Send taker order only
                    y_out = min(y_out, active_bin.y.as_u128());
                }
                info!(y_out = y_out, scaling = scaling, "Too much X, rebalancing");
                if let Some(order) = self.make_take(amm, y_out, true) {
                    return (Some(order), active_id)
                }
            } else if directional_skew < 1.0 - self.config.max_skew {
                // Too much y, buy some
                // Scaling should always be > 1
                let scaling = (1.0 - directional_skew) / self.config.max_skew;
                let mut x_out = Self::y_in_terms_of_x(px_128, (((total_value as f64 * ((1.0 - self.config.max_skew) * scaling).min(0.5)) as u128 - implied_x_value)).into()).as_u128();
                if let Some(bin) = self.positions.get(&active_id) {
                    // Pull liquidity and send taker order
                    x_out = min(x_out, active_bin.x.as_u128() - bin.x);
                } else {
                    // Send taker order only
                    x_out = min(x_out, active_bin.x.as_u128());
                }
                info!(x_out = x_out, scaling = scaling, "Too much Y, rebalancing");
                if let Some(order) = self.make_take(amm, x_out, false) {
                    return (Some(order), active_id)
                }
            }
        }
        if let Some(bin) = position_wanted.get(&active_id).copied() {
            if bin.0 < self.config.token_x_dust && bin.1 > self.config.token_y_dust {
                for delta in 1..3 {
                    if self.get_fpx(active_id + delta) > min_ask {
                        position_wanted.insert(active_id + delta, (x_deployable, 0));
                        break;
                    }
                }
            }
            if bin.1 < self.config.token_y_dust && bin.0 > self.config.token_x_dust{
                for delta in 1..3 {
                    if self.get_fpx(active_id - delta) < max_bid {
                        position_wanted.insert(active_id - delta, (0, y_deployable));
                        break;
                    }
                }
            }
        }
        

        // position_wanted.insert(active_id + 1, (x_deployable, 0));
        // position_wanted.insert(active_id - 1, (0, y_deployable));
        info!(position_wanted = ?position_wanted);
        if self.config.reduce_only {
            info!("Reduce only mode, not sending any orders");
            position_wanted.clear();
        }

        (self.get_diff(position_wanted), active_id)
        

    }

    fn make_take(&self, amm: &lb::LB, amt_out: u128, swap_for_y: bool) -> Option<Execute> {
        if swap_for_y {
            if amt_out > self.config.token_y_dust {
                info!(y_out = amt_out, "Sending taker order to sell");
                
                let x_in = amm.get_amount_in(self.token_x, self.token_y, amt_out.into()).unwrap().as_u128();
                let y_out = amt_out - (2 * amt_out * self.config.taker_profit_bps as u128 / 10000);
                if self.positions.is_empty() {
                    return Some(
                        Execute::Take { amt_in: x_in, amt_out: y_out, swap_for_y: true }
                    )
                } else {
                    return Some(
                        Execute::CancelNTake { amt_in: x_in, amt_out: y_out, swap_for_y: true, orders: self.positions.iter().map(|(id, bin)| {(Tick::Exact(*id), bin.tokens)}).collect() }
                    )
                }
            }
        } else {
            if amt_out > self.config.token_x_dust {
                info!(x_out = amt_out, "Sending taker order to buy");
                let y_in = amm.get_amount_in(self.token_y, self.token_x, amt_out.into()).unwrap().as_u128();
                let x_out = amt_out - (2 * amt_out * self.config.taker_profit_bps as u128 / 10000);
                if self.positions.is_empty() {
                    return Some(
                        Execute::Take { amt_in: y_in, amt_out: x_out, swap_for_y: false }
                    )
                } else {
                    return Some(
                        Execute::CancelNTake { amt_in: y_in, amt_out: x_out, swap_for_y: false, orders: self.positions.iter().map(|(id, bin)| {(Tick::Exact(*id), bin.tokens)}).collect() }
                    )
                }
            }
        }
        None
    }

    fn get_diff(&mut self, positions_wanted: HashMap<u32, (u128, u128)>) -> Option<Execute> {
        let mut to_add = Vec::new();
        let mut to_cancel = Vec::new();
        for (id, (x, y)) in positions_wanted.iter() {
            if let Some(bin) = self.positions.get(id) {
                // Delete
                if *x == 0 && *y == 0 && (bin.x > self.config.token_x_dust || bin.y > self.config.token_y_dust) {
                    to_cancel.push((Tick::Exact(bin.id), bin.x, bin.y, bin.tokens));
                } else if x >= &bin.x && y >= &bin.y && (x - bin.x > self.config.token_x_dust || y - bin.y > self.config.token_y_dust) {
                    // Cur pos exists
                    to_add.push((Tick::Exact(bin.id), x - bin.x, y - bin.y));
                } else if x <= &bin.x && y <= &bin.y && (bin.x - x > self.config.token_x_dust || bin.y - y > self.config.token_y_dust) {
                    // Cur pos exists
                    let tokens;
                    if bin.x == 0 {
                        tokens = bin.tokens * (bin.y - y) / bin.y;
                    } else if bin.y == 0 {
                        tokens = bin.tokens * (bin.x - x) / bin.x;
                    } else {
                        tokens = max(bin.tokens * (bin.x - x) / bin.x, bin.tokens * (bin.y - y) / bin.y);
                    }
                    to_cancel.push((Tick::Exact(bin.id), bin.x - x, bin.y - y, tokens));
                }
            } else if x > &self.config.token_x_dust || y > &self.config.token_y_dust {
                // Cur pos does not exist and it's a signficant change
                to_add.push((Tick::Exact(*id), *x, *y));
            }
        }
        debug!(to_add_pre = ?to_add, to_cancel_pre = ?to_cancel);
        // Remove liquidity additions that are too small
        to_add = to_add.into_iter().filter_map(|(tick, x, y)| {
            if x > self.config.token_x_dust || y > self.config.token_y_dust {
                Some((tick, x, y))
            } else {
                None
            }
        }).collect();
        
        for (id, bin) in self.positions.iter() {
            if !positions_wanted.contains_key(id) {
                to_cancel.push((Tick::Exact(bin.id), bin.x, bin.y, bin.tokens));
            }
        }
        // If we do not add any liquidity, check if any cancellations are significant to warrant an update
        if to_add.is_empty() && !to_cancel.iter().any(|(_, x, y, _)| x > &self.config.token_x_dust || y > &self.config.token_y_dust) {
            to_cancel.clear();
        }

        match (!to_add.is_empty(), !to_cancel.is_empty()) {
            (true, true) => {
                return Some(
                    Execute::Move{
                        from: to_cancel.into_iter().map(|(tick, x, y, tokens)| (tick, tokens)).collect(),
                        to: to_add.into_iter().map(|(tick, x, y)| (tick, x, y)).collect(),
                    }
                )
            },
            (true, false) => {
                return Some(
                    Execute::Make(
                        to_add.into_iter().map(|(tick, x, y)| (tick, x, y)).collect(),
                    )
                )
            },
            (false, true) => {
                return Some(
                    Execute::Cancel(
                        to_cancel.into_iter().map(|(tick, x, y, tokens)| (tick, tokens)).collect(),
                    )
                )
            },
            (false, false) => {
                if self.last_fee_claim.elapsed() > std::time::Duration::from_secs(60*60) {
                    self.last_fee_claim = Instant::now();
                    return Some(Execute::Claim)
                }
                if self.last_gas_check.elapsed() > std::time::Duration::from_secs(60*10) {
                    self.last_gas_check = Instant::now();
                    return Some(Execute::CheckGas)
                }
            }
        }
        None
    }

    fn x_in_terms_of_y(px_128: U256, x_amt: U256) -> U256 {
        (px_128 * x_amt) >> 128
    }
    fn y_in_terms_of_x(px_128: U256, y_amt: U256) -> U256 {
        (y_amt << 128).div_mod(px_128).0
    }

    fn get_fpx(&self, active_id: u32) -> f64 {
        let decimal_diff = self.x_decimals - self.y_decimals;
        let base = (10000 + self.bin_step) as f64 / 10000.0;
        // info!(base = base, active_id = active_id, decimal_diff = decimal_diff, base_pow = base.powf((active_id-(1<<23)) as f64), dec_offset = 10u128.pow(decimal_diff as u32));
        if active_id < (1<<23) {
            return 10u128.pow(decimal_diff as u32) as f64 / base.powf(((1<<23) - active_id) as f64)
        }
        return base.powf((active_id - (1<<23)) as f64) * 10u128.pow(decimal_diff as u32) as f64
    }

    fn get_ratio(max_x: u128, max_y: u128, cur_x: u128, cur_y: u128) -> (u128, u128) {
        if cur_x == 0 {
            return (0, max_y);
        }
        if cur_y == 0 {
            return (max_x, 0);
        }
        let calculated_y = ((U256::from(max_x) * U256::from(cur_y)) / U256::from(cur_x)).as_u128();
        let calculated_x = ((U256::from(max_y)*U256::from(cur_x))/U256::from(cur_y)).as_u128();
        if calculated_x > max_x {
            (max_x, calculated_y)
        } else if calculated_y > max_y {
            (calculated_x, max_y)
        } else {
            error!(
                cal_x= calculated_x,
                max_x,
                cur_x,
                cal_y= calculated_y,
                max_y,
                cur_y,
                "Unreachable"
            );
            panic!("Unreachable")
        }
    }

}