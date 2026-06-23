//! The typed bank account from `bank-account`, exposed durably on Restate from a
//! single `durable_object!` declaration.
//!
//! The same `CommandKind` + `Handles<C>` aggregate that runs in-process via
//! `declare_service!` is served as a Restate Virtual Object — `durable_object!`
//! generates the `#[restate_sdk::object]` trait and its server impl, each handler
//! submitting the command durably through `ctx.run`.
//!
//! Run it with:
//!
//! ```text
//! cargo run -p durable-account-example --bin durable-account
//! # then, with a running Restate server:
//! #   restate deployments register http://localhost:9080
//! #   curl localhost:8080/AccountObject/alice/open    --json '{"owner":"Alice"}'
//! #   curl localhost:8080/AccountObject/alice/deposit --json '{"amount":100}'
//! ```

use std::sync::Arc;

use restate_sdk::prelude::*;
use serde::{Deserialize, Serialize};
use streamlet::prelude::*;
use streamlet_restate::durable_object;

// --- Domain -----------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, DomainEvent)]
#[domain_event(prefix = "account.", rename_all = "snake_case")]
pub enum AccountEvent {
    Opened { owner: String },
    Deposited { amount: u64 },
    Withdrawn { amount: u64 },
}

#[derive(Debug, Clone, Serialize, Deserialize, CommandKind)]
#[command_kind(prefix = "account.")]
pub struct Open {
    pub owner: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, CommandKind)]
#[command_kind(prefix = "account.")]
pub struct Deposit {
    pub amount: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, CommandKind)]
#[command_kind(prefix = "account.")]
pub struct Withdraw {
    pub amount: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum AccountError {
    #[error("account is not open")]
    NotOpen,
    #[error("account is already open")]
    AlreadyOpen,
    #[error("insufficient funds: balance {balance}, requested {requested}")]
    InsufficientFunds { balance: u64, requested: u64 },
}

#[derive(Debug, Default)]
pub struct Account {
    open: bool,
    balance: u64,
}

impl Aggregate for Account {
    type Command = NoCommand;
    type Event = AccountEvent;
    type Rejection = AccountError;
    const TYPE: &'static str = "account";

    fn handle(&self, command: NoCommand) -> Result<Vec<AccountEvent>, AccountError> {
        match command {}
    }

    fn apply(&mut self, event: &AccountEvent) {
        match event {
            AccountEvent::Opened { .. } => self.open = true,
            AccountEvent::Deposited { amount } => self.balance += amount,
            AccountEvent::Withdrawn { amount } => self.balance -= amount,
        }
    }
}

impl Handles<Open> for Account {
    fn handle(&self, command: Open) -> Result<Vec<AccountEvent>, AccountError> {
        if self.open {
            return Err(AccountError::AlreadyOpen);
        }
        Ok(vec![AccountEvent::Opened {
            owner: command.owner,
        }])
    }
}

impl Handles<Deposit> for Account {
    fn handle(&self, command: Deposit) -> Result<Vec<AccountEvent>, AccountError> {
        if !self.open {
            return Err(AccountError::NotOpen);
        }
        Ok(vec![AccountEvent::Deposited {
            amount: command.amount,
        }])
    }
}

impl Handles<Withdraw> for Account {
    fn handle(&self, command: Withdraw) -> Result<Vec<AccountEvent>, AccountError> {
        if !self.open {
            return Err(AccountError::NotOpen);
        }
        if command.amount > self.balance {
            return Err(AccountError::InsufficientFunds {
                balance: self.balance,
                requested: command.amount,
            });
        }
        Ok(vec![AccountEvent::Withdrawn {
            amount: command.amount,
        }])
    }
}

// --- One declaration -> a full Restate Virtual Object -----------------------

durable_object! {
    /// One bank account per object key, served durably.
    pub object AccountObject for Account, store = MemoryStore {
        open     => Open,
        deposit  => Deposit,
        withdraw => Withdraw,
    }
}

#[tokio::main]
async fn main() {
    // A real deployment would use a persistent store (e.g. SqliteStore).
    let service = Arc::new(Service::<Account, _>::new(MemoryStore::new()));
    let server = AccountObjectServer::new(service);

    println!("serving AccountObject on http://0.0.0.0:9080 ...");
    HttpServer::new(Endpoint::builder().bind(server.serve()).build())
        .listen_and_serve("0.0.0.0:9080".parse().unwrap())
        .await;
}
