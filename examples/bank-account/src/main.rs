//! A bank-account demo built on `streamlet`'s strongly-typed command model.
//!
//! Where the counter example uses the enum `Command` + `execute` path, this one
//! shows the other end of the spectrum:
//!
//! * each command is its own type deriving [`CommandKind`];
//! * the aggregate implements [`Handles<C>`] once per command;
//! * a [`declare_service!`] block turns those into a typed `AccountService` whose
//!   methods read like an API and only accept the commands it actually handles
//!   (try calling `accounts.deposit(id, Withdraw(..))` — it will not compile).
//!
//! Run it with:
//!
//! ```text
//! cargo run -p bank-account-example --bin bank-account
//! cargo run -p bank-account-example --bin bank-account --features libsql
//! ```

use serde::{Deserialize, Serialize};
use streamlet::prelude::*;

// ---------------------------------------------------------------------------
// Events — `rename_all` + `prefix` give stable wire names like
// "account.opened", "account.deposited", ...
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, DomainEvent)]
#[domain_event(prefix = "account.", rename_all = "snake_case")]
pub enum AccountEvent {
    Opened { owner: String },
    Deposited { amount: u64 },
    Withdrawn { amount: u64 },
    Closed,
}

// ---------------------------------------------------------------------------
// Commands — one type each, each with a stable name from #[derive(CommandKind)].
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, CommandKind)]
#[command_kind(prefix = "account.")]
pub struct Open {
    pub owner: String,
}

#[derive(Debug, Clone, CommandKind)]
#[command_kind(prefix = "account.")]
pub struct Deposit(pub u64);

#[derive(Debug, Clone, CommandKind)]
#[command_kind(prefix = "account.")]
pub struct Withdraw(pub u64);

#[derive(Debug, Clone, CommandKind)]
#[command_kind(prefix = "account.")]
pub struct Close;

// ---------------------------------------------------------------------------
// Business-rule rejections — distinct from infrastructure errors.
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, thiserror::Error)]
pub enum AccountError {
    #[error("account is not open")]
    NotOpen,
    #[error("account is already open")]
    AlreadyOpen,
    #[error("amount must be greater than zero")]
    AmountMustBePositive,
    #[error("insufficient funds: balance {balance}, requested {requested}")]
    InsufficientFunds { balance: u64, requested: u64 },
    #[error("cannot close an account with a non-zero balance ({balance})")]
    NonZeroBalance { balance: u64 },
}

// ---------------------------------------------------------------------------
// Aggregate — typed-only, so `type Command = NoCommand`.
// ---------------------------------------------------------------------------

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
            AccountEvent::Closed => self.open = false,
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
    fn handle(&self, Deposit(amount): Deposit) -> Result<Vec<AccountEvent>, AccountError> {
        if !self.open {
            return Err(AccountError::NotOpen);
        }
        if amount == 0 {
            return Err(AccountError::AmountMustBePositive);
        }
        Ok(vec![AccountEvent::Deposited { amount }])
    }
}

impl Handles<Withdraw> for Account {
    fn handle(&self, Withdraw(amount): Withdraw) -> Result<Vec<AccountEvent>, AccountError> {
        if !self.open {
            return Err(AccountError::NotOpen);
        }
        if amount == 0 {
            return Err(AccountError::AmountMustBePositive);
        }
        if amount > self.balance {
            return Err(AccountError::InsufficientFunds {
                balance: self.balance,
                requested: amount,
            });
        }
        Ok(vec![AccountEvent::Withdrawn { amount }])
    }
}

impl Handles<Close> for Account {
    fn handle(&self, _: Close) -> Result<Vec<AccountEvent>, AccountError> {
        if !self.open {
            return Err(AccountError::NotOpen);
        }
        if self.balance != 0 {
            return Err(AccountError::NonZeroBalance {
                balance: self.balance,
            });
        }
        Ok(vec![AccountEvent::Closed])
    }
}

// ---------------------------------------------------------------------------
// A typed service, declared once. Every method is compile-time bound to the
// matching command type.
// ---------------------------------------------------------------------------

declare_service! {
    /// The operations a caller can perform on a bank account.
    pub service AccountService for Account {
        open => Open,
        deposit => Deposit,
        withdraw => Withdraw,
        close => Close,
    }
}

// ---------------------------------------------------------------------------
// A projection: total money held across every account.
// ---------------------------------------------------------------------------

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct LedgerView {
    pub accounts_opened: u64,
    pub total_balance: i128,
}

impl View for LedgerView {
    type Event = AccountEvent;
    const NAME: &'static str = "ledger";

    fn apply(&mut self, event: &Recorded<AccountEvent>) {
        match &event.payload {
            AccountEvent::Opened { .. } => self.accounts_opened += 1,
            AccountEvent::Deposited { amount } => self.total_balance += *amount as i128,
            AccountEvent::Withdrawn { amount } => self.total_balance -= *amount as i128,
            AccountEvent::Closed => {}
        }
    }
}

// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("== streamlet bank-account demo ==\n");
    run(MemoryStore::new(), "in-memory").await?;

    #[cfg(feature = "libsql")]
    {
        println!();
        run(SqliteStore::open_in_memory().await?, "libsql (sqlite)").await?;
    }

    Ok(())
}

async fn run<S>(store: S, label: &str) -> Result<(), Box<dyn std::error::Error>>
where
    S: EventStore + DocumentStore,
{
    println!("--- store: {label} ---");

    let accounts = AccountService::new(store);

    accounts
        .open(
            "alice",
            Open {
                owner: "Alice".into(),
            },
        )
        .await?;
    accounts.deposit("alice", Deposit(100)).await?;
    accounts.withdraw("alice", Withdraw(30)).await?;

    // The `entity` handle keeps the id pre-filled.
    let alice = accounts.service().entity("alice");
    println!("alice balance: {}", alice.state().await?.balance);

    // A business-rule rejection — never confused with an infrastructure error.
    match accounts.withdraw("alice", Withdraw(1_000)).await {
        Err(ServiceError::Rejected(rule)) => println!("rejected (business rule): {rule}"),
        Err(ServiceError::Store(err)) => println!("unexpected infrastructure error: {err}"),
        Ok(_) => println!("(unexpectedly accepted)"),
    }

    // A second account so the projection spans multiple streams.
    accounts
        .open(
            "bob",
            Open {
                owner: "Bob".into(),
            },
        )
        .await?;
    accounts.deposit("bob", Deposit(50)).await?;

    let ledger = replay_view::<LedgerView, _>(accounts.service().store()).await?;
    println!(
        "ledger @position {}: {} accounts, total balance {}",
        ledger.position, ledger.view.accounts_opened, ledger.view.total_balance
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// Pure given/when/then unit tests — no store required.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use streamlet::testing::Scenario;

    #[test]
    fn opening_a_fresh_account_emits_opened() {
        Scenario::<Account>::empty()
            .when_typed(Open {
                owner: "Alice".into(),
            })
            .then_events([AccountEvent::Opened {
                owner: "Alice".into(),
            }]);
    }

    #[test]
    fn cannot_open_twice() {
        Scenario::<Account>::given([AccountEvent::Opened {
            owner: "Alice".into(),
        }])
        .when_typed(Open {
            owner: "Alice".into(),
        })
        .then_rejected_with(AccountError::AlreadyOpen);
    }

    #[test]
    fn withdrawing_more_than_balance_is_rejected() {
        Scenario::<Account>::given([
            AccountEvent::Opened {
                owner: "Alice".into(),
            },
            AccountEvent::Deposited { amount: 40 },
        ])
        .when_typed(Withdraw(100))
        .then_rejected_with(AccountError::InsufficientFunds {
            balance: 40,
            requested: 100,
        });
    }

    #[test]
    fn deposit_into_open_account_emits_deposited() {
        Scenario::<Account>::given([AccountEvent::Opened {
            owner: "Alice".into(),
        }])
        .when_typed(Deposit(25))
        .then_events([AccountEvent::Deposited { amount: 25 }]);
    }

    #[test]
    fn event_names_are_prefixed_and_snake_cased() {
        assert_eq!(
            AccountEvent::event_types(),
            &[
                "account.opened",
                "account.deposited",
                "account.withdrawn",
                "account.closed"
            ]
        );
    }

    #[test]
    fn command_names_are_prefixed() {
        assert_eq!(Open::NAME, "account.Open");
        assert_eq!(Deposit::NAME, "account.Deposit");
    }
}
