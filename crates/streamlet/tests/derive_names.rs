//! Verifies the derive macros produce stable, consistent names — including the
//! container `prefix` and per-variant `rename` knobs.

use serde::{Deserialize, Serialize};
use streamlet::{Command, DomainEvent};

#[derive(Clone, Serialize, Deserialize, DomainEvent)]
#[domain_event(prefix = "acct.")]
enum AccountEvent {
    Opened,
    Deposited { cents: u64 },
    Withdrew(u64),
    #[event(rename = "Closed")]
    Terminated,
}

#[derive(Command)]
#[allow(dead_code)] // variant payloads exist only to exercise naming
enum AccountCommand {
    Open,
    Deposit(u64),
    #[command(rename = "Close")]
    Terminate,
}

#[test]
fn event_names_use_prefix_and_rename_across_variant_shapes() {
    assert_eq!(AccountEvent::Opened.event_type(), "acct.Opened");
    assert_eq!(AccountEvent::Deposited { cents: 1 }.event_type(), "acct.Deposited");
    assert_eq!(AccountEvent::Withdrew(1).event_type(), "acct.Withdrew");
    assert_eq!(AccountEvent::Terminated.event_type(), "acct.Closed");

    assert_eq!(
        AccountEvent::event_types(),
        &["acct.Opened", "acct.Deposited", "acct.Withdrew", "acct.Closed"]
    );
}

#[test]
fn command_names_default_to_variant_and_honour_rename() {
    assert_eq!(AccountCommand::Open.command_type(), "Open");
    assert_eq!(AccountCommand::Deposit(5).command_type(), "Deposit");
    assert_eq!(AccountCommand::Terminate.command_type(), "Close");

    assert_eq!(
        AccountCommand::command_types(),
        &["Open", "Deposit", "Close"]
    );
}
