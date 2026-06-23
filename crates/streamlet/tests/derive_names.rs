//! Verifies the derive macros produce stable, consistent names — including the
//! container `prefix` and per-variant `rename` knobs.

use serde::{Deserialize, Serialize};
use streamlet::{Command, CommandKind, DomainEvent};

#[derive(Clone, Serialize, Deserialize, DomainEvent)]
#[domain_event(prefix = "acct.")]
enum AccountEvent {
    Opened,
    Deposited {
        cents: u64,
    },
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
    assert_eq!(
        AccountEvent::Deposited { cents: 1 }.event_type(),
        "acct.Deposited"
    );
    assert_eq!(AccountEvent::Withdrew(1).event_type(), "acct.Withdrew");
    assert_eq!(AccountEvent::Terminated.event_type(), "acct.Closed");

    assert_eq!(
        AccountEvent::event_types(),
        &[
            "acct.Opened",
            "acct.Deposited",
            "acct.Withdrew",
            "acct.Closed"
        ]
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

#[derive(Clone, Serialize, Deserialize, DomainEvent)]
#[domain_event(prefix = "order.", rename_all = "snake_case")]
enum OrderEvent {
    OrderPlaced,
    LineItemAdded {
        sku: String,
    },
    #[event(rename = "shipped")]
    OrderShipped,
}

#[test]
fn rename_all_snake_cases_each_variant_and_honours_explicit_rename() {
    assert_eq!(OrderEvent::OrderPlaced.event_type(), "order.order_placed");
    assert_eq!(
        OrderEvent::LineItemAdded { sku: "x".into() }.event_type(),
        "order.line_item_added"
    );
    // explicit rename wins over rename_all
    assert_eq!(OrderEvent::OrderShipped.event_type(), "order.shipped");
}

#[derive(CommandKind)]
struct Reboot;

#[derive(CommandKind)]
#[command_kind(prefix = "sys.", name = "Shutdown")]
struct PowerOff;

#[test]
fn command_kind_derives_a_stable_name() {
    assert_eq!(Reboot::NAME, "Reboot");
    assert_eq!(PowerOff::NAME, "sys.Shutdown");
    assert_eq!(Reboot.command_name(), "Reboot");
}
