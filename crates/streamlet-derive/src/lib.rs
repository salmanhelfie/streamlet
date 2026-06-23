//! Derive macros for the `streamlet` toolkit.
//!
//! These macros target domain types and give every variant (or command type) a
//! stable, consistent string name automatically, so you never hand-write `match`
//! arms that map a variant to a `"PascalCase"` string and risk drift over time.
//!
//! * [`DomainEvent`] implements `streamlet::DomainEvent` for an event enum.
//! * [`Command`] implements `streamlet::Command` for a command enum.
//! * [`CommandKind`] implements `streamlet::CommandKind` for a single command
//!   type (a struct, or a one-shot enum).
//!
//! Container attribute `#[domain_event(prefix = "...")]` / `#[command(prefix = "...")]`
//! prefixes every generated name (handy for namespacing, e.g. `"counter."`).
//! Container attribute `rename_all = "snake_case"` (also `kebab-case`,
//! `SCREAMING_SNAKE_CASE`, `camelCase`, `PascalCase`, `lowercase`, `UPPERCASE`)
//! rewrites every variant name's casing. Per-variant attribute
//! `#[event(rename = "...")]` / `#[command(rename = "...")]` overrides a single
//! variant's name and wins over `rename_all`.

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput, Fields, LitStr};

/// Derive `streamlet::DomainEvent` for an enum.
#[proc_macro_derive(DomainEvent, attributes(domain_event, event))]
pub fn derive_domain_event(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    expand_enum(
        input,
        TraitSpec {
            trait_path: quote! { ::streamlet::DomainEvent },
            instance_method: quote! { event_type },
            list_method: quote! { event_types },
            container_attr: "domain_event",
            variant_attr: "event",
        },
    )
    .unwrap_or_else(|e| e.to_compile_error())
    .into()
}

/// Derive `streamlet::Command` for an enum.
#[proc_macro_derive(Command, attributes(command))]
pub fn derive_command(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    expand_enum(
        input,
        TraitSpec {
            trait_path: quote! { ::streamlet::Command },
            instance_method: quote! { command_type },
            list_method: quote! { command_types },
            container_attr: "command",
            variant_attr: "command",
        },
    )
    .unwrap_or_else(|e| e.to_compile_error())
    .into()
}

/// Derive `streamlet::CommandKind` for a single command type.
///
/// Works on structs (the common case) and on enums. The generated `NAME` is the
/// type's identifier, optionally rewritten by `#[command_kind(name = "...")]`
/// and/or prefixed by `#[command_kind(prefix = "...")]`.
#[proc_macro_derive(CommandKind, attributes(command_kind))]
pub fn derive_command_kind(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    expand_command_kind(input)
        .unwrap_or_else(|e| e.to_compile_error())
        .into()
}

struct TraitSpec {
    trait_path: proc_macro2::TokenStream,
    instance_method: proc_macro2::TokenStream,
    list_method: proc_macro2::TokenStream,
    container_attr: &'static str,
    variant_attr: &'static str,
}

#[derive(Default)]
struct Container {
    prefix: String,
    rename_all: Option<Case>,
}

fn expand_enum(input: DeriveInput, spec: TraitSpec) -> syn::Result<proc_macro2::TokenStream> {
    let Data::Enum(data) = &input.data else {
        return Err(syn::Error::new_spanned(
            &input.ident,
            "this derive can only be applied to enums",
        ));
    };

    let container = container_config(&input, spec.container_attr)?;

    let mut match_arms = Vec::new();
    let mut all_names = Vec::new();

    for variant in &data.variants {
        let variant_ident = &variant.ident;
        let base = match variant_rename(variant, spec.variant_attr)? {
            Some(explicit) => explicit,
            None => match container.rename_all {
                Some(case) => apply_case(&variant_ident.to_string(), case),
                None => variant_ident.to_string(),
            },
        };
        let name = format!("{}{}", container.prefix, base);

        let pattern = match &variant.fields {
            Fields::Unit => quote! { Self::#variant_ident },
            Fields::Unnamed(_) => quote! { Self::#variant_ident(..) },
            Fields::Named(_) => quote! { Self::#variant_ident { .. } },
        };

        match_arms.push(quote! { #pattern => #name, });
        all_names.push(quote! { #name });
    }

    let ident = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    let TraitSpec {
        trait_path,
        instance_method,
        list_method,
        ..
    } = spec;

    Ok(quote! {
        impl #impl_generics #trait_path for #ident #ty_generics #where_clause {
            fn #instance_method(&self) -> &'static str {
                match self {
                    #(#match_arms)*
                }
            }

            fn #list_method() -> &'static [&'static str] {
                &[#(#all_names),*]
            }
        }
    })
}

fn expand_command_kind(input: DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let mut prefix = String::new();
    let mut name_override: Option<String> = None;

    for attr in &input.attrs {
        if !attr.path().is_ident("command_kind") {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("prefix") {
                let value: LitStr = meta.value()?.parse()?;
                prefix = value.value();
                Ok(())
            } else if meta.path.is_ident("name") {
                let value: LitStr = meta.value()?.parse()?;
                name_override = Some(value.value());
                Ok(())
            } else {
                Err(meta.error(
                    "unsupported attribute; expected `prefix = \"...\"` or `name = \"...\"`",
                ))
            }
        })?;
    }

    let ident = &input.ident;
    let base = name_override.unwrap_or_else(|| ident.to_string());
    let name = format!("{prefix}{base}");

    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    Ok(quote! {
        impl #impl_generics ::streamlet::CommandKind for #ident #ty_generics #where_clause {
            const NAME: &'static str = #name;
        }
    })
}

fn container_config(input: &DeriveInput, attr_name: &str) -> syn::Result<Container> {
    let mut config = Container::default();
    for attr in &input.attrs {
        if !attr.path().is_ident(attr_name) {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("prefix") {
                let value: LitStr = meta.value()?.parse()?;
                config.prefix = value.value();
                Ok(())
            } else if meta.path.is_ident("rename_all") {
                let value: LitStr = meta.value()?.parse()?;
                config.rename_all = Some(
                    Case::parse(&value.value())
                        .map_err(|msg| syn::Error::new_spanned(&value, msg))?,
                );
                Ok(())
            } else {
                Err(meta.error(
                    "unsupported attribute; expected `prefix = \"...\"` or `rename_all = \"...\"`",
                ))
            }
        })?;
    }
    Ok(config)
}

fn variant_rename(variant: &syn::Variant, attr_name: &str) -> syn::Result<Option<String>> {
    let mut rename = None;
    for attr in &variant.attrs {
        if !attr.path().is_ident(attr_name) {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("rename") {
                let value: LitStr = meta.value()?.parse()?;
                rename = Some(value.value());
                Ok(())
            } else {
                Err(meta.error("unsupported attribute; expected `rename = \"...\"`"))
            }
        })?;
    }
    Ok(rename)
}

#[derive(Clone, Copy)]
enum Case {
    Snake,
    Kebab,
    ScreamingSnake,
    Camel,
    Pascal,
    Lower,
    Upper,
}

impl Case {
    fn parse(value: &str) -> Result<Self, String> {
        Ok(match value {
            "snake_case" => Case::Snake,
            "kebab-case" => Case::Kebab,
            "SCREAMING_SNAKE_CASE" => Case::ScreamingSnake,
            "camelCase" => Case::Camel,
            "PascalCase" => Case::Pascal,
            "lowercase" => Case::Lower,
            "UPPERCASE" => Case::Upper,
            other => {
                return Err(format!(
                    "unknown rename_all value `{other}`; expected one of \
                     snake_case, kebab-case, SCREAMING_SNAKE_CASE, camelCase, \
                     PascalCase, lowercase, UPPERCASE"
                ))
            }
        })
    }
}

/// Split an identifier into lowercase words, handling PascalCase, camelCase and
/// existing underscores/hyphens. e.g. `OrderShipped` -> ["order", "shipped"].
fn split_words(ident: &str) -> Vec<String> {
    let mut words = Vec::new();
    let mut current = String::new();
    let mut prev_lower_or_digit = false;

    for ch in ident.chars() {
        if ch == '_' || ch == '-' || ch == ' ' {
            if !current.is_empty() {
                words.push(std::mem::take(&mut current));
            }
            prev_lower_or_digit = false;
            continue;
        }
        if ch.is_uppercase() && prev_lower_or_digit && !current.is_empty() {
            words.push(std::mem::take(&mut current));
        }
        current.extend(ch.to_lowercase());
        prev_lower_or_digit = ch.is_lowercase() || ch.is_ascii_digit();
    }
    if !current.is_empty() {
        words.push(current);
    }
    words
}

fn apply_case(ident: &str, case: Case) -> String {
    let words = split_words(ident);
    match case {
        Case::Snake => words.join("_"),
        Case::Kebab => words.join("-"),
        Case::ScreamingSnake => words
            .iter()
            .map(|w| w.to_uppercase())
            .collect::<Vec<_>>()
            .join("_"),
        Case::Lower => words.join(""),
        Case::Upper => words.join("").to_uppercase(),
        Case::Pascal => words.iter().map(|w| capitalize(w)).collect(),
        Case::Camel => words
            .iter()
            .enumerate()
            .map(|(i, w)| if i == 0 { w.clone() } else { capitalize(w) })
            .collect(),
    }
}

fn capitalize(word: &str) -> String {
    let mut chars = word.chars();
    match chars.next() {
        Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
        None => String::new(),
    }
}
