//! Derive macros for the `streamlet` toolkit.
//!
//! These macros target domain enums and give every variant a stable, consistent
//! string name automatically, so you never hand-write `match` arms that map a
//! variant to a `"PascalCase"` string and risk drift over time.
//!
//! * [`DomainEvent`] implements `streamlet::DomainEvent` for an event enum.
//! * [`Command`] implements `streamlet::Command` for a command enum.
//!
//! Container attribute `#[domain_event(prefix = "...")]` / `#[command(prefix = "...")]`
//! prefixes every generated name (handy for namespacing, e.g. `"counter."`).
//! Per-variant attribute `#[event(rename = "...")]` / `#[command(rename = "...")]`
//! overrides a single variant's name.

use proc_macro::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Fields, LitStr, parse_macro_input};

/// Derive `streamlet::DomainEvent` for an enum.
#[proc_macro_derive(DomainEvent, attributes(domain_event, event))]
pub fn derive_domain_event(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    expand(
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
    expand(
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

struct TraitSpec {
    trait_path: proc_macro2::TokenStream,
    instance_method: proc_macro2::TokenStream,
    list_method: proc_macro2::TokenStream,
    container_attr: &'static str,
    variant_attr: &'static str,
}

fn expand(input: DeriveInput, spec: TraitSpec) -> syn::Result<proc_macro2::TokenStream> {
    let Data::Enum(data) = &input.data else {
        return Err(syn::Error::new_spanned(
            &input.ident,
            "this derive can only be applied to enums",
        ));
    };

    let prefix = container_prefix(&input, spec.container_attr)?;

    let mut match_arms = Vec::new();
    let mut all_names = Vec::new();

    for variant in &data.variants {
        let variant_ident = &variant.ident;
        let base = variant_rename(variant, spec.variant_attr)?
            .unwrap_or_else(|| variant_ident.to_string());
        let name = format!("{prefix}{base}");

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

fn container_prefix(input: &DeriveInput, attr_name: &str) -> syn::Result<String> {
    let mut prefix = String::new();
    for attr in &input.attrs {
        if !attr.path().is_ident(attr_name) {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("prefix") {
                let value: LitStr = meta.value()?.parse()?;
                prefix = value.value();
                Ok(())
            } else {
                Err(meta.error("unsupported attribute; expected `prefix = \"...\"`"))
            }
        })?;
    }
    Ok(prefix)
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
