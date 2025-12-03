use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput};

pub fn derive_flow_type_impl(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    // Use ::ergon for external crates, but provide a fallback for internal usage
    // Users can import the trait if needed
    let expanded = quote! {
        #[automatically_derived]
        impl ::ergon::core::FlowType for #name {
            fn type_id() -> &'static str {
                stringify!(#name)
            }
        }
    };

    TokenStream::from(expanded)
}
