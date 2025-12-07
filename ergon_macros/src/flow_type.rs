use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput, Meta, Type};

pub fn derive_flow_type_impl(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    // Parse #[invokable(output = Type)] attribute if present
    let output_type = input.attrs.iter().find_map(|attr| {
        if !attr.path().is_ident("invokable") {
            return None;
        }

        let meta = attr.meta.require_list().ok()?;
        let nested = meta.parse_args::<Meta>().ok()?;

        if let Meta::NameValue(nv) = nested {
            if nv.path.is_ident("output") {
                if let syn::Expr::Path(expr_path) = &nv.value {
                    return Some(Type::Path(syn::TypePath {
                        qself: None,
                        path: expr_path.path.clone(),
                    }));
                }
            }
        }
        None
    });

    // Always implement FlowType
    let flow_type_impl = quote! {
        #[automatically_derived]
        impl ::ergon::core::FlowType for #name {
            fn type_id() -> &'static str {
                stringify!(#name)
            }
        }
    };

    // Optionally implement InvokableFlow if output type is specified
    let invokable_impl = output_type.map(|output| {
        quote! {
            #[automatically_derived]
            impl ::ergon::core::InvokableFlow for #name {
                type Output = #output;
            }
        }
    });

    let expanded = quote! {
        #flow_type_impl
        #invokable_impl
    };

    TokenStream::from(expanded)
}
