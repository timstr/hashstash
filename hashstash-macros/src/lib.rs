extern crate proc_macro;
use proc_macro2::TokenStream;
use quote::{format_ident, quote};

#[proc_macro_derive(Stashable)]
pub fn derive_stashable(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let ast = syn::parse(input).unwrap();
    impl_stashable_macro(&ast).into()
}

fn impl_stashable_macro(ast: &syn::DeriveInput) -> TokenStream {
    todo!()
}

// TODO: how should context be specified in the macro? Some kind of config attribute?
// Where can I find precedence for passing a type to a macro?
// TODO: Unstashable
// TODO: UnstashableInplace
