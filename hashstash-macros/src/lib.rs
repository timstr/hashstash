extern crate proc_macro;
use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{Field, Member};

#[proc_macro_derive(Stashable, attributes(stashable_context))]
pub fn derive_stashable(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let ast = syn::parse(input).unwrap();
    generate_impl_stashable(&ast).into()
}

#[proc_macro_derive(Unstashable, attributes(unstashable_context))]
pub fn derive_unstashable(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let ast = syn::parse(input).unwrap();
    generate_impl_unstashable(&ast).into()
}

#[proc_macro_derive(UnstashableInplace, attributes(unstashable_inplace_context))]
pub fn derive_unstashable_inplace(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let ast = syn::parse(input).unwrap();
    generate_impl_unstashable_inplace(&ast).into()
}

fn is_primitive_name(name: &str) -> bool {
    match name {
        "bool" => true,
        "u8" => true,
        "i8" => true,
        "u16" => true,
        "i16" => true,
        "u32" => true,
        "i32" => true,
        "u64" => true,
        "i64" => true,
        "f32" => true,
        "f64" => true,
        "usize" => true,
        _ => false,
    }
}

#[derive(Copy, Clone, Eq, PartialEq)]
enum StashTrait {
    Stashable,
    Unstashable,
    UnstashableInplace,
}

fn type_as_primitive_name(ty: &syn::Type) -> Option<String> {
    match &ty {
        syn::Type::Path(type_path) => {
            if let Some(type_path_ident) = type_path.path.get_ident() {
                let s = type_path_ident.to_string();
                if is_primitive_name(&s) {
                    return Some(s);
                }
            }
        }
        _ => (),
    }
    None
}

fn type_is_string(ty: &syn::Type) -> bool {
    match &ty {
        syn::Type::Path(type_path) => {
            if let Some(type_path_ident) = type_path.path.get_ident() {
                type_path_ident.to_string() == "String"
            } else {
                false
            }
        }
        _ => false,
    }
}

fn type_as_vec_with_inner_type(ty: &syn::Type) -> Option<&syn::Type> {
    match &ty {
        syn::Type::Path(type_path) => {
            if type_path.path.segments.len() == 1 {
                let seg = type_path.path.segments.first().unwrap();
                if seg.ident.to_string() == "Vec" {
                    if let syn::PathArguments::AngleBracketed(args) = &seg.arguments {
                        if args.args.len() == 1 {
                            if let syn::GenericArgument::Type(elem_type) =
                                args.args.first().unwrap()
                            {
                                return Some(elem_type);
                            }
                        }
                    }
                }
            }
        }
        _ => (),
    }
    None
}

fn convert_to_named_fields(fields: &syn::Fields) -> syn::Fields {
    match fields {
        syn::Fields::Named(_) => fields.clone(),
        syn::Fields::Unnamed(fields_unnamed) => syn::Fields::Named(syn::FieldsNamed {
            brace_token: Default::default(),
            named: fields_unnamed
                .unnamed
                .iter()
                .enumerate()
                .map(|(i, f)| Field {
                    ident: Some(format_ident!("x{}", i)),
                    ..f.clone()
                })
                .collect(),
        }),
        syn::Fields::Unit => fields.clone(),
    }
}

fn generate_field(
    ty: &syn::Type,
    access_prefix: &TokenStream,
    field: &syn::Member,
    stash_trait: StashTrait,
    is_ref: bool,
) -> TokenStream {
    let assignee = match field {
        Member::Named(ident) => quote! { #ident: },
        Member::Unnamed(_) => quote! {},
    };

    let access_prefix = if is_ref {
        quote! { * #access_prefix }
    } else {
        quote! { #access_prefix }
    };

    match &ty {
        syn::Type::Path(_) => {
            if let Some(prim_type_name) = type_as_primitive_name(ty) {
                let prim_type_ident = format_ident!("{}", prim_type_name);
                match stash_trait {
                    StashTrait::Stashable => {
                        quote! { stasher.#prim_type_ident(#access_prefix #field); }
                    }
                    StashTrait::Unstashable => {
                        quote! { #assignee unstasher.#prim_type_ident()?, }
                    }
                    StashTrait::UnstashableInplace => {
                        let prim_inplace_ident = syn::Ident::new(
                            &format!("{}_inplace", prim_type_ident),
                            proc_macro2::Span::call_site(),
                        );
                        quote! { unstasher.#prim_inplace_ident(&mut #access_prefix #field)?; }
                    }
                }
            } else if type_is_string(ty) {
                match stash_trait {
                    StashTrait::Stashable => {
                        quote! { stasher.string(&#access_prefix #field); }
                    }
                    StashTrait::Unstashable => {
                        quote! { #assignee unstasher.string()?, }
                    }
                    StashTrait::UnstashableInplace => {
                        quote! { unstasher.string_inplace(&mut #access_prefix #field)?; }
                    }
                }
            } else if let Some(elem_type) = type_as_vec_with_inner_type(ty) {
                if let Some(elem_prim_type_name) = type_as_primitive_name(elem_type) {
                    match stash_trait {
                        StashTrait::Stashable => {
                            let array_of_ident =
                                format_ident!("array_of_{}_slice", elem_prim_type_name);
                            quote! { stasher.#array_of_ident(&#access_prefix #field); }
                        }
                        StashTrait::Unstashable => {
                            let array_of_ident =
                                format_ident!("array_of_{}_vec", elem_prim_type_name);
                            quote! { #assignee unstasher.#array_of_ident()?, }
                        }
                        StashTrait::UnstashableInplace => {
                            let array_of_ident =
                                format_ident!("array_of_{}_vec_inplace", elem_prim_type_name);
                            quote! { unstasher.#array_of_ident(&mut #access_prefix #field)?; }
                        }
                    }
                } else {
                    // TODO: add attribute to make the vec uno
                    match stash_trait {
                        StashTrait::Stashable => {
                            quote! {
                                stasher.array_of_objects_slice(
                                    &#access_prefix #field,
                                    ::hashstash::Order::Ordered
                                );
                            }
                        }
                        StashTrait::Unstashable => {
                            quote! { #assignee unstasher.array_of_objects_vec()?, }
                        }
                        StashTrait::UnstashableInplace => {
                            quote! {
                                unstasher
                                    .array_of_objects_vec_inplace(&mut #access_prefix #field)?;
                            }
                        }
                    }
                }
            } else {
                // Assume object implementing the desired trait
                match stash_trait {
                    StashTrait::Stashable => {
                        quote! { stasher.object(&#access_prefix #field); }
                    }
                    StashTrait::Unstashable => {
                        quote! { #assignee unstasher.object()?, }
                    }
                    StashTrait::UnstashableInplace => {
                        // TODO: add field attribute for using object_replace?
                        quote! { unstasher.object_inplace(&mut #access_prefix #field)?; }
                    }
                }
            }
        }
        syn::Type::Array(type_array) => {
            if let Some(prim_type_name) = type_as_primitive_name(&type_array.elem) {
                match stash_trait {
                    StashTrait::Stashable => {
                        let array_of_ident = format_ident!("array_of_{}_slice", prim_type_name);
                        quote! {stasher.#array_of_ident(&#access_prefix #field);}
                    }
                    StashTrait::Unstashable => {
                        let array_of_ident = format_ident!("array_of_{}_vec", prim_type_name);
                        quote! {
                            #assignee {unstasher
                                .#array_of_ident()?
                                .try_into()
                                .map_err(|_| ::hashstash::UnstashError::BadValue)}?,
                        }
                    }
                    StashTrait::UnstashableInplace => {
                        let array_of_ident = format_ident!("array_of_{}_iter", prim_type_name);
                        quote! {
                            let arr = unstasher.#array_of_ident()?
                                .collect::<Vec<_>>()
                                .try_into()
                                .map_err(|_| ::hashstash::UnstashError::BadValue)?;
                            if unstasher.time_to_write() {
                                #access_prefix #field = arr;
                            }
                        }
                    }
                }
            } else {
                todo!("non-prim array")
            }
        }
        // ---
        syn::Type::Tuple(_type_tuple) => todo!("Tuple"),
        syn::Type::FnPtr(_type_fn_ptr) => todo!("FnPtr"),
        syn::Type::Group(_type_group) => todo!("Group"),
        syn::Type::ImplTrait(_type_impl_trait) => todo!("ImplTrait"),
        syn::Type::Infer(_type_infer) => todo!("Infer"),
        syn::Type::Macro(_type_macro) => todo!("Macro"),
        syn::Type::Never(_type_never) => todo!("Never"),
        syn::Type::Paren(_type_paren) => todo!("Paren"),
        syn::Type::Ptr(_type_ptr) => todo!("Ptr"),
        syn::Type::Reference(_type_reference) => todo!("Reference"),
        syn::Type::Slice(_type_slice) => todo!("Slice"),
        syn::Type::TraitObject(_type_trait_object) => todo!("TraitObject"),
        syn::Type::Verbatim(_token_stream) => todo!("Verbatim"),
        _ => panic!("Unsupported field type"),
    }
}

fn generate_all_fields(
    name_ident: &syn::Ident,
    access_prefix: &TokenStream,
    fields: &syn::Fields,
    stash_trait: StashTrait,
    is_ref: bool,
) -> TokenStream {
    let mut all_fields = TokenStream::new();
    for (i, field) in fields.iter().enumerate() {
        let s = generate_field(
            &field.ty,
            access_prefix,
            &field
                .ident
                .as_ref()
                .map(|n| syn::Member::Named(n.clone()))
                .unwrap_or(syn::Member::Unnamed(i.into())),
            stash_trait,
            is_ref,
        );
        all_fields.extend(s);
    }

    match stash_trait {
        StashTrait::Stashable => all_fields,
        StashTrait::Unstashable => match fields {
            syn::Fields::Named(_) => quote! { #name_ident { #all_fields } },
            syn::Fields::Unnamed(_) => quote! { #name_ident(#all_fields) },
            syn::Fields::Unit => quote! { #name_ident },
        },
        StashTrait::UnstashableInplace => all_fields,
    }
}

fn generate_fn_body(ident: &syn::Ident, data: &syn::Data, stash_trait: StashTrait) -> TokenStream {
    match data {
        syn::Data::Struct(data_struct) => {
            let access_prefix = quote! { self. };
            let all_fields = generate_all_fields(
                ident,
                &access_prefix,
                &data_struct.fields,
                stash_trait,
                false,
            );
            match stash_trait {
                StashTrait::Stashable => all_fields,
                StashTrait::Unstashable => quote! { Ok(#all_fields) },
                StashTrait::UnstashableInplace => quote! { #all_fields Ok(()) },
            }
        }
        syn::Data::Enum(data_enum) => match stash_trait {
            StashTrait::Stashable => {
                let mut all_variants = TokenStream::new();
                for (i, variant) in data_enum.variants.iter().enumerate() {
                    let i_u8 = i as u8;
                    let var_ident = &variant.ident;
                    let access_prefix = quote! {};
                    let fields_with_names = convert_to_named_fields(&variant.fields);
                    let comma_separated_names: syn::punctuated::Punctuated<
                        syn::Ident,
                        syn::Token![,],
                    > = fields_with_names
                        .iter()
                        .map(|f| f.ident.clone().unwrap())
                        .collect();
                    let fields_pattern = match &variant.fields {
                        syn::Fields::Named(_) => quote! { { #comma_separated_names } },
                        syn::Fields::Unnamed(_) => quote! { ( #comma_separated_names ) },
                        syn::Fields::Unit => quote! {},
                    };
                    let all_fields = generate_all_fields(
                        var_ident,
                        &access_prefix,
                        &fields_with_names,
                        stash_trait,
                        true,
                    );
                    let s = quote! { #ident::#var_ident #fields_pattern => {
                        stasher.u8(#i_u8);
                        #all_fields
                    } };
                    all_variants.extend(s);
                }
                quote! {
                    match self {
                        #all_variants
                    }
                }
            }
            StashTrait::Unstashable => {
                let mut all_variants = TokenStream::new();
                for (i, variant) in data_enum.variants.iter().enumerate() {
                    let i_u8 = i as u8;
                    let var_ident = &variant.ident;
                    let access_prefix = quote! {};
                    let all_fields = generate_all_fields(
                        var_ident,
                        &access_prefix,
                        &variant.fields,
                        stash_trait,
                        true,
                    );
                    let s = quote! {
                        #i_u8 => Ok(#ident:: #all_fields),
                    };
                    all_variants.extend(s);
                }
                quote! {
                    match unstasher.u8()? {
                        #all_variants
                        _ => Err(::hashstash::UnstashError::BadValue),
                    }
                }
            }
            StashTrait::UnstashableInplace => {
                let mut all_variants = TokenStream::new();
                for (i, variant) in data_enum.variants.iter().enumerate() {
                    let i_u8 = i as u8;
                    let var_ident = &variant.ident;
                    let fields_with_names = convert_to_named_fields(&variant.fields);
                    let comma_separated_names: syn::punctuated::Punctuated<
                        syn::Ident,
                        syn::Token![,],
                    > = fields_with_names
                        .iter()
                        .map(|f| f.ident.clone().unwrap())
                        .collect();
                    let fields_pattern = match &variant.fields {
                        syn::Fields::Named(_) => quote! { { #comma_separated_names } },
                        syn::Fields::Unnamed(_) => quote! { ( #comma_separated_names ) },
                        syn::Fields::Unit => quote! {},
                    };
                    let access_prefix = quote! {};
                    let fields_inplace = generate_all_fields(
                        var_ident,
                        &access_prefix,
                        &fields_with_names,
                        StashTrait::UnstashableInplace,
                        true,
                    );
                    let fields_replace = generate_all_fields(
                        var_ident,
                        &access_prefix,
                        &variant.fields,
                        StashTrait::Unstashable,
                        false,
                    );
                    let s = quote! {
                        #i_u8 => if let #ident::#var_ident #fields_pattern = self {
                            #fields_inplace
                        } else {
                            let x = #ident:: #fields_replace;
                            if unstasher.time_to_write() {
                                *self = x;
                            }
                        },
                    };
                    all_variants.extend(s);
                }

                quote! {
                    match unstasher.u8()? {
                        #all_variants
                        _ => return Err(::hashstash::UnstashError::BadValue),
                    }
                    Ok(())
                }
            }
        },
        syn::Data::Union(_) => panic!("Unions are not supported"),
    }
}

struct Context {
    generics: Option<syn::Generics>,
    ty: syn::Type,
}

impl syn::parse::Parse for Context {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let lookahead = input.lookahead1();
        let generics = if lookahead.peek(syn::Token![for]) {
            input.parse::<syn::Token![for]>()?;
            Some(input.parse::<syn::Generics>()?)
        } else {
            None
        };
        let ty = input.parse::<syn::Type>()?;
        Ok(Context { generics, ty })
    }
}

fn parse_context_from_attributes(attrs: &[syn::Attribute], attr_name: &str) -> Context {
    let ctx_attr_ident = format_ident!("{}", attr_name);
    let ctx: TokenStream = attrs
        .iter()
        .filter_map(|a| {
            if a.path().get_ident() == Some(&ctx_attr_ident) {
                if let syn::Meta::List(l) = &a.meta {
                    return Some(l.tokens.clone());
                }
            }
            None
        })
        .next()
        .unwrap_or(quote! { for<C: Copy> C});

    syn::parse2(ctx).expect("Invalid context")
}

fn generate_impl_stashable(ast: &syn::DeriveInput) -> TokenStream {
    let name = &ast.ident;

    let ctx = parse_context_from_attributes(&ast.attrs, "stashable_context");
    let ctx_generics = ctx.generics;
    let ctx_ty = ctx.ty;

    let fn_body = generate_fn_body(name, &ast.data, StashTrait::Stashable);

    let gen = quote! {
        impl #ctx_generics ::hashstash::Stashable<#ctx_ty> for #name {
            fn stash(&self, stasher: &mut ::hashstash::Stasher<#ctx_ty>) {
                #fn_body
            }
        }
    };

    // println!("=====================\n\n{}\n\n=====================", gen);

    TokenStream::from(gen)
}

fn generate_impl_unstashable(ast: &syn::DeriveInput) -> TokenStream {
    let name = &ast.ident;

    let ctx = parse_context_from_attributes(&ast.attrs, "unstashable_context");
    let ctx_generics = ctx.generics;
    let ctx_ty = ctx.ty;

    let fn_body = generate_fn_body(name, &ast.data, StashTrait::Unstashable);

    let gen = quote! {
        impl #ctx_generics ::hashstash::Unstashable<#ctx_ty> for #name {
            fn unstash(unstasher: &mut ::hashstash::Unstasher<#ctx_ty>) -> Result<Self, ::hashstash::UnstashError> {
                #fn_body
            }
        }
    };

    // println!("=====================\n\n{}\n\n=====================", gen);

    TokenStream::from(gen)
}

fn generate_impl_unstashable_inplace(ast: &syn::DeriveInput) -> TokenStream {
    let name = &ast.ident;

    let ctx = parse_context_from_attributes(&ast.attrs, "unstashable_inplace_context");
    let ctx_generics = ctx.generics;
    let ctx_ty = ctx.ty;

    let fn_body = generate_fn_body(name, &ast.data, StashTrait::UnstashableInplace);

    let gen = quote! {
        impl #ctx_generics ::hashstash::UnstashableInplace<#ctx_ty> for #name {
            fn unstash_inplace(
                &mut self,
                unstasher: &mut ::hashstash::InplaceUnstasher<#ctx_ty>,
            ) -> Result<(), ::hashstash::UnstashError> {
                #fn_body
            }
        }
    };

    // println!("=====================\n\n{}\n\n=====================", gen);

    TokenStream::from(gen)
}
