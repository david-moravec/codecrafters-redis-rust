use proc_macro::TokenStream;
use quote::quote;
use syn::{DeriveInput, parse_macro_input};

fn recipe_to_bytes(attrs: Vec<syn::Attribute>) -> Option<TokenStream> {
    for attr in attrs {
        if attr.path().is_ident("to_bytes") {
            if let syn::Meta::List(syn::MetaList { tokens, .. }) = attr.meta {
                return Some(tokens.into());
            }
        }
    }

    None
}

fn is_ignored(attrs: &[syn::Attribute]) -> bool {
    for attr in attrs {
        if attr.path().is_ident("ignored") {
            return true;
        }
    }

    false
}

#[proc_macro_derive(ToFrame, attributes(to_bytes, ignored))]
pub fn derive(input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as DeriveInput);
    let ident = &ast.ident;

    let fields = if let syn::Data::Struct(syn::DataStruct {
        fields: syn::Fields::Named(syn::FieldsNamed { ref named, .. }),
        ..
    }) = ast.data
    {
        named
    } else {
        unimplemented!()
    };

    let ident_str = ident.to_string();
    let fields_to_bytes = fields.iter().filter(|f| !is_ignored(&f.attrs)).map(|f| {
        let name = &f.ident;

        if let Some(recipe_to_bytes) = recipe_to_bytes(f.attrs.clone()) {
            // quote! {#recipe_to_bytes(self.#name)}
            let mut tt: proc_macro2::TokenStream = recipe_to_bytes.into();
            tt.extend(quote! {(self.#name)});
            tt
        } else {
            quote! {Bytes::from(self.#name.clone())}
        }
    });

    let tt = quote! {impl #ident {
        pub(crate) fn to_frame(&self) -> Frame {
            use bytes::Bytes;
            Frame::bulk_strings_array(vec![Bytes::from((#ident_str).to_uppercase()), #(#fields_to_bytes,)*])
        }

    }};

    tt.into()
}

#[proc_macro_derive(ApplyDbDispatch, attributes(apply, propagate))]
pub fn cmd_apply(input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as DeriveInput);
    let enum_name = &ast.ident;

    let mut tt = proc_macro2::TokenStream::new();

    let variants = match &ast.data {
        syn::Data::Enum(e) => &e.variants,
        _ => {
            return syn::Error::new_spanned(&ast, "This works only on enums")
                .to_compile_error()
                .into();
        }
    };

    let arms = variants.iter().map(|variant| {
        let ident = &variant.ident;
        let mut needs_await = false;
        let mut should_propagate = false;

        for attr in &variant.attrs {
            if attr.path().is_ident("apply") {
                if let Err(_) = attr.parse_nested_meta(|meta| {
                    if meta.path.is_ident("await") {
                        needs_await = true;
                    } else {
                        tt.extend(
                            syn::Error::new_spanned(attr, "Unknow apply attribute variant")
                                .to_compile_error(),
                        );
                    }
                    Ok(())
                }) {
                    tt.extend(syn::Error::new_spanned(attr, "Unknown attr").to_compile_error())
                }
            } else if attr.path().is_ident("propagate") {
                should_propagate = true
            }
        }

        let mut call = if needs_await {
            quote! { cmd.apply(db).await }
        } else {
            quote! { cmd.apply(db) }
        };

        if should_propagate {
            call = quote! {
                {
                    let cmd_frame = cmd.to_frame();
                    let frame = #call;
                    connection.send_to_replicas_connections(cmd_frame)?;
                    frame
                }
            }
        }

        quote! {#enum_name::#ident(cmd) => #call,}
    });

    let expanded = quote! {
        impl #enum_name {
            pub async fn apply(self, db: &Db, connection: &mut Connection) -> Result<Frame> {
                match self {
                    #(#arms)*
                }
            }
        }
    };

    expanded.into()
}
