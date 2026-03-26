use proc_macro::TokenStream;
use quote::quote;
use syn::{
    DeriveInput, FnArg, ImplItemFn, Pat, PatType, Type, TypePath, TypeReference, parse_macro_input,
    punctuated::Punctuated, token::Comma,
};

fn get_connection_pat(inputs: &Punctuated<FnArg, Comma>) -> Result<Pat, syn::Error> {
    let mut connection_arg_pat: Option<&Pat> = None;

    for input in inputs.iter() {
        if let FnArg::Typed(PatType {
            ty: boxed_type,
            pat,
            ..
        }) = input
        {
            let arg_type = boxed_type.as_ref();

            if let Type::Reference(TypeReference {
                elem: boxed_type, ..
            }) = arg_type
            {
                let arg_type = boxed_type.as_ref();

                if let Type::Path(TypePath { path, .. }) = arg_type {
                    if let Some(last_segment) = path.segments.last() {
                        if last_segment.ident.to_string() == "Connection" {
                            connection_arg_pat = Some(pat.as_ref());
                        }
                    }
                }
            }
        }
    }

    if connection_arg_pat.is_none() {
        return Err(syn::Error::new_spanned(
            &inputs,
            "Expected function to accept Connection",
        ));
    }

    let connection_arg_pat = connection_arg_pat.unwrap();

    Ok(connection_arg_pat.clone())
}

#[proc_macro_attribute]
pub fn propagate_to_replicas(args: TokenStream, input: TokenStream) -> TokenStream {
    let _ = args;

    let mut item = parse_macro_input!(input as ImplItemFn);
    let mut error: Option<syn::Error> = None;

    match get_connection_pat(&item.sig.inputs) {
        Ok(pat) => {
            let send_to_replicas_tt: TokenStream =
                quote! {#pat.send_to_replicas_connections(self.to_frame())?;}.into();
            let send_to_replicas_stmt = parse_macro_input!(send_to_replicas_tt as syn::Stmt);
            item.block.stmts.insert(0, send_to_replicas_stmt);
        }
        Err(err) => error = Some(err),
    };

    let mut tt = quote! {#item};

    if let Some(err) = error {
        tt.extend(err.to_compile_error());
    }

    tt.into()
}

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
        fn to_frame(&self) -> Frame {
            use bytes::Bytes;
            Frame::bulk_strings_array(vec![Bytes::from((#ident_str).to_uppercase()), #(#fields_to_bytes,)*])
        }

    }};

    tt.into()
}
