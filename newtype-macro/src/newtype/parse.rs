use proc_macro2::TokenStream;
use proc_macro_error::abort;
use syn::parse::{Parse, ParseStream};
use syn::{
    parse2, Error, Field, Fields, FieldsUnnamed, Item, ItemStruct, Result,
};

pub struct Ast {
    pub item: ItemStruct,
    pub field: Field,
}

impl Parse for Ast {
    fn parse(stream: ParseStream) -> Result<Self> {
        let item: ItemStruct = stream.parse()?;
        let fields = item.fields.clone();
        let field = match fields {
            Fields::Unnamed(f) if f.unnamed.len() == 1 =>
                Ok::<Field, syn::Error>(f.unnamed.first().unwrap().clone()),
            _ => abort!(item, "expect one unnamed field"),
        }?;
        Ok(Ast {
            item,
            field,
        })
    }
}

pub fn parse(ts: TokenStream) -> Ast {
    match parse2::<Ast>(ts) {
        Ok(ast) => ast,
        Err(e) => {
            abort!(e.span(), e)
        }
    }
}

#[cfg(test)]
mod tests {
    use quote::quote;

    use super::*;
    #[test]
    fn valid_syntax() {
        parse(quote!(
            #[newtype]
            struct My(u32);
        ));
    }
}
