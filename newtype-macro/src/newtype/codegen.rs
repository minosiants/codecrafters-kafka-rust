use crate::lower::Ir;
use proc_macro2::{Ident, TokenStream};
use quote::quote;
use syn::{Field, Fields, ItemStruct, Type, Visibility};
pub type Rust = TokenStream;

pub fn codegen(ir: Ir) -> Rust {
    let Ir {
        item,
        field,
    } = ir;
    let ItemStruct {
        attrs,
        vis,
        struct_token,
        ident,
        generics,
        fields,
        semi_token,
    } = item;
    let t = field.ty.clone();

    if is_string(&t) {
        string(&vis, &t, &field, &ident)
    } else {
        non_string(&vis, &t, &field, &ident)
    }
}

fn string(
    vis: &Visibility,
    t: &Type,
    field: &Field,
    ident: &Ident,
) -> TokenStream {
    quote! {
        #[derive(Debug, Clone,PartialEq)]
        #vis struct #ident(#field);
         impl #ident {
            #vis fn new(v:#t) -> Self {
                Self(v)
            }
            #vis fn value(&self) -> #t {
                self.0.clone()
            }
        }
        impl std::ops::Deref for #ident {
            type Target = #t;
            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }
    }
}
fn non_string(
    vis: &Visibility,
    t: &Type,
    field: &Field,
    ident: &Ident,
) -> TokenStream {
    quote! {
        #[derive(Debug, Clone,Copy,PartialEq)]
        #vis struct #ident(#field);
        impl #ident {
            #vis fn new(v:#t) -> Self {
                Self(v)
            }
            #vis fn value(&self) -> #t {
                self.0
            }
        }
        impl std::ops::Deref for #ident {
            type Target = #t;
            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }
    }
}

fn is_string(ty: &Type) -> bool {
    match ty {
        Type::Path(path) => {
            let ls = path.path.segments.last().unwrap();
            ls.ident == "String"
        }
        _ => false,
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use proc_macro_error::abort;
    use syn::{parse_quote, Field};
    #[test]
    fn output_is_newtype() {
        let item: ItemStruct = parse_quote!(
            pub struct MyNewtype(u32);
            impl MyNewtype {
                pub fn new(v:u32) -> Self {
                    Self(v)
                }
            }
        );
        let field = match item.fields.clone() {
            Fields::Unnamed(f) if f.unnamed.len() == 1 =>
                Ok::<Field, syn::Error>(f.unnamed.first().unwrap().clone()),
            _ => abort!(item, "expect one unnamed field"),
        }
        .unwrap();
        let ir = Ir {
            item,
            field,
        };
        let rust = codegen(ir);
        println!("{:?}", rust);
        assert!(syn::parse2::<ItemStruct>(rust).is_ok());
    }
}
