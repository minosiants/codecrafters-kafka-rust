use proc_macro::TokenStream;

use proc_macro_error::proc_macro_error;

mod newtype;
use crate::newtype::analyze;
use crate::newtype::codegen;
use crate::newtype::lower;
use crate::newtype::parse;

#[proc_macro_attribute]
#[proc_macro_error]
pub fn newtype(_attr: TokenStream, ts: TokenStream) -> TokenStream {
    let ast = parse::parse(ts.clone().into());
    let model = analyze::analyze(ast);
    let ir = lower::lower(model);
    let rust = codegen::codegen(ir);
    rust.into()
}
