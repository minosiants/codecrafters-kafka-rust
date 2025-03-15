use crate::parse::Ast;
use syn::{
    parenthesized,
    parse::{Parse, ParseStream},
    spanned::Spanned,
    Expr, ItemStruct,
};

pub struct Model {
    pub item: Ast,
}

pub fn analyze(ast: Ast) -> Model {
    Model {
        item: ast,
    }
}
