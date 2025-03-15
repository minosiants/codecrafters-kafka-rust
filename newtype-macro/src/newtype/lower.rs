use crate::analyze::Model;
use syn::{Field, ItemStruct};

pub struct Ir {
    pub item: ItemStruct,
    pub field: Field,
}

pub fn lower(model: Model) -> Ir {
    let Model {
        item,
    } = model;
    Ir {
        item: item.item,
        field: item.field,
    }
}
