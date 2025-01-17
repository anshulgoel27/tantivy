use std::collections::{BTreeMap, HashMap};

use serde::{Deserialize, Serialize};

use crate::schema::OwnedValue;

/// Internal representation of a document used for JSON
/// serialization.
///
/// A `NamedFieldDocument` is a simple representation of a document
/// as a `BTreeMap<String, Vec<Value>>`.
#[derive(Debug, Deserialize, Serialize)]
pub struct NamedFieldDocument(pub BTreeMap<String, Vec<OwnedValue>>);

#[derive(Debug, Deserialize, Serialize)]
pub struct NamedFieldDocumentHashMap(pub HashMap<String, Vec<OwnedValue>>);
