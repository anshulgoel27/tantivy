use std::fmt;
use std::path::PathBuf;

use common::BitSet;

use super::term_weight::TermWeight;
use crate::query::bm25::Bm25Weight;
use crate::query::{BitSetDocSet, ConstScorer, EnableScoring, Explanation, Query, Weight};
use crate::schema::{IndexRecordOption, Schema};
use crate::Term;

///
#[derive(Clone)]
pub struct TermFilterQuery {
    term: Term,
    use_bitset: bool,
    index_path: Option<PathBuf>,
}

impl fmt::Debug for TermFilterQuery {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "TermFilterQuery({:?})", self.term)
    }
}

impl TermFilterQuery {
    /// Creates a new term filter query.
    pub fn new(term: Term, use_bitset: bool, index_path: Option<PathBuf>) -> TermFilterQuery {
        TermFilterQuery {
            term,
            use_bitset,
            index_path,
        }
    }

    /// The `Term` this query is built out of.
    pub fn term(&self) -> &Term {
        &self.term
    }

    /// Returns a weight object.
    ///
    /// While `.weight(...)` returns a boxed trait object,
    /// this method return a specific implementation.
    /// This is useful for optimization purpose.
    pub fn specialized_weight(&self, schema: &Schema) -> crate::Result<Box<dyn Weight>> {
        let field_entry = schema.get_field_entry(self.term.field());
        if !field_entry.is_indexed() {
            let error_msg = format!("Field {:?} is not indexed.", field_entry.name());
            return Err(crate::TantivyError::SchemaError(error_msg));
        }
        if !field_entry.is_fast() {
            let error_msg = format!("Field {:?} is not fast.", field_entry.name());
            return Err(crate::TantivyError::SchemaError(error_msg));
        }
        if self.use_bitset && self.index_path.is_some() {
            return Ok(Box::new(TermBitsetWeight::new(
                self.term.clone(),
                self.index_path.clone()
            )));
        }

        let bm25_weight = Bm25Weight::new(Explanation::new("<no score>", 1.0f32), 1.0f32);
        let index_record_option = IndexRecordOption::Basic;

        Ok(Box::new(TermWeight::new(
            self.term.clone(),
            index_record_option,
            bm25_weight,
            false,
        )))
    }
}

impl Query for TermFilterQuery {
    fn weight(&self, enable_scoring: EnableScoring<'_>) -> crate::Result<Box<dyn Weight>> {
        Ok(self.specialized_weight(enable_scoring.schema())?)
    }
    fn query_terms<'a>(&'a self, visitor: &mut dyn FnMut(&'a Term, bool)) {
        visitor(&self.term, false);
    }
}

impl TermBitsetWeight {
    pub fn new(term: Term, index_path: Option<PathBuf>) -> TermBitsetWeight {
        TermBitsetWeight { term, index_path }
    }
}

pub struct TermBitsetWeight {
    term: Term,
    index_path: Option<PathBuf>,
}

impl Weight for TermBitsetWeight {
    fn scorer(
        &self,
        reader: &crate::SegmentReader,
        _boost: crate::Score,
    ) -> crate::Result<Box<dyn crate::query::Scorer>> {
        
        if let Some(index_path) = &self.index_path {
            let rdb_path = index_path.join(reader.segment_id().uuid_string() + ".rddb");
            let mut opts = rocksdb::Options::default();
            opts.create_if_missing(false);
            opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
            opts.set_log_level(rocksdb::LogLevel::Error);
            opts.set_allow_mmap_reads(true);
            opts.set_blob_compression_type(rocksdb::DBCompressionType::Lz4);
            let rdb = rocksdb::DB::open(&opts, rdb_path);
            if let Ok(db) = &rdb {
                let value = self.term.value().as_u64();
                if let Some(key) = value {
                    let key_str = format!("fastfield:{}:{}", reader.schema().get_field_name(self.term.field()), key);
                    if let Ok(Some(blob)) = db.get(key_str) {
                        let bitset = BitSet::deserialize(&blob);
                        if let Ok(bitset) = bitset {
                            return Ok(Box::new(ConstScorer::new(BitSetDocSet::from(bitset), 1.0)));
                        }
                    }
                }
            }
        }
        Ok(Box::new(crate::query::EmptyScorer))
    }

    fn explain(
        &self,
        _reader: &crate::SegmentReader,
        _doc: crate::DocId,
    ) -> crate::Result<Explanation> {
        Ok(Explanation::new("TermFilterQuery", 1.0))
    }
}
