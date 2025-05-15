// # Basic Example
//
// This example covers the basic functionalities of
// tantivy.
//
// We will :
// - define our schema
// - create an index in a directory
// - index a few documents into our index
// - search for the best document matching a basic query
// - retrieve the best document's original content.

use std::collections::HashMap;

use common::BitSet;
use rocksdb::DB;
// ---
// Importing tantivy...
use tantivy::collector::TopDocs;
use tantivy::directory::MmapDirectory;
use tantivy::query::TermFilterQuery;
use tantivy::{doc, Index, IndexWriter, ReloadPolicy};
use tantivy::{schema::*, Directory, TantivyError};

fn main() -> tantivy::Result<()> {
    let index_path = std::path::Path::new("D:/Work/intellipins/tantivy/data");

    let mut schema_builder = Schema::builder();

    schema_builder.add_text_field("title", TEXT | STORED);

    schema_builder.add_text_field("body", TEXT);
    schema_builder.add_u64_field("filter", FAST | INDEXED);

    let schema = schema_builder.build();

    let index = Index::open_or_create(MmapDirectory::open(index_path)?, schema.clone())?;

    let mut index_writer: IndexWriter = index.writer(50_000_000)?;

    let filter = schema.get_field("filter").unwrap();
    let title = schema.get_field("title").unwrap();
    let body = schema.get_field("body").unwrap();

    let mut old_man_doc = TantivyDocument::default();
    old_man_doc.add_text(title, "The Old Man and the Sea");
    old_man_doc.add_text(
        body,
        "He was an old man who fished alone in a skiff in the Gulf Stream and he had gone \
         eighty-four days now without taking a fish.",
    );
    old_man_doc.add_u64(filter, 1u64);

    index_writer.add_document(old_man_doc)?;

    index_writer.add_document(doc!(
    title => "Of Mice and Men",
    body => "A few miles south of Soledad, the Salinas River drops in close to the hillside \
            bank and runs deep and green. The water is warm too, for it has slipped twinkling \
            over the yellow sands in the sunlight before reaching the narrow pool. On one \
            side of the river the golden foothill slopes curve up to the strong and rocky \
            Gabilan Mountains, but on the valley side the water is lined with trees—willows \
            fresh and green with every spring, carrying in their lower leaf junctures the \
            debris of the winter’s flooding; and sycamores with mottled, white, recumbent \
            limbs and branches that arch over the pool",
    filter => 2u64
    ))?;

    index_writer.add_document(doc!(
    title => "Frankenstein",
    title => "The Modern Prometheus",
    body => "You will rejoice to hear that no disaster has accompanied the commencement of an \
             enterprise which you have regarded with such evil forebodings.  I arrived here \
             yesterday, and my first task is to assure my dear sister of my welfare and \
             increasing confidence in the success of my undertaking.",
    filter => 3u64
    ))?;

    index_writer.commit()?;

    dump_fast_field_bitsets_to_rocksdb(&index, "filter")?;

    let reader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::OnCommitWithDelay)
        .try_into()?;

    let searcher = reader.searcher();

    let term = Term::from_field_u64(filter, 2u64);
    let query = TermFilterQuery::new(term, true, Some(std::path::PathBuf::from(index_path)));

    let top_docs = searcher.search(&query, &TopDocs::with_limit(10))?;

    for (_score, doc_address) in top_docs {
        let retrieved_doc: TantivyDocument = searcher.doc(doc_address)?;
        println!(
            "doc_address: {:?} doc_id: {:?} doc: {:?}",
            doc_address,
            doc_address.doc_id,
            retrieved_doc.to_json(&schema)
        );
    }

    Ok(())
}

#[cfg(windows)]
fn clean_path(path: &std::path::PathBuf) -> std::path::PathBuf {
    use std::path::PathBuf;

    PathBuf::from(path.to_string_lossy().trim_start_matches(r"\\?\"))
}

#[cfg(not(windows))]
fn clean_path(path: &PathBuf) -> PathBuf {
    path.clone()
}

fn dump_fast_field_bitsets_to_rocksdb(index: &Index, field_name: &str) -> tantivy::Result<()> {
    let reader = index.reader()?;
    let searcher = reader.searcher();

    for segment_reader in searcher.segment_readers() {
        let mut value_to_bitmap: HashMap<u64, BitSet> = HashMap::new();

        let rdb_filename = format!("{}.rddb", segment_reader.segment_id().uuid_string());
        let rdb_complete_path = index
            .directory()
            .complete_path(std::path::Path::new(&rdb_filename));
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
        opts.set_log_level(rocksdb::LogLevel::Error);
        opts.set_allow_mmap_reads(true);
        opts.set_blob_compression_type(rocksdb::DBCompressionType::Lz4);
        let rdb = DB::open(&opts, clean_path(&rdb_complete_path))
            .map_err(|op| TantivyError::InternalError(op.into_string()))?;

        let fast_fields = segment_reader.fast_fields();
        let alive_docs = segment_reader.alive_bitset();
        let segment_max_docs = segment_reader.max_doc();
        for doc_id in 0..segment_max_docs {
            if alive_docs
                .as_ref()
                .map_or(true, |alive| alive.is_alive(doc_id))
            {
                let value = fast_fields
                    .u64(field_name)?
                    .first(doc_id)
                    .unwrap_or_default();

                value_to_bitmap
                    .entry(value)
                    .or_insert_with(|| BitSet::with_max_value(segment_max_docs))
                    .insert(doc_id as u32); // doc_id fits in u32
            }
        }

        for (value, bitmap) in &value_to_bitmap {
            let key = format!("fastfield:{}:{}", field_name, value);
            let mut buf = vec![];
            bitmap
                .serialize(&mut buf)
                .expect("Serialization of Bitset failed");

            rdb.put(key, buf)
                .map_err(|e| TantivyError::InternalError(e.into_string()))?;
        }
    }

    Ok(())
}
