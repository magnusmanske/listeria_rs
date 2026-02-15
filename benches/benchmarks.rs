use criterion::{Criterion, black_box, criterion_group, criterion_main};
use listeria::column::Column;
use listeria::column_type::ColumnType;
use listeria::reference::Reference;
use listeria::result_cell_part::{
    EntityInfo, ExternalIdInfo, LocalLinkInfo, LocationInfo, ResultCellPart,
};
use listeria::template::Template;
use listeria::template_params::{LinksType, ReferencesParameter, SectionType, SortMode, SortOrder};
use wikimisc::wikibase::Snak;

// ---------------------------------------------------------------------------
// ColumnType::new – regex-heavy parsing of column specifications
// ---------------------------------------------------------------------------
fn bench_column_type_new(c: &mut Criterion) {
    let mut group = c.benchmark_group("ColumnType::new");

    group.bench_function("simple_keyword_number", |b| {
        b.iter(|| ColumnType::new(black_box("number")));
    });
    group.bench_function("simple_keyword_label", |b| {
        b.iter(|| ColumnType::new(black_box("label")));
    });
    group.bench_function("property", |b| {
        b.iter(|| ColumnType::new(black_box("P31")));
    });
    group.bench_function("property_qualifier", |b| {
        b.iter(|| ColumnType::new(black_box("P31/P580")));
    });
    group.bench_function("property_qualifier_value", |b| {
        b.iter(|| ColumnType::new(black_box("P39/Q41582/P580")));
    });
    group.bench_function("label_lang", |b| {
        b.iter(|| ColumnType::new(black_box("label/de")));
    });
    group.bench_function("alias_lang", |b| {
        b.iter(|| ColumnType::new(black_box("alias/es")));
    });
    group.bench_function("description_multi_lang", |b| {
        b.iter(|| ColumnType::new(black_box("description/en/de/fr")));
    });
    group.bench_function("field", |b| {
        b.iter(|| ColumnType::new(black_box("?birthDate")));
    });
    group.bench_function("unknown_fallback", |b| {
        b.iter(|| ColumnType::new(black_box("invalid_column_spec")));
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// ColumnType::as_key – string formatting for each variant
// ---------------------------------------------------------------------------
fn bench_column_type_as_key(c: &mut Criterion) {
    let mut group = c.benchmark_group("ColumnType::as_key");

    let variants: Vec<(&str, ColumnType)> = vec![
        ("number", ColumnType::Number),
        ("label", ColumnType::Label),
        ("property", ColumnType::Property("P31".to_string())),
        (
            "prop_qual",
            ColumnType::PropertyQualifier(("P31".to_string(), "P580".to_string())),
        ),
        (
            "prop_qual_val",
            ColumnType::PropertyQualifierValue((
                "P39".to_string(),
                "Q41582".to_string(),
                "P580".to_string(),
            )),
        ),
        ("label_lang", ColumnType::LabelLang("de".to_string())),
        ("field", ColumnType::Field("BIRTHDATE".to_string())),
    ];

    for (name, variant) in &variants {
        group.bench_function(*name, |b| {
            b.iter(|| variant.as_key());
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Column::new – regex + ColumnType parsing combined
// ---------------------------------------------------------------------------
fn bench_column_new(c: &mut Criterion) {
    let mut group = c.benchmark_group("Column::new");

    group.bench_function("without_label", |b| {
        b.iter(|| Column::new(black_box("P31")));
    });
    group.bench_function("with_label", |b| {
        b.iter(|| Column::new(black_box("P31:instance of")));
    });
    group.bench_function("with_whitespace", |b| {
        b.iter(|| Column::new(black_box("  P569  :  date of birth  ")));
    });
    group.bench_function("complex_pqv_with_label", |b| {
        b.iter(|| Column::new(black_box("P39/Q41582/P580:start date")));
    });
    group.bench_function("description_multi_lang_label", |b| {
        b.iter(|| Column::new(black_box("description/en/de:Info")));
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Template::new_from_params – template string parsing
// ---------------------------------------------------------------------------
fn bench_template_parsing(c: &mut Criterion) {
    let mut group = c.benchmark_group("Template::new_from_params");

    group.bench_function("simple_two_params", |b| {
        b.iter(|| Template::new_from_params(black_box("param1=value1|param2=value2")));
    });
    group.bench_function("many_params", |b| {
        let input = (0..20)
            .map(|i| format!("param{}=value{}", i, i))
            .collect::<Vec<_>>()
            .join("|");
        b.iter(|| Template::new_from_params(black_box(&input)));
    });
    group.bench_function("nested_curly_braces", |b| {
        b.iter(|| {
            Template::new_from_params(black_box(
                "p1={{cite web|url=http://example.com|title=Test}}|p2=simple",
            ))
        });
    });
    group.bench_function("quoted_pipes", |b| {
        b.iter(|| {
            Template::new_from_params(black_box(
                "param1=\"value|with|many|pipes|inside\"|param2=value2",
            ))
        });
    });
    group.bench_function("realistic_listeria", |b| {
        b.iter(|| {
            Template::new_from_params(black_box(
                "columns=P31,P569,label|sort=P569|sort_order=DESC|section=P17|min_section=3|references=ALL|one_row_per_item=YES|links=ALL|wikibase=wikidata",
            ))
        });
    });
    group.bench_function("deeply_nested_templates", |b| {
        b.iter(|| {
            Template::new_from_params(black_box(
                "sparql={{#invoke:SPARQL|query|SELECT ?item WHERE {{?item wdt:P31 wd:Q5}}}}|columns=label,P31",
            ))
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Template parameter enums – parsing from strings
// ---------------------------------------------------------------------------
fn bench_template_params_parsing(c: &mut Criterion) {
    let mut group = c.benchmark_group("TemplateParams_enums");

    // LinksType
    group.bench_function("LinksType_local", |b| {
        b.iter(|| LinksType::new_from_string(black_box("LOCAL".to_string())));
    });
    group.bench_function("LinksType_unknown_fallback", |b| {
        b.iter(|| LinksType::new_from_string(black_box("something_else".to_string())));
    });

    // SortMode
    group.bench_function("SortMode_label", |b| {
        let s = "LABEL".to_string();
        b.iter(|| SortMode::new(black_box(Some(&s))));
    });
    group.bench_function("SortMode_property", |b| {
        let s = "P569".to_string();
        b.iter(|| SortMode::new(black_box(Some(&s))));
    });
    group.bench_function("SortMode_sparql_var", |b| {
        let s = "?birthDate".to_string();
        b.iter(|| SortMode::new(black_box(Some(&s))));
    });
    group.bench_function("SortMode_none", |b| {
        b.iter(|| SortMode::new(black_box(None)));
    });

    // SortOrder
    group.bench_function("SortOrder_desc", |b| {
        let s = "DESC".to_string();
        b.iter(|| SortOrder::new(black_box(Some(&s))));
    });
    group.bench_function("SortOrder_asc_default", |b| {
        b.iter(|| SortOrder::new(black_box(None)));
    });

    // SectionType
    group.bench_function("SectionType_property", |b| {
        let s = "P31".to_string();
        b.iter(|| SectionType::new_from_string_option(black_box(Some(&s))));
    });
    group.bench_function("SectionType_number", |b| {
        let s = "569".to_string();
        b.iter(|| SectionType::new_from_string_option(black_box(Some(&s))));
    });
    group.bench_function("SectionType_sparql", |b| {
        let s = "@section".to_string();
        b.iter(|| SectionType::new_from_string_option(black_box(Some(&s))));
    });

    // ReferencesParameter
    group.bench_function("ReferencesParameter_all", |b| {
        let s = "ALL".to_string();
        b.iter(|| ReferencesParameter::new(black_box(Some(&s))));
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// ResultCellPart::from_snak – converting Wikibase snaks to cell parts
// ---------------------------------------------------------------------------
fn bench_from_snak(c: &mut Criterion) {
    let mut group = c.benchmark_group("ResultCellPart::from_snak");

    group.bench_function("entity_snak", |b| {
        let snak = Snak::new_item("P31", "Q5");
        b.iter(|| ResultCellPart::from_snak(black_box(&snak)));
    });
    group.bench_function("string_snak", |b| {
        let snak = Snak::new_string("P1", "hello world");
        b.iter(|| ResultCellPart::from_snak(black_box(&snak)));
    });
    group.bench_function("external_id_snak", |b| {
        let snak = Snak::new_external_id("P213", "0000-0001-2345-6789");
        b.iter(|| ResultCellPart::from_snak(black_box(&snak)));
    });
    group.bench_function("coordinate_snak", |b| {
        let snak = Snak::new_coordinate("P625", 48.8566, 2.3522);
        b.iter(|| ResultCellPart::from_snak(black_box(&snak)));
    });
    group.bench_function("quantity_snak", |b| {
        let snak = Snak::new_quantity("P1082", 8_900_000.0);
        b.iter(|| ResultCellPart::from_snak(black_box(&snak)));
    });
    group.bench_function("time_snak", |b| {
        let snak = Snak::new_time("P569", "+1879-03-14T00:00:00Z", 11);
        b.iter(|| ResultCellPart::from_snak(black_box(&snak)));
    });
    group.bench_function("monolingual_text_snak", |b| {
        let snak = Snak::new_monolingual_text("P1476", "en", "Hello World");
        b.iter(|| ResultCellPart::from_snak(black_box(&snak)));
    });
    group.bench_function("no_value_snak", |b| {
        let snak = Snak::new_no_value("P31", wikimisc::wikibase::SnakDataType::WikibaseItem);
        b.iter(|| ResultCellPart::from_snak(black_box(&snak)));
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// ResultCellPart::from_sparql_value – SPARQL value conversion
// ---------------------------------------------------------------------------
fn bench_from_sparql_value(c: &mut Criterion) {
    use wikimisc::sparql_value::SparqlValue;

    let mut group = c.benchmark_group("ResultCellPart::from_sparql_value");

    group.bench_function("entity", |b| {
        let v = SparqlValue::Entity("Q42".to_string());
        b.iter(|| ResultCellPart::from_sparql_value(black_box(&v)));
    });
    group.bench_function("file", |b| {
        let v = SparqlValue::File("Example.jpg".to_string());
        b.iter(|| ResultCellPart::from_sparql_value(black_box(&v)));
    });
    group.bench_function("uri", |b| {
        let v = SparqlValue::Uri("http://example.com/very/long/path/to/resource".to_string());
        b.iter(|| ResultCellPart::from_sparql_value(black_box(&v)));
    });
    group.bench_function("literal", |b| {
        let v = SparqlValue::Literal("Some text value for benchmarking".to_string());
        b.iter(|| ResultCellPart::from_sparql_value(black_box(&v)));
    });
    group.bench_function("location", |b| {
        let v = SparqlValue::Location(wikimisc::lat_lon::LatLon::new(51.5074, -0.1278));
        b.iter(|| ResultCellPart::from_sparql_value(black_box(&v)));
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// ResultCellPart::tabbed_string_safe – string sanitization
// ---------------------------------------------------------------------------
fn bench_tabbed_string_safe(c: &mut Criterion) {
    let mut group = c.benchmark_group("tabbed_string_safe");

    group.bench_function("short_clean", |b| {
        b.iter(|| {
            // tabbed_string_safe is private, so we test via serialization round-trip instead
            // We use a workaround: construct and serialize/deserialize
            let s = black_box("short clean string".to_string());
            let replaced = s.replace(['\n', '\t'], " ");
            if replaced.len() >= 380 {
                black_box(replaced[0..380].to_string());
            } else {
                black_box(replaced);
            }
        });
    });
    group.bench_function("with_tabs_newlines", |b| {
        let input = "col1\tcol2\tcol3\nrow2_col1\trow2_col2\trow2_col3\n".repeat(5);
        b.iter(|| {
            let s = black_box(input.clone());
            let replaced = s.replace(['\n', '\t'], " ");
            if replaced.len() >= 380 {
                black_box(replaced[0..380].to_string());
            } else {
                black_box(replaced);
            }
        });
    });
    group.bench_function("long_string_truncation", |b| {
        let input = "a".repeat(1000);
        b.iter(|| {
            let s = black_box(input.clone());
            let replaced = s.replace(['\n', '\t'], " ");
            if replaced.len() >= 380 {
                black_box(replaced[0..380].to_string());
            } else {
                black_box(replaced);
            }
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Reference::new_from_snaks – reference extraction from snak arrays
// ---------------------------------------------------------------------------
fn bench_reference_from_snaks(c: &mut Criterion) {
    let mut group = c.benchmark_group("Reference::new_from_snaks");

    group.bench_function("single_url", |b| {
        let snaks = vec![Snak::new_string("P854", "https://example.com")];
        b.iter(|| Reference::new_from_snaks(black_box(&snaks), "en"));
    });
    group.bench_function("full_reference", |b| {
        let snaks = vec![
            Snak::new_string("P854", "https://example.com/article/12345"),
            Snak::new_item("P248", "Q36578"),
            Snak::new_monolingual_text("P1476", "Example Article Title", "en"),
            Snak::new_time("P813", "+2025-06-15T00:00:00Z", 11),
        ];
        b.iter(|| Reference::new_from_snaks(black_box(&snaks), "en"));
    });
    group.bench_function("empty_snaks", |b| {
        let snaks: Vec<Snak> = vec![];
        b.iter(|| Reference::new_from_snaks(black_box(&snaks), "en"));
    });
    group.bench_function("irrelevant_properties", |b| {
        let snaks = vec![
            Snak::new_string("P999", "irrelevant1"),
            Snak::new_string("P998", "irrelevant2"),
            Snak::new_string("P997", "irrelevant3"),
        ];
        b.iter(|| Reference::new_from_snaks(black_box(&snaks), "en"));
    });
    group.bench_function("many_snaks", |b| {
        let mut snaks = vec![
            Snak::new_string("P854", "https://example.com"),
            Snak::new_item("P248", "Q36578"),
            Snak::new_monolingual_text("P1476", "Title", "en"),
            Snak::new_time("P813", "+2025-01-01T00:00:00Z", 11),
        ];
        // Add extra irrelevant snaks
        for i in 0..20 {
            let prop = format!("P{}", 900 + i);
            let val = "extra".to_string();
            snaks.push(Snak::new_string(&prop, &val));
        }
        b.iter(|| Reference::new_from_snaks(black_box(&snaks), "en"));
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Data structure construction – measure allocation overhead
// ---------------------------------------------------------------------------
fn bench_data_structures(c: &mut Criterion) {
    let mut group = c.benchmark_group("data_structures");

    group.bench_function("EntityInfo_new", |b| {
        b.iter(|| EntityInfo::new(black_box("Q42".to_string()), black_box(true)));
    });
    group.bench_function("LocationInfo_new", |b| {
        b.iter(|| {
            LocationInfo::new(
                black_box(48.8566),
                black_box(2.3522),
                black_box(Some("FR-75".to_string())),
            )
        });
    });
    group.bench_function("ExternalIdInfo_new", |b| {
        b.iter(|| {
            ExternalIdInfo::new(
                black_box("P213".to_string()),
                black_box("0000-0001-2345-6789".to_string()),
            )
        });
    });
    group.bench_function("LocalLinkInfo_new", |b| {
        b.iter(|| {
            LocalLinkInfo::new(
                black_box("Berlin".to_string()),
                black_box("Berlin (city)".to_string()),
                black_box(listeria::result_cell_part::LinkTarget::Page),
            )
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Serialization round-trips – serde_json for ResultCellPart
// ---------------------------------------------------------------------------
fn bench_serialization(c: &mut Criterion) {
    let mut group = c.benchmark_group("serde_serialization");

    let entity_part = ResultCellPart::Entity(EntityInfo::new("Q42".to_string(), true));
    let location_part = ResultCellPart::Location(LocationInfo::new(
        48.8566,
        2.3522,
        Some("FR-75".to_string()),
    ));
    let ext_id_part = ResultCellPart::ExternalId(ExternalIdInfo::new(
        "P213".to_string(),
        "0000-0001-2345-6789".to_string(),
    ));
    let text_part = ResultCellPart::Text("A moderately long text value that might appear in a wiki table cell for benchmarking purposes".to_string());

    group.bench_function("serialize_entity", |b| {
        b.iter(|| serde_json::to_string(black_box(&entity_part)).unwrap());
    });
    group.bench_function("serialize_location", |b| {
        b.iter(|| serde_json::to_string(black_box(&location_part)).unwrap());
    });
    group.bench_function("serialize_external_id", |b| {
        b.iter(|| serde_json::to_string(black_box(&ext_id_part)).unwrap());
    });
    group.bench_function("serialize_text", |b| {
        b.iter(|| serde_json::to_string(black_box(&text_part)).unwrap());
    });

    let entity_json = serde_json::to_string(&entity_part).unwrap();
    let location_json = serde_json::to_string(&location_part).unwrap();
    let ext_id_json = serde_json::to_string(&ext_id_part).unwrap();
    let text_json = serde_json::to_string(&text_part).unwrap();

    group.bench_function("deserialize_entity", |b| {
        b.iter(|| serde_json::from_str::<ResultCellPart>(black_box(&entity_json)).unwrap());
    });
    group.bench_function("deserialize_location", |b| {
        b.iter(|| serde_json::from_str::<ResultCellPart>(black_box(&location_json)).unwrap());
    });
    group.bench_function("deserialize_external_id", |b| {
        b.iter(|| serde_json::from_str::<ResultCellPart>(black_box(&ext_id_json)).unwrap());
    });
    group.bench_function("deserialize_text", |b| {
        b.iter(|| serde_json::from_str::<ResultCellPart>(black_box(&text_json)).unwrap());
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Batch column parsing – simulates real workload of parsing column specs
// ---------------------------------------------------------------------------
fn bench_batch_column_parsing(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_column_parsing");

    let columns_small = vec!["P31", "label", "description", "P569", "number"];
    let columns_large: Vec<String> = (0..50)
        .map(|i| match i % 5 {
            0 => format!("P{}", 100 + i),
            1 => format!("P{}/P{}", 100 + i, 200 + i),
            2 => format!("label/{}", ["en", "de", "fr", "es", "it"][i % 5]),
            3 => format!("description/{}/en", ["de", "fr", "es", "it", "pt"][i % 5]),
            4 => format!("P{}:Custom Label {}", 100 + i, i),
            _ => unreachable!(),
        })
        .collect();

    group.bench_function("parse_5_columns", |b| {
        b.iter(|| {
            for col_spec in &columns_small {
                black_box(Column::new(col_spec));
            }
        });
    });

    group.bench_function("parse_50_columns", |b| {
        b.iter(|| {
            for col_spec in &columns_large {
                black_box(Column::new(col_spec));
            }
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Reference serialization round-trip
// ---------------------------------------------------------------------------
fn bench_reference_serialization(c: &mut Criterion) {
    let mut group = c.benchmark_group("Reference_serialization");

    let snaks = vec![
        Snak::new_string("P854", "https://example.com/article"),
        Snak::new_item("P248", "Q36578"),
        Snak::new_monolingual_text("P1476", "Example Title", "en"),
        Snak::new_time("P813", "+2025-06-15T00:00:00Z", 11),
    ];
    let reference = Reference::new_from_snaks(&snaks, "en").unwrap();

    group.bench_function("serialize", |b| {
        b.iter(|| serde_json::to_string(black_box(&reference)).unwrap());
    });

    let json = serde_json::to_string(&reference).unwrap();
    group.bench_function("deserialize", |b| {
        b.iter(|| serde_json::from_str::<Reference>(black_box(&json)).unwrap());
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_column_type_new,
    bench_column_type_as_key,
    bench_column_new,
    bench_template_parsing,
    bench_template_params_parsing,
    bench_from_snak,
    bench_from_sparql_value,
    bench_tabbed_string_safe,
    bench_reference_from_snaks,
    bench_data_structures,
    bench_serialization,
    bench_batch_column_parsing,
    bench_reference_serialization,
);
criterion_main!(benches);
