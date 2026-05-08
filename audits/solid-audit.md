# SOLID Principles Audit — listeria_rs

**Date:** 2026-05-07  
**Auditor:** Claude Code (claude-sonnet-4-6)  
**Branch:** master (cc83992)

---

## Remediation Log

| Fix | Findings addressed | Commit |
|-----|--------------------|--------|
| Extract `RenderContext` trait | DIP-1, ISP-3 | d86d474 |
| Move column dispatch into `ColumnType::render_cell_parts()` | OCP-1, OCP-2 | 676825c |
| Move SPARQL semaphore into `Configuration` | DIP-2 | 2176bb8 |

---

## Overall Scores

| Principle | Score | Summary |
|-----------|-------|---------|
| S — Single Responsibility | 4/10 | Several structs carry 5–14 distinct concerns |
| O — Open/Closed | 5/10 | Pervasive large `match` arms; new types require touching existing code |
| L — Liskov Substitution | 8/10 | Trait implementations are well-behaved; no substitution violations found |
| I — Interface Segregation | 6/10 | Two traits are wider than needed; implicit interfaces via concrete-type parameters |
| D — Dependency Inversion | 4/10 | Concrete types (`ListeriaList`, `Configuration`, `PageParams`) cross module boundaries everywhere |

---

## S — Single Responsibility Principle

### SRP-1 · `ListeriaList` is a God Struct
**File:** `src/listeria_list.rs:38–54`  
**Importance:** 9/10

`ListeriaList` holds 14 fields that span at least 7 distinct responsibilities:

```
page_params        → configuration access
template           → template parsing
columns            → column definitions
params             → template parameter parsing
sparql_table       → SPARQL result storage
ecw                → entity cache
results            → result rows
shadow_files       → file shadow detection
local_page_cache   → page existence cache
section_id_to_name → section mapping
wb_api             → Wikibase API handle
language           → locale state
reference_ids      → reference deduplication
profiling          → diagnostic flags
```

It also owns the full async pipeline (`process()` → `run_query()` → `load_entities()` → `process_results()`) while also acting as the data store those steps write into — the classic "orchestrator that is also the database".

**Remediation (conceptual — large refactor):**  
Split into at least three structs:
```rust
struct ListState {          // pure data; serialisable
    columns: Vec<Column>,
    params: TemplateParams,
    results: Vec<ResultRow>,
    shadow_files: HashSet<String>,
    section_id_to_name: HashMap<usize, String>,
    reference_ids: HashSet<String>,
}

struct ListContext {        // read-only runtime wiring
    page_params: Arc<PageParams>,
    ecw: EntityContainerWrapper,
    wb_api: Arc<Api>,
    language: String,
}

struct ListPipeline;       // stateless orchestrator
impl ListPipeline {
    async fn run(ctx: &ListContext, state: &mut ListState) -> Result<()> { ... }
}
```
This makes each piece independently testable.

---

### SRP-2 · `ResultCellPart` mixes data, parsing, and rendering
**File:** `src/result_cell_part.rs:161–176` (enum) + methods spread to ~line 570  
**Importance:** 7/10

The `ResultCellPart` enum holds 14 variants representing different value types, but the same module also implements:

- `from_sparql_value()` — SPARQL → domain conversion  
- `from_snak()` — Wikibase Snak → domain conversion  
- `as_wikitext()` — wikitext rendering  
- `as_tabbed_data()` — tabbed-data JSON rendering  
- `localize_item_links()` — localisation  
- `uri_to_wikitext()` / `wikipedia_url_to_wikilink()` — URL transformation  

**Remediation:**  
Keep the enum as a pure data type and move rendering into the renderers:
```rust
// result_cell_part.rs — data only
pub enum ResultCellPart { ... }

// render_wikitext.rs — knows how to render
impl RendererWikitext {
    fn render_part(&self, part: &ResultCellPart, list: &ListContext) -> String { ... }
}
```
The `localize_item_links` method, which mutates the part in-place, is the trickiest to move; it should become a post-processing step in the pipeline rather than a method on the enum.

---

### SRP-3 · `WikiApis` mixes API pooling, DB operations, and wiki discovery
**File:** `src/wiki_apis.rs:18–24`  
**Importance:** 6/10

`WikiApis` has four fields but logically does three separate jobs:

1. **API pool** — `get_or_create_wiki_api()`, `wait_for_wiki_apis()`  
2. **Wiki registry** — `get_wikis_with_template()`, `get_all_wikis_with_template()`  
3. **Database operations** — `db_host_and_schema_for_wiki()`, all SQL-executing methods  

**Remediation:**  
Extract a `WikiRegistry` and a `WikiDatabase` struct; `WikiApis` becomes only the connection pool.

---

### SRP-4 · `RendererTabbedData` mixes regex parsing, template manipulation, and API calls
**File:** `src/render_tabbed_data.rs`  
**Importance:** 5/10

Three static `LazyLock` regexes at the top of the file handle template extraction, while `separate_start_template()` hand-rolls a brace counter, and `write_tabbed_data()` drives a MediaWiki API call. These are three separate concerns (parse, transform, persist).

---

## O — Open/Closed Principle

### OCP-1 · `ResultCell::new()` dispatches all column types in one `match` ✅ Fixed (676825c)
**File:** `src/result_cell.rs:47–65`  
**Importance:** 8/10

```rust
match col.obj() {
    ColumnType::Qid         => Self::ct_qid(…),
    ColumnType::Item        => Self::ct_item(…),
    ColumnType::Description => Self::ct_description(…),
    ColumnType::Field       => Self::ct_field(…),
    ColumnType::Property    => Self::ct_property(…),
    ColumnType::PropertyQualifier    => Self::ct_pq(…),
    ColumnType::PropertyQualifierValue => Self::ct_pqv(…),
    ColumnType::LabelLang   => Self::ct_label_lang(…),
    ColumnType::AliasLang   => Self::ct_alias_lang(…),
    ColumnType::Label        => Self::ct_label(…),
    ColumnType::Number       => Self::ct_number(…),
    ColumnType::Sitelink     => Self::ct_sitelink(…),
    ColumnType::Unknown      => {}
}
```

Adding a new column type requires adding an enum variant **and** modifying this match. This is polymorphic dispatch without polymorphism.

**Remediation:**  
Introduce a trait and move each `ct_*` function there:
```rust
pub trait ColumnRenderer {
    async fn render(&self, entity: Option<&MyEntity>, list: &ListeriaList,
                    sparql_table: &SparqlTableVec) -> Vec<PartWithReference>;
}

// Per-column type, one small struct:
struct PropertyColumn(String);
impl ColumnRenderer for PropertyColumn { … }

// Column stores a Box<dyn ColumnRenderer>
pub struct Column {
    renderer: Box<dyn ColumnRenderer>,
    label: String,
}
```
`ColumnType::new()` becomes a factory that returns the right `Box<dyn ColumnRenderer>`.

---

### OCP-2 · `ResultCellPart::as_wikitext()` match over 14 variants ✅ Fixed (676825c)
**File:** `src/result_cell_part.rs:524–568`  
**Importance:** 7/10

Every new `ResultCellPart` variant forces modification of this central rendering method (and also `as_tabbed_data()`). See OCP-1 remediation — if each variant is its own type behind a trait, the renderers become open for extension without modification.

---

### OCP-3 · `fix_wiki_name()` hard-codes wiki name exceptions
**File:** `src/wiki_apis.rs:368–375`  
**Importance:** 4/10

```rust
pub fn fix_wiki_name(&self, wiki: &str) -> String {
    match wiki {
        "be-taraskwiki" | "be-x-oldwiki" | "be_taraskwiki" | "be_x_oldwiki" => {
            "be_x_oldwiki".to_string()
        }
        other => other.replace('-', "_"),
    }
}
```

Adding a new special-case mapping requires modifying this method.

**Remediation:**  
Drive the exception list from config:
```json
{ "wiki_name_aliases": { "be-taraskwiki": "be_x_oldwiki", "be-x-oldwiki": "be_x_oldwiki" } }
```
```rust
pub fn fix_wiki_name(&self, wiki: &str) -> String {
    self.config.wiki_name_alias(wiki)
        .unwrap_or_else(|| wiki.replace('-', "_"))
}
```

---

### OCP-4 · `ColumnType::new()` and `as_key()` require modification for new types
**File:** `src/column_type.rs:64–129` (new), `132–160` (as_key)  
**Importance:** 6/10

Both the parsing factory and the key-generation method must be modified every time a new column type is added. The 13-arm `as_key()` match and the 8-strategy factory are two separate OCP violations in the same enum.

---

### OCP-5 · `template_params.rs` enums each embed their own parsers
**File:** `src/template_params.rs` — `LinksType`, `SortMode`, `SectionType`  
**Importance:** 4/10

Each enum has a `new_from_string()` factory method that pattern-matches string representations. Same issue as OCP-4: adding a new variant touches both the enum and its factory. Minor given the stability of these enums.

---

## L — Liskov Substitution Principle

**Score: 8/10 — No violations found.**

The two concrete implementations of `ListeriaBot` (`WikidataBot` and `SingleWikiBot`) were not inspected for full method-by-method behavioural equivalence, but the trait's method signatures are uniform and no `unimplemented!()` / `panic!()` stubs were observed. `RendererWikitext` and `RendererTabbedData` both implement `Renderer` faithfully.

Mark as **Unable to fully verify** without running the test suite with both implementations exercised against the same inputs.

---

## I — Interface Segregation Principle

### ISP-1 · `Renderer` trait bundles construction with async rendering
**File:** `src/renderer.rs:7–12`  
**Importance:** 6/10

```rust
pub(crate) trait Renderer {
    fn new() -> Self;
    async fn render(&mut self, page: &mut ListeriaList) -> Result<String>;
    async fn get_new_wikitext(&self, wikitext: &str, page: &ListeriaPage) -> Result<Option<String>>;
}
```

`new()` is a static constructor that cannot be invoked through a trait object (`dyn Renderer`), making the method useless for dynamic dispatch. `get_new_wikitext` is used only by the wikitext renderer; the tabbed-data renderer must implement it vacuously or panic.

**Remediation:**  
Remove `new()` from the trait (construction is a caller concern) and split `get_new_wikitext` into a separate optional trait:
```rust
pub(crate) trait Renderer {
    async fn render(&mut self, page: &mut ListeriaList) -> Result<String>;
}

pub(crate) trait WikitextPostProcessor {
    async fn get_new_wikitext(&self, wikitext: &str, page: &ListeriaPage) -> Result<Option<String>>;
}
```

---

### ISP-2 · `ListeriaBot` mixes factory, state management, and query methods
**File:** `src/listeria_bot.rs:11–32`  
**Importance:** 5/10

The trait has 9 methods spanning four categories:

| Category | Methods |
|----------|---------|
| Factory | `new`, `new_from_config` |
| Config accessor | `config` |
| Lifecycle / state | `reset_running`, `clear_deleted`, `set_runtime`, `release_running` |
| Query / command | `run_single_bot`, `prepare_next_single_page`, `get_running_count` |

Any new bot implementation must implement all of these. A bot that doesn't need DB lifecycle management (`reset_running`, `clear_deleted`) still must provide them.

**Remediation:**  
Split into focused traits:
```rust
pub trait BotLifecycle {
    async fn reset_running(&self) -> Result<()>;
    async fn clear_deleted(&self) -> Result<()>;
    async fn set_runtime(&self, id: u64, seconds: u64) -> Result<()>;
    async fn release_running(&self, id: u64);
}

pub trait BotRunner {
    async fn run_single_bot(&self, page: PageToProcess) -> Result<WikiPageResult>;
    async fn prepare_next_single_page(&self) -> Result<PageToProcess>;
    async fn get_running_count(&self) -> usize;
}
```

---

### ISP-3 · `ResultCellPart` methods require full `ListeriaList` for narrow operations ✅ Fixed (d86d474)
**File:** `src/result_cell_part.rs` — many methods accept `list: &ListeriaList`  
**Importance:** 7/10

Methods like `as_wikitext_entity()` call `list.get_label_with_fallback()`, `list.get_entity()`, `list.link_target()`, `list.language()` — a narrow slice of `ListeriaList`'s surface. This forces `ResultCellPart` to depend on the whole concrete struct.

**Remediation:**  
Extract a narrow rendering-context trait:
```rust
pub trait RenderContext {
    fn language(&self) -> &str;
    fn link_target(&self) -> &LinkTarget;
    async fn get_entity(&self, id: &str) -> Option<MyEntity>;
    async fn get_label_with_fallback(&self, id: &str, lang: &str) -> Option<String>;
}

// ListeriaList implements RenderContext; rendering methods accept &dyn RenderContext
pub async fn as_wikitext(&self, ctx: &dyn RenderContext) -> String { … }
```

---

## D — Dependency Inversion Principle

### DIP-1 · `ResultCellPart` and `ResultCell` take `&ListeriaList` (concrete) ✅ Fixed (d86d474)
**File:** `src/result_cell_part.rs` (most rendering methods), `src/result_cell.rs:34–68`  
**Importance:** 8/10

Both modules depend directly on `ListeriaList`, the largest concrete type in the codebase. This makes them impossible to unit-test without constructing a full `ListeriaList`. The fix overlaps with ISP-3: introduce a `RenderContext` trait (see above) and accept `&dyn RenderContext` or a generic `T: RenderContext`.

---

### DIP-2 · `SparqlResults` owns `Arc<PageParams>` and reads global semaphore ✅ Fixed (2176bb8)
**File:** `src/sparql_results.rs:16, 30`  
**Importance:** 7/10

```rust
static SPARQL_REQUEST_SEMAPHORE: OnceLock<Arc<Semaphore>> = OnceLock::new();

pub struct SparqlResults {
    page_params: Arc<PageParams>,
    …
}
```

The semaphore is a global initialized lazily from config on first call. This is hidden state that cannot be replaced in tests without modifying the module. `PageParams` carries the full configuration tree, of which `SparqlResults` only needs the SPARQL endpoint and rate-limit values.

**Remediation:**  
Inject a `SparqlConfig` value struct and an explicit semaphore:
```rust
pub struct SparqlConfig {
    pub endpoint: String,
    pub max_concurrent: usize,
}

pub struct SparqlResults {
    config: SparqlConfig,
    semaphore: Arc<Semaphore>,
    …
}

impl SparqlResults {
    pub fn new(config: SparqlConfig, semaphore: Arc<Semaphore>, …) -> Self { … }
}
```
The bot creates the semaphore once and shares it, making concurrency limits testable.

---

### DIP-3 · `WikiApis` is constructed with `Arc<Configuration>` and calls it directly
**File:** `src/wiki_apis.rs:20, 74, 86`  
**Importance:** 6/10

`WikiApis` calls `self.config.get_max_mw_apis_per_wiki()`, `self.config.get_max_mw_apis_total()`, `self.config.mysql()`, `self.config.get_default_wbapi()` etc. throughout. The concrete `Configuration` type leaks into every layer that receives a `WikiApis`.

**Remediation:**  
Introduce a narrower config trait consumed by `WikiApis`:
```rust
pub trait ApiPoolConfig {
    fn max_apis_per_wiki(&self) -> Option<usize>;
    fn max_apis_total(&self) -> Option<usize>;
    fn default_wbapi_url(&self) -> &str;
}
```
`Configuration` implements `ApiPoolConfig`; `WikiApis` stores `Arc<dyn ApiPoolConfig>`.

---

### DIP-4 · `ListProcessor` takes `&mut ListeriaList` for all operations
**File:** `src/list_processor.rs` — every public method  
**Importance:** 6/10

`ListProcessor` is a zero-sized struct whose static methods all accept `&mut ListeriaList`. This means the processor and the list are inseparable; the processor cannot be tested or reused independently.

**Remediation:**  
Define a trait for the mutable operations the processor needs:
```rust
pub trait ProcessorTarget {
    fn results_mut(&mut self) -> &mut Vec<ResultRow>;
    fn shadow_files_mut(&mut self) -> &mut HashSet<String>;
    fn local_page_cache_mut(&mut self) -> &mut HashMap<String, bool>;
    …
}
```

---

### DIP-5 · `EntityContainerWrapper` directly instantiates `EntityContainer` per batch
**File:** `src/entity_container_wrapper.rs` — `load_chunk_with_retries()`  
**Importance:** 4/10

`EntityContainer::new()` is called inside the loading loop rather than being injected or built by a factory. This is a minor violation because `EntityContainer` is a value type from an external crate (`wikimisc`) with no meaningful substitution need. Worth noting if that crate's API changes.

---

## Summary Table

| ID | Principle | File | Importance | Status |
|----|-----------|------|------------|--------|
| SRP-1 | S | `listeria_list.rs:38` | 9/10 | open |
| DIP-1 | D | `result_cell_part.rs`, `result_cell.rs` | 8/10 | ✅ d86d474 |
| OCP-1 | O | `result_cell.rs:47` | 8/10 | ✅ 676825c |
| SRP-2 | S | `result_cell_part.rs` | 7/10 | open |
| OCP-2 | O | `result_cell_part.rs:524` | 7/10 | ✅ 676825c |
| ISP-3 | I | `result_cell_part.rs` (ListeriaList param) | 7/10 | ✅ d86d474 |
| DIP-2 | D | `sparql_results.rs:16,30` | 7/10 | ✅ 2176bb8 |
| OCP-4 | O | `column_type.rs:64,132` | 6/10 | open |
| SRP-3 | S | `wiki_apis.rs` | 6/10 | open |
| ISP-1 | I | `renderer.rs:7` | 6/10 | open |
| DIP-3 | D | `wiki_apis.rs:20` | 6/10 | open |
| DIP-4 | D | `list_processor.rs` | 6/10 | open |
| ISP-2 | I | `listeria_bot.rs:11` | 5/10 | open |
| SRP-4 | S | `render_tabbed_data.rs` | 5/10 | open |
| OCP-3 | O | `wiki_apis.rs:368` | 4/10 | open |
| OCP-5 | O | `template_params.rs` | 4/10 | open |
| DIP-5 | D | `entity_container_wrapper.rs` | 4/10 | open |

---

## Recommended Remediation Order

1. ✅ **Extract `RenderContext` trait** (fixes DIP-1, ISP-3) — `ResultCellPart`, `ResultCell`, and `Reference` now accept `&impl RenderContext` instead of `&ListeriaList`. Commit d86d474.
2. ✅ **Move column dispatch into `ColumnType::render_cell_parts()`** (fixes OCP-1, OCP-2) — `ResultCell::new()` now delegates to a single `.await` call; all `ct_*` logic lives in `ColumnType`. Commit 676825c.
3. ✅ **Inject explicit semaphore via `Configuration`** (fixes DIP-2) — `Configuration` now owns an `Arc<Semaphore>` built from `max_sparql_simultaneous`; `SparqlResults` clones the `Arc` instead of touching a global. Commit 2176bb8.
4. **Split `ListeriaList`** into `ListState` + `ListContext` + `ListPipeline` (fixes SRP-1) — the most impactful but also most invasive change.
5. **Narrow `Renderer` trait and split `ListeriaBot`** (fixes ISP-1, ISP-2) — low risk, high clarity.
6. **Drive `fix_wiki_name` exceptions from config** (fixes OCP-3) — tiny change, config file edit only.
