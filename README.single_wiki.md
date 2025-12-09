# Prepare config file
## Basic configuration
In this example, we use `myproject.wikibase.cloud` as the wiki to run Listeria on. You will also need a `BOT_EDIT_TOKEN` for Listeria to edit the wiki.
```json
{
	"apis": {
		"wiki": "https://myproject.wikibase.cloud/w/api.php",
		"commons": "https://commons.wikimedia.org/w/api.php"
	},
	"default_api": "wiki",
	"query_endpoint": "https://myproject.wikibase.cloud/query/sparql",
	"sparql_prefix": "PREFIX m: <https://myproject.wikibase.cloud/entity/>\nPREFIX mt: <https://myproject.wikibase.cloud/prop/direct/>",
	"main_item_prefix": "Item:",
	"template_start": "Listeria list",
	"template_end": "Listeria list end",
	"max_threads": 1,
	"wiki_login": {
		"token": "BOT_EDIT_TOKEN"
	}
}
```
## Extended parameters
You might want to use some extended parameters to fine-tune Listeria to your wiki setup:
```json
{
	"profiling": false,
	"default_language": "en",
	"prefer_preferred": true,
	"namespace_blocks": {},
	"location_templates": {
		"default": "{{Coord|$LAT$|$LON$|display=inline}}"
	},
	"max_sparql_simultaneous": 2,
	"max_sparql_attempts": 1,
	"max_concurrent_entry_queries": 100,
	"max_local_cached_entities": 3000,
	"api_timeout": 600,
	"ms_delay_after_edit": 100,
	"location_regions": [],
	"default_thumbnail_size": 128,
	"shadow_images_check": []
}
```

# Run Listeria using plain Rust
## Setup
Install Rust and Cargo, unless you have already done so. Clone the git repo.
```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
git clone https://github.com/magnusmanske/listeria_rs.git
```
## Run Listeria
Build and run the application.
```bash
cd listeria_rs
cargo run --release --bin single -- MY_CONFIG_FILE_PATH
```

# Run Listeria using Docker
## Build the Docker Image
```bash
docker build -t listeria . -f Dockerfile.single_wiki
```

## Run from Docker Image
```bash
docker run --rm  --name listeria -v MY_CONFIG_FILE_PATH:/etc/app/config.json:ro listeria
```
