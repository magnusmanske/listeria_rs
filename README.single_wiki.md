Listeria can work in a single wiki mode, updating a single wiki independent of the Wikidata/Wikimedia ecosystem.
You will need to prepare a basic JSON config file (refered to as `MY_CONFIG_FILE`) as described below.

# Prepare config file
This example uses `myproject.wikibase.cloud` as the wiki to run Listeria on. Adjust the server (and path is required) to match your wiki.
This example uses `Template:Listeria list` and `Template:Listeria list end` as the start and end templates for the list, though you can use any names you like. These templates should be created on your wiki, and used where you want to display the list. See the [template description](https://en.wikipedia.org/wiki/Template:Wikidata_list) for more information about using the template.
Adjust the `sparql_prefix` to match your wiki's setup. It will save you repeating the prefixes in every query.
You will also need a `BOT_EDIT_TOKEN` for Listeria to edit the wiki.
## Basic configuration
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
Optionally, you can use some extended parameters to fine-tune Listeria to your wiki setup.
This should not be necessary for an initial test.
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
```
## Clone the repo
```bash
git clone https://github.com/magnusmanske/listeria_rs.git
cd listeria_rs
```
## Run Listeria
Build and run the application.
```bash
cargo run --release -- --config MY_CONFIG_FILE single-wiki --once
```
The `--once` flag will run Listeria once and then exit. Without this flag, Listeria will run indefinitely.

# Run Listeria using Docker
## Build the Docker Image
```bash
git clone https://github.com/magnusmanske/listeria_rs.git
cd listeria_rs
docker build -t listeria . -f Dockerfile.single_wiki
```

## Run from Docker Image
This will run Listeria in the background. The `--once` flag is not available. Use Docker to kill the process.
```bash
docker run -d --name listeria -v MY_CONFIG_FILE:/etc/app/config.json:ro listeria
```
