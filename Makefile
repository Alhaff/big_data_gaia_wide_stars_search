# Shortcuts for the common commands. Run `make help` for the list.
#
# Works with GNU make on Linux, macOS and Windows (WSL2 or Git Bash), with Docker or Podman.
# Paths to the data are read from .env (see .env.example), the same file docker compose reads.
# Anything can also be overridden on the command line:
#   make run-full GAIA_RAW=/media/data/big_data/gaia/raw COMPOSE="docker compose" PYTHON=python

-include .env

# podman compose if Podman is installed, otherwise docker compose
COMPOSE  ?= $(shell command -v podman >/dev/null 2>&1 && echo "podman compose" || echo "docker compose")
PYTHON   ?= python3
GAIA_RAW ?= ./data/raw
ELBADRY  ?= ./data/elbadry
STAGES   ?= 1-6

# compose needs relative host paths to start with "./"; absolute ones (/... or C:/...) stay as they are
host_path = $(if $(or $(filter /% ./% ../%,$(1)),$(findstring :,$(1))),$(1),./$(1))
override GAIA_RAW := $(call host_path,$(GAIA_RAW))
override ELBADRY  := $(call host_path,$(ELBADRY))
export GAIA_RAW ELBADRY
# download_gaia.py writes into <folder>/raw
GAIA = $(patsubst %/,%,$(dir $(GAIA_RAW)))

RUN    = $(COMPOSE) run --rm
SUBMIT = $(RUN) --service-ports spark spark-submit pipeline/run_all.py

.PHONY: help init build history history-down download verify download-elbadry sample \
        check run-sample run-full run-stages validate experiment metrics skew dirty plots

help:            ## list the commands
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  make %-17s %s\n", $$1, $$2}'
	@echo "  compose: $(COMPOSE), Gaia files: $(GAIA_RAW), El-Badry catalog: $(ELBADRY)"

init:            ## create the folders the container mounts and writes to
	mkdir -p output spark-events "$(GAIA_RAW)" "$(ELBADRY)"
	chmod 777 output spark-events

build: init      ## build the image (once, needs internet)
	$(COMPOSE) build

history: init    ## start the History Server at http://localhost:18080
	$(COMPOSE) up -d history

history-down:    ## stop the History Server
	$(COMPOSE) down

download:        ## download the Gaia files (5.16 GB) into GAIA_RAW
	$(PYTHON) scripts/download_gaia.py --path "$(GAIA)"

verify:          ## check the MD5 of the downloaded Gaia files
	$(PYTHON) scripts/verify_md5.py "$(GAIA_RAW)"

download-elbadry: ## download the El-Badry et al. (2021) catalog (1.42 GB) into ELBADRY
	$(PYTHON) scripts/download_elbadry.py --path "$(ELBADRY)"

sample:          ## rebuild sample/ from the full data
	$(PYTHON) scripts/make_sample.py --raw "$(GAIA_RAW)"

check: init      ## test the pair search without Spark
	$(RUN) spark python3 tests/check_geometry.py

run-sample: init ## pipeline on the sample (stages 1-6, about 1 min)
	$(SUBMIT) --input /work/sample

run-full: init   ## pipeline on the full dataset (stages 1-6, about 10 min)
	$(SUBMIT) --input /data/raw

run-stages: init ## selected stages on the full dataset: make run-stages STAGES=3-6
	$(SUBMIT) --input /data/raw --stages $(STAGES)

validate: init   ## stage 7 only (needs stages 2-3 in output/ and the El-Badry catalog)
	$(SUBMIT) --input /data/raw --stages 7

experiment: init ## 8 runs of stage 2, variants A and B alternating (about 30 min)
	for i in 1 2 3 4; do \
	    $(RUN) spark spark-submit pipeline/run_all.py --input /data/raw --stages 2 --no-measure; \
	    $(RUN) spark spark-submit pipeline/run_all.py --input /data/raw --stages 2 --no-measure --all-neighbours; \
	done

metrics:         ## experiment table from the History Server
	$(PYTHON) scripts/collect_metrics.py --stages 2 --skip-first

skew:            ## shuffle skew from the saved event log in evidence/
	$(PYTHON) scripts/shuffle_skew.py

dirty:           ## examples of rejected rows -> docs/dirty_examples.md
	$(PYTHON) scripts/dirty_examples.py > docs/dirty_examples.md

plots: init      ## figures -> docs/figures/
	$(RUN) spark python3 scripts/plot_results.py
	mkdir -p docs/figures
	cp output/figures/*.png docs/figures/
