SHELL := /bin/bash

PY ?= python3
VENV ?= .venv
PYV := $(VENV)/bin/python
PIP := $(VENV)/bin/pip

OUT_XLSX ?= tournaments.xlsx
OUT_ICS ?= tournaments.ics
OUT_ICS_CONFLICTS ?= tournaments-conflicts.ics
UID_DOMAIN ?= local.test

# Fenêtre relative (par défaut dans le script)
YEARS_PAST ?= 5
YEARS_FUTURE ?= 3

# Limites d'enrichissement / vitesse
PBS_ENRICH ?= 120
WPA_ENRICH ?= 200
MATCHROOM_ENRICH ?= 200
EPBF_ENRICH ?= 200
SLEEP ?= 0

.DEFAULT_GOAL := help

help:
	@echo "Targets:"
	@echo "  make venv        - create venv + install deps"
	@echo "  make build       - generate $(OUT_XLSX)"
	@echo "  make ics         - generate Excel + ICS"
	@echo "  make quick       - fast test (small window + low enrich)"
	@echo "  make show        - show basic stats"
	@echo "  make clean       - remove generated files"
	@echo ""
	@echo "Overrides (example): make build YEARS_PAST=2 YEARS_FUTURE=1 PBS_ENRICH=20"

venv:
	@test -d "$(VENV)" || $(PY) -m venv "$(VENV)"
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt
	@echo "✅ venv ready. Activate: source $(VENV)/bin/activate"

build: venv
	$(PYV) fetch_tournaments.py \
		--years-past $(YEARS_PAST) --years-future $(YEARS_FUTURE) \
		--pbs-enrich-limit $(PBS_ENRICH) \
		--wpa-enrich-limit $(WPA_ENRICH) \
		--matchroom-enrich-limit $(MATCHROOM_ENRICH) \
		--epbf-enrich-limit $(EPBF_ENRICH) \
		--sleep $(SLEEP) \
		--out $(OUT_XLSX)

ics: build
	$(PYV) export_ics.py \
		--xlsx $(OUT_XLSX) \
		--ics $(OUT_ICS) \
		--ics-conflicts $(OUT_ICS_CONFLICTS) \
		--uid-domain "$(UID_DOMAIN)"

quick: venv
	$(PYV) fetch_tournaments.py \
		--years-past 1 --years-future 1 \
		--pbs-enrich-limit 10 \
		--wpa-enrich-limit 30 \
		--matchroom-enrich-limit 30 \
		--epbf-enrich-limit 30 \
		--sleep 0.2 \
		--out $(OUT_XLSX)
	$(PYV) export_ics.py \
		--xlsx $(OUT_XLSX) \
		--ics $(OUT_ICS) \
		--ics-conflicts $(OUT_ICS_CONFLICTS) \
		--uid-domain "$(UID_DOMAIN)"

show:
	@echo "Files:"
	@ls -lh $(OUT_XLSX) $(OUT_ICS) $(OUT_ICS_CONFLICTS) 2>/dev/null || true
	@echo ""
	@echo "ICS event counts:"
	@echo -n "  all: "; grep -c "BEGIN:VEVENT" $(OUT_ICS) 2>/dev/null || echo 0
	@echo -n "  conflicts: "; grep -c "BEGIN:VEVENT" $(OUT_ICS_CONFLICTS) 2>/dev/null || echo 0
	@echo ""
	@echo "Organizer counts (Excel):"
	@if [ -f "$(OUT_XLSX)" ]; then \
		$(PYV) -c "import pandas as pd; df=pd.read_excel('$(OUT_XLSX)'); print(df['organizer'].value_counts().to_string())"; \
	else \
		echo "  (no xlsx yet)"; \
	fi
	@echo ""
	@echo "PBS sample (first 10):"
	@if [ -f "$(OUT_XLSX)" ]; then \
		$(PYV) -c "import pandas as pd; df=pd.read_excel('$(OUT_XLSX)'); p=df[df['organizer']=='Predator/PBS']; cols=[c for c in ['start_date','title','location','venue_name','venue_address','source_url'] if c in df.columns]; print(p[cols].head(10).to_string(index=False) if len(p) else '(no PBS rows)')"; \
	else \
		echo "  (no xlsx yet)"; \
	fi

clean:
	rm -f $(OUT_XLSX) $(OUT_ICS) $(OUT_ICS_CONFLICTS) pbs_debug_*.html
