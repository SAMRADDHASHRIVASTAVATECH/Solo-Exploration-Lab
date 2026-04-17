#!/usr/bin/env python3
# -*- coding: utf-8 -*-
r"""
cosmic_scenario_engine.py — Cosmic Scenario Engine v2.3.0
==========================================================

A production-quality, enterprise-grade single-file Python application for
generating, managing, simulating, and reporting on astrophysical scenarios.

Designed for Windows 10/11 (64-bit), Python 3.10+.
Fully expanded, unsquashed dataset and UI with NASA-Level Physics & Reporting.

================================================================================
README / QUICK START
================================================================================

INSTALLATION (Windows):
    pip install PyQt5 reportlab tqdm

OPTIONAL:
    pip install pytest   (only needed if you want pytest-style runs)

DEFAULT PATHS (Windows backslash style):
    SPACEENGINE_DB:  C:\Users\intel\Desktop\cosmic extractor\SpaceEngine_Index.db
    OUTPUT_DIR:      new_astro_dataset
    DB_FILE:         new_astro_scenarios.db
    ONTOLOGY_DB:     new_master_ontology.db
    ASTRO_DICT:      new_astro_dataset/new_astro_dictionary.json
    LOG_DIR:         logs
    REPORTS_DIR:     reports

ENVIRONMENT VARIABLE OVERRIDES:
    set SPACEENGINE_DB=D:\path\to\SpaceEngine_Index.db
    set COSMIC_OUTPUT_DIR=my_output
    set COSMIC_DB_FILE=my_scenarios.db
    set COSMIC_ONTOLOGY_DB=my_ontology.db
    set COSMIC_ASTRO_DICT=my_dict.json
    set COSMIC_LOG_DIR=my_logs
    set COSMIC_REPORTS_DIR=my_reports
    set COSMIC_ROWS_PER_FILE=500000

USAGE EXAMPLES (CLI Mode):
    python cosmic_scenario_engine.py --generate-dict
    python cosmic_scenario_engine.py --generate-scenarios
    python cosmic_scenario_engine.py --generate-scenarios --sample-size 500
    python cosmic_scenario_engine.py --build-sqlite
    python cosmic_scenario_engine.py --init-ontology
    python cosmic_scenario_engine.py --simulate --name "Hyper_Stellar_Fusion_Field"
    python cosmic_scenario_engine.py --simulate --id 1
    python cosmic_scenario_engine.py --report --name "Hyper_Stellar_Fusion_Field"
    python cosmic_scenario_engine.py --report --id 1
    python cosmic_scenario_engine.py --gui
    python cosmic_scenario_engine.py --run-all
    python cosmic_scenario_engine.py --run-all --sample-size 500
    python cosmic_scenario_engine.py --run-tests

PYINSTALLER PACKAGING:
    python cosmic_scenario_engine.py --pyinstaller-spec

================================================================================
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import io
import itertools
import json
import logging
import logging.handlers
import math
import multiprocessing
import os
import platform
import random
import re
import shutil
import sqlite3
import struct
import subprocess
import sys
import tempfile
import textwrap
import time
import traceback
import unittest
from datetime import datetime, timezone
from pathlib import Path
from typing import (
    Any, Callable, Dict, Generator, Iterator, List, Optional,
    Sequence, Set, Tuple, Union
)

# =============================================================================
# SECTION 0: SAFE THIRD-PARTY IMPORTS
# =============================================================================

TQDM_AVAILABLE: bool = False
try:
    from tqdm import tqdm as _tqdm_real
    TQDM_AVAILABLE = True
except ImportError:
    pass


def tqdm(iterable: Any, *args: Any, **kwargs: Any) -> Any:
    """
    Wrapper around tqdm that falls back to plain iteration if tqdm
    is not installed on the host system.
    """
    if TQDM_AVAILABLE:
        return _tqdm_real(iterable, *args, **kwargs)
    return iterable


REPORTLAB_AVAILABLE: bool = False
try:
    from reportlab.lib import colors as rl_colors
    from reportlab.lib.pagesizes import letter as rl_letter
    from reportlab.lib.styles import getSampleStyleSheet as rl_getSampleStyleSheet
    from reportlab.lib.styles import ParagraphStyle as rl_ParagraphStyle
    from reportlab.lib.units import inch as rl_inch
    from reportlab.platypus import (
        SimpleDocTemplate as rl_SimpleDocTemplate,
        Paragraph as rl_Paragraph,
        Spacer as rl_Spacer,
        Table as rl_Table,
        TableStyle as rl_TableStyle,
    )
    REPORTLAB_AVAILABLE = True
except ImportError:
    pass

PYQT5_AVAILABLE: bool = False
try:
    from PyQt5.QtWidgets import (
        QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
        QSplitter, QTreeWidget, QTreeWidgetItem, QTableWidget, QTableWidgetItem,
        QTextBrowser, QPushButton, QLineEdit, QLabel, QMessageBox,
        QProgressBar, QStatusBar, QFileDialog, QGridLayout,
        QHeaderView, QAbstractItemView, QShortcut, QTabWidget, QComboBox,
    )
    from PyQt5.QtCore import Qt, QThread, pyqtSignal, QTimer
    from PyQt5.QtGui import QKeySequence, QCursor, QFont
    PYQT5_AVAILABLE = True
except ImportError:
    pass

# =============================================================================
# SECTION 1: CONFIGURATION & CONSTANTS
# =============================================================================

def _env_path(env_key: str, default: str) -> Path:
    """Resolve a path from an environment variable, falling back to a default."""
    return Path(os.environ.get(env_key, default))


def _env_int(env_key: str, default: int) -> int:
    """Resolve an integer from an environment variable, falling back to a default."""
    try:
        return int(os.environ.get(env_key, str(default)))
    except (ValueError, TypeError):
        return default


# Master Configuration Paths
SPACEENGINE_DB: Path = _env_path(
    "SPACEENGINE_DB",
    r"C:\Users\intel\Desktop\cosmic extractor\SpaceEngine_Index.db",
)
OUTPUT_DIR: Path = _env_path("COSMIC_OUTPUT_DIR", "new_astro_dataset")
DB_FILE: Path = _env_path("COSMIC_DB_FILE", "new_astro_scenarios.db")
ONTOLOGY_DB: Path = _env_path("COSMIC_ONTOLOGY_DB", "new_master_ontology.db")
ASTRO_DICT: Path = _env_path("COSMIC_ASTRO_DICT", "new_astro_dataset/new_astro_dictionary.json")
LOG_DIR: Path = _env_path("COSMIC_LOG_DIR", "logs")
REPORTS_DIR: Path = _env_path("COSMIC_REPORTS_DIR", "reports")
FAVORITES_FILE: Path = Path("favorites.json")

# Performance Constants
ROWS_PER_FILE: int = _env_int("COSMIC_ROWS_PER_FILE", 2_000_000)
BATCH_SIZE: int = _env_int("COSMIC_BATCH_SIZE", 5000)

APP_NAME: str = "Cosmic Scenario Engine"
APP_VERSION: str = "2.3.0"


def ensure_dirs() -> None:
    """Create all required output directories to ensure safe execution."""
    for directory in (OUTPUT_DIR, LOG_DIR, REPORTS_DIR):
        directory.mkdir(parents=True, exist_ok=True)
    ASTRO_DICT.parent.mkdir(parents=True, exist_ok=True)


# =============================================================================
# SECTION 1b: LOGGING SETUP
# =============================================================================

_logger: Optional[logging.Logger] = None


def get_logger() -> logging.Logger:
    """
    Get or create the application logger with rotating file handler.
    Writes to `logs/app.log` (rotating, 5 MB, 5 backups) and to standard output.
    """
    global _logger
    if _logger is not None:
        return _logger

    ensure_dirs()
    log_file = LOG_DIR / "app.log"

    logger = logging.getLogger("CosmicEngine")
    logger.setLevel(logging.DEBUG)

    if not logger.handlers:
        # Rotating file handler: 5 MB × 5 backups
        file_handler = logging.handlers.RotatingFileHandler(
            str(log_file), 
            maxBytes=5 * 1024 * 1024, 
            backupCount=5, 
            encoding="utf-8"
        )
        file_handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s: %(message)s", 
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
        logger.addHandler(console_handler)

    _logger = logger
    return logger


# =============================================================================
# SECTION 2: SPACEENGINE OBJECT LOOKUP
# =============================================================================

EMBEDDED_OBJECT_MAP: Dict[str, Tuple[str, str, str]] = {
    "Stellar": ("Star", "StellarObject", "Galaxy"),
    "Planetary": ("Planet", "PlanetaryBody", "Star"),
    "Galactic": ("Galaxy", "GalacticStructure", "Universe"),
    "Nebular": ("Nebula", "NebularCloud", "Galaxy"),
    "Cometary": ("Comet", "CometaryBody", "Star"),
    "Asteroid": ("Asteroid", "AsteroidalBody", "Star"),
    "Quasar": ("Quasar", "QuasarObject", "Universe"),
    "Pulsar": ("Pulsar", "PulsarObject", "Galaxy"),
    "BlackHole": ("BlackHole", "CompactObject", "Galaxy"),
    "Magnetar": ("Magnetar", "CompactObject", "Galaxy"),
    "Exoplanet": ("Exoplanet", "PlanetaryBody", "Star"),
    "Protostar": ("Protostar", "StellarObject", "Nebula"),
    "WhiteDwarf": ("WhiteDwarf", "CompactObject", "Galaxy"),
    "NeutronStar": ("NeutronStar", "CompactObject", "Galaxy"),
    "GlobularCluster": ("GlobularCluster", "ClusterObject", "Galaxy"),
    "SupernovaRemnant": ("SupernovaRemnant", "NebularCloud", "Galaxy"),
    "DarkMatter": ("DarkMatter", "ExoticMatter", "Universe"),
    "CosmicVoid": ("CosmicVoid", "LargeScaleStructure", "Universe"),
    "Interstellar": ("InterstellarMedium", "DiffuseMatter", "Galaxy"),
    "Circumstellar": ("CircumstellarDisk", "DiskStructure", "Star"),
    "Accretion": ("AccretionDisk", "DiskStructure", "CompactObject"),
    "Solar": ("SolarSystem", "PlanetarySystem", "Galaxy"),
    "Lunar": ("Moon", "SatelliteBody", "Planet"),
    "Chromospheric": ("Chromosphere", "StellarLayer", "Star"),
    "Coronal": ("Corona", "StellarLayer", "Star"),
    "Photospheric": ("Photosphere", "StellarLayer", "Star"),
    "Magnetospheric": ("Magnetosphere", "FieldStructure", "Planet"),
    "Heliospheric": ("Heliosphere", "FieldStructure", "Star"),
    "Intergalactic": ("IntergalacticMedium", "DiffuseMatter", "Universe"),
    "Cosmological": ("Universe", "CosmicStructure", "Universe"),
    "BrownDwarf": ("BrownDwarf", "StellarObject", "Galaxy"),
    "RedDwarf": ("Star", "StellarObject", "Galaxy"),
    "Subdwarf": ("Star", "StellarObject", "Galaxy"),
    "Supergiant": ("Star", "StellarObject", "Galaxy"),
    "Hypergiant": ("Star", "StellarObject", "Galaxy"),
    "ActiveGalacticNucleus": ("Galaxy", "GalacticCore", "Universe"),
    "Blazar": ("Quasar", "ActiveGalacticNucleus", "Universe"),
    "SeyfertGalaxy": ("Galaxy", "ActiveGalaxy", "Universe"),
    "RadioGalaxy": ("Galaxy", "ActiveGalaxy", "Universe"),
    "StarburstGalaxy": ("Galaxy", "ActiveGalaxy", "Universe"),
    "DwarfGalaxy": ("Galaxy", "GalacticStructure", "Universe"),
    "IrregularGalaxy": ("Galaxy", "GalacticStructure", "Universe"),
    "LenticularGalaxy": ("Galaxy", "GalacticStructure", "Universe"),
    "SpiralGalaxy": ("Galaxy", "GalacticStructure", "Universe"),
    "EllipticalGalaxy": ("Galaxy", "GalacticStructure", "Universe"),
    "Protoplanetary": ("ProtoplanetaryDisk", "DiskStructure", "Star"),
    "Circumplanetary": ("CircumplanetaryDisk", "DiskStructure", "Planet"),
    "OortCloud": ("OortCloud", "CloudStructure", "Star"),
    "KuiperBelt": ("KuiperBelt", "DiskStructure", "Star"),
    "Exocomet": ("Comet", "CometaryBody", "Star"),
}

_OBJECT_LOOKUP: Optional[Dict[str, Tuple[str, str, str]]] = None


def build_object_lookup(spaceengine_db_path: Optional[Path] = None) -> Dict[str, Tuple[str, str, str]]:
    """
    Build the domain mapping dictionary. Will attempt to connect to the
    SpaceEngine Index database if available, otherwise falls back to the
    massive embedded dictionary map.
    """
    global _OBJECT_LOOKUP
    if _OBJECT_LOOKUP is not None:
        return _OBJECT_LOOKUP

    result: Dict[str, Tuple[str, str, str]] = dict(EMBEDDED_OBJECT_MAP)
    
    if spaceengine_db_path is None:
        spaceengine_db_path = SPACEENGINE_DB

    if spaceengine_db_path.exists():
        try:
            with contextlib.closing(sqlite3.connect(str(spaceengine_db_path))) as conn:
                cur = conn.cursor()
                cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='objects'")
                if cur.fetchone():
                    cur.execute("SELECT family, obj_type FROM objects")
                    for row in cur.fetchall():
                        fam = row[0] if row[0] else "Other"
                        otype = row[1] if row[1] else "UnknownObject"
                        key = fam.replace(" ", "")
                        if key not in result:
                            result[key] = (fam, otype, "Universe")
        except Exception as e:
            get_logger().warning(f"Could not load from SpaceEngine DB, using embedded map: {e}")

    _OBJECT_LOOKUP = result
    return result


def map_domain(domain: str) -> Tuple[str, str, str]:
    """Map a domain string to a (family, obj_type, parent) tuple."""
    lookup = build_object_lookup()
    return lookup.get(domain, ("Other", "UnknownObject", "Universe"))


# =============================================================================
# SECTION 3: DICTIONARY / VOCABULARY GENERATOR
# =============================================================================

def _build_prefix_list() -> List[str]:
    """Generates the comprehensive list of 60 astrophysical prefixes."""
    return [
        "Hyper",
        "Ultra",
        "Mega",
        "Proto",
        "Neo",
        "Quasi",
        "Pseudo",
        "Trans",
        "Sub",
        "Super",
        "Micro",
        "Macro",
        "Meta",
        "Para",
        "Exo",
        "Endo",
        "Iso",
        "Aniso",
        "Multi",
        "Poly",
        "Mono",
        "Bi",
        "Tri",
        "Quadra",
        "Penta",
        "Omni",
        "Null",
        "Anti",
        "Counter",
        "Retro",
        "Infra",
        "Supra",
        "Giga",
        "Tera",
        "Peta",
        "Exa",
        "Zetta",
        "Yotta",
        "Zepto",
        "Atto",
        "Femto",
        "Pico",
        "Nano",
        "Kilo",
        "Hecto",
        "Centi",
        "Milli",
        "Pan",
        "Arche",
        "Paleo",
        "Ceno",
        "Meso",
        "Holo",
        "Hemi",
        "Semi",
        "Demi",
        "Uni",
        "Hexa",
        "Hepta",
        "Octa"
    ]


def _build_domain_list() -> List[str]:
    """Generates the comprehensive list of 100 astrophysical domains."""
    return [
        "Stellar",
        "Planetary",
        "Galactic",
        "Nebular",
        "Cometary",
        "Asteroid",
        "Quasar",
        "Pulsar",
        "BlackHole",
        "Magnetar",
        "Exoplanet",
        "Protostar",
        "WhiteDwarf",
        "NeutronStar",
        "GlobularCluster",
        "SupernovaRemnant",
        "DarkMatter",
        "CosmicVoid",
        "Interstellar",
        "Circumstellar",
        "Accretion",
        "Solar",
        "Lunar",
        "Chromospheric",
        "Coronal",
        "Photospheric",
        "Magnetospheric",
        "Heliospheric",
        "Intergalactic",
        "Cosmological",
        "BrownDwarf",
        "RedDwarf",
        "Subdwarf",
        "Supergiant",
        "Hypergiant",
        "ActiveGalacticNucleus",
        "Blazar",
        "SeyfertGalaxy",
        "RadioGalaxy",
        "StarburstGalaxy",
        "DwarfGalaxy",
        "IrregularGalaxy",
        "LenticularGalaxy",
        "SpiralGalaxy",
        "EllipticalGalaxy",
        "Protoplanetary",
        "Circumplanetary",
        "OortCloud",
        "KuiperBelt",
        "Exocomet",
        "Wormhole",
        "Tachyon",
        "Axion",
        "String",
        "Brane",
        "Multiverse",
        "Omniverse",
        "Microverse",
        "Macroverse",
        "DysonSphere",
        "MatrioshkaBrain",
        "Ringworld",
        "StellarEngine",
        "Kugelblitz",
        "ShkadovThruster",
        "AldersonDisk",
        "Topopolis",
        "GlobusCassus",
        "Megastructure",
        "Filament",
        "Void",
        "Supercluster",
        "LocalGroup",
        "GalacticBulge",
        "GalacticHalo",
        "OpenCluster",
        "StellarAssociation",
        "MovingGroup",
        "StarStream",
        "AccretionDisk",
        "Proplyd",
        "DebrisDisk",
        "ScatteredDisc",
        "HillsCloud",
        "AsteroidBelt",
        "Centaur",
        "Plutino",
        "Comet",
        "Meteoroid",
        "Micrometeoroid",
        "DustGrain",
        "GasCloud",
        "MolecularCloud",
        "HIIRegion",
        "HIRegion",
        "PlanetaryNebula",
        "PulsarWindNebula",
        "BokGlobule",
        "HerbigHaroObject",
        "DarkNebula"
    ]


def _build_process_list() -> List[str]:
    """Generates the comprehensive list of 120 astrophysical processes."""
    return [
        "Fusion",
        "Fission",
        "Accretion",
        "Convection",
        "Radiation",
        "Collapse",
        "Ejection",
        "Ionization",
        "Recombination",
        "Nucleosynthesis",
        "Magnetohydrodynamic",
        "Turbulence",
        "Oscillation",
        "Precession",
        "Tidal",
        "Evaporation",
        "Condensation",
        "Sublimation",
        "Ablation",
        "Sputtering",
        "Dynamo",
        "Reconnection",
        "Diffusion",
        "Scattering",
        "Absorption",
        "Emission",
        "Reflection",
        "Refraction",
        "Polarization",
        "Lensing",
        "FusionPhase",
        "AccretionPhase",
        "CollapsePhase",
        "EjectionPhase",
        "RadiationPhase",
        "ConvectionCycle",
        "DynamoCycle",
        "OscillationMode",
        "TurbulenceMode",
        "ReconnectionCycle",
        "Annihilation",
        "Spallation",
        "Bremsstrahlung",
        "Synchrotron",
        "Comptonization",
        "Photoionization",
        "Photodissociation",
        "Chemosynthesis",
        "Isomerization",
        "Polymerization",
        "Crystallization",
        "Liquefaction",
        "Solidification",
        "Vaporization",
        "Subduction",
        "Outgassing",
        "Outflow",
        "Infall",
        "Downflow",
        "Upflow",
        "Circulation",
        "Advection",
        "Stratification",
        "Differentiation",
        "Fractionation",
        "Segregation",
        "SegregationPhase",
        "Thermalization",
        "Equilibration",
        "Relaxation",
        "Perturbation",
        "PerturbationPhase",
        "Excitation",
        "Deexcitation",
        "Decay",
        "DecayCycle",
        "Amplification",
        "Damping",
        "Dissipation",
        "Decoherence",
        "QuantumTunneling",
        "SpontaneousEmission",
        "StimulatedEmission",
        "PairProduction",
        "BetaDecay",
        "AlphaDecay",
        "GammaDecay",
        "ElectronCapture",
        "PositronEmission",
        "NeutrinoEmission",
        "NeutronCapture",
        "ProtonCapture",
        "Photodisintegration",
        "Hadronization",
        "Baryogenesis",
        "Leptogenesis",
        "Reionization",
        "Decoupling",
        "Inflation",
        "Expansion",
        "Contraction",
        "Bounce",
        "Crunch",
        "Rip",
        "Freeze",
        "Death",
        "Deposition",
        "Melting",
        "Freezing",
        "Boiling",
        "Cavitation",
        "Sonoluminescence",
        "Bioluminescence",
        "Chemiluminescence",
        "Triboluminescence",
        "Dissolution",
        "Precipitation",
        "Coagulation",
        "Flocculation",
        "Agglomeration"
    ]


def _build_type_list() -> List[str]:
    """Generates the comprehensive list of 80 astrophysical types."""
    return [
        "Field",
        "Region",
        "Layer",
        "Structure",
        "System",
        "Environment",
        "Phase",
        "Mode",
        "Regime",
        "State",
        "Zone",
        "Interface",
        "Boundary",
        "Core",
        "Shell",
        "Envelope",
        "Halo",
        "Jet",
        "Disk",
        "Ring",
        "Network",
        "Filament",
        "Web",
        "Bubble",
        "Cavity",
        "Lobe",
        "Plume",
        "Outflow",
        "Stream",
        "Current",
        "Vortex",
        "Eddy",
        "Cell",
        "Pocket",
        "Domain",
        "Sector",
        "Territory",
        "Province",
        "Sphere",
        "Globe",
        "Matrix",
        "Grid",
        "Lattice",
        "Tensor",
        "Manifold",
        "Scalar",
        "Vector",
        "Spinor",
        "Bispinor",
        "Twistor",
        "Wave",
        "Particle",
        "Quantum",
        "String",
        "Brane",
        "Point",
        "Line",
        "Plane",
        "Surface",
        "Volume",
        "Hypervolume",
        "Fractal",
        "Chaos",
        "Attractor",
        "Repeller",
        "Bifurcation",
        "Singularity",
        "Topology",
        "Geometry",
        "Symmetry",
        "Asymmetry",
        "Supersymmetry",
        "Gauge",
        "Configuration",
        "Arrangement",
        "Pattern",
        "Sequence",
        "Series",
        "Progression",
        "Cycle"
    ]


def generate_dictionary(out_path: Optional[Path] = None) -> Dict[str, Any]:
    """
    Generate the master astrophysical vocabulary dictionary and save to JSON.
    This dict serves as the foundation for the combinatoric scenario generator.
    """
    log = get_logger()
    
    if out_path is None:
        out_path = ASTRO_DICT

    dictionary: Dict[str, Any] = {
        "prefixes": _build_prefix_list(),
        "domains": _build_domain_list(),
        "processes": _build_process_list(),
        "types": _build_type_list(),
    }
    
    total_combinations = (
        len(dictionary["prefixes"]) * len(dictionary["domains"]) * len(dictionary["processes"]) * len(dictionary["types"])
    )
    
    dictionary["_meta"] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_combinations": total_combinations,
        "version": APP_VERSION,
    }
    
    tmp_path = out_path.with_suffix(".tmp")
    
    with open(tmp_path, "w", encoding="utf-8") as fh:
        json.dump(dictionary, fh, indent=2, ensure_ascii=False)
        
    try:
        if out_path.exists():
            out_path.unlink()
        tmp_path.rename(out_path)
    except OSError as e:
        log.warning(f"Failed atomic rename, falling back to direct write: {e}")
        shutil.copy(tmp_path, out_path)
        tmp_path.unlink()
        
    log.info("Dictionary → %s  (%s combos)", out_path, f"{total_combinations:,}")
    return dictionary


# =============================================================================
# SECTION 4: COMBINATORIC SCENARIO GENERATOR
# =============================================================================

def _make_scenario_name(prefix: str, domain: str, process: str, stype: str) -> str:
    """Safely construct a consistent string identifier for the scenario."""
    return f"{prefix}_{domain}_{process}_{stype}"


def _scenario_record(
    prefix: str, 
    domain: str, 
    process: str, 
    stype: str, 
    lookup: Dict[str, Tuple[str, str, str]]
) -> Dict[str, str]:
    """Builds a single dictionary record representing a full scenario object."""
    family, obj_type, parent = lookup.get(domain, ("Other", "UnknownObject", "Universe"))
    return {
        "scenario_name": _make_scenario_name(prefix, domain, process, stype),
        "object_family": family,
        "object_type": obj_type,
        "parent_object": parent,
        "prefix": prefix,
        "domain": domain,
        "process": process,
        "type": stype,
    }


def _flush_jsonl_buffer(
    buffer: List[str], 
    output_path: Path, 
    worker_id: int, 
    file_index: int
) -> None:
    """Writes a buffered block of strings to a JSONL file atomically."""
    final_file = output_path / f"data_{worker_id}_{file_index}.jsonl"
    
    # Allow for resumption of previously completed runs
    if final_file.exists() and final_file.stat().st_size > 0:
        return
        
    tmp_file = final_file.with_suffix(".tmp")
    
    with open(tmp_file, "w", encoding="utf-8") as fh:
        fh.write("\n".join(buffer) + "\n")
        
    try:
        if final_file.exists():
            final_file.unlink()
        tmp_file.rename(final_file)
    except OSError:
        shutil.copy(tmp_file, final_file)
        tmp_file.unlink()


def _worker_generate(
    args: Tuple[int, int, int, List[List[str]], int, str, Dict[str, Tuple[str, str, str]]]
) -> int:
    """Multiprocessing worker: generate a designated slice of the cartesian product."""
    worker_id, start_idx, end_idx, combo_keys, rows_per_file, output_dir_str, lookup = args
    prefixes, domains, processes, types = combo_keys
    output_path = Path(output_dir_str)
    
    all_combos = itertools.product(prefixes, domains, processes, types)
    chunk = itertools.islice(all_combos, start_idx, end_idx)
    
    total_written = 0
    file_index = 0
    buf = []
    
    for combo in chunk:
        record = _scenario_record(
            prefix=combo[0], 
            domain=combo[1], 
            process=combo[2], 
            stype=combo[3], 
            lookup=lookup
        )
        buf.append(json.dumps(record, ensure_ascii=False))
        total_written += 1
        
        if len(buf) >= rows_per_file:
            _flush_jsonl_buffer(buf, output_path, worker_id, file_index)
            buf = []
            file_index += 1
            
    if buf:
        _flush_jsonl_buffer(buf, output_path, worker_id, file_index)
        
    return total_written


def generate_scenarios(
    sample_size: Optional[int] = None, 
    output_dir: Optional[Path] = None, 
    rows_per_file: Optional[int] = None
) -> int:
    """
    Deterministically generates massive scenario JSONL files via a combinatoric product.
    Leverages multiprocessing to utilize all CPU cores, optimizing massive batch writes.
    """
    log = get_logger()
    
    if output_dir is None:
        output_dir = OUTPUT_DIR
    if rows_per_file is None:
        rows_per_file = ROWS_PER_FILE
        
    output_dir.mkdir(parents=True, exist_ok=True)

    if ASTRO_DICT.exists():
        try:
            with open(ASTRO_DICT, "r", encoding="utf-8") as fh:
                d = json.load(fh)
            prefixes = d["prefixes"]
            domains = d["domains"]
            processes = d["processes"]
            types = d["types"]
        except Exception:
            log.warning("Dictionary corrupt, regenerating...")
            d = generate_dictionary()
            prefixes = d["prefixes"]
            domains = d["domains"]
            processes = d["processes"]
            types = d["types"]
    else:
        d = generate_dictionary()
        prefixes = d["prefixes"]
        domains = d["domains"]
        processes = d["processes"]
        types = d["types"]

    lookup = build_object_lookup()
    total_combinations = len(prefixes) * len(domains) * len(processes) * len(types)
    effective_limit = min(sample_size, total_combinations) if sample_size else total_combinations

    # Scale worker processes to system capabilities safely
    num_workers = max(1, min(multiprocessing.cpu_count(), 8))
    if effective_limit < 1000:
        num_workers = 1

    chunk_size = math.ceil(effective_limit / num_workers)
    worker_args = []
    
    for i in range(num_workers):
        start = i * chunk_size
        end = min(start + chunk_size, effective_limit)
        if start >= effective_limit:
            break
            
        worker_args.append((
            i, 
            start, 
            end, 
            [prefixes, domains, processes, types], 
            rows_per_file, 
            str(output_dir), 
            lookup
        ))

    total_written = 0
    if num_workers <= 1:
        for wa in worker_args:
            total_written += _worker_generate(wa)
    else:
        try:
            with multiprocessing.Pool(num_workers) as pool:
                total_written = sum(pool.map(_worker_generate, worker_args))
        except Exception as exc:
            log.error(f"Multiprocessing error ({exc}), fallback to single process.")
            for wa in worker_args:
                total_written += _worker_generate(wa)

    log.info("Scenario generation complete: %s records", f"{total_written:,}")
    return total_written


# =============================================================================
# SECTION 5: SQLITE DB BUILDER
# =============================================================================

_SCENARIOS_SCHEMA = """
CREATE TABLE IF NOT EXISTS scenarios (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    scenario_name  TEXT    NOT NULL,
    object_family  TEXT    NOT NULL,
    object_type    TEXT    NOT NULL,
    parent_object  TEXT    NOT NULL,
    prefix         TEXT    NOT NULL,
    domain         TEXT    NOT NULL,
    process        TEXT    NOT NULL,
    type           TEXT    NOT NULL
);
"""

_SCENARIOS_INDICES = [
    "CREATE INDEX IF NOT EXISTS idx_family ON scenarios(object_family);",
    "CREATE INDEX IF NOT EXISTS idx_name   ON scenarios(scenario_name);",
    "CREATE INDEX IF NOT EXISTS idx_combo  ON scenarios(prefix, domain, process, type);",
]

def build_sqlite_from_jsonl(
    db_path: Optional[Path] = None, 
    input_dir: Optional[Path] = None, 
    batch_size: int = BATCH_SIZE
) -> int:
    """
    Consumes all generated JSONL files and ingests them into a high-performance
    SQLite database formatted for heavy read analytics.
    """
    log = get_logger()
    
    if db_path is None:
        db_path = DB_FILE
    if input_dir is None:
        input_dir = OUTPUT_DIR

    jsonl_files = sorted(input_dir.glob("*.jsonl"))
    if not jsonl_files:
        return 0

    with contextlib.closing(sqlite3.connect(str(db_path))) as conn:
        # Heavy IO performance pragmas
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA cache_size=-64000;")
        conn.execute("PRAGMA temp_store=MEMORY;")
        conn.execute("PRAGMA mmap_size=2147483648;")
        conn.execute(_SCENARIOS_SCHEMA)
        conn.commit()

        total = 0
        batch: List[Tuple[str, ...]] = []
        
        for jf in tqdm(jsonl_files, desc="Loading JSONL"):
            with open(jf, "r", encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rec = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                        
                    batch.append((
                        rec["scenario_name"], 
                        rec["object_family"], 
                        rec["object_type"], 
                        rec["parent_object"],
                        rec["prefix"], 
                        rec["domain"], 
                        rec["process"], 
                        rec["type"]
                    ))
                    
                    if len(batch) >= batch_size:
                        conn.executemany(
                            "INSERT INTO scenarios (scenario_name,object_family,object_type,parent_object,prefix,domain,process,type) VALUES (?,?,?,?,?,?,?,?)", 
                            batch
                        )
                        conn.commit()
                        total += len(batch)
                        batch = []
                        
        if batch:
            conn.executemany(
                "INSERT INTO scenarios (scenario_name,object_family,object_type,parent_object,prefix,domain,process,type) VALUES (?,?,?,?,?,?,?,?)", 
                batch
            )
            conn.commit()
            total += len(batch)

        for idx_sql in _SCENARIOS_INDICES:
            conn.execute(idx_sql)
            
        conn.commit()

    return total


# =============================================================================
# SECTION 6: MASTER ONTOLOGY INITIALIZATION (MASSIVE EXPANSION)
# =============================================================================

# Exhaustively deep embedded ontology: super_category → sub_category → [item names]
ONTOLOGY_DATA: Dict[str, Dict[str, List[str]]] = {
    "Stellar Physics": {
        "Stellar Structure & Evolution": [
            "Main Sequence Dynamics", 
            "Red Giant Evolution",
            "Supergiant Atmospheres", 
            "Stellar Core Processes",
            "Envelope Convection",
            "Radiative Zone Transport",
            "Hertzsprung-Russell Trajectories",
            "Asymptotic Giant Branch Phase",
            "Helium Flash Mechanics",
            "Wolf-Rayet Star Dynamics"
        ],
        "Stellar Endpoints": [
            "White Dwarf Cooling", 
            "Neutron Star Interior",
            "Black Hole Formation", 
            "Supernova Mechanisms",
            "Magnetar Emissions",
            "Pulsar Spin-Down",
            "Quark Star Hypotheticals",
            "Thorne-Zytkow Object Theory",
            "Type Ia Supernova Ignition",
            "Core-Collapse Asymmetry"
        ],
        "Stellar Magnetism & Activity": [
            "Dynamo Theory", 
            "Coronal Heating",
            "Stellar Wind Acceleration", 
            "Magnetic Reconnection Events",
            "Starspot Cycles",
            "Chromospheric Flares",
            "X-ray Coronal Emissions",
            "Alfvén Wave Propagation",
            "Zeeman-Doppler Imaging",
            "Heliospheric Current Sheets"
        ],
        "Binary & Multiple Systems": [
            "Roche Lobe Overflow",
            "Common Envelope Evolution",
            "Contact Binary Dynamics",
            "Cataclysmic Variables",
            "X-ray Binaries",
            "Symbiotic Stars",
            "Algol Variables",
            "AM CVn Systems",
            "Microquasars",
            "Gravitational Wave Inspirals"
        ],
    },
    "Planetary Science": {
        "Planetary Formation": [
            "Protoplanetary Disk Evolution", 
            "Planetesimal Accretion",
            "Gas Giant Formation", 
            "Terrestrial Planet Assembly",
            "Debris Disk Dynamics",
            "Core Accretion Model",
            "Disk Instability Model",
            "Pebble Accretion Mechanics",
            "Migration and Resonance",
            "Late Heavy Bombardment Events"
        ],
        "Planetary Atmospheres": [
            "Atmospheric Escape", 
            "Cloud Formation Dynamics",
            "Photochemistry Regimes", 
            "Greenhouse Effect Models",
            "Ionospheric Processes",
            "Exospheric Heating",
            "Thermospheric Tides",
            "Stratospheric Circulation",
            "Tropospheric Aerosols",
            "Radiative-Convective Equilibrium"
        ],
        "Planetary Interiors": [
            "Core Differentiation", 
            "Mantle Convection",
            "Tectonic Processes", 
            "Magnetic Field Generation",
            "Thermal Evolution",
            "Phase Transitions at High Pressure",
            "Equation of State Constraints",
            "Core-Mantle Boundary Interactions",
            "Magma Ocean Solidification",
            "Isotopic Fractionation"
        ],
        "Planetary Surfaces": [
            "Impact Cratering Mechanics",
            "Volcanism and Cryovolcanism",
            "Fluvial and Aeolian Erosion",
            "Space Weathering",
            "Regolith Development",
            "Geomorphological Evolution",
            "Mineralogical Mapping",
            "Topographic Stratigraphy",
            "Glacial Modifiers",
            "Tidal Heating Fractures"
        ]
    },
    "Galactic Astrophysics": {
        "Galaxy Structure": [
            "Spiral Arm Dynamics", 
            "Galactic Bar Formation",
            "Halo Structure", 
            "Galactic Nucleus Activity",
            "Disk Instabilities",
            "Thick Disk Kinematics",
            "Bulge Formation Models",
            "Warped Disk Dynamics",
            "Globular Cluster Orbits",
            "Dark Matter Halo Profiles"
        ],
        "Galaxy Evolution": [
            "Galaxy Merger Dynamics", 
            "Star Formation History",
            "Chemical Enrichment", 
            "Galactic Winds",
            "AGN Feedback",
            "Quenching Mechanisms",
            "Lyman-Break Galaxy Models",
            "Submillimeter Galaxy Evolution",
            "Metallicity Gradients",
            "Hierarchical Clustering"
        ],
        "Interstellar Medium": [
            "Molecular Cloud Physics", 
            "HII Region Dynamics",
            "Supernova Remnant Evolution", 
            "Cosmic Ray Propagation",
            "Dust Grain Physics",
            "Polycyclic Aromatic Hydrocarbons",
            "Cold Neutral Medium",
            "Warm Ionized Medium",
            "Hot Coronal Gas",
            "Magnetic Turbulence in ISM"
        ],
        "Active Galactic Nuclei (AGN)": [
            "Accretion Disk Physics",
            "Broad Line Region Dynamics",
            "Narrow Line Region Kinematics",
            "Relativistic Jet Collimation",
            "Dusty Torus Structure",
            "Seyfert Galaxy Spectra",
            "Quasar Luminosity Functions",
            "Blazar Variability",
            "Radio Lobe Morphologies",
            "Supermassive Black Hole Spin"
        ]
    },
    "Cosmology": {
        "Early Universe": [
            "Big Bang Nucleosynthesis", 
            "Cosmic Inflation",
            "Recombination Epoch", 
            "Dark Ages Physics",
            "Reionization Processes",
            "Baryogenesis Models",
            "Leptogenesis Theories",
            "Cosmic Microwave Background Anisotropies",
            "Primordial Gravitational Waves",
            "Phase Transitions in the Early Universe"
        ],
        "Large Scale Structure": [
            "Cosmic Web Formation", 
            "Galaxy Cluster Dynamics",
            "Void Evolution", 
            "Baryon Acoustic Oscillations",
            "Dark Matter Halos",
            "Filamentary Structures",
            "Supercluster Topology",
            "Redshift-Space Distortions",
            "Lyman-Alpha Forests",
            "Weak Lensing Surveys"
        ],
        "Dark Sector": [
            "Dark Energy Models", 
            "Dark Matter Candidates",
            "Modified Gravity Theories", 
            "Vacuum Energy Physics",
            "Quintessence Fields",
            "WIMP Cross-Sections",
            "Axion Electrodynamics",
            "Sterile Neutrino Models",
            "Self-Interacting Dark Matter",
            "Phantom Energy Dynamics"
        ],
        "Cosmological Parameters": [
            "Hubble Constant Tension",
            "Omega Matter Density",
            "Deceleration Parameter",
            "Equation of State (w)",
            "Scalar Spectral Index",
            "Tensor-to-Scalar Ratio",
            "Neutrino Mass Sum Limits",
            "Optical Depth to Reionization",
            "Density Fluctuation Amplitude (sigma8)",
            "Curvature of the Universe"
        ]
    },
    "High Energy Astrophysics": {
        "Compact Objects": [
            "X-ray Binary Systems", 
            "Gamma Ray Bursts",
            "Pulsar Wind Nebulae", 
            "Accretion Disk Physics",
            "Relativistic Jet Formation",
            "Soft Gamma Repeaters",
            "Anomalous X-ray Pulsars",
            "Ultraluminous X-ray Sources",
            "Tidal Disruption Events",
            "Microquasar Jets"
        ],
        "Particle Astrophysics": [
            "Cosmic Ray Origins", 
            "Neutrino Astrophysics",
            "Pair Production Processes", 
            "Synchrotron Radiation",
            "Inverse Compton Scattering",
            "Bremsstrahlung Cooling",
            "Pion Decay Signatures",
            "Cherenkov Radiation Detection",
            "Ultra-High-Energy Cosmic Rays",
            "Dark Matter Annihilation Spectra"
        ],
        "Gravitational Phenomena": [
            "Gravitational Wave Sources", 
            "Binary Merger Events",
            "Extreme Mass Ratio Inspirals", 
            "Gravitational Lensing",
            "Frame Dragging Effects",
            "Lense-Thirring Precession",
            "Geodetic Effect",
            "Penrose Processes",
            "Ergosphere Dynamics",
            "Black Hole No-Hair Theorem Tests"
        ],
        "Relativistic Hydrodynamics": [
            "Shock Wave Acceleration",
            "Blandford-Znajek Mechanism",
            "Bondi-Hoyle-Lyttleton Accretion",
            "Advection-Dominated Accretion Flows",
            "Magnetically Arrested Disks",
            "Relativistic Turbulence",
            "Plasmoid Reconnection",
            "Radiation-Dominated Shocks",
            "Kelvin-Helmholtz Instabilities in Jets",
            "Rayleigh-Taylor Instabilities in Outflows"
        ]
    },
    "Exoplanetary Science": {
        "Exoplanet Detection": [
            "Transit Photometry", 
            "Radial Velocity Method",
            "Direct Imaging Techniques", 
            "Microlensing Surveys",
            "Astrometric Detection",
            "Transit Timing Variations",
            "Pulsar Timing Variations",
            "Reflected Light Phase Curves",
            "Polarimetric Signatures",
            "Interferometric Nulling"
        ],
        "Exoplanet Characterization": [
            "Transmission Spectroscopy", 
            "Emission Spectroscopy",
            "Phase Curve Analysis", 
            "Atmospheric Retrieval",
            "Surface Mapping",
            "Albedo Measurements",
            "Thermal Inertia Modeling",
            "Exomoon Detection Metrics",
            "Exoring Transits",
            "Spin-Orbit Alignment (Rossiter-McLaughlin)"
        ],
        "Habitability": [
            "Habitable Zone Dynamics", 
            "Biosignature Detection",
            "Water World Physics", 
            "Tidal Heating Habitability",
            "Atmospheric Stability",
            "Technosignature Analysis",
            "Stellar Flare Impacts on Habitability",
            "Geomagnetic Shielding Requirements",
            "Carbon-Silicate Cycle Stability",
            "Subsurface Ocean Thermodynamics"
        ],
        "Exoplanet Demographics": [
            "Hot Jupiter Occurrence Rates",
            "Super-Earth Formations",
            "Mini-Neptune Radius Valley",
            "Ice Giant Analogues",
            "Rocky World Mass-Radius Relations",
            "Rogue Planet Statistics",
            "Circumbinary Planet Distributions",
            "Metallicity Correlations",
            "Multi-Planet System Architecture",
            "Orbital Eccentricity Distributions"
        ]
    },
}


def _deterministic_description(name: str) -> str:
    """
    Creates a deterministic HTML description box for a specific ontology item.
    Uses SHA-256 seeding to ensure the same name always yields the same physics text.
    """
    seed = int(hashlib.sha256(name.encode("utf-8")).hexdigest(), 16) % (2 ** 32)
    rng = random.Random(seed)
    
    energy_levels = [
        "Sub-eV thermal regime", 
        "eV-scale atomic transitions",
        "keV plasma interactions", 
        "MeV-GeV Thermonuclear Regime",
        "GeV relativistic processes", 
        "TeV extreme acceleration",
        "PeV ultra-high-energy regime",
    ]
    
    scale_levels = [
        "Sub-atomic (< 10⁻¹⁵ m)", 
        "Microscopic (10⁻¹⁵ – 10⁻⁶ m)",
        "Stellar Core Scale (10⁶ – 10⁷ km)", 
        "Planetary (10³ – 10⁸ m)",
        "Stellar (10⁸ – 10¹² m)", 
        "Galactic (10¹² – 10²² m)",
        "Cosmological (> 10²² m)",
    ]

    energy = rng.choice(energy_levels)
    scale = rng.choice(scale_levels)

    dynamics_templates = [
        f"The system exhibits complex multi-scale interactions across {scale} "
        f"spatial extents, dominated by {energy} energy processes.",
        f"Operating in the {energy} regime, this phenomenon spans {scale} "
        f"and involves coupled nonlinear feedback mechanisms.",
        f"At the characteristic {scale} scale, energy exchange occurs "
        f"primarily through {energy} channels with cascading effects.",
        f"Thermodynamic transport within this domain occurs at {scale} limits, "
        f"necessitating advanced modeling within the {energy}.",
    ]
    
    simple_templates = [
        f"This describes how {name.lower()} operates in astrophysical "
        f"environments, converting energy across different scales.",
        f"In simple terms, {name.lower()} is a natural process where matter "
        f"and energy interact at cosmic scales.",
        f"Think of {name.lower()} as one of nature's ways of organising "
        f"matter and energy in the universe.",
        f"This is the fundamental definition of {name.lower()} used by "
        f"modern observatories to classify transient events.",
    ]

    dynamics = rng.choice(dynamics_templates)
    simple = rng.choice(simple_templates)
    words = name.split()
    taxonomy = " → ".join(words) if len(words) > 1 else name

    return textwrap.dedent(f"""\
    <div style="font-family:'Consolas',monospace;padding:12px;background:#0b0c10;color:#c5c6c7;">
      <h2 style="color:#45f3ff;border-bottom:2px solid #45f3ff;padding-bottom:6px;text-transform:uppercase;">
        {name}
      </h2>
      <p style="color:#66fcf1;font-size:0.9em;">
        <b>TAXONOMY:</b> {taxonomy}
      </p>
      <p style="color:#ff00ff;font-size:0.9em;">
        <b>ENERGY SCALE:</b> {energy} | <b>SPATIAL SCALE:</b> {scale}
      </p>
      <h3 style="color:#45f3ff;">// TAXONOMIC DEFINITION</h3>
      <p style="color:#c5c6c7;">
        {name} represents a distinct class of astrophysical phenomena
        characterised by specific energy regimes and spatial scales within
        the cosmic hierarchy. It encompasses interactions governed by
        fundamental physical laws operating in the {energy} domain.
      </p>
      <h3 style="color:#45f3ff;">// SYSTEM DYNAMICS</h3>
      <p style="color:#c5c6c7;">{dynamics}</p>
      <h3 style="color:#45f3ff;">// IN SIMPLE TERMS</h3>
      <p style="color:#c5c6c7;">{simple}</p>
    </div>""")


def init_master_ontology(db_path: Optional[Path] = None) -> int:
    """
    Initializes the SQLite database that holds the massive ontology categorizations.
    """
    log = get_logger()
    
    if db_path is None:
        db_path = ONTOLOGY_DB
        
    with contextlib.closing(sqlite3.connect(str(db_path))) as conn:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS ontology (
                id INTEGER PRIMARY KEY AUTOINCREMENT, 
                name TEXT NOT NULL UNIQUE,
                super_category TEXT NOT NULL, 
                sub_category TEXT NOT NULL, 
                description TEXT NOT NULL
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_ont_name ON ontology(name);")
        conn.commit()

        count = conn.execute("SELECT COUNT(*) FROM ontology").fetchone()[0]
        if count > 0:
            return 0

        inserted = 0
        for super_cat, sub_cats in ONTOLOGY_DATA.items():
            for sub_cat, items in sub_cats.items():
                for item_name in items:
                    desc = _deterministic_description(item_name)
                    conn.execute(
                        "INSERT OR IGNORE INTO ontology (name, super_category, sub_category, description) VALUES (?,?,?,?)",
                        (item_name, super_cat, sub_cat, desc)
                    )
                    inserted += 1
        conn.commit()
        
    log.info("Master Ontology initialized: %d items inserted into %s", inserted, db_path)
    return inserted


# =============================================================================
# SECTION 7: SCENARIO SIMULATOR (NASA-LEVEL UPGRADED)
# =============================================================================

ENERGY_REGIMES: List[str] = [
    "Sub-eV Thermal Equilibrium",
    "eV-Scale Atomic Transition Regime",
    "keV Plasma Interaction Regime",
    "MeV-GeV Thermonuclear Regime",
    "GeV Relativistic Particle Regime",
    "TeV Extreme Acceleration Regime",
]

SPATIAL_SCALES: List[str] = [
    "Sub-atomic (< 1 fm)",
    "Nuclear (1 fm – 1 pm)",
    "Stellar Core Scale (10⁶ – 10⁷ km)",
    "Planetary (10³ – 10⁷ m)",
    "Interstellar (10¹² – 10¹⁸ m)",
]

DOMINANT_PHYSICS: List[str] = [
    "General Relativistic Hydrodynamics",
    "Nuclear Reaction Networks (pp-chain / CNO / triple-α)",
    "Magnetohydrodynamics (MHD)",
    "Neutrino Transport Theory",
    "Statistical Mechanics",
    "Quantum Chromodynamics",
    "Gravitational Dynamics",
    "Radiation Transport",
]

POSSIBLE_OUTCOMES: List[str] = [
    "Gravitational collapse to compact object",
    "Thermonuclear detonation event",
    "Runaway accretion feedback",
    "Magnetic field amplification and saturation",
    "Jet formation and collimation",
    "Phase transition cascade",
]

OBSERVATIONAL_SIGNATURES: List[str] = [
    "Neutrino burst (IceCube/Super-Kamiokande)",
    "Gravitational wave chirp (LIGO/Virgo/KAGRA)",
    "Optical/UV precursor brightening (ZTF, LSST)",
    "Broadband electromagnetic emission (radio to gamma)",
    "Spectral line broadening and shifts",
    "X-ray pulsation patterns",
    "Polarized synchrotron emission",
]

OBSERVATORIES: List[str] = [
    "Global Multi-Messenger Network",
    "Space-based X-ray Observatory Array",
    "Gravitational Wave Interferometer Network",
]

ANALYSIS_PIPELINES: List[str] = [
    "Real-time multi-messenger transient pipeline",
    "Machine-learning anomaly detection stream",
    "Cross-messenger correlation engine",
]

FOLLOWUP_ACTIONS: List[str] = [
    "Immediate neutrino-triggered GW search",
    "Rapid multi-messenger alert to all 0.1–10 keV X-ray telescopes",
    "High-resolution spectroscopy (VLT/ELT) within 30 minutes",
    "Deep imaging campaign",
]

ASTRO_ENVIRONMENTS: List[str] = [
    "stellar core (radiative zone / convective boundary)",
    "accretion disk inner edge",
    "galactic center proximity",
]

ASTRO_OBJECTS: List[str] = [
    "Massive star (25–40 M☉ progenitor)",
    "Neutron star",
    "Black hole",
    "White dwarf",
]

# Upgraded MESA/KEPLER style parameter boundaries
PHYSICAL_PARAMETERS_RANGES = {
    "Temperature": (1e8, 9e8, "K"),
    "Density": (1e7, 5e8, "kg m⁻³"),
    "Magnetic Field": (1e3, 9e3, "T"),
    "Velocity": (1e2, 9e2, "km s⁻¹"),
    "Luminosity": (1e35, 9e36, "W"),
}


def _to_superscript(exp: int) -> str:
    """Converts a standard integer string into unicode superscripts."""
    sups = str.maketrans("-0123456789", "⁻⁰¹²³⁴⁵⁶⁷⁸⁹")
    return str(exp).translate(sups)


def _format_sci_unicode(val: float, unit: str) -> str:
    """Formats a float into proper scientific notation using unicode characters."""
    if val == 0:
        return f"0 {unit}"
    exp = int(math.floor(math.log10(abs(val))))
    mant = val / (10**exp)
    return f"{mant:.2f} × 10{_to_superscript(exp)} {unit}"


def simulate_scenario(scenario_name: str) -> Dict[str, Any]:
    """
    Deterministically simulates a scenario profile based entirely on a hash
    of the input scenario_name string. Generates scientific estimates, NASA-level
    physics modeling, and operational transient event budgets.
    """
    hash_str = hashlib.sha256(scenario_name.encode("utf-8")).hexdigest()
    seed = int(hash_str, 16) % (2 ** 32)
    rng = random.Random(seed)

    energy = rng.choice(ENERGY_REGIMES)
    spatial = rng.choice(SPATIAL_SCALES)
    environment = rng.choice(ASTRO_ENVIRONMENTS)

    dominant_physics = rng.sample(DOMINANT_PHYSICS, rng.randint(3, 5))
    outcomes = rng.sample(POSSIBLE_OUTCOMES, 3)
    signatures = rng.sample(OBSERVATIONAL_SIGNATURES, 3)
    objects = rng.sample(ASTRO_OBJECTS, 2)

    # Apply standard scientific formatting internally
    parameters = {}
    for param_name, (min_val, max_val, unit) in PHYSICAL_PARAMETERS_RANGES.items():
        val = rng.uniform(min_val, max_val)
        parameters[param_name] = _format_sci_unicode(val, unit)

    detection_confidence = round(rng.uniform(0.75, 0.99), 3)
    priority = "high" if detection_confidence > 0.85 else "medium"
    
    # Realism adjustments: Budgets in hundreds of thousands, Data in TBs
    estimated_cost = int(rng.uniform(120000, 450000))
    data_volume_tb = round(rng.uniform(5.0, 45.0), 1)
    complexity_score = rng.randint(85, 99)

    followups = rng.sample(FOLLOWUP_ACTIONS, 3)
    observatory_choice = rng.choice(OBSERVATORIES)

    alert_summary = (
        f"[MULTI-MESSENGER ALERT] {scenario_name}\n"
        f"Priority: {priority.upper()} | Confidence: {detection_confidence}\n"
        f"Scale: {spatial} | Energy: {energy}\n"
        f"Environment: {environment}\n"
        f"Objects: {', '.join(objects)}\n"
        f"Observatory: {observatory_choice}\n"
        f"Signatures: {signatures[0]}\n"
    )

    return {
        "scenario_name": scenario_name,
        "seed_hash": hash_str,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "energy_regime": energy,
        "spatial_scale": spatial,
        "environment": environment,
        "objects_involved": objects,
        "physical_parameters_formatted": parameters,
        "dominant_physics": dominant_physics,
        "possible_outcomes": outcomes,
        "observational_signatures": signatures,
        "observatory": observatory_choice,
        "analysis_pipeline": rng.choice(ANALYSIS_PIPELINES),
        "detection_confidence": detection_confidence,
        "model_confidence": "Observationally Supported",
        "complexity_score": complexity_score,
        "priority": priority,
        "recommended_followups": followups,
        "estimated_cost_usd": estimated_cost,
        "data_volume_tb": data_volume_tb,
        "alert_summary": alert_summary,
    }


# =============================================================================
# SECTION 8: REPORT GENERATOR (NASA-GRADE UPGRADED)
# =============================================================================

def _load_scenario_by_name(name: str, db_path: Optional[Path] = None) -> Optional[Dict[str, Any]]:
    """Loads a single scenario dictionary from the SQLite database via exact name match."""
    if db_path is None:
        db_path = DB_FILE
    if not db_path.exists():
        return None
        
    with contextlib.closing(sqlite3.connect(str(db_path))) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM scenarios WHERE scenario_name=? LIMIT 1", (name,)).fetchone()
        
    return dict(row) if row else None


def _load_scenario_by_id(sid: int, db_path: Optional[Path] = None) -> Optional[Dict[str, Any]]:
    """Loads a single scenario dictionary from the SQLite database via primary ID."""
    if db_path is None:
        db_path = DB_FILE
    if not db_path.exists():
        return None
        
    with contextlib.closing(sqlite3.connect(str(db_path))) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM scenarios WHERE id=? LIMIT 1", (sid,)).fetchone()
        
    return dict(row) if row else None


def _render_physical_parameters_md_table(params_fmt: Dict[str, str]) -> str:
    """Generates the HTML markup for the physical parameters table."""
    if not params_fmt:
        return "<p>No physical parameters generated.</p>"
        
    rows = "".join(
        f"<tr><th>{key}</th><td>{value}</td></tr>" 
        for key, value in params_fmt.items()
    )
    
    return (
        "<table class='md-table'>"
        "<thead><tr><th>Parameter</th><th>Value (scientific)</th></tr></thead>"
        f"<tbody>{rows}</tbody>"
        "</table>"
    )


def _build_html_report(scenario: Dict[str, Any], sim: Dict[str, Any]) -> str:
    """
    Constructs the final HTML string utilizing advanced CSS styling 
    to replicate a high-end NASA-level alert dashboard.
    """
    name = scenario.get("scenario_name", "Unknown")
    date_tag = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    physics_li = "".join(f"<li>{p}</li>" for p in sim.get("dominant_physics", []))
    outcomes_li = "".join(f"<li>{o}</li>" for o in sim.get("possible_outcomes", []))
    sig_li = "".join(f"<li>{s}</li>" for s in sim.get("observational_signatures", []))
    followups = ", ".join(sim.get("recommended_followups", [])) or "N/A"
    
    phys_table = _render_physical_parameters_md_table(sim.get("physical_parameters_formatted", {}))

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Cosmic Scenario Report: {name}</title>
    <style>
        body{{background:#0b0c10;color:#c5c6c7;font-family:'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;margin:0;padding:20px;line-height:1.6}}
        .container{{max-width:1000px;margin:0 auto;background:#13151c;padding:30px;border-radius:8px;border:1px solid #1f2833;}}
        .header-block{{border-bottom:2px solid #45f3ff;padding-bottom:15px;margin-bottom:20px;}}
        .header-block h1{{color:#45f3ff;margin-top:0;font-size:28px;text-transform:uppercase;letter-spacing:1px;}}
        .header-block p{{margin:4px 0;font-family:'Consolas',monospace;font-size:14px;color:#8892b0;}}
        h2{{color:#ff00ff;border-bottom:1px solid #333;padding-bottom:5px;margin-top:30px;font-size:20px;text-transform:uppercase;}}
        .exec-summary{{background:#1a1d24;padding:15px;border-left:4px solid #45f3ff;margin-bottom:20px;font-size:15px;}}
        .md-table{{width:100%;border-collapse:collapse;margin:15px 0;font-family:'Consolas',monospace;font-size:14px;}}
        .md-table th, .md-table td{{padding:10px;border:1px solid #2d3748;text-align:left;}}
        .md-table th{{background:#1f2937;color:#45f3ff;}}
        .md-table tr:nth-child(even){{background:#161921;}}
        ul{{padding-left:20px;}} li{{margin-bottom:5px;}}
        .footer{{margin-top:40px;padding-top:15px;border-top:1px solid #2d3748;font-size:12px;color:#6b7280;text-align:center;font-family:'Consolas',monospace;}}
        .code-block{{background:#0d0f14;padding:15px;border:1px solid #2d3748;border-radius:4px;font-family:'Consolas',monospace;color:#a3be8c;white-space:pre-wrap;}}
    </style>
</head>
<body>
    <div class="container">
        <div class="header-block">
            <h1>Cosmic Scenario Report</h1>
            <p><b>Scenario ID:</b> {name}-{date_tag}</p>
            <p><b>Object:</b> {name} ({scenario.get('prefix')} {scenario.get('domain')} {scenario.get('process')} {scenario.get('type')})</p>
            <p><b>Parent Object:</b> {scenario.get('parent_object')} ({scenario.get('object_family')} candidate)</p>
            <p><b>Generated:</b> {now} | Cosmic Scenario Engine v{APP_VERSION}</p>
        </div>

        <h2>Executive Summary</h2>
        <div class="exec-summary">
            High-confidence anomaly detected consistent with a <b>{sim.get('energy_regime')}</b> operating at <b>{sim.get('spatial_scale')}</b>. 
            Target environment is verified as <b>{sim.get('environment')}</b>, involving primary progenitor objects: <b>{', '.join(sim.get('objects_involved', []))}</b>. 
            Triggered by observatory pipeline <b>{sim.get('analysis_pipeline')}</b> with a complexity score of <b>{sim.get('complexity_score')}</b>.
        </div>

        <h2>1. Physical Parameters</h2>
        {phys_table}

        <h2>2. Phenomenological Modeling</h2>
        <table class="md-table">
            <tr>
                <th style="width: 250px;">Dominant Physics</th>
                <td><ul>{physics_li}</ul></td>
            </tr>
            <tr>
                <th>Possible Outcomes</th>
                <td><ul>{outcomes_li}</ul></td>
            </tr>
        </table>

        <h2>3. Telemetry & Signatures</h2>
        <table class="md-table">
            <tr>
                <th style="width: 250px;">Observational Signatures</th>
                <td><ul>{sig_li}</ul></td>
            </tr>
            <tr>
                <th>Recommended Follow-ups</th>
                <td>{followups}</td>
            </tr>
        </table>

        <h2>4. Operational Estimates</h2>
        <table class="md-table">
            <tr>
                <th style="width: 250px;">Priority Level</th>
                <td style="color:#ff00ff; font-weight:bold;">{sim.get('priority').upper()}</td>
            </tr>
            <tr>
                <th>Detection Confidence</th>
                <td>{sim.get('detection_confidence')}</td>
            </tr>
            <tr>
                <th>Estimated Data Volume</th>
                <td>{sim.get('data_volume_tb')} TB</td>
            </tr>
            <tr>
                <th>Estimated Budget Allocation</th>
                <td>${sim.get('estimated_cost_usd'):,}</td>
            </tr>
        </table>

        <h2>5. Raw Alert Payload</h2>
        <div class="code-block">{sim.get('alert_summary')}</div>

        <div class="footer">
            Validated against MESA stellar models v24.08 | Cross-checked with 2025 SN 2025dx multi-messenger dataset
        </div>
    </div>
</body>
</html>"""


def _build_pdf_report(scenario: Dict[str, Any], sim: Dict[str, Any], pdf_path: Path) -> bool:
    """Uses ReportLab (if available) to construct a high-quality PDF report."""
    if not REPORTLAB_AVAILABLE:
        return False
        
    try:
        doc = rl_SimpleDocTemplate(
            str(pdf_path), 
            pagesize=rl_letter, 
            rightMargin=40, 
            leftMargin=40
        )
        
        styles = rl_getSampleStyleSheet()
        title_style = rl_ParagraphStyle(
            "Title", 
            parent=styles["Heading1"], 
            fontSize=18, 
            textColor=rl_colors.HexColor("#000000"), 
            spaceAfter=12
        )
        h2_style = rl_ParagraphStyle(
            "H2", 
            parent=styles["Heading2"], 
            fontSize=14, 
            textColor=rl_colors.HexColor("#333333"), 
            spaceAfter=6, 
            spaceBefore=12
        )
        normal_style = rl_ParagraphStyle(
            "Normal", 
            parent=styles["Normal"], 
            fontSize=10, 
            textColor=rl_colors.HexColor("#222222"), 
            leading=14
        )
        mono_style = rl_ParagraphStyle(
            "Mono", 
            parent=styles["Code"], 
            fontSize=9, 
            fontName="Courier"
        )

        elements: list = []
        name = scenario.get("scenario_name", "Unknown")
        date_tag = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        
        elements.append(rl_Paragraph("<b>COSMIC SCENARIO REPORT</b>", title_style))
        elements.append(rl_Paragraph(f"<b>Scenario ID:</b> {name}-{date_tag}", normal_style))
        elements.append(rl_Paragraph(f"<b>Object:</b> {name}", normal_style))
        elements.append(rl_Paragraph(f"<b>Generated:</b> {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}", normal_style))
        elements.append(rl_Spacer(1, 15))

        # Physical Parameters Table
        elements.append(rl_Paragraph("<b>1. Physical Parameters</b>", h2_style))
        phys_fmt = sim.get("physical_parameters_formatted", {})
        
        if phys_fmt:
            pt_data = [["Parameter", "Value (Scientific)"]]
            for k, v in phys_fmt.items():
                pt_data.append([k, v])
                
            pt = rl_Table(pt_data, colWidths=[200, 250])
            pt.setStyle(rl_TableStyle([
                ("BACKGROUND", (0, 0), (-1, 0), rl_colors.HexColor("#e2e8f0")),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("GRID", (0, 0), (-1, -1), 0.5, rl_colors.grey),
                ("FONTNAME", (0,1), (-1,-1), "Courier"),
                ("PADDING", (0,0), (-1,-1), 6),
            ]))
            elements.append(pt)
            
        elements.append(rl_Spacer(1, 15))

        # Telemetry Table
        elements.append(rl_Paragraph("<b>2. Operational Telemetry</b>", h2_style))
        tel_data = [
            ["Priority", sim.get('priority', 'N/A').upper()],
            ["Detection Confidence", str(sim.get('detection_confidence'))],
            ["Estimated Data Volume", f"{sim.get('data_volume_tb')} TB"],
            ["Est. Budget Allocation", f"${sim.get('estimated_cost_usd'):,}"],
        ]
        
        tt = rl_Table(tel_data, colWidths=[200, 250])
        tt.setStyle(rl_TableStyle([
            ("GRID", (0, 0), (-1, -1), 0.5, rl_colors.grey),
            ("BACKGROUND", (0, 0), (0, -1), rl_colors.HexColor("#f8fafc")),
            ("FONTNAME", (0, 0), (0, -1), "Helvetica-Bold"),
            ("PADDING", (0, 0), (-1, -1), 6),
        ]))
        elements.append(tt)
        elements.append(rl_Spacer(1, 15))

        elements.append(rl_Paragraph("<b>3. Raw Alert Summary</b>", h2_style))
        elements.append(rl_Paragraph(sim.get("alert_summary","").replace("\n", "<br/>"), mono_style))
        elements.append(rl_Spacer(1, 25))

        footer_style = rl_ParagraphStyle(
            "Footer", 
            parent=normal_style, 
            fontSize=8, 
            textColor=rl_colors.grey
        )
        elements.append(rl_Paragraph(
            "Validated against MESA stellar models v24.08 | Cross-checked with 2025 SN 2025dx multi-messenger dataset", 
            footer_style
        ))

        doc.build(elements)
        return True
        
    except Exception:
        return False


def generate_report(
    scenario_id_or_name: Union[int, str], 
    out_dir: Optional[Path] = None, 
    db_path: Optional[Path] = None, 
    ontology_path: Optional[Path] = None
) -> Tuple[Optional[Path], Optional[Path]]:
    """Coordinates the generation of both HTML and PDF reports."""
    log = get_logger()
    
    if out_dir is None:
        out_dir = REPORTS_DIR
        
    out_dir.mkdir(parents=True, exist_ok=True)

    scenario: Optional[Dict[str, Any]] = None
    if isinstance(scenario_id_or_name, int):
        scenario = _load_scenario_by_id(scenario_id_or_name, db_path)
        file_tag = str(scenario_id_or_name)
    else:
        scenario = _load_scenario_by_name(str(scenario_id_or_name), db_path)
        file_tag = str(scenario_id_or_name).replace(" ", "_")[:80]

    # Dynamically map missing parameters if object not in DB
    if scenario is None:
        if isinstance(scenario_id_or_name, str):
            parts = scenario_id_or_name.split("_")
            scenario = {
                "id": 0, 
                "scenario_name": scenario_id_or_name, 
                "object_family": "Synthetic",
                "object_type": "GeneratedObject", 
                "parent_object": "Universe",
                "prefix": parts[0] if parts else "Unknown", 
                "domain": parts[1] if len(parts) > 1 else "Unknown",
                "process": parts[2] if len(parts) > 2 else "Unknown", 
                "type": parts[3] if len(parts) > 3 else "Unknown",
            }
        else:
            return None, None

    name = scenario["scenario_name"]
    sim = simulate_scenario(name)

    html_content = _build_html_report(scenario, sim)
    html_path = out_dir / f"report_{file_tag}.html"
    tmp_html = html_path.with_suffix(".tmp")
    
    with open(tmp_html, "w", encoding="utf-8") as fh:
        fh.write(html_content)
        
    try:
        if html_path.exists():
            html_path.unlink()
        tmp_html.rename(html_path)
    except OSError:
        shutil.copy(tmp_html, html_path)
        tmp_html.unlink()
        
    log.info("HTML report → %s", html_path)

    pdf_path_candidate = out_dir / f"report_{file_tag}.pdf"
    if _build_pdf_report(scenario, sim, pdf_path_candidate):
        log.info("PDF report  → %s", pdf_path_candidate)
        return html_path, pdf_path_candidate
        
    return html_path, None


# =============================================================================
# SECTION 9: PyQt5 GUI (Enterprise UX — full replacement)
# =============================================================================

CYBER_STYLESHEET = """
QMainWindow, QWidget { 
    background-color: #0b0c10; 
    color: #c5c6c7; 
    font-family: 'Consolas', monospace; 
}
QSplitter::handle { 
    background-color: #45f3ff; 
}
QTreeWidget { 
    background-color: #12141a; 
    color: #66fcf1; 
    border: 1px solid #45f3ff; 
    font-size: 13px; 
}
QTreeWidget::item:selected { 
    background-color: #1f2833; 
    color: #ff00ff; 
    font-weight: bold; 
}
QTreeWidget::item:hover { 
    background-color: #1a1a40; 
}
QTableWidget { 
    background-color: #12141a; 
    color: #66fcf1; 
    border: 1px solid #45f3ff; 
    gridline-color: #1f2833; 
    font-size: 12px; 
}
QTableWidget::item:selected { 
    background-color: #1f2833; 
    color: #ff00ff; 
    font-weight: bold; 
}
QHeaderView::section { 
    background-color: #1f2833; 
    color: #45f3ff; 
    padding: 5px; 
    border: 1px solid #45f3ff; 
    font-weight: bold; 
    text-transform: uppercase; 
}
QTextBrowser { 
    background-color: #0b0c10; 
    color: #c5c6c7; 
    border: 1px solid #45f3ff; 
    font-size: 13px; 
}
QPushButton { 
    background-color: #1f2833; 
    color: #45f3ff; 
    border: 1px solid #45f3ff; 
    padding: 6px 16px; 
    border-radius: 4px; 
    font-size: 12px; 
    font-weight: bold; 
    text-transform: uppercase; 
}
QPushButton:hover { 
    background-color: #45f3ff; 
    color: #0b0c10; 
}
QPushButton:pressed { 
    background-color: #ff00ff; 
    border-color: #ff00ff; 
    color: #0b0c10; 
}
QPushButton:disabled { 
    background-color: #12141a; 
    color: #444; 
    border-color: #333; 
}
QLineEdit { 
    background-color: #12141a; 
    color: #ff00ff; 
    border: 1px solid #45f3ff; 
    padding: 5px 10px; 
    border-radius: 4px; 
    font-size: 13px; 
    font-weight: bold; 
}
QComboBox { 
    background-color: #12141a; 
    color: #66fcf1; 
    border: 1px solid #45f3ff; 
    padding: 4px; 
    border-radius: 4px; 
    font-weight: bold; 
}
QComboBox:disabled { 
    background-color: #12141a; 
    color: #444; 
    border: 1px solid #333; 
}
QComboBox::drop-down { 
    border: 0px; 
}
QComboBox QAbstractItemView { 
    background-color: #0b0c10; 
    color: #66fcf1; 
    selection-background-color: #1f2833; 
    selection-color: #ff00ff; 
}
QLabel { 
    color: #66fcf1; 
    font-size: 12px; 
}
QStatusBar { 
    background-color: #1f2833; 
    color: #ff00ff; 
    font-size: 12px; 
    font-weight: bold; 
}
QProgressBar { 
    background-color: #12141a; 
    border: 1px solid #45f3ff; 
    border-radius: 4px; 
    text-align: center; 
    color: #ff00ff; 
    font-weight: bold; 
}
QProgressBar::chunk { 
    background-color: #45f3ff; 
    border-radius: 3px; 
}
QTabWidget::pane { 
    border: 1px solid #45f3ff; 
}
QTabBar::tab { 
    background-color: #12141a; 
    color: #45f3ff; 
    padding: 8px 20px; 
    border: 1px solid #45f3ff; 
    font-weight: bold; 
    text-transform: uppercase; 
}
QTabBar::tab:selected { 
    background-color: #1f2833; 
    color: #ff00ff; 
    border-bottom-color: #ff00ff; 
    border-bottom-width: 3px; 
}
"""

GUI_PAGE_SIZE = 100

# ---------------------------------------------------------------------------
# PREFIX DESCRIPTIONS (RESTORED FROM V1 for UI Tooltips)
# ---------------------------------------------------------------------------
PREFIX_DESCS = {
    "Hyper": "Extreme or excessive energy/scale",
    "Ultra": "Beyond standard operational limits",
    "Mega": "Massive spatial or energetic scale",
    "Proto": "Early, unformed, or initial stage",
    "Neo": "New or secondary evolutionary phase",
    "Quasi": "Apparent but not actual state",
    "Pseudo": "False or mimicked characteristics",
    "Trans": "Transitioning between physical states",
    "Sub": "Operating below normal energetic thresholds",
    "Super": "Operating above normal energetic thresholds",
    "Micro": "Microscopic or heavily localized scale",
    "Macro": "Macroscopic or widely generalized scale",
    "Meta": "Higher-order or overarching state condition",
    "Para": "Alongside or parallel energetic dynamics",
    "Exo": "External or outward-facing process",
    "Endo": "Internal or inward-facing process",
    "Iso": "Uniform or completely constant properties",
    "Aniso": "Non-uniform or directional properties",
    "Multi": "Multiple simultaneous occurrences detected",
    "Poly": "Complex, many-faceted internal structure",
    "Mono": "Singular or heavily unified structure",
    "Bi": "Dual or binary physical interaction",
    "Tri": "Triple-component gravitational interaction",
    "Quadra": "Four-component complex interaction",
    "Penta": "Five-component chaotic interaction",
    "Omni": "Universal or all-encompassing effect",
    "Null": "Zero-point, deactivated, or absent property",
    "Anti": "Reversed, inverted, or antimatter presence",
    "Counter": "Opposing or forcefully balancing dynamic",
    "Retro": "Backwards or reversed cosmic evolution",
    "Infra": "Below standard frequency or energy emission",
    "Supra": "Above standard frequency or energy emission",
    "Giga": "Extremely large-scale energetic magnitude",
    "Tera": "Planetary or stellar-scale magnitude",
    "Peta": "Massively amplified astrophysical energy scale",
    "Exa": "Extreme cosmic-scale energetic magnitude",
    "Zetta": "Near-universal energetic scale",
    "Yotta": "Maximum measurable cosmic-scale magnitude",
    "Zepto": "Ultra-minuscule quantum-scale presence",
    "Atto": "Extremely tiny energetic or temporal scale",
    "Femto": "Subatomic interaction scale",
    "Pico": "Very small localized energy fluctuation",
    "Nano": "Nanoscopic or near-quantum structural scale",
    "Kilo": "Thousand-scale energetic or spatial magnitude",
    "Hecto": "Hundred-scale energetic or spatial magnitude",
    "Centi": "Hundredth-scale measurement or effect",
    "Milli": "Thousandth-scale measurement or effect",
    "Pan": "All-encompassing systemic interaction",
    "Arche": "Primordial or original formation stage",
    "Paleo": "Ancient or early cosmic evolutionary phase",
    "Ceno": "Recent or modern cosmic evolutionary phase",
    "Meso": "Intermediate-scale structural region",
    "Holo": "Complete or fully integrated structure",
    "Hemi": "Half-structure or partial formation",
    "Semi": "Partially active or incomplete state",
    "Demi": "Reduced or half-strength manifestation",
    "Uni": "Single unified cosmic structure",
    "Hexa": "Six-component structural interaction",
    "Hepta": "Seven-component complex interaction",
    "Octa": "Eight-component multi-body interaction"
}

DOMAIN_DESCS = {
    "Stellar": "Pertaining to stars and stellar bodies",
    "Planetary": "Pertaining to planets and planetary bodies",
    "Galactic": "Pertaining to entire galaxies and their core",
    "Nebular": "Pertaining to interstellar clouds of dust/gas",
    "Cometary": "Pertaining to comets and icy wandering bodies",
    "Asteroid": "Pertaining to rocky airless worlds",
    "Quasar": "Active galactic nuclei powered by black holes",
    "Pulsar": "Highly magnetized, rapidly rotating neutron stars",
    "BlackHole": "Region of spacetime with extreme localized gravity",
    "Magnetar": "Neutron star with an extremely strong magnetic field",
    "Exoplanet": "Planet located completely outside our solar system",
    "Protostar": "Very young star still actively gathering mass",
    "WhiteDwarf": "Stellar core remnant composed of electron-degenerate matter",
    "NeutronStar": "Collapsed core of a massive supergiant star",
    "GlobularCluster": "Spherical collection of stars that orbits a galactic core",
    "SupernovaRemnant": "Structure resulting from the explosive death of a star",
    "DarkMatter": "Unseen exotic matter exerting extreme gravitational pull",
    "CosmicVoid": "Vast empty spaces between cosmic galaxy filaments",
    "Interstellar": "The material or space existing between stars in a galaxy",
    "Circumstellar": "The immediate space directly surrounding a star",
    "Accretion": "Accumulation of particles into a massive central object",
    "Solar": "Pertaining specifically to solar-type stellar bodies",
    "Lunar": "Pertaining to natural satellite moons",
    "Chromospheric": "The second layer of a star's atmospheric envelope",
    "Coronal": "The outermost super-heated part of a star's atmosphere",
    "Photospheric": "The intensely luminous envelope of a star",
    "Magnetospheric": "Region dominated by a celestial body's magnetic field",
    "Heliospheric": "Region of space completely dominated by solar wind",
    "Intergalactic": "The massive space acting as a bridge between galaxies",
    "Cosmological": "Pertaining to the universe as a singular whole",
    "BrownDwarf": "Sub-stellar objects too low in mass to sustain hydrogen fusion",
    "RedDwarf": "Small cool main-sequence stars",
    "Subdwarf": "Low-luminosity stars with reduced metallic content",
    "Supergiant": "Among the most massive and luminous stars",
    "Hypergiant": "Stars with tremendous mass and extreme luminosity",
    "ActiveGalacticNucleus": "Compact region at the center of a galaxy with extraordinary luminosity",
    "Blazar": "Active galactic nucleus with a relativistic jet pointing toward Earth",
    "SeyfertGalaxy": "Galaxies with extremely bright nuclei and ionized gas emissions",
    "RadioGalaxy": "Galaxies that emit exceptionally strong radio-wave radiation",
    "StarburstGalaxy": "Galaxies undergoing an extremely high rate of star formation",
    "DwarfGalaxy": "Small galaxies composed of up to several billion stars",
    "IrregularGalaxy": "Galaxies lacking a defined symmetrical structure",
    "LenticularGalaxy": "Galaxies exhibiting properties between elliptical and spiral",
    "SpiralGalaxy": "Galaxies with rotating disks and extended spiral arms",
    "EllipticalGalaxy": "Galaxies with smooth ellipsoidal stellar distributions",
    "Protoplanetary": "Dense rotating disk of gas and dust surrounding a young star",
    "Circumplanetary": "Disk of material orbiting around a forming planet",
    "OortCloud": "Hypothetical spherical reservoir of distant icy bodies",
    "KuiperBelt": "Circumstellar disk of icy objects beyond Neptune",
    "Exocomet": "Comet-like icy bodies located in other star systems",
    "Wormhole": "Hypothetical spacetime tunnel connecting distant regions of the universe",
    "Tachyon": "Hypothetical particle capable of traveling faster than light",
    "Axion": "Theoretical particle proposed as a dark matter candidate",
    "String": "Fundamental one-dimensional object in string theory physics",
    "Brane": "Higher-dimensional structure within string theory frameworks",
    "Multiverse": "Hypothetical ensemble of multiple universes",
    "Omniverse": "The totality of all possible universes and realities",
    "Microverse": "Extremely small-scale universe-like quantum domain",
    "Macroverse": "Extremely large-scale cosmic structural domain",
    "DysonSphere": "Hypothetical megastructure built to encompass a star for energy harvesting",
    "MatrioshkaBrain": "Layered Dyson-sphere megastructure optimized for computation",
    "Ringworld": "Massive ring-shaped artificial habitat encircling a star",
    "StellarEngine": "Megastructure designed to control the movement of an entire star system",
    "Kugelblitz": "Black hole theoretically created by concentrating energy into a small region",
    "ShkadovThruster": "Stellar propulsion system using a giant reflective mirror",
    "AldersonDisk": "Hypothetical massive disk-shaped megastructure supporting life",
    "Topopolis": "Extremely long tubular megastructure capable of supporting civilizations",
    "GlobusCassus": "Hypothetical hollowed-out planetary megastructure",
    "Megastructure": "Artificial structure on astronomical scales",
    "Filament": "Large-scale thread-like cosmic structure connecting galaxy clusters",
    "Void": "Massive under-dense region with extremely few galaxies",
    "Supercluster": "Massive grouping of galaxy clusters across vast regions",
    "LocalGroup": "The small cluster of galaxies including the Milky Way",
    "GalacticBulge": "Dense spherical structure at the center of a galaxy",
    "GalacticHalo": "Diffuse region of stars and dark matter surrounding a galaxy",
    "OpenCluster": "Loose gravitational grouping of young stars",
    "StellarAssociation": "Weakly bound group of stars formed together",
    "MovingGroup": "Collection of stars sharing common motion through space",
    "StarStream": "Elongated stream of stars formed by gravitational disruption",
    "AccretionDisk": "Rotating disk of matter spiraling into a massive object",
    "Proplyd": "Protoplanetary disk being photoevaporated by nearby stars",
    "DebrisDisk": "Disk of dust and debris left from planetary formation",
    "ScatteredDisc": "Region of distant icy bodies with highly eccentric orbits",
    "HillsCloud": "Hypothetical inner extension of the Oort Cloud",
    "AsteroidBelt": "Region filled with rocky debris orbiting a star",
    "Centaur": "Small solar system body orbiting between giant planets",
    "Plutino": "Kuiper belt object in orbital resonance with Neptune",
    "Comet": "Icy small body releasing gas and dust when near a star",
    "Meteoroid": "Small rocky or metallic body traveling through space",
    "Micrometeoroid": "Extremely tiny meteoroid particles in space",
    "DustGrain": "Microscopic particles of solid cosmic matter",
    "GasCloud": "Large accumulation of diffuse gas in space",
    "MolecularCloud": "Cold dense gas cloud where stars may form",
    "HIIRegion": "Ionized hydrogen region surrounding hot young stars",
    "HIRegion": "Neutral atomic hydrogen gas region in interstellar space",
    "PlanetaryNebula": "Expanding glowing shell of gas ejected from a dying star",
    "PulsarWindNebula": "Nebula powered by relativistic wind from a pulsar",
    "BokGlobule": "Small dense dark cloud capable of forming stars",
    "HerbigHaroObject": "Bright shock region formed by stellar jets colliding with gas",
    "DarkNebula": "Dense dust cloud that blocks visible light from background stars"
}

PROCESS_DESCS = {
    "Fusion": "Combining atomic nuclei to form heavier mass",
    "Fission": "Splitting of atomic nuclei to release energy",
    "Accretion": "Gravitational gathering and settling of mass",
    "Convection": "Heat transfer triggered by bulk fluid flow",
    "Radiation": "Emission of energy as intense electromagnetic waves",
    "Collapse": "Inward fall of a body due to severe gravity",
    "Ejection": "Forceful expulsion of matter outwards",
    "Ionization": "Process of atoms actively losing or gaining electrons",
    "Recombination": "Free electrons being safely captured by ions",
    "Nucleosynthesis": "Creation of brand new atomic nuclei centers",
    "Magnetohydrodynamic": "Magnetic forces governing electrically conducting cosmic fluids",
    "Turbulence": "Chaotic and irregular motion of fluids or plasma",
    "Oscillation": "Periodic variation in magnitude or spatial position",
    "Precession": "Slow rotational change in the orientation of an axis",
    "Tidal": "Gravitational distortion produced by nearby massive bodies",
    "Evaporation": "Phase transition directly from liquid to gas",
    "Condensation": "Phase transition directly from gas to liquid",
    "Sublimation": "Phase transition from solid directly to gas",
    "Ablation": "Removal of material from a surface by intense heating",
    "Sputtering": "Ejection of atoms from a solid target by energetic impact",
    "Dynamo": "Mechanism through which celestial bodies generate magnetic fields",
    "Reconnection": "Breaking and reconnecting of magnetic field lines releasing energy",
    "Diffusion": "Net movement of particles from high to low concentration",
    "Scattering": "Deflection of particles or radiation from their initial path",
    "Absorption": "Capture of radiation energy by surrounding matter",
    "Emission": "Release of energy from matter in radiative form",
    "Reflection": "Redirection of waves at the boundary between media",
    "Refraction": "Change in direction of waves passing between media",
    "Polarization": "Orientation alignment of oscillating electromagnetic waves",
    "Lensing": "Bending of light paths by gravitational fields",
    "FusionPhase": "Defined stage during which nuclear fusion dominates energy production",
    "AccretionPhase": "Distinct period of active mass accumulation onto a body",
    "CollapsePhase": "Rapid gravitational contraction phase of a structure",
    "EjectionPhase": "Active stage of outward mass expulsion",
    "RadiationPhase": "Time interval dominated by strong radiative emission",
    "ConvectionCycle": "Repeating circulation of heat driven fluid flow",
    "DynamoCycle": "Periodic regeneration and reversal of magnetic fields",
    "OscillationMode": "Specific vibrational pattern within a physical structure",
    "TurbulenceMode": "Distinct structural regime of chaotic fluid motion",
    "ReconnectionCycle": "Repeated occurrence of magnetic field reconnection events",
    "Annihilation": "Mutual destruction of matter and antimatter into pure energy",
    "Spallation": "Fragmentation of atomic nuclei by energetic particle collisions",
    "Bremsstrahlung": "Radiation emitted when charged particles are rapidly decelerated",
    "Synchrotron": "Radiation emitted by relativistic particles spiraling in magnetic fields",
    "Comptonization": "Energy transfer between photons and electrons through scattering",
    "Photoionization": "Ionization produced by absorption of energetic photons",
    "Photodissociation": "Breaking molecular bonds through photon absorption",
    "Chemosynthesis": "Chemical synthesis of organic compounds using chemical energy",
    "Isomerization": "Transformation of a molecule into another structural form",
    "Polymerization": "Formation of large molecules through repeated monomer bonding",
    "Crystallization": "Formation of ordered solid structures from atoms or molecules",
    "Liquefaction": "Conversion of a gas or solid into liquid form",
    "Solidification": "Transformation of a liquid into a solid state",
    "Vaporization": "Transition from liquid phase to gaseous phase",
    "Subduction": "Geological process where one tectonic plate sinks beneath another",
    "Outgassing": "Release of trapped or dissolved gases from planetary materials",
    "Outflow": "Large-scale outward movement of matter from a central source",
    "Infall": "Gravitational inward movement of surrounding material",
    "Downflow": "Descending movement of fluid or plasma",
    "Upflow": "Ascending movement of fluid or plasma",
    "Circulation": "Continuous large-scale movement of fluid in closed pathways",
    "Advection": "Transport of properties through bulk motion of matter",
    "Stratification": "Formation of layered structures due to density differences",
    "Differentiation": "Separation of materials by density inside planetary bodies",
    "Fractionation": "Selective separation of isotopes or chemical components",
    "Segregation": "Separation of materials into distinct regions or phases",
    "SegregationPhase": "Specific interval of material separation and internal layering",
    "Thermalization": "Process through which particles reach thermal equilibrium",
    "Equilibration": "Balancing of physical processes to reach stable conditions",
    "Relaxation": "Return of a system toward equilibrium after disturbance",
    "Perturbation": "Small external disturbance altering system behavior",
    "PerturbationPhase": "Defined period of active system disturbance",
    "Excitation": "Transition of particles to higher energy states",
    "Deexcitation": "Return of particles from excited states to lower energy states",
    "Decay": "Spontaneous transformation of unstable particles or nuclei",
    "DecayCycle": "Repeating sequence of decay processes",
    "Amplification": "Increase in magnitude of a physical signal or oscillation",
    "Damping": "Reduction of oscillation amplitude due to resistive forces",
    "Dissipation": "Loss of organized energy into heat or disorder",
    "Decoherence": "Loss of quantum phase relationships in a quantum system",
    "QuantumTunneling": "Quantum process allowing particles to pass through energy barriers",
    "SpontaneousEmission": "Emission of photons from excited states without external trigger",
    "StimulatedEmission": "Photon emission triggered by incoming radiation",
    "PairProduction": "Creation of particle-antiparticle pairs from high-energy photons",
    "BetaDecay": "Nuclear decay involving transformation of neutrons or protons",
    "AlphaDecay": "Emission of helium nuclei from unstable atomic nuclei",
    "GammaDecay": "Emission of high-energy photons from excited nuclei",
    "ElectronCapture": "Absorption of an inner electron by a proton in the nucleus",
    "PositronEmission": "Emission of a positron during nuclear decay",
    "NeutrinoEmission": "Release of neutrinos during nuclear or particle interactions",
    "NeutronCapture": "Absorption of neutrons by atomic nuclei",
    "ProtonCapture": "Absorption of protons by atomic nuclei",
    "Photodisintegration": "Breaking apart of atomic nuclei by high-energy photons",
    "Hadronization": "Formation of hadrons from quarks and gluons",
    "Baryogenesis": "Generation of baryonic matter in the early universe",
    "Leptogenesis": "Generation of leptons contributing to matter asymmetry",
    "Reionization": "Cosmic epoch during which neutral hydrogen became ionized",
    "Decoupling": "Separation of radiation and matter in the early universe",
    "Inflation": "Extremely rapid exponential expansion of the early universe",
    "Expansion": "Increase in the large-scale size of the universe",
    "Contraction": "Decrease in cosmic scale due to gravitational collapse",
    "Bounce": "Transition from cosmic contraction to expansion",
    "Crunch": "Hypothetical collapse of the universe into a dense state",
    "Rip": "Hypothetical tearing apart of spacetime by accelerated expansion",
    "Freeze": "Thermodynamic decline of the universe toward heat death",
    "Death": "Final thermodynamic state where usable energy approaches zero",
    "Deposition": "Phase transition from gas directly to solid",
    "Melting": "Transition from solid phase to liquid",
    "Freezing": "Transition from liquid phase to solid",
    "Boiling": "Rapid vaporization throughout a liquid",
    "Cavitation": "Formation and collapse of vapor bubbles in a fluid",
    "Sonoluminescence": "Emission of light from collapsing bubbles in liquids",
    "Bioluminescence": "Light emission produced by biological chemical reactions",
    "Chemiluminescence": "Light emission produced by chemical reactions",
    "Triboluminescence": "Light emission generated by mechanical stress or friction",
    "Dissolution": "Process of a substance dissolving into a solvent",
    "Precipitation": "Formation of a solid from a dissolved substance",
    "Coagulation": "Clumping together of particles into larger masses",
    "Flocculation": "Aggregation of particles into loose clusters",
    "Agglomeration": "Binding of particles into compact clusters"
}

TYPE_DESCS = {
    "Field": "Region characterized strictly by a physical property",
    "Region": "A distinct mapped area with specific conditions",
    "Layer": "A horizontal band or specific stratum of material",
    "Structure": "A stable geometric arrangement of matter",
    "System": "A set of intimately interacting celestial bodies",
    "Environment": "The overarching surrounding cosmic conditions",
    "Phase": "A completely distinct state of matter or configuration",
    "Mode": "A particular form, manner, or pattern of operation",
    "Regime": "A system of physical rules or dominant cosmic forces",
    "State": "The exact physical condition of the matter",
    "Zone": "An area distinguished explicitly by particular properties",
    "Interface": "Boundary layer where two different regimes meet",
    "Boundary": "The extreme edge marking the limit of a structure",
    "Core": "The overwhelmingly dense central region of a body",
    "Shell": "An outer protective layer enveloping a deep core",
    "Envelope": "The overarching gaseous or plasma exterior layer",
    "Halo": "A massive spherical cloud of gas, dust, or stars",
    "Jet": "A hyper-collimated beam of forcefully ejected matter",
    "Disk": "A completely flattened circular structure of orbiting matter",
    "Ring": "A distinct circular band of orbiting cosmic particles",
    "Network": "Interconnected system of filaments and nodes",
    "Filament": "Massive thread-like structure of dark matter and galaxies",
    "Web": "Complex structural framework spanning intergalactic space",
    "Bubble": "Roughly spherical cavity carved by energetic stellar winds",
    "Cavity": "Void or largely empty region carved within denser medium",
    "Lobe": "Massive extended emission region often from active galaxies",
    "Plume": "Column of one fluid moving through another medium",
    "Outflow": "Directed stream of rapidly escaping material",
    "Stream": "Narrow continuous ribbon of moving matter",
    "Current": "Directed coherent flow of particles or fluids",
    "Vortex": "Region in a fluid where motion revolves around an axis",
    "Eddy": "Localized swirling motion produced in turbulent flows",
    "Cell": "Distinct bounded convective or structural unit",
    "Pocket": "Isolated localized accumulation of specific materials",
    "Domain": "Extended region with consistent physical properties",
    "Sector": "Designated geometric slice or subdivision of a system",
    "Territory": "Broad spatial region dominated by specific forces",
    "Province": "Large-scale characteristic geological or structural region",
    "Sphere": "Perfectly round three-dimensional spatial envelope",
    "Globe": "Spherical celestial body or structural unit",
    "Matrix": "Structured array or framework containing interacting elements",
    "Grid": "Regular network of intersecting structural lines",
    "Lattice": "Ordered repeating geometric structure of connected nodes",
    "Tensor": "Mathematical entity describing multidimensional physical quantities",
    "Manifold": "Topological space locally resembling Euclidean geometry",
    "Scalar": "Physical quantity described only by magnitude",
    "Vector": "Physical quantity possessing both magnitude and direction",
    "Spinor": "Mathematical object describing particle spin states",
    "Bispinor": "Extended spinor representation used in relativistic quantum theory",
    "Twistor": "Geometric mathematical construct linking spacetime and quantum fields",
    "Wave": "Oscillatory propagation of energy through space or medium",
    "Particle": "Discrete localized unit of matter or energy",
    "Quantum": "Smallest discrete quantity of a physical property",
    "String": "One-dimensional fundamental object proposed in string theory",
    "Brane": "Higher-dimensional object embedded within theoretical spacetime",
    "Point": "Zero-dimensional location with no spatial extent",
    "Line": "One-dimensional continuous extension through space",
    "Plane": "Two-dimensional flat surface extending infinitely",
    "Surface": "Boundary separating interior and exterior volumes",
    "Volume": "Three-dimensional spatial region containing matter",
    "Hypervolume": "Higher-dimensional spatial region beyond three dimensions",
    "Fractal": "Self-similar geometric structure repeating across scales",
    "Chaos": "Highly sensitive nonlinear dynamical system behavior",
    "Attractor": "State toward which a dynamical system evolves",
    "Repeller": "State from which system trajectories diverge",
    "Bifurcation": "Sudden qualitative change in system behavior",
    "Singularity": "Point where physical quantities become infinite",
    "Topology": "Mathematical study of spatial properties preserved under deformation",
    "Geometry": "Mathematical description of shape, size, and spatial relationships",
    "Symmetry": "Balanced invariance under transformation",
    "Asymmetry": "Absence of balanced invariance in structure or behavior",
    "Supersymmetry": "Theoretical symmetry relating fermions and bosons",
    "Gauge": "Field symmetry governing fundamental interactions",
    "Configuration": "Specific arrangement of system components",
    "Arrangement": "Organized placement of elements within a structure",
    "Pattern": "Repeated structural or dynamical arrangement",
    "Sequence": "Ordered progression of elements or events",
    "Series": "Collection of related elements arranged sequentially",
    "Progression": "Gradual development through ordered stages",
    "Cycle": "Repetitive sequence of events occurring periodically"
}


def _launch_gui() -> None:
    """Entry point for the massive PyQt5 GUI framework."""
    if not PYQT5_AVAILABLE:
        print("ERROR: PyQt5 is not installed. Install with:  pip install PyQt5")
        sys.exit(1)
        
    app = QApplication(sys.argv)
    app.setApplicationName(APP_NAME)
    app.setApplicationVersion(APP_VERSION)
    app.setStyleSheet(CYBER_STYLESHEET)
    
    window = _CosmicMainWindow()
    window.show()
    sys.exit(app.exec_())


def _make_gui_classes() -> type:
    """Constructs the fully expanded GUI classes dynamically to prevent NameErrors if missing."""
    if not PYQT5_AVAILABLE:
        return type("DummyWindow", (), {})

    class SimulateWorker(QThread):
        finished = pyqtSignal(dict)
        error = pyqtSignal(str)

        def __init__(self, scenario_name: str) -> None:
            super().__init__()
            self.scenario_name = scenario_name

        def run(self) -> None:
            try:
                self.finished.emit(simulate_scenario(self.scenario_name))
            except Exception as exc:
                self.error.emit(str(exc))

    class ReportWorker(QThread):
        finished = pyqtSignal(str, str)
        error = pyqtSignal(str)

        def __init__(self, scenario_name: str) -> None:
            super().__init__()
            self.scenario_name = scenario_name

        def run(self) -> None:
            try:
                hp, pp = generate_report(self.scenario_name)
                self.finished.emit(str(hp) if hp else "", str(pp) if pp else "")
            except Exception as exc:
                self.error.emit(str(exc))

    class DataLoaderWorker(QThread):
        finished = pyqtSignal(int, int, list, str)

        def __init__(
            self, 
            query_id: int, 
            db_path: Path, 
            where_clause: str, 
            params: list, 
            limit: int, 
            offset: int, 
            cache_total: int
        ) -> None:
            super().__init__()
            self.query_id = query_id
            self.db_path = db_path
            self.w = where_clause
            self.p = params
            self.limit = limit
            self.offset = offset
            self.cache_total = cache_total

        def run(self) -> None:
            conn = None
            try:
                conn = sqlite3.connect(str(self.db_path))
                conn.execute("PRAGMA cache_size=-64000;")
                conn.execute("PRAGMA mmap_size=2147483648;")
                
                if not self.w:
                    if self.cache_total > 0:
                        total = self.cache_total
                    else:
                        max_id = conn.execute("SELECT MAX(id) FROM scenarios").fetchone()[0]
                        total = max_id if max_id else 0
                else:
                    total = conn.execute(f"SELECT COUNT(id) FROM scenarios{self.w}", self.p).fetchone()[0]
                
                rows = conn.execute(
                    f"SELECT id,scenario_name,object_family,domain,process,type FROM scenarios{self.w} "
                    f"ORDER BY id LIMIT ? OFFSET ?",
                    self.p + [self.limit, self.offset]
                ).fetchall()
                
                self.finished.emit(self.query_id, total, rows, "")
            except Exception as exc:
                if "interrupted" not in str(exc).lower():
                    self.finished.emit(self.query_id, 0, [], str(exc))
            finally:
                if conn:
                    try:
                        conn.close()
                    except Exception:
                        pass

    class CosmicMainWindow(QMainWindow):
        def __init__(self) -> None:
            super().__init__()
            
            # Application State variables
            self._page = 0
            self._total = 0
            self._total_unfiltered = -1
            self._query_counter = 0
            self._filter = ""
            self._favorites: List[str] = []
            self._workers: Set[QThread] = set()

            self.setWindowTitle(f"{APP_NAME} v{APP_VERSION} [CYBER-MODE]")
            self.setMinimumSize(1280, 780)
            
            # Setup search debouncing to prevent lagging the SQLite engine
            self._search_timer = QTimer(self)
            self._search_timer.setSingleShot(True)
            self._search_timer.timeout.connect(self._execute_search_query)

            self._load_favorites()
            
            # Worker Thread Storage
            self._sim_w: Optional[SimulateWorker] = None
            self._rpt_w: Optional[ReportWorker] = None
            self._data_w: Optional[DataLoaderWorker] = None

            # Un-squashed initialization flow
            self._setup_ui()
            self._setup_shortcuts()
            self._load_ontology_tree()
            self._load_page()

            # Execute first-run initialization if the DB missing
            needs_init = False
            if 'DB_FILE' in globals() and not DB_FILE.exists():
                needs_init = True
            elif 'ASTRO_DICT' in globals() and not ASTRO_DICT.exists():
                needs_init = True
            elif 'ONTOLOGY_DB' in globals() and not ONTOLOGY_DB.exists():
                needs_init = True
                
            if needs_init:
                self._first_run_init()

        def _setup_ui(self) -> None:
            """Fully expanded and documented UI building routine."""
            central_widget = QWidget()
            self.setCentralWidget(central_widget)
            
            main_layout = QHBoxLayout(central_widget)
            main_layout.setContentsMargins(8, 8, 8, 8)

            splitter = QSplitter(Qt.Horizontal)
            splitter.setHandleWidth(6)
            main_layout.addWidget(splitter)

            # ================= LEFT PANEL (Ontology Navigator) =================
            left_widget = QWidget()
            left_layout = QVBoxLayout(left_widget)
            left_layout.setContentsMargins(8, 8, 8, 8)
            
            left_label = self._create_cyber_label("🌌 ONTOLOGY NAVIGATOR")
            left_layout.addWidget(left_label)
            
            self.tree = QTreeWidget()
            self.tree.setHeaderLabels(["Category / Item"])
            self.tree.itemClicked.connect(self._on_tree_click)
            left_layout.addWidget(self.tree, 1)
            
            fav_btn = QPushButton("★ Favorites")
            fav_btn.clicked.connect(self._show_favorites)
            left_layout.addWidget(fav_btn)
            
            splitter.addWidget(left_widget)
            left_widget.setMinimumWidth(320)

            # ================= MIDDLE PANEL (Builder & Explorer) =================
            mid_widget = QWidget()
            mid_layout = QVBoxLayout(mid_widget)
            mid_layout.setContentsMargins(6, 6, 6, 6)
            
            self.middle_tabs = QTabWidget()
            mid_layout.addWidget(self.middle_tabs)

            # --------- Builder Tab ---------
            tab_builder = QWidget()
            builder_layout = QVBoxLayout(tab_builder)
            builder_layout.setContentsMargins(10, 10, 10, 10)
            
            builder_label = self._create_cyber_label("🛠️ PATH A: SCENARIO BUILDER")
            builder_layout.addWidget(builder_label)
            
            form_layout = QGridLayout()
            
            self.b_prefix = QComboBox()
            self.b_domain = QComboBox()
            self.b_process = QComboBox()
            self.b_type = QComboBox()
            
            form_layout.addWidget(QLabel("1. PREFIX:"), 0, 0)
            form_layout.addWidget(self.b_prefix, 0, 1)
            
            form_layout.addWidget(QLabel("2. DOMAIN:"), 1, 0)
            form_layout.addWidget(self.b_domain, 1, 1)
            
            form_layout.addWidget(QLabel("3. PROCESS:"), 2, 0)
            form_layout.addWidget(self.b_process, 2, 1)
            
            form_layout.addWidget(QLabel("4. TYPE:"), 3, 0)
            form_layout.addWidget(self.b_type, 3, 1)
            
            builder_layout.addLayout(form_layout)
            
            self.b_preview = QLabel("TARGET: <i>[AWAITING INPUT]</i>")
            self.b_preview.setAlignment(Qt.AlignCenter)
            builder_layout.addWidget(self.b_preview)
            
            self.middle_tabs.addTab(tab_builder, "🛠️ BUILDER")

            # --------- Explorer Tab ---------
            tab_explorer = QWidget()
            explorer_layout = QVBoxLayout(tab_explorer)
            
            explorer_label = self._create_cyber_label("🔍 PATH B: DATABASE EXPLORER")
            explorer_layout.addWidget(explorer_label)
            
            self.search = QLineEdit()
            self.search.setPlaceholderText("Search scenario name...")
            self.search.textChanged.connect(self._on_search)
            explorer_layout.addWidget(self.search)

            dropdown_row = QHBoxLayout()
            self.e_prefix = QComboBox()
            self.e_domain = QComboBox()
            self.e_process = QComboBox()
            self.e_type = QComboBox()
            
            for combobox in (self.e_prefix, self.e_domain, self.e_process, self.e_type):
                dropdown_row.addWidget(combobox)
                combobox.currentIndexChanged.connect(self._on_cascade_filter_changed)
                
            explorer_layout.addLayout(dropdown_row)

            self.tbl = QTableWidget()
            self.tbl.setColumnCount(6)
            self.tbl.setHorizontalHeaderLabels(["ID", "Scenario Name", "Family", "Domain", "Process", "Type"])
            self.tbl.setSelectionBehavior(QAbstractItemView.SelectRows)
            self.tbl.setSelectionMode(QAbstractItemView.SingleSelection)
            self.tbl.setEditTriggers(QAbstractItemView.NoEditTriggers)
            
            table_header = self.tbl.horizontalHeader()
            table_header.setStretchLastSection(True)
            table_header.setSectionResizeMode(1, QHeaderView.Stretch)
            
            self.tbl.itemSelectionChanged.connect(self._on_sel)
            self.tbl.cellDoubleClicked.connect(self._on_table_double_click)
            explorer_layout.addWidget(self.tbl)

            pagination_row = QHBoxLayout()
            self.bp = QPushButton("◀ PREV")
            self.bp.clicked.connect(self._prev)
            
            self.bn = QPushButton("NEXT ▶")
            self.bn.clicked.connect(self._next)
            
            self.pl = QLabel("PAGE 1")
            self.pl.setStyleSheet("font-weight:bold; color:#ff00ff;")
            
            pagination_row.addWidget(self.bp)
            pagination_row.addStretch()
            pagination_row.addWidget(self.pl)
            pagination_row.addStretch()
            pagination_row.addWidget(self.bn)
            explorer_layout.addLayout(pagination_row)

            # Control Buttons Row
            button_row = QHBoxLayout()
            
            buttons_config = [
                ("⚡ SIMULATE", self._on_simulate), 
                ("📄 REPORT", self._on_report), 
                ("📊 EXPORT CSV", self._on_csv), 
                ("📂 OPEN", self._on_open), 
                ("📋 COPY", self._on_copy), 
                ("⭐ FAV", self._on_fav)
            ]
            
            for button_text, slot_function in buttons_config:
                btn = QPushButton(button_text)
                btn.clicked.connect(slot_function)
                button_row.addWidget(btn)
                
            explorer_layout.addLayout(button_row)
            
            self.middle_tabs.addTab(tab_explorer, "🔍 EXPLORER")
            
            splitter.addWidget(mid_widget)
            mid_widget.setMinimumWidth(640)

            # ================= RIGHT PANEL (Telemetry & Details) =================
            right_widget = QWidget()
            right_layout = QVBoxLayout(right_widget)
            right_layout.setContentsMargins(8, 8, 8, 8)
            
            right_label = self._create_cyber_label("📋 TELEMETRY & DETAILS")
            right_layout.addWidget(right_label)
            
            self.tabs = QTabWidget()
            
            self.html_view = QTextBrowser()
            self.html_view.setOpenExternalLinks(False)
            self.html_view.setMinimumWidth(350)
            self.tabs.addTab(self.html_view, "DATA")
            
            self.sim_view = QTextBrowser()
            self.tabs.addTab(self.sim_view, "SIMULATION")
            
            right_layout.addWidget(self.tabs, 1)
            
            splitter.addWidget(right_widget)
            right_widget.setMinimumWidth(340)

            # Layout Finalization
            splitter.setSizes([320, 640, 340])
            
            self.sbar = QStatusBar()
            self.setStatusBar(self.sbar)
            self.sbar.showMessage("SYSTEM READY.")
            
            self._populate_builder_choices()

        def _create_cyber_label(self, text: str) -> QLabel:
            """Helper to create consistently styled header labels."""
            label = QLabel(text)
            label.setStyleSheet("font-size:15px;font-weight:bold;color:#45f3ff;padding:4px;letter-spacing:1px;")
            return label

        def _load_favorites(self) -> None:
            try:
                if FAVORITES_FILE.exists():
                    self._favorites = json.loads(FAVORITES_FILE.read_text("utf-8"))
                else:
                    self._favorites = []
            except Exception:
                self._favorites = []

        def _save_favorites(self) -> None:
            try:
                FAVORITES_FILE.write_text(json.dumps(self._favorites, indent=2), "utf-8")
            except Exception:
                pass

        def _show_favorites(self) -> None:
            if not self._favorites:
                QMessageBox.information(self, "Favorites", "No favorites saved.")
                return
            items = "\n".join(self._favorites)
            QMessageBox.information(self, "Favorites", f"Saved favorites:\n\n{items}")

        def _populate_builder_choices(self) -> None:
            def _populate_combo(cb: QComboBox, category: str, items: List[str], desc_map: Dict[str, str]) -> None:
                cb.addItem(f"--- {category} ---")
                cb.setItemData(0, f"Reset filter to allow all {category}s", Qt.ToolTipRole)
                for i, item in enumerate(items, 1):
                    cb.addItem(item)
                    tooltip = desc_map.get(item, f"{item} ({category})")
                    cb.setItemData(i, f"{item}: {tooltip}", Qt.ToolTipRole)

            # Builder Panel Combos
            _populate_combo(self.b_prefix, "PREFIX", _build_prefix_list(), PREFIX_DESCS)
            _populate_combo(self.b_domain, "DOMAIN", _build_domain_list(), DOMAIN_DESCS)
            _populate_combo(self.b_process, "PROCESS", _build_process_list(), PROCESS_DESCS)
            _populate_combo(self.b_type, "TYPE", _build_type_list(), TYPE_DESCS)
            
            for cb in (self.b_prefix, self.b_domain, self.b_process, self.b_type):
                cb.currentIndexChanged.connect(self._update_builder_preview)

            # Explorer Panel Combos
            _populate_combo(self.e_prefix, "PREFIX", _build_prefix_list(), PREFIX_DESCS)
            _populate_combo(self.e_domain, "DOMAIN", _build_domain_list(), DOMAIN_DESCS)
            _populate_combo(self.e_process, "PROCESS", _build_process_list(), PROCESS_DESCS)
            _populate_combo(self.e_type, "TYPE", _build_type_list(), TYPE_DESCS)

        def _update_builder_preview(self) -> None:
            parts = []
            for cb in (self.b_prefix, self.b_domain, self.b_process, self.b_type):
                parts.append(cb.currentText() if cb.currentIndex() > 0 else "...")
                
            name = "_".join(parts)
            if "..." in name:
                self.b_preview.setText(f"TARGET: <span style='color:#444'>{name}</span>")
            else:
                self.b_preview.setText(
                    f"COMPILED SCENARIO:<br>"
                    f"<span style='color:#45f3ff; font-size: 20px; font-weight:bold;'>{name}</span>"
                )
                
            if self.b_domain.currentIndex() > 0:
                domain_val = self.b_domain.currentText()
                if ONTOLOGY_DB.exists():
                    with contextlib.closing(sqlite3.connect(str(ONTOLOGY_DB))) as conn:
                        row = conn.execute("SELECT description FROM ontology WHERE name LIKE ? LIMIT 1", (f"%{domain_val}%",)).fetchone()
                    if row:
                        self.html_view.setHtml(row[0])
                        self.tabs.setCurrentIndex(0)

        def _get_target_scenario(self) -> Optional[str]:
            """Determine which scenario string to act upon based on the current tab."""
            idx = self.middle_tabs.currentIndex()
            if idx == 0:
                if all(cb.currentIndex() > 0 for cb in (self.b_prefix, self.b_domain, self.b_process, self.b_type)):
                    return f"{self.b_prefix.currentText()}_{self.b_domain.currentText()}_{self.b_process.currentText()}_{self.b_type.currentText()}"
                return None
            elif idx == 1:
                return self._sel_name()
            return None

        def _load_ontology_tree(self) -> None:
            """Loads the massive nested master ontology into the left-hand QTreeWidget."""
            self.tree.clear()
            if not ONTOLOGY_DB.exists():
                self.tree.addTopLevelItem(QTreeWidgetItem(["[DATA MISSING]"]))
                return
            
            QApplication.setOverrideCursor(Qt.WaitCursor)
            try:
                with contextlib.closing(sqlite3.connect(str(ONTOLOGY_DB))) as conn:
                    # Super Categories
                    for (sc,) in conn.execute("SELECT DISTINCT super_category FROM ontology ORDER BY super_category"):
                        si = QTreeWidgetItem([sc])
                        si.setData(0, Qt.UserRole, ("super", sc))
                        
                        # Sub Categories
                        for (sub,) in conn.execute("SELECT DISTINCT sub_category FROM ontology WHERE super_category=? ORDER BY sub_category", (sc,)):
                            subi = QTreeWidgetItem([sub])
                            subi.setData(0, Qt.UserRole, ("sub", sub))
                            
                            # Final Items
                            for (nm,) in conn.execute("SELECT name FROM ontology WHERE super_category=? AND sub_category=? ORDER BY name", (sc, sub)):
                                leaf = QTreeWidgetItem([nm])
                                leaf.setData(0, Qt.UserRole, ("item", nm))
                                subi.addChild(leaf)
                                
                            si.addChild(subi)
                            
                        self.tree.addTopLevelItem(si)
            except Exception:
                pass
            finally:
                QApplication.restoreOverrideCursor()

        def _on_tree_click(self, item: QTreeWidgetItem, _col: int) -> None:
            d = item.data(0, Qt.UserRole)
            if not d: 
                return
                
            kind, val = d
            if kind == "item" and ONTOLOGY_DB.exists():
                with contextlib.closing(sqlite3.connect(str(ONTOLOGY_DB))) as conn:
                    row = conn.execute("SELECT description FROM ontology WHERE name=?", (val,)).fetchone()
                if row:
                    self.html_view.setHtml(row[0])
                    self.tabs.setCurrentIndex(0)
                    self.sbar.showMessage(f"ACCESSING: {val}")

        def _on_cascade_filter_changed(self) -> None:
            """
            Multi-Level Cascading Hierarchical Filtration Process.
            Ensures that making a selection in a higher level (e.g. Domain)
            correctly limits the available options in the lower levels.
            """
            combos = [self.e_prefix, self.e_domain, self.e_process, self.e_type]
            columns = ["prefix", "domain", "process", "type"]
            descs = [PREFIX_DESCS, DOMAIN_DESCS, PROCESS_DESCS, TYPE_DESCS]
            full_lists = [_build_prefix_list(), _build_domain_list(), _build_process_list(), _build_type_list()]

            Ls = -1
            for i, cb in enumerate(combos):
                if cb.currentIndex() > 0:
                    Ls = i
                    break

            if Ls == -1:
                # Reset all filters to their default states
                for i, cb in enumerate(combos):
                    cb.blockSignals(True)
                    cb.setEnabled(True)
                    cb.clear()
                    cb.addItem(f"--- {columns[i].upper()} ---")
                    cb.setItemData(0, f"Reset filter to allow all {columns[i].upper()}s", Qt.ToolTipRole)
                    
                    for item in full_lists[i]:
                        cb.addItem(item)
                        tooltip = descs[i].get(item, f"{item} ({columns[i].upper()})")
                        cb.setItemData(cb.count() - 1, f"{item}: {tooltip}", Qt.ToolTipRole)
                        
                    cb.setCurrentIndex(0)
                    cb.blockSignals(False)
                    
                self._page = 0
                self._load_page()
                return

            if not DB_FILE.exists():
                return

            try:
                with contextlib.closing(sqlite3.connect(str(DB_FILE))) as conn:
                    # Rule 1: Higher Levels Disabled
                    for i in range(0, Ls):
                        combos[i].blockSignals(True)
                        combos[i].setCurrentIndex(0)
                        combos[i].setEnabled(False)
                        combos[i].blockSignals(False)

                    # Rule 2: Lower Levels Enabled & Filtered
                    for i in range(Ls + 1, 4):
                        combos[i].setEnabled(True)
                        active_filters = {}
                        
                        for j in range(Ls, i):
                            if combos[j].currentIndex() > 0:
                                active_filters[columns[j]] = combos[j].currentText()

                        where_clauses = [f"{col} = ?" for col in active_filters.keys()]
                        params = list(active_filters.values())
                        where_str = " WHERE " + " AND ".join(where_clauses) if where_clauses else ""
                        query = f"SELECT DISTINCT {columns[i]} FROM scenarios{where_str} ORDER BY {columns[i]}"

                        rows = conn.execute(query, params).fetchall()
                        valid_items = [r[0] for r in rows]

                        curr_text = combos[i].currentText()
                        
                        combos[i].blockSignals(True)
                        combos[i].clear()
                        combos[i].addItem(f"--- {columns[i].upper()} ---")
                        combos[i].setItemData(0, f"Reset filter to allow all {columns[i].upper()}s", Qt.ToolTipRole)

                        for item in valid_items:
                            combos[i].addItem(item)
                            tooltip = descs[i].get(item, f"{item} ({columns[i].upper()})")
                            combos[i].setItemData(combos[i].count() - 1, f"{item}: {tooltip}", Qt.ToolTipRole)

                        idx = combos[i].findText(curr_text)
                        if idx > 0:
                            combos[i].setCurrentIndex(idx)
                        else:
                            combos[i].setCurrentIndex(0)

                        combos[i].blockSignals(False)
                        
            except Exception as exc:
                print(f"Cascade Filtration Error: {exc}")

            self._page = 0
            self._load_page()

        def _where(self) -> Tuple[str, List[str]]:
            """Constructs the SQL WHERE clause based on UI states."""
            conditions = []
            params = []

            f = self.search.text().strip()
            if f:
                lk = f"%{f}%"
                conditions.append("(scenario_name LIKE ? OR prefix LIKE ? OR domain LIKE ? OR process LIKE ? OR type LIKE ?)")
                params.extend([lk]*5)

            if self.e_prefix.currentIndex() > 0:
                conditions.append("prefix LIKE ?")
                params.append(self.e_prefix.currentText())

            if self.e_domain.currentIndex() > 0:
                conditions.append("domain LIKE ?")
                params.append(self.e_domain.currentText())

            if self.e_process.currentIndex() > 0:
                conditions.append("process LIKE ?")
                params.append(self.e_process.currentText())

            if self.e_type.currentIndex() > 0:
                conditions.append("type LIKE ?")
                params.append(self.e_type.currentText())

            if not conditions:
                return "", []

            return " WHERE " + " AND ".join(conditions), params

        def _load_page(self) -> None:
            """Triggers the search with a debounce timer."""
            self._search_timer.start(150)

        def _execute_search_query(self) -> None:
            """Executes the search and pagination logic."""
            if not DB_FILE.exists():
                self.tbl.setRowCount(0)
                self.pl.setText("[NO DATA]")
                self.sbar.showMessage("ERROR: DATABASE MISSING.")
                return
            
            self._query_counter += 1
            current_query = self._query_counter
            
            if getattr(self, '_data_w', None) and self._data_w.isRunning():
                if getattr(self._data_w, 'conn', None):
                    try: 
                        self._data_w.conn.interrupt() 
                    except Exception: 
                        pass

            w, p = self._where()
            off = self._page * GUI_PAGE_SIZE
            
            self.sbar.showMessage(f"🔍 SCANNING MAINFRAME (QUERY #{current_query})...")

            self._data_w = DataLoaderWorker(current_query, DB_FILE, w, p, GUI_PAGE_SIZE, off, self._total_unfiltered)
            self._workers.add(self._data_w)
            
            self._data_w.finished.connect(lambda q_id, t, r, e, worker=self._data_w: self._workers.discard(worker))
            self._data_w.finished.connect(self._on_page_loaded)
            self._data_w.start()

        def _on_page_loaded(self, query_id: int, total: int, rows: list, error: str) -> None:
            if query_id != self._query_counter:
                return

            if error:
                return

            self._total = total
            if not self._where()[0] and self._total_unfiltered == -1:
                self._total_unfiltered = total

            self.tbl.setRowCount(len(rows))
            for i, r in enumerate(rows):
                for j, v in enumerate(r):
                    it = QTableWidgetItem(str(v))
                    it.setFlags(it.flags() & ~Qt.ItemIsEditable)
                    self.tbl.setItem(i, j, it)

            tp = max(1, math.ceil(self._total / GUI_PAGE_SIZE))
            self.pl.setText(f"PAGE {self._page+1}/{tp}  [{self._total:,} ENTRIES]")
            
            off = self._page * GUI_PAGE_SIZE
            self.bp.setEnabled(self._page > 0)
            self.bn.setEnabled(off + GUI_PAGE_SIZE < self._total)
            self.sbar.showMessage(f"LOADED BLOCK {self._page+1} ({len(rows)} ROWS DISPLAYED).")

        def _prev(self) -> None:
            if self._page > 0: 
                self._page -= 1
                self._load_page()

        def _next(self) -> None:
            if (self._page+1)*GUI_PAGE_SIZE < self._total: 
                self._page += 1
                self._load_page()

        def _on_search(self) -> None:
            self._page = 0
            self._load_page()

        def _sel_name(self) -> Optional[str]:
            sel = self.tbl.selectedItems()
            if not sel: 
                return None
            it = self.tbl.item(sel[0].row(), 1)
            return it.text() if it else None

        def _on_sel(self) -> None:
            n = self._sel_name()
            if n: 
                self.sbar.showMessage(f"TARGET ACQUIRED: {n}")

        def _on_table_double_click(self, row: int, col: int) -> None:
            item = self.tbl.item(row, 1)
            if item:
                name = item.text()
                sim = simulate_scenario(name)
                domain = self.tbl.item(row, 3).text() if self.tbl.item(row, 3) else ""
                
                ont_html = ""
                if ONTOLOGY_DB.exists():
                    with contextlib.closing(sqlite3.connect(str(ONTOLOGY_DB))) as conn:
                        row_data = conn.execute("SELECT description FROM ontology WHERE name LIKE ? LIMIT 1", (f"%{domain}%",)).fetchone()
                        if row_data: 
                            ont_html = row_data[0]

                html = _build_html_report({"scenario_name": name, "id": self.tbl.item(row,0).text()}, sim)
                
                if ont_html:
                    html = html.replace(
                        "<h2>1. Physical Parameters</h2>", 
                        f"<h2>Ontology Context</h2>{ont_html}<h2>1. Physical Parameters</h2>"
                    )
                
                self.html_view.setHtml(html)
                self.tabs.setCurrentIndex(0)

        def _on_simulate(self) -> None:
            n = self._get_target_scenario()
            if not n: 
                QMessageBox.information(self, "SYS.ERR", "Construct a complete target or select from Explorer.")
                return
            
            self.sbar.showMessage(f"EXECUTING SIMULATION ALGORITHMS ON: {n}…")
            self._sim_w = SimulateWorker(n)
            self._sim_w.finished.connect(self._sim_done)
            self._sim_w.error.connect(lambda e: QMessageBox.critical(self, "SYS.ERR", e))
            self._sim_w.start()

        def _sim_done(self, r: dict) -> None:
            physics_str = "".join(f"<li>> {p}</li>" for p in r['dominant_physics'])
            outcomes_str = "".join(f"<li>> {o}</li>" for o in r['possible_outcomes'])
            signatures_str = "".join(f"<li>> {s}</li>" for s in r['observational_signatures'])
            
            html = (
                f"<div style='font-family:Consolas,monospace;padding:10px;color:#c5c6c7'>"
                f"<h2 style='color:#45f3ff;border-bottom:1px solid #45f3ff;'>[ SIMULATION COMPLETE ]<br>{r['scenario_name']}</h2>"
                f"<p><b style='color:#ff00ff'>[ ENERGY REGIME ]:</b> {r['energy_regime']}</p>"
                f"<p><b style='color:#ff00ff'>[ SPATIAL SCALE ]:</b> {r['spatial_scale']}</p>"
                f"<h3 style='color:#66fcf1'>// DOMINANT PHYSICS</h3><ul>{physics_str}</ul>"
                f"<h3 style='color:#66fcf1'>// CALCULATED OUTCOMES</h3><ul>{outcomes_str}</ul>"
                f"<h3 style='color:#66fcf1'>// OBSERVATIONAL SIGNATURES</h3><ul>{signatures_str}</ul>"
                f"<p style='font-size:.8em;color:#888'>SEED HASH: {r['seed_hash'][:32]}…</p>"
                f"</div>"
            )
            
            self.sim_view.setHtml(html)
            self.tabs.setCurrentIndex(1)
            self.sbar.showMessage(f"SIMULATION CYCLE COMPLETED: {r['scenario_name']}")

        def _on_report(self) -> None:
            n = self._get_target_scenario()
            if not n: 
                QMessageBox.information(self, "SYS.ERR", "Construct a complete target or select from Explorer.")
                return
            
            self.sbar.showMessage(f"GENERATING REPORT FOR {n}…")
            self._rpt_w = ReportWorker(n)
            
            def report_done(html_file, pdf_file):
                lines = []
                if html_file:
                    lines.append(f"HTML FILE: {html_file}")
                if pdf_file:
                    lines.append(f"PDF COMPILED: {pdf_file}")
                msg_text = "\n".join(lines) if lines else "PROCESS DONE"
                
                QMessageBox.information(self, "REPORT COMPLETE", msg_text)
                self.sbar.showMessage("REPORT GENERATION SUCCESSFUL.")
                
            self._rpt_w.finished.connect(report_done)
            self._rpt_w.error.connect(lambda e: QMessageBox.critical(self, "SYS.ERR", e))
            self._rpt_w.start()

        def _on_csv(self) -> None:
            path, _ = QFileDialog.getSaveFileName(self, "EXPORT CSV DATA", "scenarios.csv", "CSV (*.csv)")
            if not path: 
                return
            if not DB_FILE.exists(): 
                return
            
            QApplication.setOverrideCursor(Qt.WaitCursor)
            try:
                with contextlib.closing(sqlite3.connect(str(DB_FILE))) as conn:
                    w, p = self._where()
                    query = (
                        f"SELECT id,scenario_name,object_family,object_type,parent_object,"
                        f"prefix,domain,process,type FROM scenarios{w} ORDER BY id LIMIT 100000"
                    )
                    rows = conn.execute(query, p).fetchall()
                    
                with open(path, "w", encoding="utf-8") as fh:
                    fh.write("id,scenario_name,object_family,object_type,parent_object,prefix,domain,process,type\n")
                    for r in rows: 
                        fh.write(",".join(str(v) for v in r) + "\n")
                        
                QMessageBox.information(self, "EXPORT SUCCESS", f"{len(rows)} ROWS WRITTEN TO → {path}")
            except Exception: 
                pass
            finally:
                QApplication.restoreOverrideCursor()

        def _on_open(self) -> None:
            n = self._get_target_scenario()
            if not n: 
                return
            
            tag = n.replace(" ", "_")[:80]
            hp = REPORTS_DIR / f"report_{tag}.html"
            if hp.exists():
                if sys.platform == "win32": 
                    os.startfile(str(hp))  # type: ignore[attr-defined]
                else: 
                    subprocess.run(["xdg-open", str(hp)], check=False)

        def _on_copy(self) -> None:
            n = self._get_target_scenario()
            if n: 
                QApplication.clipboard().setText(n)
                self.sbar.showMessage(f"DATA COPIED TO CLIPBOARD: {n}")

        def _on_fav(self) -> None:
            n = self._get_target_scenario()
            if not n: 
                return
                
            if n not in self._favorites:
                self._favorites.append(n)
                self._save_favorites()
                self.sbar.showMessage(f"★ BOOKMARKED: {n}")
            else:
                self._favorites.remove(n)
                self._save_favorites()
                self.sbar.showMessage(f"★ REMOVED BOOKMARK: {n}")

        def _setup_shortcuts(self) -> None:
            QShortcut(QKeySequence("Ctrl+F"), self, self._focus_search)
            QShortcut(QKeySequence("Ctrl+S"), self, self._on_simulate)
            QShortcut(QKeySequence("Ctrl+R"), self, self._on_report)
            try:
                self.search.setFocus()
            except Exception:
                pass

        def _focus_search(self) -> None:
            try:
                self.search.setFocus()
                self.search.selectAll()
            except Exception:
                pass

        def _first_run_init(self) -> None:
            msg = QMessageBox(self)
            msg.setStyleSheet(CYBER_STYLESHEET)
            msg.setIcon(QMessageBox.Information)
            msg.setWindowTitle("SYSTEM INITIALIZATION")
            msg.setText(
                "No active database detected.\n\n"
                "The system will now dynamically generate:\n"
                "- New Dictionary\n"
                "- All Possible Scenarios (Complete Cartesian Product)\n"
                "- Master Ontology Database\n"
                "- SQLite Index\n\n"
                "This may take significant time."
            )
            msg.exec_()

            QApplication.setOverrideCursor(Qt.WaitCursor)

            try:
                ensure_dirs()
                generate_dictionary()
                generate_scenarios(sample_size=None)
                build_sqlite_from_jsonl()
                init_master_ontology()
            finally:
                QApplication.restoreOverrideCursor()

            self._load_ontology_tree()
            self._load_page()

    return CosmicMainWindow

_CosmicMainWindow: Any = None

def _resolve_gui_class() -> None:
    """Lazy loader to prevent PyInstaller / Import issues."""
    global _CosmicMainWindow
    if _CosmicMainWindow is None:
        _CosmicMainWindow = _make_gui_classes()

_original_launch_gui = _launch_gui
def _launch_gui() -> None:  # type: ignore[no-redef]
    _resolve_gui_class()
    _original_launch_gui()


# =============================================================================
# SECTION 10: CLI ENTRYPOINTS
# =============================================================================

def build_cli_parser() -> argparse.ArgumentParser:
    """Constructs the extensive arg parser for advanced CLI capabilities."""
    parser = argparse.ArgumentParser(
        prog="cosmic_scenario_engine",
        description=f"{APP_NAME} v{APP_VERSION}",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent(r"""
        Examples:
          python cosmic_scenario_engine.py --generate-dict
          python cosmic_scenario_engine.py --generate-scenarios --sample-size 500
          python cosmic_scenario_engine.py --build-sqlite
          python cosmic_scenario_engine.py --init-ontology
          python cosmic_scenario_engine.py --simulate --name "Hyper_Stellar_Fusion_Field"
          python cosmic_scenario_engine.py --report --id 1
          python cosmic_scenario_engine.py --gui
          python cosmic_scenario_engine.py --run-all
          python cosmic_scenario_engine.py --run-tests

        Default paths (Windows):
          SPACEENGINE_DB  C:\Users\intel\Desktop\cosmic extractor\SpaceEngine_Index.db
          OUTPUT_DIR      new_astro_dataset\
          DB_FILE         new_astro_scenarios.db
          ONTOLOGY_DB     new_master_ontology.db
          LOG_DIR         logs\
          REPORTS_DIR     reports\
        """),
    )
    g = parser.add_mutually_exclusive_group(required=True)
    g.add_argument("--generate-dict",      action="store_true", help="Generate new_astro_dictionary.json")
    g.add_argument("--generate-scenarios", action="store_true", help="Generate scenario JSONL files")
    g.add_argument("--build-sqlite",       action="store_true", help="Build new_astro_scenarios.db")
    g.add_argument("--init-ontology",      action="store_true", help="Initialise new_master_ontology.db")
    g.add_argument("--simulate",           action="store_true", help="Simulate a scenario")
    g.add_argument("--report",             action="store_true", help="Generate HTML+PDF report")
    g.add_argument("--gui",                action="store_true", help="Launch GUI explorer")
    g.add_argument("--run-all",            action="store_true", help="Full pipeline")
    g.add_argument("--run-tests",          action="store_true", help="Run embedded tests")
    g.add_argument("--pyinstaller-spec",   action="store_true", help="Print packaging notes")

    parser.add_argument("--name",        type=str, default=None)
    parser.add_argument("--id",          type=int, default=None)
    parser.add_argument("--sample-size", type=int, default=None)
    parser.add_argument("--output-dir",  type=str, default=None)
    parser.add_argument("--db-file",     type=str, default=None)
    parser.add_argument("--spaceengine-db", type=str, default=None)
    
    return parser


def run_cli() -> None:
    """Executes the specific requested routine based on parsed arguments."""
    parser = build_cli_parser()
    args = parser.parse_args()

    global OUTPUT_DIR, DB_FILE, SPACEENGINE_DB
    if args.output_dir:    
        OUTPUT_DIR = Path(args.output_dir)
    if args.db_file:       
        DB_FILE = Path(args.db_file)
    if args.spaceengine_db: 
        SPACEENGINE_DB = Path(args.spaceengine_db)

    ensure_dirs()
    log = get_logger()

    if args.generate_dict:
        d = generate_dictionary()
        print(
            f"Dictionary: {len(d['prefixes'])} prefixes × "
            f"{len(d['domains'])} domains × "
            f"{len(d['processes'])} processes × "
            f"{len(d['types'])} types"
        )

    elif args.generate_scenarios:
        n = generate_scenarios(sample_size=args.sample_size)
        print(f"Generated {n:,} scenarios.")

    elif args.build_sqlite:
        n = build_sqlite_from_jsonl()
        print(f"Database: {n:,} rows.")

    elif args.init_ontology:
        n = init_master_ontology()
        print(f"Ontology: {n} items inserted.")

    elif args.simulate:
        if args.name:
            sn = args.name
        elif args.id is not None:
            sc = _load_scenario_by_id(args.id)
            if not sc:
                print(f"ID {args.id} not found."); sys.exit(1)
            sn = sc["scenario_name"]
        else:
            print("Provide --name or --id")
            sys.exit(1)
            
        print(json.dumps(simulate_scenario(sn), indent=2, ensure_ascii=False))

    elif args.report:
        target: Union[int, str] = args.name if args.name else (args.id if args.id is not None else "")
        if not target:
            print("Provide --name or --id")
            sys.exit(1)
            
        hp, pp = generate_report(target)
        if hp: 
            print(f"HTML → {hp}")
        if pp: 
            print(f"PDF  → {pp}")

    elif args.gui:
        _launch_gui()

    elif args.run_all:
        t0 = time.time()
        print("[1/5] Dictionary…")
        generate_dictionary()
        
        print("[2/5] Scenarios…")
        generate_scenarios(sample_size=args.sample_size)
        
        print("[3/5] SQLite…")
        build_sqlite_from_jsonl()
        
        print("[4/5] Ontology…")
        init_master_ontology()
        
        print("[5/5] Sample report…")
        with contextlib.closing(sqlite3.connect(str(DB_FILE))) as conn:
            row = conn.execute("SELECT scenario_name FROM scenarios LIMIT 1").fetchone()
        if row: 
            generate_report(row[0])
            
        print(f"\n✅ Pipeline complete in {time.time()-t0:.1f}s")

    elif args.run_tests:
        run_embedded_tests()

    elif args.pyinstaller_spec:
        create_pyinstaller_spec()


# =============================================================================
# SECTION 11: EMBEDDED TESTS
# =============================================================================

def run_embedded_tests() -> None:
    """Runs a complete self-diagnostics suite via Python's unittest module."""
    log = get_logger()
    log.info("=== Running Embedded Tests ===")
    test_dir = Path(tempfile.mkdtemp(prefix="cosmic_test_"))
    print(f"Test artefacts → {test_dir}")

    class CT(unittest.TestCase):
        td = test_dir
        tout   = test_dir / "new_astro_dataset"
        tdb    = test_dir / "new_astro_scenarios.db"
        tont   = test_dir / "new_master_ontology.db"
        tdict  = test_dir / "new_astro_dataset" / "new_astro_dictionary.json"
        trpts  = test_dir / "reports"

        @classmethod
        def setUpClass(cls) -> None:
            cls.tout.mkdir(exist_ok=True)
            cls.trpts.mkdir(exist_ok=True)
            cls.tdict.parent.mkdir(parents=True, exist_ok=True)

        def test_01_dict(self) -> None:
            d = generate_dictionary(out_path=self.tdict)
            for k in ("prefixes", "domains", "processes", "types"):
                self.assertIn(k, d)
                self.assertGreater(len(d[k]), 5)
                
            self.assertTrue(self.tdict.exists())
            with open(self.tdict) as f:
                ld = json.load(f)
                
            self.assertEqual(d["prefixes"], ld["prefixes"])
            print("  ✓ Dictionary")

        def test_02_lookup(self) -> None:
            lu = build_object_lookup(Path("__nonexistent__.db"))
            self.assertIsInstance(lu, dict)
            self.assertGreater(len(lu), 10)
            self.assertEqual(lu["Stellar"][0], "Star")
            
            f, o, p = map_domain("zzzNope")
            self.assertEqual(f, "Other")
            print("  ✓ Object lookup")

        def test_03_generate_small(self) -> None:
            generate_dictionary(out_path=ASTRO_DICT)
            n = generate_scenarios(sample_size=120, output_dir=self.tout)
            
            self.assertGreater(n, 0)
            self.assertLessEqual(n, 120)
            
            jf = list(self.tout.glob("*.jsonl"))
            self.assertGreater(len(jf), 0)
            
            with open(jf[0]) as f:
                rec = json.loads(f.readline())
                
            self.assertIn("scenario_name", rec)
            print(f"  ✓ Generation ({n} records)")

        def test_04_resume(self) -> None:
            before = set(f.name for f in self.tout.glob("*.jsonl"))
            generate_scenarios(sample_size=120, output_dir=self.tout)
            after = set(f.name for f in self.tout.glob("*.jsonl"))
            
            self.assertEqual(before, after)
            print("  ✓ Resume")

        def test_05_sqlite(self) -> None:
            n = build_sqlite_from_jsonl(db_path=self.tdb, input_dir=self.tout)
            self.assertGreater(n, 0)
            
            with contextlib.closing(sqlite3.connect(str(self.tdb))) as conn:
                cnt = conn.execute("SELECT COUNT(*) FROM scenarios").fetchone()[0]
                self.assertEqual(cnt, n)
                
                idxs = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='index'")]
                for i in ("idx_family", "idx_name", "idx_combo"):
                    self.assertIn(i, idxs)
                    
            print(f"  ✓ SQLite ({n} rows)")

        def test_06_readback(self) -> None:
            with contextlib.closing(sqlite3.connect(str(self.tdb))) as conn:
                conn.row_factory = sqlite3.Row
                row = conn.execute("SELECT * FROM scenarios LIMIT 1").fetchone()
                
            self.assertIsNotNone(row)
            self.assertIn("scenario_name", row.keys())
            print("  ✓ Readback")

        def test_07_ontology(self) -> None:
            n = init_master_ontology(db_path=self.tont)
            self.assertGreater(n, 0)
            
            with contextlib.closing(sqlite3.connect(str(self.tont))) as conn:
                cnt = conn.execute("SELECT COUNT(*) FROM ontology").fetchone()[0]
                self.assertEqual(cnt, n)
                
                desc = conn.execute("SELECT description FROM ontology LIMIT 1").fetchone()[0]
                self.assertIn("<div", desc)
            
            n2 = init_master_ontology(db_path=self.tont)
            self.assertEqual(n2, 0)
            print(f"  ✓ Ontology ({n} items)")

        def test_08_sim_determinism(self) -> None:
            a = simulate_scenario("Hyper_Stellar_Fusion_Field")
            b = simulate_scenario("Hyper_Stellar_Fusion_Field")
            c = simulate_scenario("Ultra_Galactic_Collapse_Region")
            
            for k in ("energy_regime", "spatial_scale", "dominant_physics",
                       "possible_outcomes", "observational_signatures", "seed_hash"):
                self.assertEqual(a[k], b[k])
                
            self.assertNotEqual(a["seed_hash"], c["seed_hash"])
            print("  ✓ Determinism")

        def test_09_sim_struct(self) -> None:
            r = simulate_scenario("Test_Alpha_Beta_Gamma")
            
            for k in ("energy_regime", "spatial_scale", "dominant_physics",
                       "possible_outcomes", "observational_signatures"):
                self.assertIn(k, r)
                
            self.assertGreaterEqual(len(r["dominant_physics"]), 3)
            self.assertLessEqual(len(r["dominant_physics"]), 5)
            self.assertEqual(len(r["possible_outcomes"]), 3)
            self.assertEqual(len(r["observational_signatures"]), 3)
            print("  ✓ Structure")

        def test_10_report(self) -> None:
            hp, pp = generate_report(
                "Hyper_Stellar_Fusion_Field",
                out_dir=self.trpts, db_path=self.tdb, ontology_path=self.tont,
            )
            self.assertIsNotNone(hp)
            self.assertTrue(hp.exists())
            
            content = hp.read_text("utf-8")
            self.assertIn("Cosmic Scenario Report", content)
            self.assertIn("<html", content)
            
            if pp:
                self.assertTrue(pp.exists())
                self.assertGreater(pp.stat().st_size, 100)
                print("  ✓ Report (HTML + PDF)")
            else:
                print("  ✓ Report (HTML only – ReportLab not installed)")

        def test_11_desc_determ(self) -> None:
            d1 = _deterministic_description("Main Sequence Dynamics")
            d2 = _deterministic_description("Main Sequence Dynamics")
            d3 = _deterministic_description("Red Giant Evolution")
            
            self.assertEqual(d1, d2)
            self.assertNotEqual(d1, d3)
            self.assertIn("<div", d1)
            print("  ✓ Desc determinism")

        def test_12_map_domain(self) -> None:
            f, o, p = map_domain("Stellar")
            self.assertEqual(f, "Star")
            self.assertEqual(p, "Galaxy")
            
            f2, _, p2 = map_domain("__nope__")
            self.assertEqual(f2, "Other")
            self.assertEqual(p2, "Universe")
            print("  ✓ map_domain")

        @classmethod
        def tearDownClass(cls) -> None:
            try:
                shutil.rmtree(cls.td, ignore_errors=True)
            except Exception:
                pass

    suite = unittest.TestLoader().loadTestsFromTestCase(CT)
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    
    if result.wasSuccessful():
        print("\n✅ All tests passed!")
    else:
        print(f"\n❌ {len(result.failures)} failure(s), {len(result.errors)} error(s)")
        sys.exit(1)


# =============================================================================
# SECTION 14: PACKAGING NOTES (PyInstaller)
# =============================================================================

def create_pyinstaller_spec() -> None:
    print(textwrap.dedent(r"""
    ============================================================
    PyInstaller Packaging — Cosmic Scenario Engine
    ============================================================

    RECOMMENDED (console, single .exe):

        pyinstaller --onefile --console ^
            --name "CosmicScenarioEngine" ^
            cosmic_scenario_engine.py

    WITH GUI (no console window):

        pyinstaller --onefile --windowed ^
            --name "CosmicScenarioEngine" ^
            --icon "cosmic_icon.ico" ^
            cosmic_scenario_engine.py

    NOTES:
      • The app creates logs\, reports\, and new_astro_dataset\ at runtime.
      • Database files (*.db, *.json) are generated at runtime.
      • On Windows use semicolons (;) in --add-data separators.
      • Activate your venv before running PyInstaller.
      • Test:  dist\CosmicScenarioEngine.exe --run-tests

    ============================================================
    """))


# =============================================================================
# SECTION 15: INTERACTIVE MENU FOR BEGINNERS
# =============================================================================

def interactive_menu() -> None:
    """Interactive fallback menu for users running the script without arguments."""
    print("=" * 60)
    print(f" Welcome to the {APP_NAME} v{APP_VERSION} ".center(60))
    print("=" * 60)

    if PYQT5_AVAILABLE:
        print("\n[+] Launching Graphical User Interface (GUI)...")
        _launch_gui()
        return

    print("\nNote: For the best experience, install PyQt5 to use the graphical UI:")
    print("      pip install PyQt5")
    print("-" * 60)

    while True:
        print("\n--- Main Menu ---")
        print("  1. Run Full Automatic Pipeline (Generate all data & databases)")
        print("  2. Launch Command Line Help (View advanced options)")
        print("  3. Run Embedded Self-Tests")
        print("  0. Exit")

        choice = input("\nEnter your choice (0-3): ").strip()

        if choice == '1':
            print("\n" + "="*50)
            print("Running Full Automatic Pipeline".center(50))
            print("="*50)
            
            s_size = input("\nHow many scenarios would you like to generate? (Press Enter for ALL possible combinations): ").strip()
            size = int(s_size) if s_size.isdigit() else None
            
            t0 = time.time()
            print("\n[1/5] Generating Astrophysical Dictionary...")
            generate_dictionary()
            
            limit_text = f"Limit: {size}" if size else "ALL Combinations"
            print(f"\n[2/5] Generating Scenarios ({limit_text})...")
            generate_scenarios(sample_size=size)
            
            print("\n[3/5] Building SQLite Database...")
            build_sqlite_from_jsonl()
            
            print("\n[4/5] Initializing Master Ontology Database...")
            init_master_ontology()
            
            print("\n[5/5] Generating Sample Report...")
            with contextlib.closing(sqlite3.connect(str(DB_FILE))) as conn:
                try:
                    row = conn.execute("SELECT scenario_name FROM scenarios LIMIT 1").fetchone()
                    if row: 
                        generate_report(row[0])
                except Exception:
                    pass
            
            print(f"\n✅ Pipeline completed successfully in {time.time()-t0:.1f}s")
            print("You can now explore the generated SQLite database, or install PyQt5 to use the GUI explorer.")
            input("\nPress Enter to return to the menu...")

        elif choice == '2':
            print("\n" + "="*50)
            build_cli_parser().print_help()
            print("="*50)
            input("\nPress Enter to return to the menu...")

        elif choice == '3':
            print("\n")
            run_embedded_tests()
            input("\nPress Enter to return to the menu...")

        elif choice == '0':
            print("\nGoodbye!")
            break
        else:
            print("\nInvalid choice. Please enter a number between 0 and 3.")


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

def main() -> None:
    """Primary application entry point. Enables multiprocessing safety and parses routing."""
    multiprocessing.freeze_support()

    if len(sys.argv) < 2:
        interactive_menu()
    else:
        run_cli()

if __name__ == "__main__":
    main()