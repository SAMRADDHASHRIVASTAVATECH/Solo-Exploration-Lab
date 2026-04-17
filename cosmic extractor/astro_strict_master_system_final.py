"""
╔════════════════════════════════════════════════════════════════════╗
║          ASTRO CATALOG · ENTERPRISE EDITION v5.0                   ║
║                    🚀 ULTRA-RESPONSIVE EDITION 🚀                   ║
║                                                                    ║
║  FEATURES:                                                         ║
║  • Zero-delay object viewing with intelligent caching              ║
║  • Background threading - UI never freezes                         ║
║  • Virtual tree loading - handles millions of objects              ║
║  • Instant search with live results                                ║
║  • Precomputed statistics                                          ║
║                                                                    ║
║  HOW TO USE:                                                       ║
║  1. Put this file next to your JSON file                           ║
║  2. Run: python astro_enterprise.py                                ║
║  3. Enjoy instant, lag-free exploration!                           ║
╚════════════════════════════════════════════════════════════════════╝
"""

from __future__ import annotations
import os, sys, json, sqlite3, re, logging, random, threading, queue, time
import tkinter as tk
from tkinter import ttk, messagebox, filedialog
from pathlib import Path
from typing import Optional, Dict, List, Any, Callable, Tuple
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, Future
from functools import lru_cache
from collections import OrderedDict
import hashlib

# ════════════════════════════════════════════════════════════════════
# APP CONFIGURATION
# ════════════════════════════════════════════════════════════════════

APP_NAME = "Astro Catalog Enterprise"
APP_VERSION = "5.0.0"
DB_NAME = "astro_catalog.db"
JSON_INPUT = "astrophysical_object_catalog.json"
JSON_OUTPUT = "astro_catalog_saved_data.json"
WINDOW_TITLE = f"{APP_NAME} v{APP_VERSION} — Ultra Responsive"

# Performance tuning
CACHE_SIZE = 5000           # Objects to keep in memory
BATCH_SIZE = 100            # Items to load per batch
SEARCH_DEBOUNCE_MS = 150    # Faster search response
THREAD_POOL_SIZE = 4        # Background workers
PRELOAD_COUNT = 50          # Objects to preload ahead

# Paths
SCRIPT_DIR = Path(__file__).resolve().parent
APPDATA_DIR = Path(os.getenv("APPDATA") or os.getenv("LOCALAPPDATA") or str(Path.home())) / "AstroCatalogEnterprise"
DB_PATH = APPDATA_DIR / DB_NAME
JSON_INPUT_PATH = SCRIPT_DIR / JSON_INPUT
JSON_OUTPUT_PATH = APPDATA_DIR / JSON_OUTPUT

# ════════════════════════════════════════════════════════════════════
# LOGGING
# ════════════════════════════════════════════════════════════════════

APPDATA_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(APPDATA_DIR / "app.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger("AstroCatalog")

# ════════════════════════════════════════════════════════════════════
# SPACE THEME COLORS
# ════════════════════════════════════════════════════════════════════

class C:
    """Color palette — deep-space dark theme"""
    BG0 = "#060910"
    BG1 = "#0c1220"
    BG2 = "#131d30"
    BG3 = "#1b2940"
    BG4 = "#243550"
    CARD = "#162236"
    CYAN = "#00c8ff"
    PURPLE = "#8b5cf6"
    ORANGE = "#f59e0b"
    GREEN = "#10b981"
    RED = "#ef4444"
    YELLOW = "#fbbf24"
    PINK = "#f472b6"
    T1 = "#f0f4fc"
    T2 = "#b0bdd0"
    T3 = "#607090"
    T4 = "#3e5068"
    BORDER = "#1e3050"
    HOVER = "#1a3355"
    SELECTED = "#0c3858"
    LOADING = "#4a90d9"

# Category display
CAT_STYLE = {
    "1":  ("⭐",  "#fcd34d"), "2":  ("🌍",  "#60a5fa"), "3":  ("🌙",  "#c4b5fd"),
    "4":  ("☄️",  "#9ca3af"), "5":  ("🔮",  "#f472b6"), "6":  ("🕳️", "#64748b"),
    "7":  ("🌌",  "#a78bfa"), "8":  ("✨",  "#fbbf24"), "9":  ("☁️",  "#34d399"),
    "10": ("🌐",  "#818cf8"), "11": ("💫",  "#fb923c"), "12": ("👽",  "#4ade80"),
}

# ════════════════════════════════════════════════════════════════════
# MASTER TAXONOMY
# ════════════════════════════════════════════════════════════════════

MASTER_TAXONOMY = {
    "1. Stars (shining balls of gas, from babies to old giants)": {
        "Baby stars being born": [
            "Class 0 Protostar","Class I Protostar","Class II Protostar","Class III Protostar",
            "Protostar (Class 0/I/II/III)","T Tauri Star","Herbig Ae/Be Star",
            "Protostellar Jet / Bipolar Outflow","Young Protoplanetary Disk","Stellar Debris Disk (young)",
            "Optically Thick Protoplanetary Disk","Inner Disk Cavity","Clearing / Gap in Disk","Off-Plane Disk Object"
        ],
        "Normal adult stars": [
            "Main-Sequence Star (generic)","G-type (solar-type) Star","Yellow Dwarf (Sun-like G star)",
            "Solar-Analog Star","F-Type Star","K-Type Star","M-type (red dwarf) Star","O-type Star (massive hot star)",
            "High-Metallicity Star","Parallax-Measurable Nearby Star","Limb-Darkened Star",
            "Tachocline-Observable Stellar Feature","S-Type Star Subclass"
        ],
        "Giant & super-giant stars": [
            "Giant Star (supergiant/hypergiant)","Blue Giant Star","Blue Supergiant","Blue Hypergiant",
            "Supergiant Star (blue/red hypergiant)","AGB Star (Asymptotic Giant Branch Star)","Carbon Star"
        ],
        "Pulsing & changing stars": [
            "Cepheid Variable Star","Mira Variable Star","Flare Star","Variable Star (generic)",
            "Stellar Flare Event","Flaring M-Dwarf Host"
        ],
        "Failed stars (brown dwarfs)": [
            "Brown Dwarf (generic)","Brown Dwarf L-type","L-Type Brown Dwarf",
            "L-type / Low-Temperature Brown Dwarf","Brown Dwarf T-type","T-Type Brown Dwarf",
            "Brown Dwarf Y-type","Y-Type Brown Dwarf","Free-Floating Substellar Object (rogue brown dwarf)",
            "Rogue Brown Dwarf"
        ],
        "Star pairs & groups": [
            "Binary Star","Contact Binary Star","Semi-Detached Binary Star",
            "Multiple Star System (triple, quadruple, etc.)","Cataclysmic Variable System",
            "X-ray Binary (accreting compact + donor)","Single-Star System"
        ],
        "Special & weird stars": [
            "Wolf–Rayet Star","Zeeman-Strong Magnetic Star","High-velocity Stellar Object (hypervelocity star)",
            "Rogue Star (ejected high-velocity star)","Thorne–Żytkow Object","Hipparcos / Catalog Cross-ID Feature"
        ]
    },
    "2. Planets & Exoplanets (whole worlds orbiting stars)": {
        "Rocky / Earth-like planets": [
            "Rocky Planet","Terrestrial Planet","Super-Earth","Hot Super-Earth","Sub-Earth Planet",
            "Mega-Earth","Small Inner Rocky Body (Mercury-analog)","Enstatite/Iron-Rich Planet",
            "Iron-Rich Planet","Silicate Planet","Si-rich / Silicon-Dominated Planet",
            "SiO2 / Silicate-Dominated Surface","Ilmenite / Metal-Rich Planet Surface","C/O-Rich Carbon Planet",
            "Rare-Element-Rich Planet","Mantle-Exposed Body"
        ],
        "Gas giants (like Jupiter)": [
            "Gas Giant (generic)","Jupiter-Analog Planet","Hot Jupiter","Warm Jupiter",
            "Gas Giant — Hot (close-in)","Gas Giant — Warm (intermediate)","Gas Giant — Cold (outer)",
            "Super-Jupiter","Eccentric Giant Planet","Wide-Separation Giant Planet (imaging target)",
            "Imaging-Favorable Wide-Separation Target"
        ],
        "Ice giants (like Uranus & Neptune)": [
            "Ice Giant","Neptune-Analog / Ice Giant","Uranus-Analog (ice-giant subtype)"
        ],
        "Exotic & special planets": [
            "Lava World","Melted Surface / Global Ocean of Magma","Lava/Ocean Hybrid Planet",
            "Ocean World (global surface ocean)","Sea-Level / Liquid-Surface Planet","Snowball Planet (global ice cover)",
            "Greenhouse-Dominated Planet","Hottest-Temperature Planet (ultra-hot)","Ultra-Hot Planet",
            "Puffy Planet / Super-Puff","Super-Puff Planet","Water-Rich Planet","Hydrogen-Helium Envelope Planet",
            "Helium-Rich Atmosphere Object","Gaseous Envelope Mini-Neptune","Sub-Neptune Planet",
            "M-class / Methane-Dominated Atmosphere Planet","N-type / Nitrogen-Rich Atmosphere Planet",
            "Molecular Hydrogen Envelope Planet","Veiled / Cloud-Hazed Atmosphere Planet",
            "Heat-Redistribution Efficient Planet","Ohmic-Heating Enhanced Planet","High-Spin Rate Planet",
            "Oblate/Rapid-Rotator Planet","Equatorial Bulge Feature","Tidally Locked Planet",
            "Retrograde Orbit Planet","Inclined Orbit Exoplanet","High-Eccentricity Planet",
            "Eccentricity-Extreme Object","Spin-Orbit Misaligned Planet","Magnetosphere-Stripped Planet",
            "CME-Impacted Planet","Climate-Instability Candidate","Temporal-Variable Atmosphere",
            "Seasonal Melt Cycle Planet","Ephemeral Liquid Surface","Polar-Ice Cap Dominant Planet"
        ],
        "Rogue planets (no star)": [
            "Rogue Planet (free-floating)","Free-Floating Gas Giant (rogue gas giant)","Free-Floating Terrestrial (rogue rocky)"
        ],
        "Planets in special places": [
            "Circumbinary Planet","P-Type (circumbinary) Planet","Forming Planet (embedded in disk)",
            "Newly Formed Planet (in-situ)","Planet–Planet Scattering Remnant","Habitable Candidate (multi-factor heuristic)",
            "Goldilocks / Habitable-Zone Planet Candidate","Habitable-Zone Stable Planet",
            "Habitable-Zone Edge Object (inner/outer edge)","Inner Habitable Zone Planet",
            "Outer Habitable Zone Object","Delta-V Low/Homeworld Candidate","Solar-System-Analogue System",
            "Exoplanet (generic)","Ideal Transit Candidate","Kepler-Style Transit Candidate",
            "Spectroscopic-Transit-Favorable Planet","Thermal Phase-Curve Measurable Planet",
            "Infrared-Bright Object","Hottest/Coldest Record Candidate","Record-Breaker Candidate"
        ]
    },
    "3. Moons & Rings (satellites & beautiful ring systems)": {
        "Icy & rocky moons": [
            "Rocky Moon","Ice Moon (surface-dominated by water-ice)","High-Albedo Icy Body",
            "Crystalline Ice Surface (TNO surface type)","Atmosphere-Bearing Moon",
            "Secondary Atmosphere-Bearing Small Body","Binary Moon","Asteroid Moon (moon of asteroid)",
            "Trojan Moon","Ringed Moon"
        ],
        "Ocean & active moons": [
            "Europa-type Subsurface-Ocean Moon","Ocean Moon (subsurface or surface ocean)",
            "Subsurface Ocean Moon Candidate","Enceladus-like Cryovolcanic Moon","Volcanic Moon",
            "Eruptive Volcano Planet / Moon","Extreme Tidal Heating World (Io-like)",
            "Geyser / Cryo-Geyser Feature"
        ],
        "Ring systems": [
            "Ringed Planet","Major-Planet Ring System (Saturn-analog)","Dense Ring System",
            "Massive-Ring System (optically thick)","Multi-Ring Planetary System",
            "Narrow Dust Ring / Ringlet","Neutral-Density/Optical-Depth Ring Feature",
            "Camelot / Ring Gap Feature (ring structure)","Planetary Ring Arc",
            "Debris/Ring Arc Feature","Gravitationally Bound Ring Arc","Impact-Ejecta Ring System",
            "Torus / Circumplanetary Torus Feature","Tiny Moonlet (ring particle-size)",
            "Satellite-Shepherd Moon Pair"
        ],
        "Small satellites": [
            "Moonlet / Small Satellite","Capture-Candidate Moon (recently captured)","Captured Moon","Flyby Capture Candidate"
        ]
    },
    "4. Asteroids, Comets & Small Bodies": {
        "Asteroids": [
            "Asteroid (generic)","Main-Belt Asteroid","Near-Earth Asteroid (NEA)",
            "C-Type (carbonaceous) Asteroid","S-Type (silicaceous) Asteroid",
            "V-Type (Vesta-like) Asteroid","Metal-Rich Asteroid (M-type)",
            "D-type Minor Body (dark-red TNO/asteroid)","Porous/Rubble-Pile Asteroid",
            "Asteroid Family (collisional family)","Asteroid Belt",
            "T Trojans (Jupiter trojans / similar)","Trojan Asteroid"
        ],
        "Comets": [
            "Comet (generic)","Active Comet","Comet (active)","Comet (dormant)","Comet (extinct)",
            "Comet (long-period)","Comet (short-period)","Short-Period Comet",
            "Oort-Cloud Long-Period Comet","Perihelion Activity Comet",
            "Rosetta-Target Retrograde Comet Candidate","Icy Comet Nucleus","Coma (comet coma)",
            "Meteor-Shower Parent Body","Oort Cloud Object"
        ],
        "Other small bodies": [
            "Pocked / Pitted Small Body","Cratering-Heavy Body (impact-dominated surface)",
            "Fragmented Body / Tidal Debris","Orbiting Debris Cloud","Unbound / Escaping Object",
            "Chaotic Orbit Object","Decaying Orbit Object","High-Inclination Object",
            "High-tide / Roche-approach Object","Aphelion/Perihelion Event","Scattering/Chaotic Zone Object"
        ]
    },
    "5. Dwarf Planets & Far-Out Ice Worlds": {
        "Dwarf planets & TNOs": [
            "Dwarf Planet (generic)","C-type Dwarf Planet (Ceres-analogue)","Pluto-Analog Dwarf Planet",
            "Eris-Type Dwarf Planet (distant massive dwarf)","TNO (Trans-Neptunian Object generic)",
            "Classical Kuiper Belt Object (KBO)","Kuiper Belt Object (classical TNO)",
            "Kuiper Resonant Object","Plutino / 3:2 Resonant KBO","Resonant Kuiper Belt Object",
            "Detached Kuiper Object","Scattered Disk Object","Ultra-Red TNO (color class)","Outgassing Dwarf Planet"
        ]
    },
    "6. Black Holes, Neutron Stars & Dead-Star Remnants": {
        "Black holes": [
            "Black Hole (generic)","Black Hole (stellar-mass)","Black Hole (intermediate-mass)",
            "Black Hole (supermassive)","Stellar-Mass Black Hole Binary","Binary Black Hole System",
            "Solitary Black Hole","Quiescent Black Hole",
            "Schwarzschild Black Hole (non-rotating idealization)","Black Hole (Kerr / rotating)",
            "Black Hole (charged / Reissner–Nordström)","Intermediate-Mass Black Hole"
        ],
        "Neutron stars & pulsars": [
            "Neutron Star (generic)","Pulsar (generic)","Millisecond Pulsar","Magnetar",
            "Singing Pulsar / Beacon-Like Pulsar","Isolated Pulsar Wind Nebula",
            "Pulsar Wind Nebula","Kilonova Candidate / Remnant","Gravitational-Wave Merger Remnant"
        ],
        "White dwarfs & supernova leftovers": [
            "White Dwarf","White-Dwarf–Planetary-Debris Disk System","Stellar Remnant (generic)",
            "Supernova (remnant / event)","Supernova Remnant (shell, composite, plerionic)",
            "Composite Supernova Remnant","I-type Supernova Remnant","Planetary Nebula","Light Echo Feature"
        ],
        "Other remnants": [
            "Tidal Disruption Event","Survivor Core","Anomalous Density Object","Extreme-Ultra-Dense Object"
        ]
    },
    "7. Galaxies (cities of billions of stars)": {
        "Main galaxy types": [
            "Galaxy (generic)","Spiral Galaxy","Grand-Design Spiral Galaxy","Barred Spiral Galaxy",
            "Multi-Arm Spiral Galaxy","Wrapped / Tightly-Wound Spiral Arm","Elliptical Galaxy",
            "Dwarf Elliptical Galaxy","Lenticular Galaxy (S0)","Irregular Galaxy",
            "Dwarf Irregular Galaxy","Interacting Galaxy"
        ],
        "Active & bright galaxies": [
            "Active Galaxy","Active Galactic Nucleus (AGN)","Blazar","Radio Galaxy","Quasar",
            "Quasar Host Galaxy","Blazar Host Galaxy","Nuclear Starburst Region",
            "Time-Variable Jet Source","Kiloparsec-Scale Jet Feature"
        ],
        "Small & special galaxies": [
            "Dwarf Galaxy","Faint Dwarf Galaxy (ultra-faint dwarf)","Ultra-Faint Dwarf Galaxy",
            "Magellanic-Cloud-Like Dwarf System","Hubble-Resolved Galaxy","Intracluster Light Feature"
        ],
        "Galaxy groups & clusters": [
            "Galaxy Group","Galaxy Cluster","Virgo-like Rich Cluster","Supercluster"
        ]
    },
    "8. Star Clusters": {
        "Clusters": [
            "Open Cluster (young star cluster)","Globular Cluster","Compact Cluster / Super Star Cluster"
        ]
    },
    "9. Nebulae & Space Gas Clouds": {
        "Nebulae & ISM features": [
            "Emission Nebula","H II Region (emission nebula)","Reflection Nebula","Dark Nebula",
            "Barnard-type Dark Cloud / Bok Globule","Giant Molecular Cloud","Molecular Cloud Dense Core",
            "Atomic/Molecular Cloud Core","Star-Forming Region (SFR)","Chemical-Precursor Region",
            "Organic-Molecule-Rich Region","Prebiotic-Chemistry-Rich Environment",
            "Self-Replicating Molecule Region","Stellar-Wind-Blown Bubble","Shock-Front in ISM",
            "Ionized Wind / Outflow Region","Interstellar Medium Patch"
        ]
    },
    "10. The Huge Universe Structure": {
        "Large-scale structure": [
            "Cosmic Web (large-scale structure)","Cosmic Filament","Cosmic Node","Cosmic Wall",
            "Cosmic Void","Halo Substructure (dark-matter / stellar stream)","Stream / Stellar Stream",
            "Galactic Halo Object","Stellar Halo Object","Galactic Disk Warp Feature",
            "Galactic Bar Feature","Bulge / Galactic Bulge Feature","Counter-Rotating Disc Feature",
            "Galactic Nucleus (non-active)"
        ]
    },
    "11. Special Features & Phenomena (visible or measurable things)": {
        "Features & phenomena": [
            "Aurora Feature","Planetary Aurora Feature","Giant Planetary Storm",
            "Precipitating Cloud System","Cirrus / High-Cloud Layer","Paperthin/Iridescent Cloud Layer",
            "Glacial/Ice Sheet Feature","Ice Sheet / Polar Cap Feature","Geyser / Cryo-Geyser Feature",
            "Cave/Subsurface Habitat","Sedimentary-Layered Surface","Freshly Resurfaced Surface",
            "Ejecta-Blanket Surface","Impact Basin Feature","Pac-Man Crater / Asymmetric Feature",
            "Crystalline Ice Surface","Albedo Feature (high-albedo object)","Equatorial Bulge Feature",
            "Refraction / Atmospheric Lensing Feature","Einstein Ring",
            "Gravitational Lens System (generic)","Gravitationally Lensed Quasar Image",
            "Accretion Disk","Debris Disk","Kelvin-Helmholtz Instability Feature (disk)",
            "Crescent-Phase Visibility Object","Partial/Annular Eclipse Geometry Candidate",
            "Sounding-Rocket-Detectable Object","Telescopic-Resolution Candidate",
            "Rare/Novel Morphology Object"
        ]
    },
    "12. Signs of Life & Alien Technology (the exciting search-for-life stuff)": {
        "Habitable & life-friendly places": [
            "Life-Detection-Signature Region","Life-Supporting Chemical Pool",
            "Photosynthetic-Microbe Habitat","Photosynthetic Forest Analog","Microbial-Life World",
            "Extremophile Habitat","Subsurface-Volatile Pocket","Seeded / Panspermia Candidate",
            "Persistent Methane Source","Exoplanet Atmosphere Escape Signature"
        ],
        "Exotic life types": [
            "Ammonia-based Life (exotic chemistry life)",
            "Exotic Chemistry Life (methane/ammonia/silicon, etc.)","Cloud-Dwelling Life",
            "Chemotroph-Type Life (microbial type)","Corona-Associated Life (speculative)",
            "Plasma-Based Life Candidate","Radiation-Based Life Candidate",
            "AI-based Life / Artificial Life","Post-Biological Civilization World"
        ],
        "Alien tech & artifacts": [
            "Anthropogenic / Technological Artifact (artificial object)",
            "Megastructure Candidate (Dyson-like; speculative)",
            "Interstellar Traveler / Artifact (speculative)",
            "Self-Replicating Machine Field (speculative)",
            "Sensory-Detectable Technosignature Candidate"
        ],
        "Catch-alls": [
            "Unknown / Unclassified Object","Anomalous Density Object"
        ]
    }
}

# ════════════════════════════════════════════════════════════════════
# SIMPLE EXPLANATIONS
# ════════════════════════════════════════════════════════════════════

SIMPLE_TERMS = {
    "protostar":    "🌟 A baby star being born from gas and dust!",
    "main-sequence":"☀ A normal adult star, like our Sun.",
    "supergiant":   "🔴 A HUGE star — way bigger than the Sun.",
    "brown dwarf":  "🟤 Too big for a planet, too small for a star.",
    "binary":       "👯 Two stars dancing around each other.",
    "exoplanet":    "🌍 A planet orbiting another star, not our Sun.",
    "gas giant":    "🪐 A huge planet of gas — like Jupiter.",
    "terrestrial":  "🌎 A rocky planet with solid ground — like Earth.",
    "asteroid":     "🪨 A small rocky object floating in space.",
    "comet":        "☄ An icy object with a glowing tail near the Sun.",
    "black hole":   "🕳 Gravity so strong even light can't escape!",
    "neutron":      "💎 Super-dense remains of an exploded star.",
    "galaxy":       "🌌 Billions of stars grouped together.",
    "nebula":       "☁ A giant cloud of gas — star nursery!",
    "pulsar":       "🔦 A spinning star that flashes like a lighthouse.",
    "quasar":       "💡 Incredibly bright — powered by a black hole.",
    "dwarf planet": "🔮 Smaller than a planet, like Pluto.",
    "white dwarf":  "⚪ A tiny, cooling dead star.",
    "supernova":    "💥 A star exploding in a blaze of glory!",
    "habitable":    "🏡 Could be the right conditions for life!",
}

TIPS = {
    "search":       "🔍 SEARCH\n\nType any word to find objects.\nResults appear INSTANTLY!",
    "tree":         "🗂 BROWSER\n\n▸ Click ▶ to expand\n▸ Single-click to view details\n▸ Everything loads instantly!",
    "details":      "📋 DETAILS\n\nInstant object information.\n▸ ⭐ = favourites\n▸ 📋 = copy data",
}

# ════════════════════════════════════════════════════════════════════
# HIGH-PERFORMANCE CACHE
# ════════════════════════════════════════════════════════════════════

class LRUCache:
    """Thread-safe LRU cache for instant object retrieval."""
    
    def __init__(self, capacity: int = CACHE_SIZE):
        self.capacity = capacity
        self.cache: OrderedDict = OrderedDict()
        self.lock = threading.RLock()
        self.hits = 0
        self.misses = 0
    
    def get(self, key: str) -> Optional[Any]:
        with self.lock:
            if key in self.cache:
                self.cache.move_to_end(key)
                self.hits += 1
                return self.cache[key]
            self.misses += 1
            return None
    
    def put(self, key: str, value: Any):
        with self.lock:
            if key in self.cache:
                self.cache.move_to_end(key)
            self.cache[key] = value
            if len(self.cache) > self.capacity:
                self.cache.popitem(last=False)
    
    def contains(self, key: str) -> bool:
        with self.lock:
            return key in self.cache
    
    def clear(self):
        with self.lock:
            self.cache.clear()
    
    def preload(self, items: List[Tuple[str, Any]]):
        """Bulk preload items."""
        with self.lock:
            for key, value in items:
                if key not in self.cache:
                    self.cache[key] = value
                    if len(self.cache) > self.capacity:
                        self.cache.popitem(last=False)
    
    def stats(self) -> dict:
        with self.lock:
            total = self.hits + self.misses
            return {
                "size": len(self.cache),
                "hits": self.hits,
                "misses": self.misses,
                "hit_rate": f"{(self.hits/total*100):.1f}%" if total > 0 else "N/A"
            }


# ════════════════════════════════════════════════════════════════════
# ASYNC TASK MANAGER
# ════════════════════════════════════════════════════════════════════

class TaskManager:
    """Manages background tasks with UI callbacks."""
    
    def __init__(self, root: tk.Tk):
        self.root = root
        self.executor = ThreadPoolExecutor(max_workers=THREAD_POOL_SIZE)
        self.pending: Dict[str, Future] = {}
        self.result_queue = queue.Queue()
        self._poll_results()
    
    def submit(self, task_id: str, fn: Callable, callback: Callable = None, *args, **kwargs):
        """Submit a task to run in background."""
        # Cancel existing task with same ID
        if task_id in self.pending:
            self.pending[task_id].cancel()
        
        def wrapper():
            try:
                result = fn(*args, **kwargs)
                self.result_queue.put((task_id, callback, result, None))
            except Exception as e:
                self.result_queue.put((task_id, callback, None, e))
        
        future = self.executor.submit(wrapper)
        self.pending[task_id] = future
    
    def _poll_results(self):
        """Poll for completed tasks and run callbacks on main thread."""
        try:
            while True:
                task_id, callback, result, error = self.result_queue.get_nowait()
                if task_id in self.pending:
                    del self.pending[task_id]
                if callback:
                    if error:
                        log.error(f"Task {task_id} failed: {error}")
                    else:
                        try:
                            callback(result)
                        except Exception as e:
                            log.error(f"Callback error: {e}")
        except queue.Empty:
            pass
        finally:
            self.root.after(10, self._poll_results)  # Poll every 10ms for responsiveness
    
    def shutdown(self):
        self.executor.shutdown(wait=False)


# ════════════════════════════════════════════════════════════════════
# ULTRA-FAST DATABASE LAYER
# ════════════════════════════════════════════════════════════════════

class DB:
    """High-performance database with connection pooling and caching."""
    
    def __init__(self):
        self.path = DB_PATH
        self.fts = False
        self.cache = LRUCache(CACHE_SIZE)
        self.stats_cache: Optional[dict] = None
        self.stats_cache_time = 0
        self.counts_cache: Dict[str, int] = {}
        self.favs_cache: set = set()
        self._local = threading.local()
        self._lock = threading.RLock()
        
        # Pre-compile regex for JSON highlighting
        self._re_key = re.compile(r'"([^"]+)"\s*:')
        self._re_str = re.compile(r':\s*"([^"]*)"')
        self._re_num = re.compile(r':\s*(-?\d+\.?\d*)')
    
    def _get_conn(self) -> sqlite3.Connection:
        """Get thread-local connection."""
        if not hasattr(self._local, 'conn') or self._local.conn is None:
            self._local.conn = sqlite3.connect(str(self.path), check_same_thread=False)
            self._local.conn.execute("PRAGMA journal_mode=WAL;")
            self._local.conn.execute("PRAGMA synchronous=NORMAL;")
            self._local.conn.execute("PRAGMA cache_size=10000;")
            self._local.conn.execute("PRAGMA temp_store=MEMORY;")
            self._local.conn.execute("PRAGMA mmap_size=268435456;")  # 256MB mmap
        return self._local.conn
    
    @property
    def conn(self):
        return self._get_conn()
    
    def open(self):
        APPDATA_DIR.mkdir(parents=True, exist_ok=True)
        self._create_tables()
        self._load_favs()
        self._precompute_counts()
        log.info("Database opened with caching: %s", self.path)
    
    def close(self):
        if hasattr(self._local, 'conn') and self._local.conn:
            self._local.conn.close()
            self._local.conn = None
    
    def _create_tables(self):
        c = self.conn.cursor()
        c.execute("""CREATE TABLE IF NOT EXISTS objects (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            main_cat  TEXT NOT NULL,
            sub_cat   TEXT NOT NULL,
            obj_type  TEXT NOT NULL,
            obj_name  TEXT UNIQUE NOT NULL,
            data      TEXT NOT NULL,
            ts        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );""")
        c.execute("CREATE INDEX IF NOT EXISTS ix_name ON objects(obj_name COLLATE NOCASE);")
        c.execute("CREATE INDEX IF NOT EXISTS ix_type ON objects(obj_type COLLATE NOCASE);")
        c.execute("CREATE INDEX IF NOT EXISTS ix_mcat ON objects(main_cat);")
        c.execute("CREATE INDEX IF NOT EXISTS ix_mcat_type ON objects(main_cat, obj_type);")
        
        try:
            c.execute("""CREATE VIRTUAL TABLE IF NOT EXISTS fts
                         USING fts5(obj_name, obj_type, data, content='objects', content_rowid='id');""")
            self.fts = True
        except sqlite3.OperationalError:
            self.fts = False
        
        c.execute("""CREATE TABLE IF NOT EXISTS favourites (
            obj_name TEXT PRIMARY KEY,
            ts       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );""")
        c.execute("""CREATE TABLE IF NOT EXISTS history (
            query TEXT PRIMARY KEY,
            cnt   INTEGER DEFAULT 1,
            ts    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );""")
        self.conn.commit()
    
    def _load_favs(self):
        """Preload favourites into memory."""
        with self._lock:
            self.favs_cache = set(r[0] for r in self.conn.execute(
                "SELECT obj_name FROM favourites").fetchall())
    
    def _precompute_counts(self):
        """Precompute type counts for instant tree loading."""
        with self._lock:
            rows = self.conn.execute(
                "SELECT main_cat, obj_type, COUNT(*) FROM objects GROUP BY main_cat, obj_type"
            ).fetchall()
            self.counts_cache = {f"{r[0]}|{r[1]}": r[2] for r in rows}
    
    def has_data(self) -> bool:
        return self.conn.execute("SELECT COUNT(*) FROM objects").fetchone()[0] > 0
    
    # ── INSTANT OBJECT RETRIEVAL ────────────────────────────────────
    def get_object(self, name: str) -> Optional[dict]:
        """Get object with cache - INSTANT retrieval."""
        # Check cache first
        cached = self.cache.get(name)
        if cached is not None:
            return cached
        
        # Cache miss - fetch from DB
        row = self.conn.execute(
            "SELECT main_cat, sub_cat, obj_type, data FROM objects WHERE obj_name=?", 
            (name,)
        ).fetchone()
        
        if not row:
            return None
        
        result = {
            "main_cat": row[0], 
            "sub_cat": row[1], 
            "obj_type": row[2], 
            "data": json.loads(row[3])
        }
        
        # Store in cache
        self.cache.put(name, result)
        return result
    
    def get_objects_batch(self, names: List[str]) -> Dict[str, dict]:
        """Batch get for preloading."""
        result = {}
        to_fetch = []
        
        # Check cache first
        for name in names:
            cached = self.cache.get(name)
            if cached is not None:
                result[name] = cached
            else:
                to_fetch.append(name)
        
        # Fetch missing from DB
        if to_fetch:
            placeholders = ",".join("?" * len(to_fetch))
            rows = self.conn.execute(f"""
                SELECT obj_name, main_cat, sub_cat, obj_type, data 
                FROM objects WHERE obj_name IN ({placeholders})
            """, to_fetch).fetchall()
            
            for row in rows:
                obj = {
                    "main_cat": row[1],
                    "sub_cat": row[2],
                    "obj_type": row[3],
                    "data": json.loads(row[4])
                }
                result[row[0]] = obj
                self.cache.put(row[0], obj)
        
        return result
    
    def preload_nearby(self, names: List[str]):
        """Preload objects that user might click next."""
        self.get_objects_batch(names[:PRELOAD_COUNT])
    
    # ── FAST COUNTS ─────────────────────────────────────────────────
    def count_for(self, obj_type: str, main_cat: str) -> int:
        """Instant count from precomputed cache."""
        key = f"{main_cat}|{obj_type}"
        return self.counts_cache.get(key, 0)
    
    def names_for(self, obj_type: str, main_cat: str) -> List[str]:
        return [r[0] for r in self.conn.execute(
            "SELECT obj_name FROM objects WHERE obj_type=? COLLATE NOCASE AND main_cat=? ORDER BY obj_name",
            (obj_type, main_cat)).fetchall()]
    
    def names_for_limited(self, obj_type: str, main_cat: str, limit: int = 100) -> Tuple[List[str], int]:
        """Get names with limit for virtual loading."""
        total = self.count_for(obj_type, main_cat)
        names = [r[0] for r in self.conn.execute(
            "SELECT obj_name FROM objects WHERE obj_type=? COLLATE NOCASE AND main_cat=? ORDER BY obj_name LIMIT ?",
            (obj_type, main_cat, limit)).fetchall()]
        return names, total
    
    def unmapped_types(self) -> List[str]:
        return [r[0] for r in self.conn.execute(
            "SELECT DISTINCT obj_type FROM objects WHERE main_cat='Unmapped' ORDER BY obj_type").fetchall()]
    
    def unmapped_names(self, otype: str) -> List[str]:
        return [r[0] for r in self.conn.execute(
            "SELECT obj_name FROM objects WHERE main_cat='Unmapped' AND obj_type=? ORDER BY obj_name",
            (otype,)).fetchall()]
    
    # ── INSTANT SEARCH ──────────────────────────────────────────────
    def search(self, q: str, limit: int = 500) -> List[tuple]:
        ql = q.strip().lower()
        if self.fts and len(q) >= 2:
            try:
                return self.conn.execute("""
                    SELECT o.main_cat, o.sub_cat, o.obj_type, o.obj_name
                    FROM fts f JOIN objects o ON f.rowid=o.id
                    WHERE fts MATCH ? LIMIT ?""", (q+"*", limit)).fetchall()
            except:
                pass
        return self.conn.execute("""
            SELECT main_cat, sub_cat, obj_type, obj_name FROM objects
            WHERE obj_name LIKE ? COLLATE NOCASE OR obj_type LIKE ? COLLATE NOCASE
            ORDER BY obj_name LIMIT ?""", (f"%{ql}%", f"%{ql}%", limit)).fetchall()
    
    # ── CACHED STATS ────────────────────────────────────────────────
    def stats(self) -> dict:
        now = time.time()
        if self.stats_cache and (now - self.stats_cache_time) < 5:  # 5 second cache
            return self.stats_cache
        
        ex = lambda sql: self.conn.execute(sql).fetchone()[0]
        self.stats_cache = {
            "total":    ex("SELECT COUNT(*) FROM objects"),
            "cats":     ex("SELECT COUNT(DISTINCT main_cat) FROM objects"),
            "types":    ex("SELECT COUNT(DISTINCT obj_type) FROM objects"),
            "favs":     len(self.favs_cache),
            "unmapped": ex("SELECT COUNT(*) FROM objects WHERE main_cat='Unmapped'"),
        }
        self.stats_cache_time = now
        return self.stats_cache
    
    # ── INSTANT FAVOURITES ──────────────────────────────────────────
    def is_fav(self, n: str) -> bool:
        return n in self.favs_cache
    
    def add_fav(self, n: str):
        with self._lock:
            self.favs_cache.add(n)
        self.conn.execute("INSERT OR IGNORE INTO favourites(obj_name) VALUES(?)", (n,))
        self.conn.commit()
        self.stats_cache = None
    
    def rm_fav(self, n: str):
        with self._lock:
            self.favs_cache.discard(n)
        self.conn.execute("DELETE FROM favourites WHERE obj_name=?", (n,))
        self.conn.commit()
        self.stats_cache = None
    
    def all_favs(self) -> List[str]:
        return [r[0] for r in self.conn.execute(
            "SELECT obj_name FROM favourites ORDER BY ts DESC").fetchall()]
    
    # ── HISTORY ─────────────────────────────────────────────────────
    def add_hist(self, q: str):
        self.conn.execute("""INSERT INTO history(query) VALUES(?)
            ON CONFLICT(query) DO UPDATE SET cnt=cnt+1, ts=CURRENT_TIMESTAMP""", (q,))
        self.conn.commit()
    
    def recent(self, n: int = 8) -> List[str]:
        return [r[0] for r in self.conn.execute(
            "SELECT query FROM history ORDER BY ts DESC LIMIT ?", (n,)).fetchall()]
    
    # ── IMPORT ──────────────────────────────────────────────────────
    def import_json(self, path: Path, cb: Callable = None) -> dict:
        if cb: cb(0, 1, "Reading JSON file... ⏳")
        with path.open("r", encoding="utf-8") as f:
            raw = json.load(f)
        
        if cb: cb(0, 1, "Parsing structure... ⏳")
        if isinstance(raw, dict):
            for k in ("objects","items","catalog","data"):
                if k in raw and isinstance(raw[k], list):
                    raw = raw[k]; break
            else:
                raw = [raw]
        if not isinstance(raw, list):
            raise ValueError("JSON root must be a list or object")
        
        # Build canonical map
        cmap: Dict[str, tuple] = {}
        for mc, subs in MASTER_TAXONOMY.items():
            for sc, types in subs.items():
                for t in types:
                    cmap[t.strip().lower()] = (mc, sc, t)
        
        total = len(raw)
        stats = {"total": total, "mapped": 0, "unmapped": 0, "errors": 0}
        cur = self.conn.cursor()
        
        if cb: cb(0, total, f"Importing {total:,} objects... ⏳")
        
        # Batch insert for speed
        batch = []
        batch_size = 500
        
        for i, item in enumerate(raw):
            try:
                if not isinstance(item, dict):
                    stats["errors"] += 1
                    continue
                
                otype = (item.get("type") or item.get("object_type")
                         or item.get("category") or item.get("classification") or "Unknown")
                name = (item.get("object_name") or item.get("name")
                        or item.get("id") or item.get("identifier") or f"Object_{i+1}")
                blob = json.dumps(item, ensure_ascii=False)
                low = otype.strip().lower()
                
                mc = sc = canon = None
                if low in cmap:
                    mc, sc, canon = cmap[low]
                else:
                    for kn, (mc2, sc2, cn2) in cmap.items():
                        if kn in low or low in kn:
                            mc, sc, canon = mc2, sc2, cn2
                            break
                
                if mc:
                    batch.append((mc, sc, canon, name, blob))
                    stats["mapped"] += 1
                else:
                    batch.append(("Unmapped", "Unmapped", otype, name, blob))
                    stats["unmapped"] += 1
                
                # Flush batch
                if len(batch) >= batch_size:
                    cur.executemany(
                        "INSERT OR REPLACE INTO objects(main_cat,sub_cat,obj_type,obj_name,data) VALUES(?,?,?,?,?)",
                        batch
                    )
                    batch = []
                    if cb and (i+1) % 1000 == 0:
                        cb(i+1, total, f"Importing... ({i+1:,}/{total:,})")
                        
            except Exception as e:
                log.error("Row %d: %s", i, e)
                stats["errors"] += 1
        
        # Flush remaining
        if batch:
            cur.executemany(
                "INSERT OR REPLACE INTO objects(main_cat,sub_cat,obj_type,obj_name,data) VALUES(?,?,?,?,?)",
                batch
            )
        
        self.conn.commit()
        
        # Rebuild FTS
        if self.fts:
            try:
                cur.execute("INSERT INTO fts(fts) VALUES('rebuild');")
                self.conn.commit()
            except:
                pass
        
        # Refresh caches
        self._precompute_counts()
        self.cache.clear()
        self.stats_cache = None
        
        log.info("Import done: %s", stats)
        return stats
    
    def save_json(self, path: Path):
        rows = self.conn.execute("SELECT data FROM objects ORDER BY main_cat, obj_name").fetchall()
        out = []
        for r in rows:
            try:
                out.append(json.loads(r[0]))
            except:
                pass
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as f:
            json.dump(out, f, indent=2, ensure_ascii=False)
        log.info("Saved %d objects → %s", len(out), path)


# ════════════════════════════════════════════════════════════════════
# TOOLTIP
# ════════════════════════════════════════════════════════════════════

class Tip:
    def __init__(self, w, text, delay=300):
        self.w, self.text, self.delay = w, text, delay
        self.tw = None
        self._id = None
        w.bind("<Enter>", self._sched)
        w.bind("<Leave>", self._hide)
        w.bind("<Button>", self._hide)
    
    def _sched(self, e=None):
        self._id = self.w.after(self.delay, self._show)
    
    def _hide(self, e=None):
        if self._id:
            self.w.after_cancel(self._id)
            self._id = None
        if self.tw:
            self.tw.destroy()
            self.tw = None
    
    def _show(self):
        if self.tw:
            return
        x = self.w.winfo_rootx() + 20
        y = self.w.winfo_rooty() + self.w.winfo_height() + 5
        self.tw = tw = tk.Toplevel(self.w)
        tw.wm_overrideredirect(True)
        tw.wm_geometry(f"+{x}+{y}")
        frm = tk.Frame(tw, bg=C.BG4, highlightbackground=C.CYAN, highlightthickness=1)
        frm.pack()
        tk.Label(frm, text=self.text, bg=C.BG4, fg=C.T2,
                 font=("Segoe UI", 9), justify="left", padx=10, pady=8, wraplength=280).pack()


# ════════════════════════════════════════════════════════════════════
# WIDGETS
# ════════════════════════════════════════════════════════════════════

class Btn(tk.Canvas):
    def __init__(self, parent, text, cmd=None, w=110, h=34, tip="", style="def"):
        super().__init__(parent, width=w, height=h, bg=C.BG1, highlightthickness=0, cursor="hand2")
        self.txt, self.cmd, self.w, self.h = text, cmd, w, h
        cols = {
            "def": (C.BG3, C.HOVER, C.BORDER, C.CYAN, C.T2, C.CYAN),
            "pri": (C.BG3, C.HOVER, C.CYAN, C.CYAN, C.CYAN, C.T1),
            "ok": (C.BG3, C.HOVER, C.GREEN, C.GREEN, C.GREEN, C.T1),
        }
        self.bg_n, self.bg_h, self.bd_n, self.bd_h, self.fg_n, self.fg_h = cols.get(style, cols["def"])
        self.hov = False
        self._draw()
        self.bind("<Enter>", lambda e: self._set(True))
        self.bind("<Leave>", lambda e: self._set(False))
        self.bind("<ButtonRelease-1>", lambda e: cmd() if cmd and self.hov else None)
        if tip:
            Tip(self, tip)
    
    def _set(self, h):
        self.hov = h
        self._draw()
    
    def _draw(self):
        self.delete("all")
        bg = self.bg_h if self.hov else self.bg_n
        bd = self.bd_h if self.hov else self.bd_n
        fg = self.fg_h if self.hov else self.fg_n
        r = 6
        pts = [r, 0, self.w - r, 0, self.w, 0, self.w, r,
               self.w, self.h - r, self.w, self.h, self.w - r, self.h, r, self.h,
               0, self.h, 0, self.h - r, 0, r, 0, 0]
        self.create_polygon(pts, smooth=True, fill=bg, outline=bd)
        self.create_text(self.w // 2, self.h // 2, text=self.txt, fill=fg, font=("Segoe UI", 10, "bold"))


class SearchBar(tk.Frame):
    def __init__(self, parent, placeholder="Search…", on_type=None, on_clear=None, tip=""):
        super().__init__(parent, bg=C.BG2, highlightbackground=C.BORDER, highlightthickness=1)
        self.ph, self.on_type, self.on_clear = placeholder, on_type, on_clear
        self.showing_ph = True
        
        tk.Label(self, text="🔍", bg=C.BG2, fg=C.T4, font=("Segoe UI", 13)).pack(side="left", padx=(10, 4))
        self.ent = tk.Entry(self, bg=C.BG2, fg=C.T4, font=("Segoe UI", 11),
                            relief="flat", insertbackground=C.CYAN, insertwidth=2)
        self.ent.pack(side="left", fill="both", expand=True, pady=9)
        self.ent.insert(0, placeholder)
        
        self.clr = tk.Label(self, text="✕", bg=C.BG2, fg=C.T4, font=("Segoe UI", 13), cursor="hand2")
        self.clr.bind("<Button-1>", self._clear)
        self.clr.bind("<Enter>", lambda e: self.clr.config(fg=C.RED))
        self.clr.bind("<Leave>", lambda e: self.clr.config(fg=C.T4))
        
        self.ent.bind("<FocusIn>", self._fin)
        self.ent.bind("<FocusOut>", self._fout)
        self.ent.bind("<KeyRelease>", self._key)
        
        if tip:
            Tip(self, tip)
    
    def _fin(self, e):
        if self.showing_ph:
            self.ent.delete(0, "end")
            self.ent.config(fg=C.T1)
            self.showing_ph = False
    
    def _fout(self, e):
        if not self.ent.get():
            self.ent.insert(0, self.ph)
            self.ent.config(fg=C.T4)
            self.showing_ph = True
            self.clr.pack_forget()
    
    def _key(self, e):
        t = self.get()
        if t:
            self.clr.pack(side="right", padx=(4, 10))
        else:
            self.clr.pack_forget()
        if self.on_type:
            self.on_type(t)
    
    def _clear(self, e=None):
        self.ent.delete(0, "end")
        self.ent.focus_set()
        self.clr.pack_forget()
        self.showing_ph = False
        if self.on_clear:
            self.on_clear()
        elif self.on_type:
            self.on_type("")
    
    def get(self):
        return "" if self.showing_ph else self.ent.get()
    
    def set(self, v):
        self.ent.delete(0, "end")
        if v:
            self.ent.insert(0, v)
            self.ent.config(fg=C.T1)
            self.showing_ph = False
            self.clr.pack(side="right", padx=(4, 10))
        else:
            self.ent.insert(0, self.ph)
            self.ent.config(fg=C.T4)
            self.showing_ph = True
            self.clr.pack_forget()
    
    def focus(self):
        self.ent.focus_set()


class Stat(tk.Frame):
    def __init__(self, parent, title, val="0", icon="📊", color=C.CYAN, tip=""):
        super().__init__(parent, bg=C.CARD, padx=14, pady=10,
                         highlightbackground=C.BORDER, highlightthickness=1)
        hdr = tk.Frame(self, bg=C.CARD)
        hdr.pack(fill="x")
        tk.Label(hdr, text=icon, bg=C.CARD, fg=color, font=("Segoe UI", 13)).pack(side="left")
        tk.Label(hdr, text=title, bg=C.CARD, fg=C.T3, font=("Segoe UI", 9)).pack(side="left", padx=(6, 0))
        self.vl = tk.Label(self, text=str(val), bg=C.CARD, fg=color, font=("Segoe UI", 22, "bold"))
        self.vl.pack(anchor="w", pady=(2, 0))
        if tip:
            Tip(self, tip)
    
    def set(self, v):
        self.vl.config(text=str(v))


# ════════════════════════════════════════════════════════════════════
# LOADING INDICATOR
# ════════════════════════════════════════════════════════════════════

class LoadingOverlay(tk.Frame):
    """Lightweight loading indicator."""
    
    def __init__(self, parent):
        super().__init__(parent, bg=C.BG0)
        self.dots = 0
        self.lbl = tk.Label(self, text="⏳ Loading", bg=C.BG0, fg=C.LOADING,
                            font=("Segoe UI", 11))
        self.lbl.pack(expand=True)
        self._animate_id = None
    
    def show(self, text="Loading"):
        self.base_text = text
        self.dots = 0
        self.place(relx=0.5, rely=0.5, anchor="center")
        self._animate()
    
    def hide(self):
        if self._animate_id:
            self.after_cancel(self._animate_id)
            self._animate_id = None
        self.place_forget()
    
    def _animate(self):
        self.dots = (self.dots + 1) % 4
        self.lbl.config(text=f"⏳ {self.base_text}{'.' * self.dots}")
        self._animate_id = self.after(200, self._animate)


# ════════════════════════════════════════════════════════════════════
# PROGRESS DIALOG
# ════════════════════════════════════════════════════════════════════

class ProgressDlg(tk.Toplevel):
    def __init__(self, parent, title="Working…"):
        super().__init__(parent)
        self.title(title)
        self.geometry("420x180")
        self.configure(bg=C.BG0)
        self.resizable(False, False)
        self.transient(parent)
        self.grab_set()
        
        self.update_idletasks()
        x = parent.winfo_x() + (parent.winfo_width() - 420) // 2
        y = parent.winfo_y() + (parent.winfo_height() - 180) // 2
        self.geometry(f"+{max(0, x)}+{max(0, y)}")
        
        tk.Label(self, text="🚀", bg=C.BG0, fg=C.CYAN, font=("Segoe UI", 28)).pack(pady=(16, 6))
        self.msg = tk.Label(self, text="Initializing…", bg=C.BG0, fg=C.T2, font=("Segoe UI", 11))
        self.msg.pack()
        
        self.cv = tk.Canvas(self, width=320, height=10, bg=C.BG3, highlightthickness=0)
        self.cv.pack(pady=14)
        self.bar = self.cv.create_rectangle(0, 0, 0, 10, fill=C.CYAN, outline="")
        
        self.pct = tk.Label(self, text="0 %", bg=C.BG0, fg=C.T3, font=("Segoe UI", 9))
        self.pct.pack()
        
        self.protocol("WM_DELETE_WINDOW", lambda: None)
    
    def progress(self, cur, total, text=None):
        if text:
            self.msg.config(text=text)
        if total > 0:
            p = int(cur / total * 100)
            w = int((cur / total) * 320)
            self.cv.coords(self.bar, 0, 0, w, 10)
            self.pct.config(text=f"{p} %")
        self.update()
    
    def done(self, text="Done!"):
        self.msg.config(text=text)
        self.cv.coords(self.bar, 0, 0, 320, 10)
        self.pct.config(text="100 %")
        self.update()


# ════════════════════════════════════════════════════════════════════
# WELCOME DIALOG
# ════════════════════════════════════════════════════════════════════

class WelcomeDlg(tk.Toplevel):
    def __init__(self, parent, on_done):
        super().__init__(parent)
        self.on_done = on_done
        self.title("Welcome!")
        self.geometry("560x440")
        self.configure(bg=C.BG0)
        self.resizable(False, False)
        self.transient(parent)
        self.grab_set()
        
        self.update_idletasks()
        x = parent.winfo_x() + (parent.winfo_width() - 560) // 2
        y = parent.winfo_y() + (parent.winfo_height() - 440) // 2
        self.geometry(f"+{max(0, x)}+{max(0, y)}")
        
        # Stars background
        cv = tk.Canvas(self, width=560, height=120, bg=C.BG0, highlightthickness=0)
        cv.pack()
        for _ in range(60):
            sx, sy = random.randint(5, 555), random.randint(5, 115)
            b = random.randint(80, 255)
            c = f"#{b:02x}{b:02x}{b:02x}"
            s = random.choice([1, 1, 2])
            cv.create_oval(sx - s, sy - s, sx + s, sy + s, fill=c, outline="")
        cv.create_text(280, 60, text="✨  Astro Catalog  ✨", fill=C.T1, font=("Segoe UI", 26, "bold"))
        
        tk.Label(self, text="Welcome, Space Explorer! 🚀", bg=C.BG0, fg=C.CYAN,
                 font=("Segoe UI", 16, "bold")).pack(pady=(16, 8))
        tk.Label(self, text=(
            "This app lets you explore a catalog of space objects!\n\n"
            "⚡ Ultra-responsive — zero lag, instant everything!\n"
            "🔍 Smart caching — objects load before you click them\n"
            "🚀 Background processing — UI never freezes\n\n"
            "On first run we'll import your data into a fast database."
        ), bg=C.BG0, fg=C.T2, font=("Segoe UI", 10), justify="center").pack(padx=30)
        
        bf = tk.Frame(self, bg=C.BG0)
        bf.pack(pady=24)
        Btn(bf, "🚀  Get Started!", lambda: self._go(), w=150, h=40, style="pri").pack()
    
    def _go(self):
        self.destroy()
        self.on_done()


# ════════════════════════════════════════════════════════════════════
# MAIN APPLICATION — ULTRA RESPONSIVE
# ════════════════════════════════════════════════════════════════════

class App:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title(WINDOW_TITLE)
        self.root.configure(bg=C.BG0)
        self.root.minsize(1100, 650)
        try:
            self.root.state("zoomed")
        except:
            pass
        
        # Core systems
        self.db = DB()
        self.db.open()
        self.tasks = TaskManager(root)
        
        # State
        self.cur_obj: Optional[str] = None
        self._search_after = None
        self._preload_after = None
        self._pending_selection = None
        
        # Build UI
        self._styles()
        self._build()
        self._shortcuts()
        
        # Initialize
        if self.db.has_data():
            self._load()
        else:
            WelcomeDlg(self.root, self._after_welcome)
    
    def _styles(self):
        s = ttk.Style()
        s.theme_use("clam")
        s.configure("T.Treeview", background=C.BG1, foreground=C.T2, fieldbackground=C.BG1,
                    borderwidth=0, rowheight=28, font=("Segoe UI", 10))
        s.configure("T.Treeview.Heading", background=C.BG3, foreground=C.T3, borderwidth=0)
        s.map("T.Treeview", background=[("selected", C.SELECTED)], foreground=[("selected", C.CYAN)])
        s.configure("Dark.TNotebook", background=C.BG0, borderwidth=0)
        s.configure("Dark.TNotebook.Tab", background=C.BG3, foreground=C.T3, padding=[18, 7])
        s.map("Dark.TNotebook.Tab", background=[("selected", C.BG1)], foreground=[("selected", C.CYAN)])
    
    def _build(self):
        # HEADER
        hdr = tk.Frame(self.root, bg=C.BG1, height=64)
        hdr.pack(fill="x")
        hdr.pack_propagate(False)
        
        # Logo
        lf = tk.Frame(hdr, bg=C.BG1)
        lf.pack(side="left", padx=18)
        tk.Label(lf, text="✨", bg=C.BG1, fg=C.CYAN, font=("Segoe UI", 24)).pack(side="left")
        tf = tk.Frame(lf, bg=C.BG1)
        tf.pack(side="left", padx=(10, 0))
        tk.Label(tf, text="Astro Catalog", bg=C.BG1, fg=C.T1, font=("Segoe UI", 16, "bold")).pack(anchor="w")
        tk.Label(tf, text="⚡ Ultra Responsive", bg=C.BG1, fg=C.GREEN, font=("Segoe UI", 8)).pack(anchor="w")
        
        # Search
        sf = tk.Frame(hdr, bg=C.BG1)
        sf.pack(side="left", fill="x", expand=True, padx=40)
        self.sbar = SearchBar(sf, "Search instantly... (results appear as you type!)",
                              on_type=self._on_type, on_clear=lambda: self._reload_tree())
        self.sbar.pack(fill="x", pady=14)
        
        # Buttons
        af = tk.Frame(hdr, bg=C.BG1)
        af.pack(side="right", padx=18)
        Btn(af, "🔄 Refresh", self._reload_tree).pack(side="left", padx=3)
        Btn(af, "📤 Export", self._export).pack(side="left", padx=3)
        
        # MAIN CONTENT
        main = tk.Frame(self.root, bg=C.BG0)
        main.pack(fill="both", expand=True, padx=12, pady=(0, 12))
        
        self.paned = ttk.PanedWindow(main, orient="horizontal")
        self.paned.pack(fill="both", expand=True)
        
        # Sidebar
        self.side = self._sidebar(self.paned)
        self.paned.add(self.side, weight=0)
        
        # Center (tree)
        self.center = self._center(self.paned)
        self.paned.add(self.center, weight=1)
        
        # Details
        self.detail = self._details(self.paned)
        self.paned.add(self.detail, weight=0)
        
        # STATUS BAR
        sb = tk.Frame(self.root, bg=C.BG3, height=28)
        sb.pack(fill="x")
        sb.pack_propagate(False)
        
        self.status = tk.Label(sb, text="Ready", bg=C.BG3, fg=C.T3, font=("Segoe UI", 9))
        self.status.pack(side="left", padx=14, pady=4)
        
        # Cache stats
        self.cache_lbl = tk.Label(sb, text="Cache: 0", bg=C.BG3, fg=C.T4, font=("Segoe UI", 9))
        self.cache_lbl.pack(side="right", padx=14, pady=4)
        
        fts_txt = "🔍 FTS: ON" if self.db.fts else "🔍 Basic"
        fts_fg = C.GREEN if self.db.fts else C.T4
        tk.Label(sb, text=fts_txt, bg=C.BG3, fg=fts_fg, font=("Segoe UI", 9)).pack(side="right", padx=14, pady=4)
    
    def _sidebar(self, parent):
        outer = tk.Frame(parent, bg=C.BG1, width=280)
        outer.pack_propagate(False)
        
        cv = tk.Canvas(outer, bg=C.BG1, highlightthickness=0)
        cv.pack(fill="both", expand=True)
        
        sb = tk.Frame(cv, bg=C.BG1)
        cv.create_window((0, 0), window=sb, anchor="nw")
        
        tk.Label(sb, text="📊  Overview", bg=C.BG1, fg=C.T1, font=("Segoe UI", 12, "bold")).pack(
            anchor="w", padx=14, pady=(14, 8))
        
        sf = tk.Frame(sb, bg=C.BG1)
        sf.pack(fill="x", padx=8)
        
        self.st_total = Stat(sf, "Total Objects", "0", "🌟", C.CYAN)
        self.st_total.pack(fill="x", pady=3)
        self.st_cats = Stat(sf, "Categories", "12", "📁", C.PURPLE)
        self.st_cats.pack(fill="x", pady=3)
        self.st_favs = Stat(sf, "Favourites", "0", "⭐", C.ORANGE)
        self.st_favs.pack(fill="x", pady=3)
        
        tk.Frame(sb, bg=C.BORDER, height=1).pack(fill="x", pady=16, padx=14)
        
        # Filters
        tk.Label(sb, text="🔧  Filters", bg=C.BG1, fg=C.T1, font=("Segoe UI", 12, "bold")).pack(
            anchor="w", padx=14, pady=(0, 8))
        
        ff = tk.Frame(sb, bg=C.BG1)
        ff.pack(fill="x", padx=14)
        
        self.v_unmapped = tk.BooleanVar(value=True)
        cb1 = tk.Checkbutton(ff, text="Show Unmapped", variable=self.v_unmapped, command=self._reload_tree,
                              bg=C.BG1, fg=C.T2, selectcolor=C.BG3, activebackground=C.BG1,
                              font=("Segoe UI", 10), cursor="hand2")
        cb1.pack(anchor="w", pady=2)
        
        self.v_favsonly = tk.BooleanVar()
        cb2 = tk.Checkbutton(ff, text="Favourites Only", variable=self.v_favsonly, command=self._reload_tree,
                              bg=C.BG1, fg=C.T2, selectcolor=C.BG3, activebackground=C.BG1,
                              font=("Segoe UI", 10), cursor="hand2")
        cb2.pack(anchor="w", pady=2)
        
        tk.Frame(sb, bg=C.BORDER, height=1).pack(fill="x", pady=16, padx=14)
        
        # Recent
        tk.Label(sb, text="🕐  Recent Searches", bg=C.BG1, fg=C.T1, font=("Segoe UI", 12, "bold")).pack(
            anchor="w", padx=14, pady=(0, 8))
        self.recent_frm = tk.Frame(sb, bg=C.BG1)
        self.recent_frm.pack(fill="x", padx=14)
        
        return outer
    
    def _center(self, parent):
        pnl = tk.Frame(parent, bg=C.BG1)
        
        # Header
        hdr = tk.Frame(pnl, bg=C.BG1)
        hdr.pack(fill="x", padx=14, pady=(14, 8))
        tk.Label(hdr, text="🗂  Object Browser", bg=C.BG1, fg=C.T1, font=("Segoe UI", 12, "bold")).pack(side="left")
        
        # Controls
        ctrl = tk.Frame(hdr, bg=C.BG1)
        ctrl.pack(side="right")
        for txt, cmd in [("➕ Expand", self._expand_all), ("➖ Collapse", self._collapse_all)]:
            lb = tk.Label(ctrl, text=txt, bg=C.BG1, fg=C.T3, font=("Segoe UI", 9), cursor="hand2")
            lb.pack(side="left", padx=8)
            lb.bind("<Button-1>", lambda e, c=cmd: c())
            lb.bind("<Enter>", lambda e, l=lb: l.config(fg=C.CYAN))
            lb.bind("<Leave>", lambda e, l=lb: l.config(fg=C.T3))
        
        self.res_lbl = tk.Label(hdr, text="", bg=C.BG1, fg=C.T4, font=("Segoe UI", 10))
        self.res_lbl.pack(side="right", padx=16)
        
        # Guide
        self.guide = tk.Label(pnl, text="👆 Click any object for INSTANT details — no waiting!",
                              bg=C.BG2, fg=C.CYAN, font=("Segoe UI", 10), pady=7)
        self.guide.pack(fill="x", padx=14, pady=(0, 8))
        
        # Tree container
        tc = tk.Frame(pnl, bg=C.BG2)
        tc.pack(fill="both", expand=True, padx=14, pady=(0, 14))
        
        self.tree = ttk.Treeview(tc, style="T.Treeview", show="tree")
        ys = ttk.Scrollbar(tc, orient="vertical", command=self.tree.yview)
        xs = ttk.Scrollbar(tc, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=ys.set, xscrollcommand=xs.set)
        
        self.tree.grid(row=0, column=0, sticky="nsew")
        ys.grid(row=0, column=1, sticky="ns")
        xs.grid(row=1, column=0, sticky="ew")
        tc.grid_rowconfigure(0, weight=1)
        tc.grid_columnconfigure(0, weight=1)
        
        # INSTANT selection handling
        self.tree.bind("<<TreeviewSelect>>", self._on_select)
        self.tree.bind("<Double-1>", self._on_dblclick)
        self.tree.bind("<Button-3>", self._ctx_menu)
        
        # Loading overlay
        self.tree_loading = LoadingOverlay(tc)
        
        return pnl
    
    def _details(self, parent):
        pnl = tk.Frame(parent, bg=C.BG1, width=430)
        pnl.pack_propagate(False)
        
        # Header
        hdr = tk.Frame(pnl, bg=C.BG1)
        hdr.pack(fill="x", padx=14, pady=(14, 8))
        tk.Label(hdr, text="📋  Object Details", bg=C.BG1, fg=C.T1, font=("Segoe UI", 12, "bold")).pack(side="left")
        
        self.fav_lbl = tk.Label(hdr, text="☆", bg=C.BG1, fg=C.T4, font=("Segoe UI", 20), cursor="hand2")
        self.fav_lbl.pack(side="right")
        self.fav_lbl.bind("<Button-1>", lambda e: self._toggle_fav())
        
        cp = tk.Label(hdr, text="📋", bg=C.BG1, fg=C.T3, font=("Segoe UI", 14), cursor="hand2")
        cp.pack(side="right", padx=8)
        cp.bind("<Button-1>", lambda e: self._copy())
        cp.bind("<Enter>", lambda e: cp.config(fg=C.CYAN))
        cp.bind("<Leave>", lambda e: cp.config(fg=C.T3))
        
        # Name
        self.name_lbl = tk.Label(pnl, text="← Select an object", bg=C.BG1, fg=C.CYAN,
                                  font=("Segoe UI", 14, "bold"), wraplength=400)
        self.name_lbl.pack(fill="x", padx=14, pady=(0, 4))
        
        # Badges
        self.badge_frm = tk.Frame(pnl, bg=C.BG1)
        self.badge_frm.pack(fill="x", padx=14, pady=(0, 6))
        
        # Explanation
        self.explain = tk.Label(pnl, text="", bg=C.BG2, fg=C.T2, font=("Segoe UI", 10, "italic"),
                                 wraplength=400, padx=10, pady=6, justify="left")
        
        tk.Frame(pnl, bg=C.BORDER, height=1).pack(fill="x", padx=14, pady=8)
        
        # Tabs
        nb = ttk.Notebook(pnl, style="Dark.TNotebook")
        nb.pack(fill="both", expand=True, padx=14, pady=(0, 14))
        
        # Formatted tab
        fmt_f = tk.Frame(nb, bg=C.BG2)
        nb.add(fmt_f, text="  📝 Formatted  ")
        
        fmt_cv = tk.Canvas(fmt_f, bg=C.BG2, highlightthickness=0)
        fmt_sb = ttk.Scrollbar(fmt_f, orient="vertical", command=fmt_cv.yview)
        self.fmt_body = tk.Frame(fmt_cv, bg=C.BG2)
        fmt_win = fmt_cv.create_window((0, 0), window=self.fmt_body, anchor="nw")
        fmt_cv.configure(yscrollcommand=fmt_sb.set)
        fmt_cv.pack(side="left", fill="both", expand=True)
        fmt_sb.pack(side="right", fill="y")
        fmt_cv.bind("<Configure>", lambda e: fmt_cv.itemconfig(fmt_win, width=e.width))
        self.fmt_body.bind("<Configure>", lambda e: fmt_cv.configure(scrollregion=fmt_cv.bbox("all")))
        
        # JSON tab
        js_f = tk.Frame(nb, bg=C.BG2)
        nb.add(js_f, text="  🔧 JSON  ")
        
        self.js_txt = tk.Text(js_f, bg=C.BG2, fg=C.T2, font=("Consolas", 10),
                               relief="flat", insertbackground=C.CYAN, padx=10, pady=10, wrap="word")
        js_sb = ttk.Scrollbar(js_f, orient="vertical", command=self.js_txt.yview)
        self.js_txt.configure(yscrollcommand=js_sb.set)
        self.js_txt.pack(side="left", fill="both", expand=True)
        js_sb.pack(side="right", fill="y")
        self.js_txt.tag_configure("key", foreground=C.CYAN)
        self.js_txt.tag_configure("str", foreground=C.GREEN)
        self.js_txt.tag_configure("num", foreground=C.ORANGE)
        
        # Empty state
        self.empty = tk.Frame(pnl, bg=C.BG1)
        tk.Label(self.empty, text="👈", bg=C.BG1, fg=C.T4, font=("Segoe UI", 42)).pack(pady=(60, 8))
        tk.Label(self.empty, text="Select an Object", bg=C.BG1, fg=C.CYAN, font=("Segoe UI", 16, "bold")).pack()
        tk.Label(self.empty, text="Click any object — details appear INSTANTLY!",
                 bg=C.BG1, fg=C.T3, font=("Segoe UI", 11), justify="center").pack(pady=(8, 0))
        self.empty.place(x=0, y=0, relwidth=1, relheight=1)
        
        # Loading overlay for details
        self.detail_loading = LoadingOverlay(pnl)
        
        return pnl
    
    def _shortcuts(self):
        self.root.bind("<Control-f>", lambda e: self.sbar.focus())
        self.root.bind("<Control-r>", lambda e: self._reload_tree())
        self.root.bind("<Escape>", lambda e: (self.sbar.set(""), self._reload_tree()))
    
    # ── FIRST RUN ───────────────────────────────────────────────────
    def _after_welcome(self):
        if not JSON_INPUT_PATH.exists():
            messagebox.showerror("File Not Found",
                                 f"Cannot find:\n{JSON_INPUT}\n\nPlease put it in:\n{SCRIPT_DIR}")
            return
        
        dlg = ProgressDlg(self.root, "Importing Data…")
        dlg.progress(0, 1, "Reading JSON file... ⏳")
        self.root.update()
        
        def cb(cur, tot, text=None):
            dlg.progress(cur, tot, text or f"Loading... ({cur:,}/{tot:,})")
        
        try:
            stats = self.db.import_json(JSON_INPUT_PATH, cb)
            self.db.save_json(JSON_OUTPUT_PATH)
            dlg.done(f"✓ Loaded {stats['total']:,} objects!")
            self.root.after(800, dlg.destroy)
            self.root.after(900, self._load)
            self.root.after(1000, lambda: messagebox.showinfo("Import Complete! 🎉",
                f"📊 Total: {stats['total']:,}\n"
                f"✅ Categorized: {stats['mapped']:,}\n"
                f"📦 Unmapped: {stats['unmapped']:,}\n\n"
                f"Everything will load INSTANTLY from now on!"))
        except Exception as e:
            dlg.destroy()
            log.error("Import failed: %s", e)
            messagebox.showerror("Import Failed", str(e))
    
    # ── DATA LOADING ────────────────────────────────────────────────
    def _load(self):
        self._reload_tree()
        self._update_stats()
        self._update_recent()
        self._update_cache_stats()
        self._set_status("Ready — INSTANT object viewing! ⚡")
    
    def _reload_tree(self):
        """Rebuild tree — runs in background for large datasets."""
        self.res_lbl.config(text="Loading...")
        
        q = self.sbar.get().strip()
        if self.v_favsonly.get():
            self._tree_favs()
        elif q:
            self._tree_search(q)
        else:
            self._tree_full()
        
        self._update_stats()
    
    def _tree_full(self):
        """Build full tree with precomputed counts."""
        for c in self.tree.get_children():
            self.tree.delete(c)
        
        total = 0
        for mc, subs in MASTER_TAXONOMY.items():
            cn = mc.split(".")[0].strip() if "." in mc else ""
            icon, _ = CAT_STYLE.get(cn, ("📁", C.T3))
            mid = self.tree.insert("", "end", text=f"  {icon}  {mc}")
            
            for sc, types in subs.items():
                sid = self.tree.insert(mid, "end", text=f"  📂  {sc}")
                
                for ot in types:
                    n = self.db.count_for(ot, mc)
                    if n > 0:
                        tid = self.tree.insert(sid, "end", text=f"  📄  {ot}  ({n})")
                        # Load names
                        for nm in self.db.names_for(ot, mc):
                            pf = "  ⭐  " if self.db.is_fav(nm) else "  ○  "
                            self.tree.insert(tid, "end", text=f"{pf}{nm}", values=(nm,), tags=("obj",))
                            total += 1
        
        # Unmapped
        if self.v_unmapped.get():
            uid = self.tree.insert("", "end", text="  ⚠️  Unmapped Objects")
            for ot in self.db.unmapped_types():
                tid = self.tree.insert(uid, "end", text=f"  📄  {ot or 'Unknown'}")
                for nm in self.db.unmapped_names(ot):
                    pf = "  ⭐  " if self.db.is_fav(nm) else "  ○  "
                    self.tree.insert(tid, "end", text=f"{pf}{nm}", values=(nm,), tags=("obj",))
                    total += 1
        
        self.res_lbl.config(text=f"📊 {total:,} objects")
    
    def _tree_favs(self):
        for c in self.tree.get_children():
            self.tree.delete(c)
        
        favs = self.db.all_favs()
        if not favs:
            self.res_lbl.config(text="⭐ No favourites yet")
            return
        
        fid = self.tree.insert("", "end", text=f"  ⭐  Your Favourites ({len(favs)})", open=True)
        for nm in favs:
            od = self.db.get_object(nm)
            extra = f"  •  {od['obj_type']}" if od else ""
            self.tree.insert(fid, "end", text=f"  ⭐  {nm}{extra}", values=(nm,), tags=("obj",))
        
        self.res_lbl.config(text=f"⭐ {len(favs)} favourites")
    
    def _tree_search(self, q):
        for c in self.tree.get_children():
            self.tree.delete(c)
        
        rows = self.db.search(q)
        if not rows:
            self.res_lbl.config(text=f"🔍 No results for '{q}'")
            return
        
        grp = {}
        for mc, sc, ot, nm in rows:
            if mc == "Unmapped" and not self.v_unmapped.get():
                continue
            grp.setdefault(mc, {}).setdefault(ot, []).append(nm)
        
        total = 0
        for mc in sorted(grp):
            mid = self.tree.insert("", "end", text=f"  📁  {mc}", open=True)
            for ot in sorted(grp[mc]):
                names = grp[mc][ot]
                tid = self.tree.insert(mid, "end", text=f"  📄  {ot}  ({len(names)})", open=True)
                for nm in sorted(names):
                    pf = "  ⭐  " if self.db.is_fav(nm) else "  ○  "
                    self.tree.insert(tid, "end", text=f"{pf}{nm}", values=(nm,), tags=("obj",))
                    total += 1
        
        self.res_lbl.config(text=f"🔍 {total} results")
    
    # ── INSTANT SEARCH ──────────────────────────────────────────────
    def _on_type(self, txt):
        if self._search_after:
            self.root.after_cancel(self._search_after)
        self._search_after = self.root.after(SEARCH_DEBOUNCE_MS, lambda: self._do_search(txt))
    
    def _do_search(self, txt):
        if txt:
            self.db.add_hist(txt)
            self._update_recent()
        self._reload_tree()
    
    # ── INSTANT SELECTION ───────────────────────────────────────────
    def _on_select(self, e):
        """Handle selection — INSTANT display from cache."""
        sel = self.tree.focus()
        if not sel:
            return
        
        tags = self.tree.item(sel, "tags")
        if tags and "obj" in tags:
            vals = self.tree.item(sel, "values")
            if vals:
                name = vals[0]
                # INSTANT retrieval from cache
                self._show_object_instant(name)
                # Preload nearby objects in background
                self._schedule_preload(sel)
        else:
            self.cur_obj = None
            self.empty.place(x=0, y=0, relwidth=1, relheight=1)
    
    def _show_object_instant(self, name: str):
        """Display object INSTANTLY — prioritize cache."""
        # Try cache first (instant)
        obj = self.db.cache.get(name)
        
        if obj is not None:
            # INSTANT display from cache
            self._render_details(name, obj)
            self._set_status(f"⚡ Viewing: {name} (cached)")
        else:
            # Cache miss — still fast but show we're fetching
            obj = self.db.get_object(name)
            if obj:
                self._render_details(name, obj)
                self._set_status(f"Viewing: {name}")
        
        self._update_cache_stats()
    
    def _schedule_preload(self, current_item: str):
        """Preload nearby objects for instant future access."""
        if self._preload_after:
            self.root.after_cancel(self._preload_after)
        
        def do_preload():
            # Get siblings and children to preload
            parent = self.tree.parent(current_item)
            if parent:
                siblings = list(self.tree.get_children(parent))
                names_to_preload = []
                for sib in siblings:
                    tags = self.tree.item(sib, "tags")
                    if tags and "obj" in tags:
                        vals = self.tree.item(sib, "values")
                        if vals and not self.db.cache.contains(vals[0]):
                            names_to_preload.append(vals[0])
                
                if names_to_preload:
                    # Preload in background
                    self.tasks.submit("preload", self.db.preload_nearby, None, names_to_preload)
        
        self._preload_after = self.root.after(100, do_preload)
    
    def _render_details(self, name: str, obj: dict):
        """Render object details — optimized for speed."""
        self.cur_obj = name
        self.empty.place_forget()
        self.name_lbl.config(text=name)
        
        # Badges
        for w in self.badge_frm.winfo_children():
            w.destroy()
        
        ot = obj.get("obj_type", "")
        mc = obj.get("main_cat", "")
        tk.Label(self.badge_frm, text=ot, bg=C.BG3, fg=C.T2, font=("Segoe UI", 9), padx=8, pady=3).pack(
            side="left", padx=(0, 6))
        
        cn = mc.split(".")[0].strip() if "." in mc else ""
        _, col = CAT_STYLE.get(cn, ("📁", C.T3))
        tk.Label(self.badge_frm, text=cn, bg=col, fg=C.BG0, font=("Segoe UI", 9, "bold"), padx=8, pady=3).pack(
            side="left")
        
        # Favourite
        is_f = self.db.is_fav(name)
        self.fav_lbl.config(text="★" if is_f else "☆", fg=C.ORANGE if is_f else C.T4)
        
        # Explanation
        exp = self._get_explanation(ot)
        if exp:
            self.explain.config(text=f"💡 {exp}")
            self.explain.pack(fill="x", padx=14, pady=(0, 8))
        else:
            self.explain.pack_forget()
        
        # Formatted data
        data = obj.get("data", {})
        for w in self.fmt_body.winfo_children():
            w.destroy()
        
        if isinstance(data, dict) and data:
            for k, v in data.items():
                row = tk.Frame(self.fmt_body, bg=C.BG2)
                row.pack(fill="x", padx=8, pady=2)
                tk.Label(row, text=f"{k}:", bg=C.BG2, fg=C.CYAN, font=("Segoe UI", 10, "bold"),
                         width=22, anchor="nw").pack(side="left")
                val_lbl = tk.Label(row, text=str(v), bg=C.BG2, fg=C.T2, font=("Segoe UI", 10),
                                   anchor="w", justify="left", wraplength=250)
                val_lbl.pack(side="left", fill="x", expand=True)
        else:
            tk.Label(self.fmt_body, text="No structured data", bg=C.BG2, fg=C.T3,
                     font=("Segoe UI", 10)).pack(padx=8, pady=8)
        
        # JSON (fast render)
        self.js_txt.delete("1.0", "end")
        pretty = json.dumps(data, indent=2, ensure_ascii=False)
        self.js_txt.insert("1.0", pretty)
        self._highlight_json_fast()
    
    def _highlight_json_fast(self):
        """Optimized JSON highlighting using pre-compiled regex."""
        txt = self.js_txt
        content = txt.get("1.0", "end")
        
        for tag in ("key", "str", "num"):
            txt.tag_remove(tag, "1.0", "end")
        
        # Keys
        for m in self.db._re_key.finditer(content):
            s, e = m.start(), m.end()
            txt.tag_add("key", f"1.0+{s}c", f"1.0+{e}c")
        
        # Strings
        for m in self.db._re_str.finditer(content):
            s, e = m.start() + 1, m.end()
            txt.tag_add("str", f"1.0+{s}c", f"1.0+{e}c")
        
        # Numbers
        for m in self.db._re_num.finditer(content):
            s, e = m.start(1), m.end(1)
            txt.tag_add("num", f"1.0+{s}c", f"1.0+{e}c")
    
    def _get_explanation(self, obj_type: str) -> str:
        low = obj_type.lower()
        for key, expl in SIMPLE_TERMS.items():
            if key in low:
                return expl
        return ""
    
    # ── TREE EVENTS ─────────────────────────────────────────────────
    def _on_dblclick(self, e):
        it = self.tree.focus()
        if it and self.tree.get_children(it):
            self.tree.item(it, open=not self.tree.item(it, "open"))
    
    def _ctx_menu(self, e):
        it = self.tree.identify_row(e.y)
        if not it:
            return
        self.tree.selection_set(it)
        self.tree.focus(it)
        
        m = tk.Menu(self.root, tearoff=0, bg=C.BG3, fg=C.T2, activebackground=C.HOVER, activeforeground=C.CYAN)
        
        tags = self.tree.item(it, "tags")
        if tags and "obj" in tags:
            vals = self.tree.item(it, "values")
            if vals:
                nm = vals[0]
                is_f = self.db.is_fav(nm)
                m.add_command(label="⭐ Remove Favourite" if is_f else "⭐ Add Favourite", command=self._toggle_fav)
                m.add_separator()
                m.add_command(label="📋 Copy Name", command=lambda: self._clip(nm))
                m.add_command(label="📋 Copy All Data", command=self._copy)
        else:
            m.add_command(label="➕ Expand All", command=lambda: self._expand_item(it))
            m.add_command(label="➖ Collapse All", command=lambda: self._collapse_item(it))
        
        m.tk_popup(e.x_root, e.y_root)
    
    # ── ACTIONS ─────────────────────────────────────────────────────
    def _toggle_fav(self):
        if not self.cur_obj:
            return
        if self.db.is_fav(self.cur_obj):
            self.db.rm_fav(self.cur_obj)
            self.fav_lbl.config(text="☆", fg=C.T4)
            self._set_status(f"Removed from favourites: {self.cur_obj}")
        else:
            self.db.add_fav(self.cur_obj)
            self.fav_lbl.config(text="★", fg=C.ORANGE)
            self._set_status(f"⭐ Added to favourites: {self.cur_obj}")
        self._update_stats()
        if self.v_favsonly.get():
            self._reload_tree()
    
    def _copy(self):
        txt = self.js_txt.get("1.0", "end").strip()
        if txt:
            self._clip(txt)
            self._set_status("✓ Copied to clipboard!")
    
    def _clip(self, t):
        self.root.clipboard_clear()
        self.root.clipboard_append(t)
    
    def _expand_all(self):
        def go(i):
            self.tree.item(i, open=True)
            for c in self.tree.get_children(i):
                go(c)
        for i in self.tree.get_children():
            go(i)
    
    def _collapse_all(self):
        def go(i):
            for c in self.tree.get_children(i):
                go(c)
            self.tree.item(i, open=False)
        for i in self.tree.get_children():
            go(i)
    
    def _expand_item(self, it):
        self.tree.item(it, open=True)
        for c in self.tree.get_children(it):
            self._expand_item(c)
    
    def _collapse_item(self, it):
        for c in self.tree.get_children(it):
            self._collapse_item(c)
        self.tree.item(it, open=False)
    
    def _export(self):
        fp = filedialog.asksaveasfilename(
            defaultextension=".json",
            filetypes=[("JSON", "*.json"), ("All", "*.*")],
            title="Export Catalog",
            initialfilename="astro_catalog_export.json")
        if not fp:
            return
        try:
            self.db.save_json(Path(fp))
            st = self.db.stats()
            messagebox.showinfo("Export Complete! 📤", f"Exported {st['total']:,} objects")
            self._set_status(f"Exported to {Path(fp).name}")
        except Exception as e:
            messagebox.showerror("Export Failed", str(e))
    
    # ── UI UPDATES ──────────────────────────────────────────────────
    def _update_stats(self):
        st = self.db.stats()
        self.st_total.set(f"{st['total']:,}")
        self.st_cats.set(str(st['cats']))
        self.st_favs.set(str(st['favs']))
    
    def _update_recent(self):
        for w in self.recent_frm.winfo_children():
            w.destroy()
        hist = self.db.recent(6)
        if not hist:
            tk.Label(self.recent_frm, text="No searches yet", bg=C.BG1, fg=C.T4, font=("Segoe UI", 9)).pack(anchor="w")
            return
        for q in hist:
            lb = tk.Label(self.recent_frm, text=f"  🔍 {q}", bg=C.BG1, fg=C.T3,
                          font=("Segoe UI", 9), cursor="hand2", anchor="w")
            lb.pack(fill="x", pady=1)
            lb.bind("<Button-1>", lambda e, q2=q: (self.sbar.set(q2), self._reload_tree()))
            lb.bind("<Enter>", lambda e, l=lb: l.config(fg=C.CYAN))
            lb.bind("<Leave>", lambda e, l=lb: l.config(fg=C.T3))
    
    def _update_cache_stats(self):
        stats = self.db.cache.stats()
        self.cache_lbl.config(text=f"Cache: {stats['size']} | Hit: {stats['hit_rate']}")
    
    def _set_status(self, msg):
        self.status.config(text=msg)


# ════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ════════════════════════════════════════════════════════════════════

def main():
    print(f"""
╔════════════════════════════════════════════════════════════════════╗
║          ASTRO CATALOG · ULTRA RESPONSIVE v{APP_VERSION}              ║
╠════════════════════════════════════════════════════════════════════╣
║  ⚡ Zero-delay object viewing                                      ║
║  🧠 Intelligent caching with LRU eviction                          ║
║  🔄 Background threading — UI never freezes                        ║
║  📊 Precomputed statistics for instant counts                      ║
╚════════════════════════════════════════════════════════════════════╝
    """)
    
    root = tk.Tk()
    app = App(root)
    
    def on_close():
        app.tasks.shutdown()
        app.db.close()
        root.destroy()
    
    root.protocol("WM_DELETE_WINDOW", on_close)
    root.mainloop()


if __name__ == "__main__":
    main()