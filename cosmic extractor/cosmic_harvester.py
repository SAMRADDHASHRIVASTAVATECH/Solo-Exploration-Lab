# -*- coding: utf-8 -*-
"""
SpaceEngine Complete Data Extractor v24.1 - Scientific Workspace Edition
FEATURES (Legacy & Maxed Out):
- [NEW] SEPARATED DOMAINS: Read-Only Scientific Catalog vs. Export Workspace.
- [NEW] 15-TIER CATEGORIZATION: Structured property mapping. Hides empty panels.
- [NEW] TERRAIN PREVIEWS: Auto-detects and displays Cube/Cylindrical maps in Workspace.
- [NEW] CATALOG SAFEGUARDS: Copy raw data functionality. No exports, no edits.
- [UPDATED] WATCHDOG: Strictly monitors the SE 'export' folder for real-time and existing files.
- ITERATIVE PARSER: No recursion limits, handles any nesting depth. ZERO DATA LOSS.
- SQLite Database with WAL mode and ultra-fast indexing.
"""
from __future__ import annotations
import os, sys, re, gc, json, csv, time, sqlite3, math, glob
import zipfile, threading, traceback, hashlib
import html as html_module
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple, Set
from dataclasses import dataclass, field
from datetime import datetime
from collections import OrderedDict, Counter, defaultdict
from enum import Enum, auto
from concurrent.futures import ThreadPoolExecutor
from queue import Queue, Empty
import tkinter as tk
from tkinter import ttk, filedialog, messagebox, scrolledtext

# Graceful degradation for Terrain Image rendering
try:
    from PIL import Image, ImageTk
    HAS_PIL = True
except ImportError:
    HAS_PIL = False

# Increase recursion limit as safety net
sys.setrecursionlimit(50000)

###############################################################################
# CONFIGURATION
###############################################################################

if getattr(sys, 'frozen', False):
    _BASE_DIR = os.path.dirname(sys.executable)
else:
    _BASE_DIR = os.path.dirname(os.path.abspath(__file__))

@dataclass(frozen=True)
class Config:
    APP_NAME: str = "SpaceEngine Extractor v24.1 (Workspace Edition)"
    OUT_DIR: str = os.path.join(_BASE_DIR, "Planets Data")
    DB_NAME: str = "SpaceEngine_Index.db"
    CATALOG_DIR: str = "CatalogObjects"
    LIVE_DIR: str = "WorkspaceExports"
    BATCH_DIR: str = "BatchExports"
    LOG_DIR: str = "Logs"
    MAX_WORKERS: int = 8
    BATCH_SIZE: int = 500
    PAGE_SIZE: int = 500
    DEBOUNCE_MS: int = 200
    WATCHDOG_INTERVAL: float = 1.5
    FILE_EXTENSIONS: Tuple[str, ...] = (
        '.sc', '.ssc', '.cfg', '.txt', '.ini', '.dat', '.cat'
    )
    ENCODINGS: Tuple[str, ...] = (
        'utf-8', 'utf-8-sig', 'latin-1', 'cp1252', 'iso-8859-1', 'ascii'
    )

    @classmethod
    def setup(cls):
        base = Path(cls.OUT_DIR)
        families = ['Stars', 'Planets', 'Moons', 'Asteroids', 'Comets',
                    'Galaxies', 'Nebulae', 'BlackHoles', 'Systems', 'Other']
        for d in [cls.CATALOG_DIR, cls.LIVE_DIR, cls.BATCH_DIR, cls.LOG_DIR]:
            (base / d).mkdir(parents=True, exist_ok=True)
        for cat in [cls.CATALOG_DIR, cls.LIVE_DIR]:
            for fam in families:
                (base / cat / fam).mkdir(parents=True, exist_ok=True)

CFG = Config()
CFG.setup()

###############################################################################
# LOGGING
###############################################################################

class Log:
    _file = None
    _lock = threading.Lock()

    @classmethod
    def init(cls):
        if cls._file: return
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        cls._file = Path(CFG.OUT_DIR) / CFG.LOG_DIR / f"log_{ts}.log"

    @classmethod
    def write(cls, msg: str, level: str = "INFO"):
        if not cls._file: cls.init()
        ts = datetime.now().strftime("%H:%M:%S")
        line = f"{ts}|{level}|{msg}"
        with cls._lock:
            try:
                with open(cls._file, 'a', encoding='utf-8') as f:
                    f.write(line + "\n")
            except: pass
        print(line)

    @classmethod
    def info(cls, msg): cls.write(msg, "INFO")
    @classmethod
    def error(cls, msg): cls.write(msg, "ERROR")
    @classmethod
    def discovery(cls, msg): cls.write(msg, "WORKSPACE")

###############################################################################
# INTELLIGENT CLASSIFICATION ENGINE & PROPERTY CATEGORIZATION
###############################################################################

class ObjectFamily(Enum):
    STAR = "Stars"
    PLANET = "Planets"
    MOON = "Moons"
    ASTEROID = "Asteroids"
    COMET = "Comets"
    GALAXY = "Galaxies"
    NEBULA = "Nebulae"
    BLACK_HOLE = "BlackHoles"
    SYSTEM = "Systems"
    OTHER = "Other"

FAMILY_KEYWORDS = {
    ObjectFamily.BLACK_HOLE: {'blackhole', 'neutronstar', 'pulsar', 'magnetar', 'quasar'},
    ObjectFamily.GALAXY: {'galaxy', 'cluster', 'supercluster', 'globcluster', 'opencluster'},
    ObjectFamily.NEBULA: {'nebula', 'remnant', 'hiiregion', 'diffuseneb', 'darkneb', 'planetaryneb', 'snremnant'},
    ObjectFamily.SYSTEM: {'barycenter', 'starsystem', 'binary'},
    ObjectFamily.STAR: {'star', 'whitedwarf', 'browndwarf', 'giant', 'blazar', 'agn'},
    ObjectFamily.MOON: {'moon', 'satellite', 'dwarfmoon', 'selena', 'titan'},
    ObjectFamily.ASTEROID: {'asteroid', 'trojan', 'centaur', 'kbo', 'tno', 'dwarfplanet'},
    ObjectFamily.COMET: {'comet'},
    ObjectFamily.PLANET: {'planet', 'terra', 'gasgiant', 'icegiant', 'superearth', 'minineptune', 'hotjupiter', 'subbrowndwarf'},
}

FAMILY_ICONS = {
    ObjectFamily.STAR: "☀", ObjectFamily.PLANET: "🌍", ObjectFamily.MOON: "🌙",
    ObjectFamily.ASTEROID: "🪨", ObjectFamily.COMET: "☄️", ObjectFamily.GALAXY: "🌌",
    ObjectFamily.NEBULA: "🌫️", ObjectFamily.BLACK_HOLE: "🌀", ObjectFamily.SYSTEM: "⚙️",
    ObjectFamily.OTHER: "❓",
}

# 15-Tier Scientific Categories Logic
PROPERTY_CATEGORIES = OrderedDict([
    ("Identity & General", ['name', 'type', 'class', 'locname', 'discoverytime']),
    ("Hierarchy & System", ['parent', 'parentbody', 'parentstar', 'barycenter']),
    ("Orbit & Motion", ['orbit', 'semimajoraxis', 'eccentricity', 'inclination', 'ascendingnode', 'argofpericenter', 'meananomaly', 'period', 'rotationperiod', 'obliquity']),
    ("Physical Properties", ['mass', 'radius', 'density', 'gravity', 'oblateness', 'age']),
    ("Atmosphere", ['atmosphere', 'pressure', 'greenhouse', 'model', 'skycolor', 'composition']),
    ("Clouds & Haze", ['cloud', 'haze', 'velocity', 'coverage']),
    ("Surface & Terrain", ['surface', 'terrain', 'heightmap', 'crater', 'volcanism', 'tectonics', 'color']),
    ("Hydrosphere & Oceans", ['ocean', 'liquid', 'sealevel', 'hydrosphere']),
    ("Rings", ['ring', 'innerradius', 'outerradius', 'texture']),
    ("Thermal & Climate", ['temperature', 'teff', 'climate', 'albedo']),
    ("Life & Habitability", ['life', 'organic', 'exotic', 'biome', 'habitability']),
    ("Stellar Emissions", ['luminosity', 'spectrum', 'corona', 'chromosphere', 'flare']),
    ("Galaxies & Nebulae", ['hubble', 'dimension', 'brightness', 'bulge', 'halo', 'arms']),
    ("Rendering & Materials", ['material', 'rendering', 'texture', 'bump', 'specular', 'glow']),
    ("Miscellaneous", [])
])

def classify_object(obj_type: str, obj_class: str = "") -> ObjectFamily:
    combined = (obj_type + obj_class).lower().replace(' ', '').replace('-', '')
    for fam in [ObjectFamily.BLACK_HOLE, ObjectFamily.NEBULA, ObjectFamily.GALAXY,
                ObjectFamily.SYSTEM, ObjectFamily.COMET, ObjectFamily.ASTEROID,
                ObjectFamily.MOON, ObjectFamily.STAR, ObjectFamily.PLANET]:
        for kw in FAMILY_KEYWORDS.get(fam, set()):
            if kw in combined:
                return fam
    return ObjectFamily.OTHER

def categorize_properties(fields: OrderedDict) -> OrderedDict:
    grouped = OrderedDict((k, OrderedDict()) for k in PROPERTY_CATEGORIES.keys())
    for k, v in fields.items():
        k_lower = k.lower().replace('_', '')
        placed = False
        for cat, keywords in PROPERTY_CATEGORIES.items():
            if cat == "Miscellaneous": continue
            if any(kw in k_lower for kw in keywords):
                grouped[cat][k] = v
                placed = True
                break
        if not placed:
            grouped["Miscellaneous"][k] = v
            
    # Remove entirely empty categories
    return OrderedDict((k, v) for k, v in grouped.items() if len(v) > 0)

###############################################################################
# CELESTIAL OBJECT MODEL
###############################################################################

class ObjectOrigin(Enum):
    CATALOG = "Catalog"
    WORKSPACE = "Workspace"

@dataclass
class CelestialObject:
    name: str = ""
    object_type: str = ""
    object_class: str = ""
    parent: str = ""
    family: ObjectFamily = ObjectFamily.OTHER
    origin: ObjectOrigin = ObjectOrigin.CATALOG
    source_file: str = ""
    source_pak: str = ""
    raw_block: str = ""
    all_fields: OrderedDict = field(default_factory=OrderedDict)
    discovery_time: str = ""
    file_hash: str = ""
    saved_json_path: str = ""
    saved_sc_path: str = ""
    
    has_life: bool = False
    habitability_score: float = 0.0
    mass_earth: float = 0.0
    radius_km: float = 0.0
    temp_k: float = 0.0

    def add_field(self, key: str, value: Any, context: str = ""):
        base_key = f"{context}.{key}" if context else key
        full_key = base_key
        
        counter = 1
        while full_key in self.all_fields:
            full_key = f"{base_key}_{counter}"
            counter += 1
            
        self.all_fields[full_key] = value
        
        kl = key.lower()
        if kl in ('name', 'locname') and not self.name:
            self.name = str(value).strip('"\'')
        elif kl in ('parent', 'parentbody', 'parentstar'):
            self.parent = str(value).strip('"\'')
        elif kl in ('type', 'bodytype') and not self.object_type:
            self.object_type = str(value).strip('"\'')
        elif kl in ('class', 'planetclass', 'startype'):
            self.object_class = str(value).strip('"\'')

    def finalize(self):
        self.family = classify_object(self.object_type, self.object_class)
        if not self.discovery_time:
            self.discovery_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if not self.file_hash and self.raw_block:
            self.file_hash = hashlib.sha256(self.raw_block.encode()).hexdigest()[:32]
        self._run_analytics()

    def _run_analytics(self):
        for k, v in self.all_fields.items():
            kl = k.lower()
            vs = str(v).lower()
            if 'life' in kl and ('organic' in vs or 'exotic' in vs or 'true' in vs):
                self.has_life = True; self.habitability_score += 50.0
            if kl == 'teff' or kl == 'temperature':
                try: self.temp_k = float(re.sub(r'[^\d.]', '', vs))
                except: pass
            if kl == 'radius':
                try: self.radius_km = float(re.sub(r'[^\d.]', '', vs))
                except: pass
            if kl == 'mass':
                try: self.mass_earth = float(re.sub(r'[^\d.]', '', vs))
                except: pass

        if self.family == ObjectFamily.PLANET:
            if 270 <= self.temp_k <= 320: self.habitability_score += 25.0
            if 0.5 <= self.mass_earth <= 2.5: self.habitability_score += 15.0
            if any('atmosphere' in k.lower() for k in self.all_fields): self.habitability_score += 10.0

    def to_dict(self) -> dict:
        return {
            'name': self.name, 'type': self.object_type, 'class': self.object_class,
            'parent': self.parent, 'family': self.family.value, 'origin': self.origin.value,
            'discovery_time': self.discovery_time, 'source': self.source_file,
            'source_pak': self.source_pak, 'field_count': len(self.all_fields),
            'has_life': self.has_life, 'habitability_score': self.habitability_score,
            'all_fields': dict(self.all_fields), 'raw_block': self.raw_block,
        }

    def to_flat(self) -> OrderedDict:
        r = OrderedDict()
        r['Name'] = self.name; r['Type'] = self.object_type; r['Class'] = self.object_class
        r['Parent'] = self.parent; r['Family'] = self.family.value; r['Origin'] = self.origin.value
        r['DiscoveryTime'] = self.discovery_time; r['FieldCount'] = len(self.all_fields)
        r['HasLife'] = self.has_life; r['HabitabilityScore'] = self.habitability_score
        r['Source'] = self.source_file
        for k, v in self.all_fields.items():
            if isinstance(v, (list, tuple)): r[k] = ", ".join(str(x) for x in v)
            elif isinstance(v, dict): r[k] = json.dumps(v, default=str)
            else: r[k] = str(v) if v is not None else ""
        return r

###############################################################################
# DATABASE
###############################################################################

class Database:
    def __init__(self, db_path: str):
        self.path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(db_path, check_same_thread=False, isolation_level=None)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=OFF")
        self.conn.execute("PRAGMA cache_size=50000")
        self._lock = threading.Lock()
        self._create_schema()

    def _create_schema(self):
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS objects (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                obj_type TEXT, obj_class TEXT, parent TEXT,
                family TEXT, origin TEXT DEFAULT 'Catalog',
                field_count INTEGER DEFAULT 0, raw_lines INTEGER DEFAULT 0,
                source_file TEXT, source_pak TEXT, discovery_time TEXT,
                file_hash TEXT, json_path TEXT, sc_path TEXT,
                all_fields_json TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_name ON objects(name);
            CREATE INDEX IF NOT EXISTS idx_family ON objects(family);
            CREATE INDEX IF NOT EXISTS idx_origin ON objects(origin);
            CREATE INDEX IF NOT EXISTS idx_hash ON objects(file_hash);
            
            CREATE TABLE IF NOT EXISTS stats (
                id INTEGER PRIMARY KEY, total_files INTEGER DEFAULT 0,
                total_paks INTEGER DEFAULT 0, total_objects INTEGER DEFAULT 0,
                total_fields INTEGER DEFAULT 0
            );
            INSERT OR IGNORE INTO stats (id) VALUES (1);
        """)

        existing_cols = {row[1] for row in self.conn.execute("PRAGMA table_info(objects)").fetchall()}
        analytics_cols = {
            "has_life": "INTEGER DEFAULT 0",
            "habitability_score": "REAL DEFAULT 0.0",
            "mass": "REAL DEFAULT 0.0",
            "radius": "REAL DEFAULT 0.0",
            "temp": "REAL DEFAULT 0.0"
        }
        
        for col, ctype in analytics_cols.items():
            if col not in existing_cols:
                try: self.conn.execute(f"ALTER TABLE objects ADD COLUMN {col} {ctype}")
                except sqlite3.OperationalError: pass

        self.conn.executescript("""
            CREATE INDEX IF NOT EXISTS idx_parent ON objects(parent);
            CREATE INDEX IF NOT EXISTS idx_life ON objects(has_life);
        """)

    def insert_object(self, obj: CelestialObject) -> int:
        raw_lines = len(obj.raw_block.split('\n')) if obj.raw_block else 0
        with self._lock:
            try:
                cur = self.conn.execute("""
                    INSERT INTO objects (
                        name, obj_type, obj_class, parent, family, origin,
                        field_count, raw_lines, has_life, habitability_score,
                        mass, radius, temp, source_file, source_pak,
                        discovery_time, file_hash, json_path, sc_path, all_fields_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    obj.name, obj.object_type, obj.object_class, obj.parent,
                    obj.family.value, obj.origin.value, len(obj.all_fields),
                    raw_lines, int(obj.has_life), obj.habitability_score,
                    obj.mass_earth, obj.radius_km, obj.temp_k,
                    obj.source_file, obj.source_pak, obj.discovery_time,
                    obj.file_hash, obj.saved_json_path, obj.saved_sc_path,
                    json.dumps(dict(obj.all_fields), default=str)
                ))
                return cur.lastrowid
            except: return -1

    def insert_batch(self, objects: List[CelestialObject]):
        with self._lock:
            self.conn.execute("BEGIN TRANSACTION")
            try:
                for obj in objects:
                    raw_lines = len(obj.raw_block.split('\n')) if obj.raw_block else 0
                    self.conn.execute("""
                        INSERT INTO objects (
                            name, obj_type, obj_class, parent, family, origin,
                            field_count, raw_lines, has_life, habitability_score,
                            mass, radius, temp, source_file, source_pak,
                            discovery_time, file_hash, json_path, sc_path, all_fields_json
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        obj.name, obj.object_type, obj.object_class, obj.parent,
                        obj.family.value, obj.origin.value, len(obj.all_fields),
                        raw_lines, int(obj.has_life), obj.habitability_score,
                        obj.mass_earth, obj.radius_km, obj.temp_k,
                        obj.source_file, obj.source_pak, obj.discovery_time,
                        obj.file_hash, obj.saved_json_path, obj.saved_sc_path,
                        json.dumps(dict(obj.all_fields), default=str)
                    ))
                self.conn.execute("COMMIT")
            except Exception as e:
                self.conn.execute("ROLLBACK")
                Log.error(f"Batch insert failed: {e}")

    def update_stats(self, files: int, paks: int, objects: int, fields: int):
        with self._lock:
            self.conn.execute("""UPDATE stats SET total_files = total_files + ?, 
                                 total_paks = total_paks + ?, total_objects = total_objects + ?, 
                                 total_fields = total_fields + ? WHERE id=1""",
                              (files, paks, objects, fields))

    def get_stats(self) -> dict:
        row = self.conn.execute("SELECT * FROM stats WHERE id=1").fetchone()
        return {'files': row[1], 'paks': row[2], 'objects': row[3], 'fields': row[4]} if row else {}

    def hash_exists(self, file_hash: str) -> bool:
        return self.conn.execute("SELECT 1 FROM objects WHERE file_hash=? LIMIT 1", (file_hash,)).fetchone() is not None

    def count(self, origin: str = None, family: str = None) -> int:
        q = "SELECT COUNT(*) FROM objects WHERE 1=1"
        params = []
        if origin: q += " AND origin=?"; params.append(origin)
        if family and family != "All Families": q += " AND family=?"; params.append(family)
        return self.conn.execute(q, params).fetchone()[0]

    def fetch_page(self, search: str = "", origin: str = None, family: str = None, offset: int = 0, limit: int = 500) -> List[tuple]:
        q = """SELECT id, name, family, obj_type, obj_class, origin, field_count, 
                      raw_lines, discovery_time, json_path, source_file 
               FROM objects WHERE 1=1"""
        params = []
        if origin: q += " AND origin=?"; params.append(origin)
        if family and family != "All Families": q += " AND family=?"; params.append(family)
        if search: q += " AND name LIKE ?"; params.append(f"%{search}%")
        q += f" ORDER BY id DESC LIMIT {limit} OFFSET {offset}"
        return self.conn.execute(q, params).fetchall()

    def get_by_id(self, obj_id: int) -> Optional[CelestialObject]:
        row = self.conn.execute("""
            SELECT name, obj_type, obj_class, parent, family, origin, source_file,
                   source_pak, discovery_time, json_path, sc_path, all_fields_json,
                   has_life, habitability_score
            FROM objects WHERE id=?
        """, (obj_id,)).fetchone()
        if not row: return None
        
        obj = CelestialObject()
        (obj.name, obj.object_type, obj.object_class, obj.parent, fam, orig,
         obj.source_file, obj.source_pak, obj.discovery_time,
         obj.saved_json_path, obj.saved_sc_path, fields_json, hl, hs) = row
         
        obj.family = ObjectFamily(fam); obj.origin = ObjectOrigin(orig)
        obj.has_life = bool(hl); obj.habitability_score = hs
        try: obj.all_fields = OrderedDict(json.loads(fields_json) if fields_json else {})
        except: pass
        if obj.saved_sc_path and Path(obj.saved_sc_path).exists():
            try: obj.raw_block = Path(obj.saved_sc_path).read_text(encoding='utf-8')
            except: pass
        return obj

    def close(self):
        try: self.conn.close()
        except: pass

###############################################################################
# NOISED DATA MANAGEMENT & EXPORTERS
###############################################################################

def _clean_str(text: Any) -> str:
    """NOISED DATA MANAGEMENT: Strips hidden control characters that corrupt CSV/JSON/MD exports"""
    if text is None: return ""
    return re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]', '', str(text)).strip()

def export_csv(objects: List[CelestialObject], path: Path):
    if not objects: return
    keys = OrderedDict()
    for o in objects:
        for k in o.to_flat().keys(): keys[k] = None
    with open(path, 'w', newline='', encoding='utf-8-sig') as f:
        w = csv.DictWriter(f, list(keys.keys()), extrasaction='ignore')
        w.writeheader()
        for o in objects:
            clean_dict = {k: _clean_str(v) for k, v in o.to_flat().items()}
            w.writerow(clean_dict)

def export_json(objects: List[CelestialObject], path: Path):
    with open(path, 'w', encoding='utf-8') as f:
        json.dump({
            'count': len(objects),
            'objects': [{k: _clean_str(v) if isinstance(v, str) else v for k, v in o.to_dict().items()} for o in objects]
        }, f, indent=2, ensure_ascii=False, default=str)

def export_html(objects: List[CelestialObject], path: Path):
    E = html_module.escape
    with open(path, 'w', encoding='utf-8') as f:
        f.write('<!DOCTYPE html><html><head><meta charset="UTF-8"><title>Export</title><style>body{font-family:Arial;background:#080818;color:#e0e8ff;padding:20px} table{width:100%;border-collapse:collapse;margin-bottom:30px} th{background:#1a1a3a;color:#5588ff;padding:6px;text-align:left} td{padding:5px;border:1px solid #2a2a4a}</style></head><body>\n')
        for o in objects:
            f.write(f'<h2>{E(o.name)} ({o.family.value})</h2><table><tr><th>Field</th><th>Value</th></tr>\n')
            for k, v in o.to_flat().items():
                f.write(f'<tr><td>{E(str(k))}</td><td>{E(_clean_str(v))}</td></tr>\n')
            f.write('</table>\n')
        f.write('</body></html>')

def export_md(objects: List[CelestialObject], path: Path):
    with open(path, 'w', encoding='utf-8') as f:
        f.write(f"# SpaceEngine Data Export\n\nTotal Objects: {len(objects)}\n\n---\n\n")
        for o in objects:
            f.write(f"## {o.name} ({o.family.value})\n\n| Field | Value |\n|---|---|\n")
            for k, v in o.to_flat().items():
                val_str = _clean_str(v).replace('\n', '<br>').replace('|', '\\|')
                f.write(f"| {k} | {val_str} |\n")
            f.write("\n---\n\n")

def export_txt(objects: List[CelestialObject], path: Path):
    with open(path, 'w', encoding='utf-8') as f:
        for o in objects:
            f.write(f"{'='*70}\n {o.name} ({o.family.value})\n{'='*70}\n")
            for k, v in o.to_flat().items():
                f.write(f" {k} = {_clean_str(v)}\n")
            f.write("\n")

EXPORTERS = {
    'csv': ('.csv', export_csv),
    'json': ('.json', export_json),
    'html': ('.html', export_html),
    'md': ('.md', export_md),
    'text': ('.txt', export_txt),
}

###############################################################################
# PARSER - FULLY ITERATIVE (NO RECURSION, ZERO DATA LOSS)
###############################################################################

class IterativeParser:
    OBJECT_TYPES = {
        'Star', 'Planet', 'Moon', 'DwarfMoon', 'Asteroid', 'Comet', 'DwarfPlanet',
        'Galaxy', 'Nebula', 'Barycenter', 'BlackHole', 'NeutronStar', 'Pulsar',
        'Magnetar', 'WhiteDwarf', 'BrownDwarf', 'GasGiant', 'IceGiant', 'Terra',
        'Selena', 'Titan', 'KBO', 'TNO', 'Spacecraft', 'GlobCluster', 'OpenCluster',
        'Quasar', 'Modify', 'Replace', 'Add', 'Remove', 'Object', 'RefObject',
        'Ship', 'Station', 'Probe', 'Satellite', 'Blazar', 'AGN', 'StarCluster',
        'Centaur', 'Trojan', 'Location', 'Bookmark', 'PObject', 'RObject',
        'DiffuseNeb', 'DarkNeb', 'PlanetaryNeb', 'SNRemnant', 'CustomObject',
        'SuperEarth', 'MiniNeptune', 'HotJupiter', 'SubBrownDwarf',
    }

    SECTION_NAMES = {
        'Orbit', 'BinaryOrbit', 'Atmosphere', 'Clouds', 'CloudLayer', 'Haze',
        'Ocean', 'Surface', 'Terrain', 'TerrainParams', 'Interior', 'Rings',
        'Ring', 'RingParams', 'Rotation', 'Life', 'Corona', 'Chromosphere',
        'AccretionDisk', 'Jet', 'Magnetosphere', 'CometTail', 'Tail', 'Coma',
        'Composition', 'Lensing', 'Disk', 'Bulge', 'Halo', 'Arms', 'Climate',
        'Volcanism', 'Tectonics', 'Craters', 'Material', 'Rendering', 'Texture',
        'Model', 'CustomData', 'Info', 'Description', 'Aurora', 'Biome',
        'General', 'Physics', 'Hydrosphere', 'Geology'
    }

    def __init__(self):
        self._re_kv = re.compile(r'^([A-Za-z_][\w.-]*)\s+(.+)$')
        self._re_header = re.compile(r'^([A-Za-z_]\w*)\s*(?:"([^"]*)")?\s*(\{)?\s*(?://.*)?$')
        self._re_include = re.compile(r'^(Include|Remove)\s+"([^"]*)"', re.I)

    def parse(self, text: str, source: str, pak: str = "", origin: ObjectOrigin = ObjectOrigin.CATALOG) -> List[CelestialObject]:
        if not text or not text.strip(): return []
        text = text.lstrip('\ufeff\xef\xbb\xbf').replace('\r\n', '\n').replace('\r', '\n')
        text = re.sub(r'/\*.*?\*/', '', text, flags=re.DOTALL)
        lines = text.split('\n')
        objects, i, n = [], 0, len(lines)
        
        while i < n:
            line = lines[i].strip()
            if not line or line.startswith('//') or line.startswith('#') or self._re_include.match(line):
                i += 1; continue
            
            m = self._re_header.match(line)
            if m:
                obj_type = m.group(1)
                is_obj = (obj_type in self.OBJECT_TYPES or (obj_type[0].isupper() and len(obj_type) > 2 and obj_type not in self.SECTION_NAMES))
                if is_obj:
                    block, end = self._extract_block(lines, i, n)
                    if block:
                        obj = self._parse_object(block, source, pak, obj_type, m.group(2) or "")
                        if obj:
                            obj.origin = origin
                            obj.finalize()
                            objects.append(obj)
                    i = end; continue
            i += 1
        return objects

    def _extract_block(self, lines: List[str], start: int, total: int) -> Tuple[List[str], int]:
        block, depth, i = [lines[start]], re.sub(r'(//|#).*$', '', lines[start]).count('{') - re.sub(r'(//|#).*$', '', lines[start]).count('}'), start + 1
        while i < total and depth <= 0:
            block.append(lines[i]); c = re.sub(r'(//|#).*$', '', lines[i])
            depth += c.count('{') - c.count('}'); i += 1
            if depth > 0: break
        while i < total and depth > 0:
            block.append(lines[i]); c = re.sub(r'(//|#).*$', '', lines[i])
            depth += c.count('{') - c.count('}'); i += 1
        return block, i

    def _parse_object(self, lines: List[str], source: str, pak: str, obj_type: str, name: str) -> Optional[CelestialObject]:
        obj = CelestialObject(source_file=source, source_pak=pak, object_type=obj_type if obj_type not in ('Modify', 'Replace', 'Add', 'Remove') else "", name=name, raw_block='\n'.join(lines))
        context_stack, n, i = [], len(lines), 1
        
        while i < n:
            line = lines[i].strip()
            if not line or line.startswith('//') or line.startswith('#'): i += 1; continue
            clean = re.sub(r'(//|#).*$', '', line)
            opens, closes = clean.count('{'), clean.count('}')
            
            if line == '}':
                if context_stack: context_stack.pop()
                i += 1; continue
            
            if self._re_include.match(line):
                obj.add_field("IncludedFile", self._re_include.match(line).group(2), '.'.join(context_stack))
                i += 1; continue
            
            sec_match = re.match(r'^([A-Za-z_][\w]*)\s*(?:"[^"]*")?\s*(\{)?\s*$', line)
            if sec_match:
                has_brace, potential_sec = bool(sec_match.group(2)), sec_match.group(1)
                if not has_brace:
                    for j in range(i+1, n):
                        nx = lines[j].strip()
                        if nx and not nx.startswith('//'):
                            if nx == '{': has_brace = True
                            break
                if has_brace:
                    context_stack.append(potential_sec)
                    if not sec_match.group(2):
                        i += 1
                        while i < n:
                            if lines[i].strip() == '{': i += 1; break
                            elif not lines[i].strip() or lines[i].strip().startswith('//'): i += 1
                            else: break
                    else: i += 1
                    continue

            if opens > 0 and closes == 0:
                kvm = self._re_kv.match(clean.replace('{', '').strip())
                if kvm:
                    obj.add_field(kvm.group(1), self._parse_val(kvm.group(2).strip()), '.'.join(context_stack))
                    context_stack.append(kvm.group(1))
                else: context_stack.append("block")
                i += 1; continue
            
            if closes > 0:
                stripped = clean.replace('}', '').strip()
                if stripped:
                    kvm = self._re_kv.match(stripped)
                    if kvm: obj.add_field(kvm.group(1), self._parse_val(kvm.group(2).strip()), '.'.join(context_stack))
                for _ in range(closes):
                    if context_stack: context_stack.pop()
                i += 1; continue

            ml = re.match(r'^([A-Za-z_][\w.-]*)\s+"([^"]*)$', line)
            if ml:
                key, parts, j = ml.group(1), [ml.group(2)], i + 1
                while j < n:
                    parts.append(lines[j].rstrip())
                    if '"' in lines[j]: break
                    j += 1
                obj.add_field(key, '\n'.join(parts).rstrip('"'), '.'.join(context_stack))
                i = j + 1; continue

            dangling = re.match(r'^([A-Za-z_][\w.-]*)$', line)
            if dangling and i + 1 < n and lines[i+1].strip().startswith('"'):
                key, j, parts = dangling.group(1), i + 1, [lines[i+1].strip()]
                if '"' not in lines[j].strip()[1:]:
                    j += 1
                    while j < n:
                        parts.append(lines[j].rstrip())
                        if '"' in lines[j]: break
                        j += 1
                obj.add_field(key, '\n'.join(parts).strip().strip('"'), '.'.join(context_stack))
                i = j + 1; continue

            kvm = self._re_kv.match(line)
            if kvm:
                key, val = kvm.group(1), kvm.group(2).strip()
                for cc in ['//', '#']:
                    idx = val.find(cc)
                    if idx >= 0 and val[:idx].count('"') % 2 == 0: val = val[:idx].strip()
                obj.add_field(key, self._parse_val(val), '.'.join(context_stack))
                i += 1; continue
            
            i += 1

        if not obj.name:
            for k in ['Name', 'LocName', 'CommonName']:
                if k in obj.all_fields:
                    obj.name = str(obj.all_fields[k]).strip('"\''); break
            if not obj.name: obj.name = f"Unknown_{obj_type}_{hashlib.md5(obj.raw_block[:50].encode()).hexdigest()[:6]}"
        return obj

    def _parse_val(self, raw: str) -> Any:
        if not raw: return ""
        raw = raw.strip()
        if raw.lower() in ('true', 'yes', 'on'): return True
        if raw.lower() in ('false', 'no', 'off', 'none'): return False
        if raw.startswith('"') and raw.endswith('"'): return raw[1:-1]
        if raw.startswith("'") and raw.endswith("'"): return raw[1:-1]
        for s, e in [('(', ')'), ('[', ']'), ('{', '}')]:
            if raw.startswith(s) and raw.endswith(e):
                return [self._parse_val(p) for p in raw[1:-1].split(',') if p.strip()]
        m = re.match(r'^([+-]?\d+\.?\d*(?:[eE][+-]?\d+)?)\s*(\S+)?$', raw)
        if m:
            try:
                ns, unit = m.group(1), m.group(2)
                num = float(ns) if ('.' in ns or 'e' in ns.lower()) else int(ns)
                return f"{num} {unit}" if unit else num
            except: pass
        try: return [float(p) if ('.' in p or 'e' in p.lower()) else int(p) for p in raw.split()] if len(raw.split()) > 1 else (float(raw) if ('.' in raw or 'e' in raw.lower()) else int(raw))
        except: return raw

###############################################################################
# FILE I/O & OBJECT SAVER
###############################################################################

class FileIO:
    @staticmethod
    def read(path: Path) -> Optional[str]:
        if not path.exists(): return None
        for enc in CFG.ENCODINGS:
            try: return path.read_text(encoding=enc)
            except: continue
        return path.read_bytes().decode('utf-8', errors='replace')

    @staticmethod
    def iter_pak(pak_path: Path):
        try:
            with zipfile.ZipFile(pak_path, 'r') as zf:
                for info in zf.infolist():
                    if Path(info.filename).suffix.lower() in CFG.FILE_EXTENSIONS:
                        try:
                            raw = zf.read(info.filename)
                            for enc in CFG.ENCODINGS:
                                try: yield info.filename, raw.decode(enc), info.file_size; break
                                except: continue
                        except: continue
        except: pass

class ObjectSaver:
    def __init__(self, base: Path): self.base = base

    def save(self, obj: CelestialObject) -> Tuple[str, str]:
        cat = CFG.LIVE_DIR if obj.origin == ObjectOrigin.WORKSPACE else CFG.CATALOG_DIR
        target = self.base / cat / obj.family.value
        target.mkdir(parents=True, exist_ok=True)
        safe = re.sub(r'[<>:"/\\|?*\x00-\x1f]', '_', obj.name)[:80].strip('. ') or "unknown"
        json_path, sc_path, c = target / f"{safe}.json", target / f"{safe}.sc", 1
        while json_path.exists():
            json_path, sc_path, c = target / f"{safe}_{c}.json", target / f"{safe}_{c}.sc", c + 1
        with open(json_path, 'w', encoding='utf-8') as f: json.dump(obj.to_dict(), f, indent=2, ensure_ascii=False, default=str)
        if obj.raw_block:
            with open(sc_path, 'w', encoding='utf-8') as f: f.write(obj.raw_block)
        obj.saved_json_path, obj.saved_sc_path = str(json_path), str(sc_path)
        return str(json_path), str(sc_path)

###############################################################################
# EXTRACTION & WATCHDOG
###############################################################################

class LiveWatchdog:
    def __init__(self, db: Database, parser: IterativeParser, saver: ObjectSaver):
        self.db, self.parser, self.saver, self.running, self.thread = db, parser, saver, False, None
        self.watch_dirs, self.queue, self._known = [], Queue(), {}

    def start(self, se_path: Path):
        # Exclusively monitoring the EXPORT folder
        self.watch_dirs = [se_path / 'export']
        for d in self.watch_dirs: d.mkdir(parents=True, exist_ok=True)
        self.running = True
        self._scan_existing(startup=True)
        self.thread = threading.Thread(target=self._loop, daemon=True)
        self.thread.start()
        Log.info("Workspace Watchdog started monitoring SE export folder.")

    def stop(self):
        self.running = False
        if self.thread: self.thread.join(timeout=2)
        Log.info("Workspace Watchdog stopped")

    def _scan_existing(self, startup=False):
        for d in self.watch_dirs:
            if d.exists():
                for ext in CFG.FILE_EXTENSIONS:
                    for fp in d.rglob(f'*{ext}'):
                        try: 
                            fpath = str(fp)
                            mtime = fp.stat().st_mtime
                            if startup and fpath not in self._known:
                                # Retroactively parse existing exports at startup
                                self._process(fp)
                            self._known[fpath] = mtime
                        except: pass

    def _loop(self):
        while self.running:
            try: self._check()
            except Exception as e: Log.error(f"Watchdog: {e}")
            time.sleep(CFG.WATCHDOG_INTERVAL)

    def _check(self):
        for d in self.watch_dirs:
            if d.exists():
                for ext in CFG.FILE_EXTENSIONS:
                    for fp in d.rglob(f'*{ext}'):
                        try:
                            fpath, mtime = str(fp), fp.stat().st_mtime
                            if self._known.get(fpath) is None or mtime > self._known[fpath]:
                                self._process(fp); self._known[fpath] = mtime
                        except: pass

    def _process(self, fp: Path):
        time.sleep(0.2); historical_size = -1
        for _ in range(15):
            try:
                curr_size = fp.stat().st_size
                if curr_size == historical_size and curr_size > 0: break
                historical_size = curr_size
            except: pass
            time.sleep(0.1)
        text = FileIO.read(fp)
        if text:
            for obj in self.parser.parse(text, str(fp), "", ObjectOrigin.WORKSPACE):
                if not self.db.hash_exists(obj.file_hash):
                    self.saver.save(obj); self.db.insert_object(obj)
                    Log.discovery(f"WORKSPACE ADDED: {obj.name} ({obj.family.value})"); self.queue.put(obj)

    def get_pending(self) -> List[CelestialObject]:
        items = []
        while True:
            try: items.append(self.queue.get_nowait())
            except Empty: break
        return items

class Extractor:
    def __init__(self, se_path: str, db: Database):
        self.root, self.db, self.parser, self.saver = Path(se_path), db, IterativeParser(), ObjectSaver(Path(CFG.OUT_DIR))
        self.stats = {'files': 0, 'paks': 0, 'objects': 0, 'fields': 0}

    def query_single_object(self, target_name: str, progress_cb=None, cancel_cb=None, proc_only=True) -> Optional[CelestialObject]:
        target_lower, pattern = target_name.lower(), re.compile(rf'^[A-Za-z_]\w*\s+["\']?{re.escape(target_name)}["\']?\s*', re.MULTILINE | re.IGNORECASE)
        loose = []
        for ext in CFG.FILE_EXTENSIONS:
            if proc_only:
                for d in [self.root / 'export', self.root / 'addons']:
                    if d.exists(): loose.extend(list(d.rglob(f'*{ext}')))
            else: loose.extend([f for f in self.root.rglob(f'*{ext}') if f.suffix.lower() not in ('.pak', '.zip')])
        loose.sort(key=lambda x: x.stat().st_mtime if x.exists() else 0, reverse=True)
        paks = [] if proc_only else (list(self.root.rglob('*.pak')) + list(self.root.rglob('*.zip')))
        total, done = len(paks) + len(loose), 0

        for fp in loose:
            if cancel_cb and cancel_cb(): break
            text = FileIO.read(fp)
            if text and pattern.search(text):
                for obj in self.parser.parse(text, str(fp), "", ObjectOrigin.CATALOG):
                    if obj.name.lower() == target_lower: return obj
            done += 1; progress_cb and progress_cb(done, total, fp.name)
        for pak in paks:
            if cancel_cb and cancel_cb(): break
            for fname, text, size in FileIO.iter_pak(pak):
                if pattern.search(text):
                    for obj in self.parser.parse(text, fname, pak.name, ObjectOrigin.CATALOG):
                        if obj.name.lower() == target_lower: return obj
            done += 1; progress_cb and progress_cb(done, total, pak.name)
        return None

    def run_procedural(self, progress_cb=None, cancel_cb=None) -> dict:
        loose = []
        for ext in CFG.FILE_EXTENSIONS:
            for d in [self.root / 'export', self.root / 'addons']:
                if d.exists(): loose.extend(list(d.rglob(f'*{ext}')))
        loose.sort(key=lambda x: x.stat().st_mtime if x.exists() else 0, reverse=True)
        total, done, batch = len(loose), 0, []
        for fp in loose:
            if cancel_cb and cancel_cb(): break
            try:
                text = FileIO.read(fp)
                if text:
                    for obj in self.parser.parse(text, str(fp), "", ObjectOrigin.CATALOG):
                        self.saver.save(obj); batch.append(obj)
                        self.stats['objects'] += 1; self.stats['fields'] += len(obj.all_fields); self.stats['files'] += 1
            except: pass
            if len(batch) >= CFG.BATCH_SIZE: self.db.insert_batch(batch); batch.clear(); gc.collect()
            done += 1; (progress_cb and done % 10 == 0) and progress_cb(done, total, self.stats['objects'], self.stats['fields'])
        if batch: self.db.insert_batch(batch)
        self.db.update_stats(self.stats['files'], 0, self.stats['objects'], self.stats['fields'])
        return self.stats

    def run(self, progress_cb=None, cancel_cb=None) -> dict:
        paks, zips = list(self.root.rglob('*.pak')), list(self.root.rglob('*.zip'))
        loose = [f for ext in CFG.FILE_EXTENSIONS for f in self.root.rglob(f'*{ext}') if f.suffix.lower() not in ('.pak', '.zip')]
        total, done, batch = len(paks) + len(zips) + len(loose), 0, []
        
        for pak in paks + zips:
            if cancel_cb and cancel_cb(): break
            self.stats['paks'] += 1
            for fname, text, size in FileIO.iter_pak(pak):
                if cancel_cb and cancel_cb(): break
                try:
                    for obj in self.parser.parse(text, fname, pak.name, ObjectOrigin.CATALOG):
                        self.saver.save(obj); batch.append(obj)
                        self.stats['objects'] += 1; self.stats['fields'] += len(obj.all_fields); self.stats['files'] += 1
                except: pass
                if len(batch) >= CFG.BATCH_SIZE: self.db.insert_batch(batch); batch.clear(); gc.collect()
            done += 1; progress_cb and progress_cb(done, total, self.stats['objects'], self.stats['fields'])
            
        for fp in loose:
            if cancel_cb and cancel_cb(): break
            if 'export' in str(fp).lower(): done += 1; continue
            try:
                text = FileIO.read(fp)
                if text:
                    for obj in self.parser.parse(text, str(fp), "", ObjectOrigin.CATALOG):
                        self.saver.save(obj); batch.append(obj)
                        self.stats['objects'] += 1; self.stats['fields'] += len(obj.all_fields); self.stats['files'] += 1
            except: pass
            if len(batch) >= CFG.BATCH_SIZE: self.db.insert_batch(batch); batch.clear(); gc.collect()
            done += 1; (progress_cb and done % 50 == 0) and progress_cb(done, total, self.stats['objects'], self.stats['fields'])
            
        if batch: self.db.insert_batch(batch)
        self.db.update_stats(self.stats['files'], self.stats['paks'], self.stats['objects'], self.stats['fields'])
        return self.stats

###############################################################################
# GUI & MAIN APP LOOP
###############################################################################

class TaskStatus(Enum): OK = auto(); FAIL = auto()
@dataclass
class TaskResult: status: TaskStatus; result: Any = None; error: str = ""; duration: float = 0
class Tasks:
    def __init__(self): self.pool = ThreadPoolExecutor(max_workers=CFG.MAX_WORKERS)
    def run(self, fn, *args, cb=None):
        def wrap():
            t0 = time.perf_counter()
            try: return TaskResult(TaskStatus.OK, fn(*args), duration=time.perf_counter() - t0)
            except Exception as e: return TaskResult(TaskStatus.FAIL, error=str(e), duration=time.perf_counter() - t0)
        fut = self.pool.submit(wrap)
        if cb: fut.add_done_callback(lambda f: cb(f.result()))
    def shutdown(self): self.pool.shutdown(wait=False)

class App:
    def __init__(self):
        self.tasks, self.db_path = Tasks(), Path(_BASE_DIR) / CFG.DB_NAME
        self.db = Database(str(self.db_path))
        self.parser, self.saver, self.watchdog = IterativeParser(), ObjectSaver(Path(CFG.OUT_DIR)), None
        self.root = tk.Tk()
        self.root.title(CFG.APP_NAME); self.root.configure(bg='#080818')
        try: self.root.state('zoomed')
        except: self.root.geometry("1600x900")
        
        self.syncing, self.cancel, self.search_job, self.live_objects = False, False, None, []
        self.current_raw_data = ""
        self.loaded_images = [] # Prevents garbage collection of Tkinter images
        
        self._styles(); self._build(); self._auto_detect()
        self.root.after(100, self._refresh_all); self.root.after(500, self._check_live)

    def _styles(self):
        s = ttk.Style(); s.theme_use('clam')
        bg, fg, ac = '#080818', '#e0e8ff', '#5588ff'
        s.configure('TFrame', background=bg)
        s.configure('TLabel', background=bg, foreground=fg, font=('Segoe UI', 10))
        s.configure('Title.TLabel', background=bg, foreground=ac, font=('Segoe UI', 18, 'bold'))
        s.configure('Stats.TLabel', background=bg, foreground='#55ff88', font=('Segoe UI', 11, 'bold'))
        s.configure('Live.TLabel', background=bg, foreground='#ff5588', font=('Segoe UI', 12, 'bold'))
        s.configure('TButton', font=('Segoe UI', 10, 'bold'), padding=8)
        s.configure('Big.TButton', font=('Segoe UI', 13, 'bold'), padding=14)
        s.configure('OP.TButton', font=('Segoe UI', 14, 'bold'), padding=14, foreground='#ff5588')
        s.configure('Watch.TButton', font=('Segoe UI', 12, 'bold'), padding=12)
        s.configure('TLabelframe', background=bg)
        s.configure('TLabelframe.Label', background=bg, foreground=ac, font=('Segoe UI', 11, 'bold'))
        s.configure('TProgressbar', background=ac, troughcolor='#1a1a2e')
        s.configure('TNotebook', background=bg)
        s.configure('TNotebook.Tab', font=('Segoe UI', 11, 'bold'), padding=[18, 6])
        s.configure('Treeview', background='#0a0a1e', foreground=fg, fieldbackground='#0a0a1e', font=('Segoe UI', 10), rowheight=26)
        s.configure('Treeview.Heading', background='#12122a', foreground=ac, font=('Segoe UI', 10, 'bold'))
        s.map('Treeview', background=[('selected', '#2a2a5a')])

    def _build(self):
        main = ttk.Frame(self.root, padding=15); main.pack(fill=tk.BOTH, expand=True)
        
        # --- NEW HEADER / EXPORT BAR (TOP LEFT) ---
        hdr = ttk.Frame(main)
        hdr.pack(fill=tk.X, pady=(0, 10))
        
        # Export elements on left side
        self.exp_frame = ttk.Frame(hdr)
        self.exp_frame.pack(side=tk.LEFT)
        
        self.export_btn = ttk.Button(self.exp_frame, text="EXPORT WORKSPACE SELECTED", command=self._export, style='Big.TButton')
        self.export_btn.pack(side=tk.LEFT)
        
        ff = ttk.Frame(self.exp_frame)
        ff.pack(side=tk.LEFT, padx=15)
        self.fmt_vars = {}
        for n in ('csv', 'json', 'html', 'md', 'text'):
            v = tk.BooleanVar(value=True)
            self.fmt_vars[n] = v
            ttk.Checkbutton(ff, text=n.upper(), variable=v).pack(side=tk.LEFT, padx=2)
            
        self.sel_var = tk.StringVar(value="0 selected")
        ttk.Label(self.exp_frame, textvariable=self.sel_var, foreground='#ffaa55', font=('Segoe UI', 11, 'bold')).pack(side=tk.LEFT, padx=10)

        # Title elements dynamically pushed to right
        title_frame = ttk.Frame(hdr)
        title_frame.pack(side=tk.RIGHT)
        self.status = tk.StringVar(value="Ready")
        ttk.Label(title_frame, textvariable=self.status, foreground='#88aaff', font=('Segoe UI', 11)).pack(side=tk.RIGHT, padx=15)
        ttk.Label(title_frame, text="🌌 SpaceEngine Data Extractor", style='Title.TLabel').pack(side=tk.RIGHT)

        # --- REST OF UI ---
        ctrl = ttk.LabelFrame(main, text="Control System", padding=12); ctrl.pack(fill=tk.X, pady=(0, 10))
        r1 = ttk.Frame(ctrl); r1.pack(fill=tk.X, pady=(0, 8))
        ttk.Label(r1, text="SpaceEngine:", font=('Segoe UI', 11, 'bold')).pack(side=tk.LEFT)
        self.path_var = tk.StringVar(); ttk.Entry(r1, textvariable=self.path_var, font=('Segoe UI', 10), width=50).pack(side=tk.LEFT, padx=8)
        ttk.Button(r1, text="Browse", command=self._browse).pack(side=tk.LEFT, padx=(0, 20))
        self.proc_btn = ttk.Button(r1, text="EXTRACT PROCEDURAL", command=self._start_procedural, style='Big.TButton'); self.proc_btn.pack(side=tk.LEFT, padx=5)
        self.sync_btn = ttk.Button(r1, text="EXTRACT ALL (PAKs)", command=self._start, style='TButton'); self.sync_btn.pack(side=tk.LEFT, padx=5)
        self.stop_btn = ttk.Button(r1, text="STOP", command=self._stop, style='Big.TButton', state='disabled'); self.stop_btn.pack(side=tk.LEFT, padx=5)
        self.progress = ttk.Progressbar(r1, mode='indeterminate', length=180); self.progress.pack(side=tk.LEFT, padx=15)

        r2 = ttk.Frame(ctrl); r2.pack(fill=tk.X)
        self.watch_btn = ttk.Button(r2, text=">>> START WORKSPACE WATCHDOG <<<", command=self._toggle_watch, style='Watch.TButton'); self.watch_btn.pack(side=tk.LEFT)
        self.watch_status = tk.StringVar(value="Watchdog: OFF")
        ttk.Label(r2, textvariable=self.watch_status, style='Live.TLabel').pack(side=tk.LEFT, padx=20)
        self.stats_var = tk.StringVar(value="")
        ttk.Label(r2, textvariable=self.stats_var, style='Stats.TLabel').pack(side=tk.RIGHT)

        r3 = ttk.Frame(ctrl); r3.pack(fill=tk.X, pady=(15, 0))
        ttk.Label(r3, text="Direct Query:", font=('Segoe UI', 11, 'bold'), foreground='#ffaa55').pack(side=tk.LEFT)
        self.query_var = tk.StringVar(); ttk.Entry(r3, textvariable=self.query_var, font=('Segoe UI', 10), width=30).pack(side=tk.LEFT, padx=8)
        self.proc_only_var = tk.BooleanVar(value=True); ttk.Checkbutton(r3, text="Procedural Only", variable=self.proc_only_var).pack(side=tk.LEFT, padx=10)
        self.query_btn = ttk.Button(r3, text="FIND OBJECT", command=self._direct_query, style='TButton'); self.query_btn.pack(side=tk.LEFT, padx=5)

        content = ttk.PanedWindow(main, orient=tk.HORIZONTAL); content.pack(fill=tk.BOTH, expand=True, pady=(0, 10))
        left = ttk.Frame(content); content.add(left, weight=3)
        self.notebook = ttk.Notebook(left); self.notebook.pack(fill=tk.BOTH, expand=True)

        # Tab Setup (Separated Domains)
        self.tab_cat = ttk.Frame(self.notebook); self.notebook.add(self.tab_cat, text=" 🌌 SCIENTIFIC CATALOG (READ-ONLY) "); self._build_list(self.tab_cat, "Catalog")
        self.tab_live = ttk.Frame(self.notebook); self.notebook.add(self.tab_live, text=" 📂 EXPORT WORKSPACE "); self._build_list(self.tab_live, "Workspace")
        self.notebook.bind('<<NotebookTabChanged>>', self._on_tab)

        right = ttk.LabelFrame(content, text="Structured Object Details", padding=10); content.add(right, weight=2)
        
        # Action Bar for Right Panel
        self.detail_actions = ttk.Frame(right); self.detail_actions.pack(fill=tk.X, pady=(0, 5))
        self.copy_btn = ttk.Button(self.detail_actions, text="Copy Raw Data", command=self._copy_raw_data)
        self.copy_btn.pack(side=tk.RIGHT)
        
        self.detail = scrolledtext.ScrolledText(right, wrap=tk.WORD, bg='#0a0a1e', fg='#c0d0ff', font=('Consolas', 10), insertbackground='white'); self.detail.pack(fill=tk.BOTH, expand=True)
        self.detail.insert(tk.END, "Select an object to view deeply categorized data."); self.detail.configure(state='disabled')
        
        # Hide export options initially because tab 1 is Catalog
        self._toggle_export_bar(False)

    def _build_list(self, parent, origin: str):
        f = ttk.Frame(parent); f.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        sf = ttk.Frame(f); sf.pack(fill=tk.X, pady=(0, 8))
        
        # Search Entry
        sv, cv = tk.StringVar(), tk.StringVar(value="0 objects")
        ttk.Label(sf, text="Search:", font=('Segoe UI', 10, 'bold')).pack(side=tk.LEFT)
        ttk.Entry(sf, textvariable=sv, font=('Segoe UI', 10), width=30).pack(side=tk.LEFT, padx=8)
        sv.trace_add('write', lambda *a: self._on_search(origin))
        
        # Family Dropdown Filter
        fv = tk.StringVar(value="All Families")
        fams = ["All Families"] + [e.value for e in ObjectFamily]
        combo = ttk.Combobox(sf, textvariable=fv, values=fams, state="readonly", width=15)
        combo.pack(side=tk.LEFT, padx=10)
        combo.bind("<<ComboboxSelected>>", lambda e: self._on_search(origin))
        
        ttk.Label(sf, textvariable=cv, style='Stats.TLabel').pack(side=tk.LEFT, padx=15)
        ttk.Button(sf, text="Clear Selected", command=lambda: [getattr(self, f'tree_{origin}').selection_remove(getattr(self, f'tree_{origin}').get_children()), self._update_sel()]).pack(side=tk.RIGHT, padx=3)
        
        tf = ttk.Frame(f); tf.pack(fill=tk.BOTH, expand=True)
        cols = ('id', 'name', 'family', 'type', 'life', 'habitability', 'path')
        tree = ttk.Treeview(tf, columns=cols, show='headings', selectmode='extended')
        tree.heading('id', text='#'); tree.column('id', width=0, stretch=False)
        tree.heading('name', text='Name'); tree.column('name', width=180)
        tree.heading('family', text='Family'); tree.column('family', width=90)
        tree.heading('type', text='Type'); tree.column('type', width=90)
        tree.heading('life', text='Bio'); tree.column('life', width=40)
        tree.heading('habitability', text='Habitability'); tree.column('habitability', width=70)
        tree.heading('path', text='Saved Location'); tree.column('path', width=180)
        vsb = ttk.Scrollbar(tf, orient=tk.VERTICAL, command=tree.yview); tree.configure(yscrollcommand=vsb.set)
        tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True); vsb.pack(side=tk.RIGHT, fill=tk.Y)
        tree.bind('<<TreeviewSelect>>', lambda e: self._on_sel(tree))

        pf, pv = ttk.Frame(f), tk.StringVar(value="Page 1")
        pf.pack(fill=tk.X, pady=(8, 0))
        ttk.Button(pf, text="<< Prev", command=lambda: self._prev(origin)).pack(side=tk.LEFT)
        ttk.Label(pf, textvariable=pv).pack(side=tk.LEFT, padx=15)
        ttk.Button(pf, text="Next >>", command=lambda: self._next(origin)).pack(side=tk.LEFT)

        setattr(self, f'tree_{origin}', tree); setattr(self, f'search_{origin}', sv); setattr(self, f'count_{origin}', cv)
        setattr(self, f'page_{origin}', pv); setattr(self, f'pnum_{origin}', 0)
        setattr(self, f'family_{origin}', fv)

    def _auto_detect(self):
        for p in [r"C:\Program Files (x86)\Steam\steamapps\common\SpaceEngine", r"C:\Program Files\Steam\steamapps\common\SpaceEngine",
                  r"D:\Steam\steamapps\common\SpaceEngine", r"D:\SteamLibrary\steamapps\common\SpaceEngine", r"C:\GOG Games\SpaceEngine"]:
            if Path(p).exists() and (Path(p) / 'data').exists(): self.path_var.set(p); break

    def _browse(self):
        if f := filedialog.askdirectory(title="SpaceEngine Folder"): self.path_var.set(f)

    def _toggle_watch(self):
        if self.watchdog and self.watchdog.running:
            self.watchdog.stop(); self.watchdog = None
            self.watch_btn.configure(text=">>> START WORKSPACE WATCHDOG <<<"); self.watch_status.set("Watchdog: OFF")
        else:
            p = self.path_var.get()
            if not p or not Path(p).exists(): return messagebox.showerror("Error", "Select SpaceEngine folder first")
            self.watchdog = LiveWatchdog(self.db, self.parser, self.saver); self.watchdog.start(Path(p))
            self.watch_btn.configure(text="<<< STOP WATCHDOG <<<"); self.watch_status.set("Watchdog: RUNNING")

    def _check_live(self):
        if self.watchdog:
            for obj in self.watchdog.get_pending():
                self.live_objects.insert(0, obj)
                self.status.set(f"WORKSPACE ADDED: {obj.name}")
                self._refresh("Workspace")
        self.root.after(500, self._check_live)

    def _stop(self): self.cancel = True; self.stop_btn.configure(state='disabled')

    def _direct_query(self):
        target, p = self.query_var.get().strip(), self.path_var.get()
        if not target or not p or not Path(p).exists(): return
        self.query_btn.configure(state='disabled'); self.cancel, proc_only = False, self.proc_only_var.get()
        self.progress.start(10); self.status.set(f"Searching for '{target}'...")

        def prog(d, t, f): self.root.after(0, lambda: self.status.set(f"Scanning: {d}/{t} | {f}"))
        def do(): return Extractor(p, self.db).query_single_object(target, prog, lambda: self.cancel, proc_only)
        def done(r: TaskResult):
            self.progress.stop(); self.query_btn.configure(state='normal')
            if r.status == TaskStatus.OK and r.result:
                obj = r.result; self.saver.save(obj)
                if not self.db.hash_exists(obj.file_hash): self.db.insert_object(obj)
                self._refresh_all(); self._show_obj(obj); messagebox.showinfo("Found", f"Extracted {obj.name}!")
            else: messagebox.showinfo("Not Found", "Object not found.")
        self.tasks.run(do, cb=lambda r: self.root.after(0, lambda: done(r)))

    def _start_procedural(self): self._run_extraction(True)
    def _start(self): self._run_extraction(False)

    def _run_extraction(self, procedural_only: bool):
        p = self.path_var.get()
        if not p or not Path(p).exists(): return
        self.syncing, self.cancel = True, False
        self.proc_btn.configure(state='disabled'); self.sync_btn.configure(state='disabled'); self.stop_btn.configure(state='normal')
        self.progress.start(10); self.status.set("Extracting...")

        def prog(d, t, o, f): self.root.after(0, lambda: self.status.set(f"Extracted: {o} objects ({f} fields) | {d}/{t} files"))
        def do(): return Extractor(p, self.db).run_procedural(prog, lambda: self.cancel) if procedural_only else Extractor(p, self.db).run(prog, lambda: self.cancel)
        def done(r: TaskResult):
            self.progress.stop(); self.syncing = False
            self.proc_btn.configure(state='normal'); self.sync_btn.configure(state='normal'); self.stop_btn.configure(state='disabled')
            if r.status == TaskStatus.OK: self.status.set("Complete!"); self._refresh_all()
        self.tasks.run(do, cb=lambda r: self.root.after(0, lambda: done(r)))

    def _toggle_export_bar(self, show: bool):
        if show:
            self.exp_frame.pack(side=tk.LEFT)
        else:
            self.exp_frame.pack_forget()

    def _on_tab(self, e):
        tab = self.notebook.index(self.notebook.select())
        if tab == 0: 
            self._refresh("Catalog")
            self._toggle_export_bar(False) # Catalog is strictly Read-Only
        elif tab == 1: 
            self._refresh("Workspace")
            self._toggle_export_bar(True) # Workspace allows exporting
        self._update_sel()

    def _on_search(self, origin):
        if self.search_job: self.root.after_cancel(self.search_job)
        self.search_job = self.root.after(CFG.DEBOUNCE_MS, lambda: self._refresh(origin))

    def _refresh_all(self): self._refresh("Catalog"); self._refresh("Workspace"); self._update_stats()

    def _refresh(self, origin):
        setattr(self, f'pnum_{origin}', 0)
        tree = getattr(self, f'tree_{origin}')
        sv = getattr(self, f'search_{origin}')
        cv = getattr(self, f'count_{origin}')
        pv = getattr(self, f'page_{origin}')
        fv = getattr(self, f'family_{origin}')
        pn = 0
        
        search = sv.get().strip()
        fam = fv.get()
        
        rows = self.db.fetch_page(search, origin, fam, pn * CFG.PAGE_SIZE, CFG.PAGE_SIZE)
        tree.delete(*tree.get_children())
        for r in rows:
            oid, name, family, otype, ocls, orig_val, fcnt, lines, dtime, jpath, src = r
            icon = FAMILY_ICONS.get(ObjectFamily(family), "?")
            tree.insert('', 'end', values=(oid, name, f"{icon} {family}", otype, "🦠" if ("life" in json.dumps(r).lower() or "organic" in json.dumps(r).lower()) else "-", f"{fcnt*2.5:.1f}", jpath.replace(str(CFG.OUT_DIR), "...") if jpath else ""))
        cv.set(f"{self.db.count(origin, fam)} objects"); pv.set(f"Page {pn + 1}")
        self._update_sel()

    def _prev(self, origin):
        pn = getattr(self, f'pnum_{origin}')
        if pn > 0: setattr(self, f'pnum_{origin}', pn - 1); self._refresh(origin) 

    def _next(self, origin):
        setattr(self, f'pnum_{origin}', getattr(self, f'pnum_{origin}') + 1); self._refresh(origin)

    def _update_stats(self):
        if st := self.db.get_stats(): self.stats_var.set(f"Extracted Objects: {st.get('objects', 0):,} | DB Fields: {st.get('fields', 0):,}")

    def _update_sel(self):
        total = 0
        for o in ['Catalog', 'Workspace']:
            t = getattr(self, f'tree_{o}', None)
            if t: total += len(t.selection())
        self.sel_var.set(f"{total} workspace selected")

    def _on_sel(self, tree):
        self._update_sel()
        if sel := tree.selection(): self._show(tree.item(sel[-1])['values'][0])

    def _show(self, oid):
        if obj := self.db.get_by_id(oid): self._show_obj(obj)

    def _copy_raw_data(self):
        if hasattr(self, 'current_raw_data') and self.current_raw_data:
            self.root.clipboard_clear()
            self.root.clipboard_append(self.current_raw_data)
            messagebox.showinfo("Copied", "Raw object block copied to clipboard.")

    def _show_obj(self, obj: CelestialObject):
        self.current_raw_data = obj.raw_block
        self.loaded_images.clear() # clear memory for images
        
        self.detail.configure(state='normal')
        self.detail.delete('1.0', tk.END)
        
        # 1. Header Information
        icon = FAMILY_ICONS.get(obj.family, "?")
        header = f"{'='*70}\n  {icon} {obj.name}   [{obj.origin.value.upper()}]\n{'='*70}\n"
        header += f"  Type: {obj.object_type} | Class: {obj.object_class}\n"
        header += f"  Parent: {obj.parent or 'None'}\n"
        header += f"  [AstroAnalyzer] Bio-Signature Detected: {'YES 🦠' if obj.has_life else 'NO'}\n"
        header += f"  [AstroAnalyzer] Habitability Rating: {obj.habitability_score:.1f}/100\n"
        header += f"  Discovery Time: {obj.discovery_time}\n"
        self.detail.insert(tk.END, header)
        
        # 2. Workspace Image / Terrain Detection
        if obj.origin == ObjectOrigin.WORKSPACE and obj.source_file:
            d = Path(obj.source_file).parent
            safe_name = re.sub(r'[<>:"/\\|?*\x00-\x1f]', '_', obj.name)
            found_images = []
            
            # Simple broad check in the same folder
            for ext in ['.png', '.jpg', '.jpeg']:
                for img_path in d.glob(f"*{ext}"):
                    if obj.name.lower() in img_path.name.lower():
                        found_images.append(img_path)
            
            if found_images:
                self.detail.insert(tk.END, f"\n{'-'*70}\n[TERRAIN/TEXTURE MAPS DETECTED]\n{'-'*70}\n")
                if HAS_PIL:
                    for img_path in found_images:
                        try:
                            self.detail.insert(tk.END, f"\n{img_path.name}:\n")
                            img = Image.open(img_path)
                            # Resize to fit display nicely
                            img.thumbnail((450, 450))
                            tk_img = ImageTk.PhotoImage(img)
                            self.loaded_images.append(tk_img) # keep ref
                            self.detail.image_create(tk.END, image=tk_img)
                            self.detail.insert(tk.END, "\n")
                        except Exception as e:
                            self.detail.insert(tk.END, f"Failed to load image: {e}\n")
                else:
                    self.detail.insert(tk.END, "(Pillow / PIL library not installed. Showing paths instead)\n")
                    for img_path in found_images:
                        self.detail.insert(tk.END, f"Found: {img_path}\n")

        # 3. Categorized Structured Properties
        categorized = categorize_properties(obj.all_fields)
        self.detail.insert(tk.END, f"\n\n{'='*70}\nSTRUCTURED SCIENTIFIC PROPERTIES:\n{'='*70}\n")
        
        for category_name, fields in categorized.items():
            self.detail.insert(tk.END, f"\n▶ {category_name.upper()}\n")
            for k, v in fields.items():
                self.detail.insert(tk.END, f"   {k} = {v}\n")

        # 4. Raw Block
        if obj.raw_block:
            self.detail.insert(tk.END, f"\n\n{'='*70}\nRAW ENGINE CODE:\n{'='*70}\n{obj.raw_block}\n")
            
        self.detail.configure(state='disabled')

    def _export(self):
        # Exclusively allows Workspace exporting. Catalog exports are forbidden.
        t = getattr(self, f'tree_Workspace', None)
        ids = set()
        if t:
            for i in t.selection(): ids.add(t.item(i)['values'][0])

        if not ids:
            messagebox.showwarning("Warning", "Select at least one object from the EXPORT WORKSPACE to export.\n(Hold Shift or Ctrl to select multiple items)")
            return

        fmts = [n for n, v in self.fmt_vars.items() if v.get()]
        if not fmts:
            messagebox.showwarning("Warning", "Please select at least one format to export to.")
            return

        self.export_btn.configure(state='disabled')
        self.progress.start(10)

        def do():
            objs = [self.db.get_by_id(i) for i in ids]
            objs = [o for o in objs if o]
            
            # Noised Data Management: Deduplication to avoid crashing exports
            seen, deduped = set(), []
            for o in objs:
                identifier = f"{o.name}_{o.object_class}_{len(o.all_fields)}"
                if identifier not in seen:
                    seen.add(identifier)
                    deduped.append(o)

            ts = datetime.now().strftime('%Y%m%d_%H%M%S')
            out = Path(CFG.OUT_DIR) / CFG.BATCH_DIR
            out.mkdir(parents=True, exist_ok=True)
            exported_paths = []
            
            for fmt in fmts:
                if fmt in EXPORTERS:
                    ext, fn = EXPORTERS[fmt]
                    fp = out / f"export_{len(deduped)}_{ts}{ext}"
                    fn(deduped, fp)
                    exported_paths.append(str(fp))
            return exported_paths

        def done(r: TaskResult):
            self.progress.stop()
            self.export_btn.configure(state='normal')
            if r.status == TaskStatus.OK:
                messagebox.showinfo("Export Complete", f"Successfully exported {len(ids)} items into the BatchExports folder.")
            else:
                messagebox.showerror("Export Failed", r.error)

        self.tasks.run(do, cb=lambda r: self.root.after(0, lambda: done(r)))

    def run(self):
        if self.db_path.exists(): self._refresh_all()
        try: self.root.mainloop()
        finally:
            if self.watchdog: self.watchdog.stop()
            self.db.close(); self.tasks.shutdown()

if __name__ == "__main__":
    App().run()