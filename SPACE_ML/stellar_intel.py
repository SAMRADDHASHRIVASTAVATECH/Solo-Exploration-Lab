#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                    STELLAR INTELLIGENCE SYSTEM v7.0                          ║
║                   DEEP LEARNING RESEARCH EDITION                             ║
║                                                                              ║
║   ✓ Deep Learning (PyTorch)      ✓ Bias-Breaking Training                   ║
║   ✓ K-Fold Cross Validation      ✓ Feature Importance (XAI)                 ║
║   ✓ Dataset Balance Analysis     ✓ Normalized Targets                       ║
║   ✓ Uncertainty Quantification   ✓ Statistical Validation                   ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

import os
import sys

os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'
os.environ['TF_ENABLE_ONEDNN_OPTS'] = '0'
os.environ['CUDA_VISIBLE_DEVICES'] = ''

import warnings
warnings.filterwarnings('ignore')

import logging
logging.getLogger('tensorflow').setLevel(logging.FATAL)

import re
import json
import math
import pickle
import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, field
from collections import Counter
import webbrowser

def install_quiet(pkg):
    os.system(f"{sys.executable} -m pip install {pkg} -q 2>nul")

try:
    import numpy as np
except ImportError:
    install_quiet("numpy")
    import numpy as np

try:
    import torch
    import torch.nn as nn
    import torch.optim as optim
    from torch.utils.data import DataLoader, TensorDataset
except ImportError:
    install_quiet("torch")
    import torch
    import torch.nn as nn
    import torch.optim as optim
    from torch.utils.data import DataLoader, TensorDataset

try:
    from sklearn.preprocessing import StandardScaler, LabelEncoder
    from sklearn.model_selection import train_test_split, StratifiedKFold, cross_val_score
    from sklearn.metrics import accuracy_score, classification_report, mean_absolute_error, r2_score
    from sklearn.neural_network import MLPClassifier
except ImportError:
    install_quiet("scikit-learn")
    from sklearn.preprocessing import StandardScaler, LabelEncoder
    from sklearn.model_selection import train_test_split, StratifiedKFold, cross_val_score
    from sklearn.metrics import accuracy_score, classification_report, mean_absolute_error, r2_score
    from sklearn.neural_network import MLPClassifier

try:
    from rich.console import Console
    from rich.panel import Panel
    from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, MofNCompleteColumn, TimeElapsedColumn
    from rich.table import Table
    from rich.text import Text
    from rich.box import DOUBLE, ROUNDED
    from rich.columns import Columns
    from rich.rule import Rule
except ImportError:
    install_quiet("rich")
    from rich.console import Console
    from rich.panel import Panel
    from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, MofNCompleteColumn, TimeElapsedColumn
    from rich.table import Table
    from rich.text import Text
    from rich.box import DOUBLE, ROUNDED
    from rich.columns import Columns
    from rich.rule import Rule

console = Console()

TK_AVAILABLE = False
try:
    import tkinter as tk
    from tkinter import filedialog
    TK_AVAILABLE = True
except:
    pass

# Set device
DEVICE = torch.device('cuda' if torch.cuda.is_available() else 'cpu')

# ═══════════════════════════════════════════════════════════════════════════════
# CONSTANTS
# ═══════════════════════════════════════════════════════════════════════════════

MODEL_DIR = Path("./stellar_dl_v7")
MODEL_FILE = MODEL_DIR / "model.pt"
SCALER_FILE = MODEL_DIR / "scaler.pkl"
ENCODER_FILE = MODEL_DIR / "encoder.pkl"
EVAL_FILE = MODEL_DIR / "evaluation.json"
IMPORTANCE_FILE = MODEL_DIR / "feature_importance.json"
REPORTS_DIR = Path("./stellar_reports")

class PhysicsConstants:
    G = 6.67430e-11
    M_EARTH = 5.972e24
    R_EARTH = 6.371e6
    G_EARTH = 9.81

CAPABILITIES = [
    "Resource Extraction (Space Mining)",
    "Life Prediction & Search (Astrobiology)",
    "Planetary Past Analysis",
    "Present Conditions Monitoring",
    "Future Condition Prediction",
    "Satellite Deployment & Communication",
    "Space Exploration & Mapping",
    "Microgravity Experiments",
    "Space Construction",
    "Energy Generation",
    "Planetary Defense",
    "Propulsion & Travel Technology",
    "AI & Autonomous Systems",
    "Human Survival Research",
    "Terraforming (Future Concept)",
    "Space Observation & Astronomy"
]

CAPABILITY_ICONS = {
    "Resource Extraction (Space Mining)": "⛏️",
    "Life Prediction & Search (Astrobiology)": "🧬",
    "Planetary Past Analysis": "🏛️",
    "Present Conditions Monitoring": "📡",
    "Future Condition Prediction": "🔮",
    "Satellite Deployment & Communication": "🛰️",
    "Space Exploration & Mapping": "🗺️",
    "Microgravity Experiments": "🔬",
    "Space Construction": "🏗️",
    "Energy Generation": "⚡",
    "Planetary Defense": "🛡️",
    "Propulsion & Travel Technology": "🚀",
    "AI & Autonomous Systems": "🤖",
    "Human Survival Research": "👨‍🚀",
    "Terraforming (Future Concept)": "🌍",
    "Space Observation & Astronomy": "🔭"
}

TIME_LAYERS = {
    "PAST": ["Planetary Past Analysis", "Space Observation & Astronomy"],
    "PRESENT": ["Resource Extraction (Space Mining)", "Life Prediction & Search (Astrobiology)",
                "Present Conditions Monitoring", "Satellite Deployment & Communication",
                "Space Exploration & Mapping", "Microgravity Experiments",
                "Energy Generation", "Planetary Defense", "AI & Autonomous Systems"],
    "FUTURE": ["Future Condition Prediction", "Space Construction",
               "Propulsion & Travel Technology", "Human Survival Research",
               "Terraforming (Future Concept)"]
}

FEATURE_NAMES = [
    "log_mass", "log_radius", "log_temperature", "log_luminosity", "log_age",
    "log_semi_major_axis", "eccentricity", "inclination_norm", "log_period",
    "log_surface_pressure", "log_surface_gravity", "albedo", "log_density",
    "log_escape_velocity", "has_atmosphere", "has_water", "has_magnetic_field",
    "has_rings", "is_metal_rich", "has_surface", "metallicity", "ice_fraction",
    "rock_fraction", "gas_fraction", "is_star", "is_giant", "is_terrestrial",
    "is_moon", "is_asteroid", "is_nebula"
]

# ═══════════════════════════════════════════════════════════════════════════════
# DATA STRUCTURES
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class SCObject:
    name: str = "Unknown"
    obj_type: str = ""
    category: str = "Unknown Object"
    source_file: str = ""
    
    mass: float = 0.0
    mass_kg: float = 0.0
    radius: float = 0.0
    radius_m: float = 0.0
    temperature: float = 0.0
    luminosity: float = 0.0
    age: float = 0.0
    
    semi_major_axis: float = 0.0
    eccentricity: float = 0.0
    period: float = 0.0
    inclination: float = 0.0
    
    surface_gravity: float = 0.0
    surface_gravity_g: float = 0.0
    surface_pressure: float = 0.0
    albedo: float = 0.0
    
    density: float = 0.0
    escape_velocity: float = 0.0
    
    has_atmosphere: bool = False
    has_water: bool = False
    has_magnetic_field: bool = False
    has_rings: bool = False
    is_metal_rich: bool = False
    has_surface: bool = True
    
    metallicity: float = 0.0
    ice_fraction: float = 0.0
    rock_fraction: float = 0.0
    gas_fraction: float = 0.0
    
    raw_content: str = ""


@dataclass
class AnalysisResult:
    obj: SCObject
    
    # DL Predictions
    dl_category: str
    dl_confidence: float
    dl_probabilities: Dict[str, float]
    dl_capability_scores: Dict[str, float]
    
    # Validation
    rule_category: str
    agreement: bool
    disagreement_note: str
    
    # Final
    final_category: str
    final_confidence: float
    final_scores: Dict[str, float]
    ranked_capabilities: List[Tuple[str, float]]
    top_capabilities: List[Tuple[str, float]]
    
    # XAI - Feature Importance for this prediction
    feature_contributions: Dict[str, float]
    
    hazards: List[str]
    recommendations: List[str]
    analysis_time: str


@dataclass
class EvaluationMetrics:
    category_accuracy: float
    category_report: str
    capability_mae: float
    capability_r2: float
    cv_scores: List[float]
    cv_mean: float
    cv_std: float
    robustness_accuracy: float
    robustness_drop: float
    feature_importance: Dict[str, float]
    dataset_balance: Dict[str, int]
    train_size: int
    test_size: int


# ═══════════════════════════════════════════════════════════════════════════════
# DEEP LEARNING MODELS (PyTorch)
# ═══════════════════════════════════════════════════════════════════════════════

class CategoryClassifierDL(nn.Module):
    """Deep Neural Network for Category Classification"""
    
    def __init__(self, n_features: int, n_classes: int):
        super().__init__()
        
        self.network = nn.Sequential(
            nn.Linear(n_features, 256),
            nn.BatchNorm1d(256),
            nn.ReLU(),
            nn.Dropout(0.3),
            
            nn.Linear(256, 128),
            nn.BatchNorm1d(128),
            nn.ReLU(),
            nn.Dropout(0.3),
            
            nn.Linear(128, 64),
            nn.BatchNorm1d(64),
            nn.ReLU(),
            nn.Dropout(0.2),
            
            nn.Linear(64, 32),
            nn.ReLU(),
            
            nn.Linear(32, n_classes)
        )
        
        # For feature importance
        self.gradients = None
        self.activations = None
    
    def forward(self, x):
        return self.network(x)
    
    def predict_proba(self, x):
        """Get probability distribution"""
        with torch.no_grad():
            logits = self.forward(x)
            probs = torch.softmax(logits, dim=1)
        return probs


class CapabilityRegressorDL(nn.Module):
    """Deep Neural Network for Capability Score Regression"""
    
    def __init__(self, n_features: int, n_outputs: int = 16):
        super().__init__()
        
        self.network = nn.Sequential(
            nn.Linear(n_features, 256),
            nn.BatchNorm1d(256),
            nn.ReLU(),
            nn.Dropout(0.3),
            
            nn.Linear(256, 128),
            nn.BatchNorm1d(128),
            nn.ReLU(),
            nn.Dropout(0.3),
            
            nn.Linear(128, 64),
            nn.BatchNorm1d(64),
            nn.ReLU(),
            nn.Dropout(0.2),
            
            nn.Linear(64, 32),
            nn.ReLU(),
            
            nn.Linear(32, n_outputs),
            nn.Sigmoid()  # Output 0-1, will scale to 0-10
        )
    
    def forward(self, x):
        return self.network(x) * 10  # Scale to 0-10


# ═══════════════════════════════════════════════════════════════════════════════
# PHYSICS ENGINE
# ═══════════════════════════════════════════════════════════════════════════════

class PhysicsEngine:
    
    @staticmethod
    def calculate_derived(obj: SCObject) -> SCObject:
        if obj.mass > 0 and obj.mass_kg == 0:
            obj.mass_kg = obj.mass * PhysicsConstants.M_EARTH
        if obj.radius > 0 and obj.radius_m == 0:
            obj.radius_m = obj.radius * 1000
        
        if obj.mass_kg > 0 and obj.radius_m > 0:
            volume = (4/3) * math.pi * (obj.radius_m ** 3)
            obj.density = obj.mass_kg / volume
            obj.surface_gravity = PhysicsConstants.G * obj.mass_kg / (obj.radius_m ** 2)
            obj.surface_gravity_g = obj.surface_gravity / PhysicsConstants.G_EARTH
            obj.escape_velocity = math.sqrt(2 * PhysicsConstants.G * obj.mass_kg / obj.radius_m)
        
        if obj.density > 0:
            if obj.density > 5000:
                obj.rock_fraction, obj.metallicity = 0.7, 0.3
            elif obj.density > 3000:
                obj.rock_fraction, obj.metallicity = 0.8, 0.15
            elif obj.density > 1500:
                obj.rock_fraction, obj.gas_fraction = 0.4, 0.4
            else:
                obj.gas_fraction = 0.8
        
        return obj
    
    @staticmethod
    def get_rule_category(obj: SCObject) -> str:
        if obj.temperature > 3000 or obj.luminosity > 0.001:
            if obj.temperature > 30000: return "Star (O-type)"
            if obj.temperature > 10000: return "Star (B-type)"
            if obj.temperature > 7500: return "Star (A-type)"
            if obj.temperature > 6000: return "Star (F-type)"
            if obj.temperature > 5200: return "Star (G-type)"
            if obj.temperature > 3700: return "Star (K-type)"
            return "Star (M-type)"
        
        if obj.density > 0:
            if obj.density < 2000 and obj.radius > 20000:
                return "Gas Giant"
            if obj.density > 3000 and obj.radius < 15000:
                return "Terrestrial Planet"
        
        return obj.category
    
    @staticmethod
    def get_hazards(obj: SCObject) -> List[str]:
        hazards = []
        if obj.temperature > 5000:
            hazards.append(f"🔴 EXTREME HEAT: {obj.temperature:.0f}K")
        elif obj.temperature > 1000:
            hazards.append(f"🟠 HIGH HEAT: {obj.temperature:.0f}K")
        if 0 < obj.temperature < 100:
            hazards.append(f"🔵 CRYOGENIC: {obj.temperature:.0f}K")
        if obj.surface_gravity_g > 10:
            hazards.append(f"🔴 CRUSHING GRAVITY: {obj.surface_gravity_g:.1f}g")
        elif obj.surface_gravity_g > 3:
            hazards.append(f"🟠 HIGH GRAVITY: {obj.surface_gravity_g:.1f}g")
        if obj.luminosity > 100:
            hazards.append("🔴 HIGH RADIATION")
        if not obj.has_atmosphere:
            hazards.append("🟡 NO ATMOSPHERE")
        if not hazards:
            hazards.append("🟢 NOMINAL")
        return hazards


# ═══════════════════════════════════════════════════════════════════════════════
# TRAINING DATA GENERATOR WITH BIAS BREAKING
# ═══════════════════════════════════════════════════════════════════════════════

class BiasBreakingDataGenerator:
    """
    Generates training targets WITH NOISE to break self-training loop.
    
    KEY INSIGHT:
    - Without noise: model memorizes formulas (circular learning)
    - With noise: model learns PATTERNS from data (real ML)
    """
    
    # Noise parameters
    GAUSSIAN_NOISE_STD = 0.3    # Standard deviation for Gaussian noise
    DROPOUT_PROB = 0.1          # Probability of random scaling
    DROPOUT_RANGE = (0.7, 1.3)  # Range for random scaling
    
    @classmethod
    def generate_targets(cls, obj: SCObject, add_noise: bool = True) -> Dict[str, float]:
        """Generate capability targets with bias-breaking noise"""
        scores = {}
        
        # Base scoring logic (same as before, creates training signal)
        s = obj.metallicity * 4 + (2 if obj.has_surface else 0) + (2 if obj.radius > 1000 else 0)
        if obj.surface_gravity_g > 0 and obj.surface_gravity_g < 2: s += 2
        scores["Resource Extraction (Space Mining)"] = min(s, 10)
        
        s = (3 if obj.has_water else 0) + (3 if obj.has_atmosphere else 0)
        if 200 < obj.temperature < 350: s += 2
        if obj.has_magnetic_field: s += 1
        scores["Life Prediction & Search (Astrobiology)"] = min(s, 10)
        
        s = (3 if obj.has_surface else 0) + (2 if obj.age > 0 else 0) + obj.rock_fraction * 3
        scores["Planetary Past Analysis"] = min(s, 10)
        
        s = (3 if obj.has_atmosphere else 0) + (2 if obj.temperature > 0 else 0) + (2 if obj.has_surface else 0)
        scores["Present Conditions Monitoring"] = min(s, 10)
        
        s = (3 if obj.eccentricity < 0.3 else 0) + (2 if obj.mass > 0 else 0) + (2 if obj.age > 0 else 0)
        scores["Future Condition Prediction"] = min(s, 10)
        
        s = (3 if obj.eccentricity < 0.2 else 0) + (2 if obj.has_surface else 0)
        if 0 < obj.surface_gravity_g < 3: s += 2
        scores["Satellite Deployment & Communication"] = min(s, 10)
        
        s = (3 if obj.has_surface else 0) + (2 if obj.radius > 0 else 0)
        if 0 < obj.surface_gravity_g < 2: s += 2
        scores["Space Exploration & Mapping"] = min(s, 10)
        
        s = 0
        if 0 < obj.surface_gravity_g < 0.1: s += 4
        elif obj.surface_gravity_g < 0.5: s += 2
        if obj.radius > 0 and obj.radius < 500: s += 3
        scores["Microgravity Experiments"] = min(s, 10)
        
        s = (3 if obj.eccentricity < 0.2 else 0) + obj.metallicity * 3 + (2 if obj.has_surface else 0)
        scores["Space Construction"] = min(s, 10)
        
        s = (4 if obj.luminosity > 0 else 0) + (3 if obj.temperature > 1000 else 0)
        scores["Energy Generation"] = min(s, 10)
        
        s = (3 if 0 < obj.eccentricity < 0.5 else 0) + (2 if obj.semi_major_axis > 0 else 0)
        scores["Planetary Defense"] = min(s, 10)
        
        s = (3 if obj.mass > 10 else 1 if obj.mass > 1 else 0) + (2 if obj.semi_major_axis > 0 else 0)
        scores["Propulsion & Travel Technology"] = min(s, 10)
        
        s = (2 if obj.has_surface else 0) + (2 if obj.has_atmosphere else 0) + (2 if obj.temperature > 0 else 0)
        scores["AI & Autonomous Systems"] = min(s, 10)
        
        s = (3 if obj.has_atmosphere else 0) + (2 if obj.has_water else 0)
        if 250 < obj.temperature < 320: s += 3
        if 0.5 < obj.surface_gravity_g < 1.5: s += 2
        scores["Human Survival Research"] = min(s, 10)
        
        s = (3 if obj.has_atmosphere else 0) + (2 if obj.has_water else 0) + (2 if obj.has_magnetic_field else 0)
        if 150 < obj.temperature < 400: s += 2
        scores["Terraforming (Future Concept)"] = min(s, 10)
        
        s = (3 if obj.luminosity > 0 else 0) + (2 if obj.temperature > 0 else 0)
        scores["Space Observation & Astronomy"] = min(s, 10)
        
        # ═══════════════════════════════════════════════════════════════════
        # BIAS BREAKING: Add noise to prevent memorization
        # ═══════════════════════════════════════════════════════════════════
        if add_noise:
            for k in scores:
                # Method 1: Gaussian noise
                scores[k] += np.random.normal(0, cls.GAUSSIAN_NOISE_STD)
                
                # Method 2: Random dropout/scaling (10% chance)
                if np.random.rand() < cls.DROPOUT_PROB:
                    scores[k] *= np.random.uniform(*cls.DROPOUT_RANGE)
                
                # Clamp to valid range
                scores[k] = max(0, min(10, scores[k]))
        
        return scores


# ═══════════════════════════════════════════════════════════════════════════════
# FEATURE EXTRACTOR
# ═══════════════════════════════════════════════════════════════════════════════

class FeatureExtractor:
    N_FEATURES = 30
    
    def extract(self, obj: SCObject) -> np.ndarray:
        f = np.zeros(self.N_FEATURES, dtype=np.float32)
        log = lambda v: np.log10(v + 1) if v > 0 else 0
        
        f[0] = log(obj.mass)
        f[1] = log(obj.radius)
        f[2] = log(obj.temperature)
        f[3] = log(obj.luminosity)
        f[4] = log(obj.age)
        f[5] = log(obj.semi_major_axis)
        f[6] = min(obj.eccentricity, 1.0)
        f[7] = obj.inclination / 180 if obj.inclination else 0
        f[8] = log(obj.period)
        f[9] = log(obj.surface_pressure)
        f[10] = log(obj.surface_gravity) if obj.surface_gravity > 0 else 0
        f[11] = min(obj.albedo, 1.0)
        f[12] = log(obj.density) if obj.density > 0 else 0
        f[13] = log(obj.escape_velocity) if obj.escape_velocity > 0 else 0
        
        f[14] = float(obj.has_atmosphere)
        f[15] = float(obj.has_water)
        f[16] = float(obj.has_magnetic_field)
        f[17] = float(obj.has_rings)
        f[18] = float(obj.is_metal_rich)
        f[19] = float(obj.has_surface)
        
        f[20] = obj.metallicity
        f[21] = obj.ice_fraction
        f[22] = obj.rock_fraction
        f[23] = obj.gas_fraction
        
        c = obj.category.lower()
        f[24] = float('star' in c)
        f[25] = float('giant' in c)
        f[26] = float('terrestrial' in c)
        f[27] = float('moon' in c)
        f[28] = float('asteroid' in c)
        f[29] = float('nebula' in c)
        
        return f


# ═══════════════════════════════════════════════════════════════════════════════
# FEATURE IMPORTANCE CALCULATOR (XAI)
# ═══════════════════════════════════════════════════════════════════════════════

class FeatureImportanceCalculator:
    """
    Calculates feature importance using gradient-based methods.
    This provides EXPLAINABILITY (XAI).
    """
    
    @staticmethod
    def calculate_gradient_importance(model: nn.Module, X: torch.Tensor, 
                                       scaler: StandardScaler) -> Dict[str, float]:
        """
        Calculate feature importance using input gradients.
        Higher gradient magnitude = more important feature.
        """
        model.eval()
        X.requires_grad = True
        
        # Forward pass
        outputs = model(X)
        
        # Get gradients for all classes
        importance = torch.zeros(X.shape[1])
        
        for i in range(outputs.shape[1]):
            model.zero_grad()
            outputs[:, i].sum().backward(retain_graph=True)
            importance += torch.abs(X.grad).mean(dim=0)
        
        importance = importance / outputs.shape[1]
        importance = importance.detach().numpy()
        
        # Normalize
        importance = importance / (importance.sum() + 1e-8)
        
        # Map to feature names
        importance_dict = {}
        for i, name in enumerate(FEATURE_NAMES[:len(importance)]):
            importance_dict[name] = float(importance[i])
        
        return dict(sorted(importance_dict.items(), key=lambda x: -x[1]))
    
    @staticmethod
    def calculate_permutation_importance(model: nn.Module, X: np.ndarray, 
                                         y: np.ndarray, n_repeats: int = 5) -> Dict[str, float]:
        """
        Calculate permutation importance.
        Shuffle each feature and measure performance drop.
        """
        model.eval()
        
        # Baseline accuracy
        with torch.no_grad():
            X_tensor = torch.FloatTensor(X).to(DEVICE)
            outputs = model(X_tensor)
            preds = outputs.argmax(dim=1).cpu().numpy()
            baseline_acc = accuracy_score(y, preds)
        
        importance = {}
        
        for i, name in enumerate(FEATURE_NAMES[:X.shape[1]]):
            scores = []
            for _ in range(n_repeats):
                X_permuted = X.copy()
                np.random.shuffle(X_permuted[:, i])
                
                with torch.no_grad():
                    X_tensor = torch.FloatTensor(X_permuted).to(DEVICE)
                    outputs = model(X_tensor)
                    preds = outputs.argmax(dim=1).cpu().numpy()
                    acc = accuracy_score(y, preds)
                
                scores.append(baseline_acc - acc)
            
            importance[name] = float(np.mean(scores))
        
        return dict(sorted(importance.items(), key=lambda x: -x[1]))


# ═══════════════════════════════════════════════════════════════════════════════
# SC PARSER
# ═══════════════════════════════════════════════════════════════════════════════

class SCParser:
    
    def parse_file(self, filepath: Path) -> List[SCObject]:
        try:
            with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
        except:
            return []
        
        objects = []
        pattern = r'(Star|Planet|Moon|DwarfMoon|Asteroid|Comet|Barycenter|Nebula)\s*"([^"]+)"\s*\{([^{}]*(?:\{[^{}]*\}[^{}]*)*)\}'
        
        for match in re.finditer(pattern, content, re.IGNORECASE | re.DOTALL):
            obj = self._parse_block(match.group(2), match.group(1), match.group(3))
            obj.source_file = str(filepath)
            obj.raw_content = match.group(0)
            objects.append(obj)
        
        if not objects:
            obj = self._parse_simple(content, filepath.stem)
            obj.source_file = str(filepath)
            if obj.name != "Unknown" or obj.mass > 0:
                objects.append(obj)
        
        return objects
    
    def _parse_block(self, name: str, obj_type: str, content: str) -> SCObject:
        obj = SCObject(name=name, obj_type=obj_type)
        
        obj.mass = self._extract(content, ['Mass'])
        obj.radius = self._extract(content, ['Radius', 'RadSol', 'RadKm'])
        obj.temperature = self._extract(content, ['Teff', 'Temperature', 'SurfTemp'])
        obj.luminosity = self._extract(content, ['Lum', 'Luminosity'])
        obj.age = self._extract(content, ['Age'])
        obj.semi_major_axis = self._extract(content, ['SemiMajorAxis'])
        obj.eccentricity = self._extract(content, ['Eccentricity'])
        obj.period = self._extract(content, ['Period'])
        obj.inclination = self._extract(content, ['Inclination'])
        obj.surface_gravity = self._extract(content, ['SurfGrav', 'Gravity'])
        obj.surface_pressure = self._extract(content, ['SurfPressure', 'Pressure'])
        obj.albedo = self._extract(content, ['Albedo'])
        
        c = content.lower()
        obj.has_atmosphere = 'atmosphere' in c
        obj.has_water = any(w in c for w in ['water', 'ocean', 'ice', 'h2o'])
        obj.has_magnetic_field = 'magfield' in c or 'magnetic' in c
        obj.has_rings = 'ring' in c
        obj.is_metal_rich = any(w in c for w in ['metal', 'iron', 'ferria'])
        obj.has_surface = obj_type.lower() not in ['star', 'barycenter']
        
        if obj.is_metal_rich:
            obj.metallicity = 0.4
        
        obj.category = self._classify(obj, content)
        obj = PhysicsEngine.calculate_derived(obj)
        
        return obj
    
    def _parse_simple(self, content: str, fallback: str) -> SCObject:
        obj = SCObject(name=fallback)
        
        name_match = re.search(r'(?:Name|StarName|PlanetName)\s*"([^"]+)"', content, re.IGNORECASE)
        if name_match:
            obj.name = name_match.group(1)
        
        obj.mass = self._extract(content, ['Mass'])
        obj.radius = self._extract(content, ['Radius'])
        obj.temperature = self._extract(content, ['Teff', 'Temperature'])
        obj.luminosity = self._extract(content, ['Lum', 'Luminosity'])
        obj.category = self._guess(content)
        obj.raw_content = content
        obj = PhysicsEngine.calculate_derived(obj)
        
        return obj
    
    def _extract(self, content: str, keys: List[str]) -> float:
        for key in keys:
            match = re.search(rf'{key}\s+([-+]?[\d.eE+-]+)', content, re.IGNORECASE)
            if match:
                try:
                    return float(match.group(1))
                except:
                    pass
        return 0.0
    
    def _classify(self, obj: SCObject, content: str) -> str:
        c = content.lower()
        t = obj.obj_type.lower()
        
        spec = re.search(r'SpType\s*"?([OBAFGKM]\d?)', content, re.IGNORECASE)
        if spec:
            return f"Star ({spec.group(1)[0].upper()}-type)"
        
        if t == 'star':
            if obj.temperature > 30000: return "Star (O-type)"
            if obj.temperature > 10000: return "Star (B-type)"
            if obj.temperature > 7500: return "Star (A-type)"
            if obj.temperature > 6000: return "Star (F-type)"
            if obj.temperature > 5200: return "Star (G-type)"
            if obj.temperature > 3700: return "Star (K-type)"
            return "Star (M-type)"
        
        if t == 'planet':
            if 'gasgiant' in c or 'jupiter' in c: return "Gas Giant"
            if 'icegiant' in c: return "Ice Giant"
            if 'terra' in c or 'terrestrial' in c: return "Terrestrial Planet"
            return "Terrestrial Planet"
        
        if t in ['moon', 'dwarfmoon']: return "Moon (Rocky)"
        if t == 'asteroid': return "Asteroid"
        if t == 'comet': return "Comet"
        if 'nebula' in c: return "Nebula"
        
        return "Unknown Object"
    
    def _guess(self, content: str) -> str:
        c = content.lower()[:300]
        if 'star' in c: return "Star (G-type)"
        if 'planet' in c: return "Terrestrial Planet"
        if 'moon' in c: return "Moon (Rocky)"
        if 'asteroid' in c: return "Asteroid"
        return "Unknown Object"


# ═══════════════════════════════════════════════════════════════════════════════
# DEEP LEARNING MODEL WITH FULL RESEARCH PIPELINE
# ═══════════════════════════════════════════════════════════════════════════════

class StellarDLModel:
    """
    Deep Learning Model with:
    1. Bias-breaking training (noise injection)
    2. K-Fold Cross Validation
    3. Feature Importance (XAI)
    4. Dataset Balance Analysis
    5. Normalized Targets
    """
    
    def __init__(self):
        self.scaler = StandardScaler()
        self.label_encoder = LabelEncoder()
        self.category_model: Optional[CategoryClassifierDL] = None
        self.capability_model: Optional[CapabilityRegressorDL] = None
        self.feature_extractor = FeatureExtractor()
        self.is_trained = False
        self.n_features = 30
        self.n_classes = 0
        self.evaluation: Optional[EvaluationMetrics] = None
        self.feature_importance: Dict[str, float] = {}
    
    @staticmethod
    def model_exists() -> bool:
        return MODEL_FILE.exists()
    
    def train(self, catalog_path: Path) -> bool:
        """Full research-grade training pipeline"""
        MODEL_DIR.mkdir(parents=True, exist_ok=True)
        
        console.print(Panel.fit(
            "[bold cyan]DEEP LEARNING RESEARCH TRAINING[/bold cyan]\n"
            "[dim]With bias-breaking, cross-validation, and XAI[/dim]",
            border_style="cyan"
        ))
        
        # ═══════════════════════════════════════════════════════════════════
        # STEP 1: Parse Data
        # ═══════════════════════════════════════════════════════════════════
        console.print("[cyan]⟫ [1/8] Scanning catalog...[/cyan]")
        sc_files = list(catalog_path.rglob("*.sc"))
        console.print(f"[green]  Found {len(sc_files)} files[/green]")
        
        if not sc_files:
            console.print("[red]No .sc files found![/red]")
            return False
        
        parser = SCParser()
        objects = []
        
        with Progress(SpinnerColumn(), TextColumn("[cyan]{task.description}[/cyan]"),
                     BarColumn(bar_width=40), TaskProgressColumn(),
                     MofNCompleteColumn(), TimeElapsedColumn(), console=console) as progress:
            task = progress.add_task("Parsing...", total=len(sc_files))
            for sc_file in sc_files:
                try:
                    objects.extend(parser.parse_file(sc_file))
                except:
                    pass
                progress.update(task, advance=1)
        
        console.print(f"[green]  Extracted {len(objects)} objects[/green]")
        
        if len(objects) < 50:
            console.print("[red]Need at least 50 objects![/red]")
            return False
        
        # ═══════════════════════════════════════════════════════════════════
        # STEP 2: Build Training Data with BIAS-BREAKING NOISE
        # ═══════════════════════════════════════════════════════════════════
        console.print("[cyan]⟫ [2/8] Building training data with bias-breaking noise...[/cyan]")
        
        X, y_cat, y_cap = [], [], []
        for obj in objects:
            X.append(self.feature_extractor.extract(obj))
            y_cat.append(obj.category)
            
            # CRITICAL: Add noise to break self-training loop
            scores = BiasBreakingDataGenerator.generate_targets(obj, add_noise=True)
            
            # Normalize to 0-1 for training stability
            y_cap.append([scores[c] / 10.0 for c in CAPABILITIES])
        
        X = np.array(X, dtype=np.float32)
        y_cap = np.array(y_cap, dtype=np.float32)
        y_cat = np.array(y_cat)
        
        self.n_features = X.shape[1]
        console.print(f"[green]  Features: {X.shape}, Noise STD: {BiasBreakingDataGenerator.GAUSSIAN_NOISE_STD}[/green]")
        
        # ═══════════════════════════════════════════════════════════════════
        # STEP 3: Dataset Balance Analysis
        # ═══════════════════════════════════════════════════════════════════
        console.print("[cyan]⟫ [3/8] Analyzing dataset balance...[/cyan]")
        
        category_counts = Counter(y_cat)
        console.print("[dim]  Category distribution:[/dim]")
        
        table = Table(box=ROUNDED, border_style="blue", show_header=True)
        table.add_column("Category", style="cyan")
        table.add_column("Count", style="green")
        table.add_column("Percentage", style="yellow")
        
        total = len(y_cat)
        for cat, count in sorted(category_counts.items(), key=lambda x: -x[1])[:10]:
            table.add_row(cat[:30], str(count), f"{count/total*100:.1f}%")
        
        console.print(table)
        
        # ═══════════════════════════════════════════════════════════════════
        # STEP 4: Train/Test Split
        # ═══════════════════════════════════════════════════════════════════
        console.print("[cyan]⟫ [4/8] Train/Test split (80/20)...[/cyan]")
        
        X_train, X_test, y_cat_train, y_cat_test, y_cap_train, y_cap_test = train_test_split(
            X, y_cat, y_cap, test_size=0.2, random_state=42, stratify=y_cat
        )
        
        console.print(f"[green]  Train: {len(X_train)} | Test: {len(X_test)}[/green]")
        
        # Scale features
        X_train_scaled = self.scaler.fit_transform(X_train)
        X_test_scaled = self.scaler.transform(X_test)
        
        # Encode categories
        y_cat_train_enc = self.label_encoder.fit_transform(y_cat_train)
        y_cat_test_enc = self.label_encoder.transform(y_cat_test)
        self.n_classes = len(self.label_encoder.classes_)
        
        # ═══════════════════════════════════════════════════════════════════
        # STEP 5: K-FOLD CROSS VALIDATION (Gold Standard)
        # ═══════════════════════════════════════════════════════════════════
        console.print("[cyan]⟫ [5/8] K-Fold Cross Validation (5-fold)...[/cyan]")
        
        # Use sklearn classifier for CV (faster for this purpose)
        cv_model = MLPClassifier(hidden_layer_sizes=(128, 64, 32), max_iter=300, 
                                  early_stopping=True, random_state=42, verbose=False)
        
        cv_scores = cross_val_score(cv_model, X_train_scaled, y_cat_train_enc, cv=5, scoring='accuracy')
        
        console.print(f"[green]  CV Scores: {[f'{s:.3f}' for s in cv_scores]}[/green]")
        console.print(f"[green]  CV Mean: {cv_scores.mean():.3f} ± {cv_scores.std():.3f}[/green]")
        
        # ═══════════════════════════════════════════════════════════════════
        # STEP 6: Train Deep Learning Models
        # ═══════════════════════════════════════════════════════════════════
        console.print("[cyan]⟫ [6/8] Training Deep Learning models...[/cyan]")
        
        # Convert to tensors
        X_train_t = torch.FloatTensor(X_train_scaled).to(DEVICE)
        X_test_t = torch.FloatTensor(X_test_scaled).to(DEVICE)
        y_cat_train_t = torch.LongTensor(y_cat_train_enc).to(DEVICE)
        y_cap_train_t = torch.FloatTensor(y_cap_train).to(DEVICE)
        
        # Create data loaders
        train_dataset = TensorDataset(X_train_t, y_cat_train_t, y_cap_train_t)
        train_loader = DataLoader(train_dataset, batch_size=32, shuffle=True)
        
        # Initialize models
        self.category_model = CategoryClassifierDL(self.n_features, self.n_classes).to(DEVICE)
        self.capability_model = CapabilityRegressorDL(self.n_features, 16).to(DEVICE)
        
        # Optimizers
        cat_optimizer = optim.Adam(self.category_model.parameters(), lr=0.001)
        cap_optimizer = optim.Adam(self.capability_model.parameters(), lr=0.001)
        
        # Loss functions
        cat_criterion = nn.CrossEntropyLoss()
        cap_criterion = nn.MSELoss()
        
        # Training loop
        epochs = 100
        best_loss = float('inf')
        patience = 10
        patience_counter = 0
        
        with Progress(SpinnerColumn(), TextColumn("[cyan]{task.description}[/cyan]"),
                     BarColumn(bar_width=30), TaskProgressColumn(), console=console) as progress:
            task = progress.add_task("Training DL models...", total=epochs)
            
            for epoch in range(epochs):
                self.category_model.train()
                self.capability_model.train()
                
                epoch_cat_loss = 0
                epoch_cap_loss = 0
                
                for batch_X, batch_y_cat, batch_y_cap in train_loader:
                    # Category classifier
                    cat_optimizer.zero_grad()
                    cat_out = self.category_model(batch_X)
                    cat_loss = cat_criterion(cat_out, batch_y_cat)
                    cat_loss.backward()
                    cat_optimizer.step()
                    epoch_cat_loss += cat_loss.item()
                    
                    # Capability regressor
                    cap_optimizer.zero_grad()
                    cap_out = self.capability_model(batch_X) / 10.0  # Scale back
                    cap_loss = cap_criterion(cap_out, batch_y_cap)
                    cap_loss.backward()
                    cap_optimizer.step()
                    epoch_cap_loss += cap_loss.item()
                
                total_loss = epoch_cat_loss + epoch_cap_loss
                
                # Early stopping
                if total_loss < best_loss:
                    best_loss = total_loss
                    patience_counter = 0
                else:
                    patience_counter += 1
                
                if patience_counter >= patience:
                    console.print(f"[yellow]  Early stopping at epoch {epoch+1}[/yellow]")
                    break
                
                progress.update(task, advance=1)
        
        # ═══════════════════════════════════════════════════════════════════
        # STEP 7: Evaluation
        # ═══════════════════════════════════════════════════════════════════
        console.print("[cyan]⟫ [7/8] Evaluating models...[/cyan]")
        
        self.category_model.eval()
        self.capability_model.eval()
        
        with torch.no_grad():
            # Category predictions
            cat_probs = self.category_model.predict_proba(X_test_t)
            cat_preds = cat_probs.argmax(dim=1).cpu().numpy()
            cat_pred_labels = self.label_encoder.inverse_transform(cat_preds)
            
            # Capability predictions
            cap_preds = self.capability_model(X_test_t).cpu().numpy()
        
        # Category metrics
        cat_accuracy = accuracy_score(y_cat_test_enc, cat_preds)
        cat_report = classification_report(y_cat_test, cat_pred_labels, zero_division=0)
        
        # Capability metrics
        cap_mae = mean_absolute_error(y_cap_test * 10, cap_preds)  # Scale back
        cap_r2 = r2_score(y_cap_test * 10, cap_preds)
        
        console.print(f"[green]  Category Accuracy: {cat_accuracy*100:.1f}%[/green]")
        console.print(f"[green]  Capability MAE: {cap_mae:.2f}[/green]")
        console.print(f"[green]  Capability R²: {cap_r2:.3f}[/green]")
        
        # Error analysis
        errors = []
        for i in range(len(y_cat_test)):
            if cat_pred_labels[i] != y_cat_test[i]:
                errors.append({'true': y_cat_test[i], 'predicted': cat_pred_labels[i]})
        
        if errors:
            console.print(f"[yellow]  Errors: {len(errors)}/{len(y_cat_test)}[/yellow]")
            console.print("[dim]  Sample errors:[/dim]")
            for err in errors[:3]:
                console.print(f"[dim]    • {err['true']} → {err['predicted']}[/dim]")
        
        # Robustness test
        console.print("[cyan]  Robustness test (5% noise)...[/cyan]")
        noise = np.random.normal(0, 0.05, X_test_scaled.shape)
        X_test_noisy = torch.FloatTensor(X_test_scaled + noise).to(DEVICE)
        
        with torch.no_grad():
            noisy_preds = self.category_model(X_test_noisy).argmax(dim=1).cpu().numpy()
        
        robust_accuracy = accuracy_score(y_cat_test_enc, noisy_preds)
        robustness_drop = cat_accuracy - robust_accuracy
        
        console.print(f"[yellow]  Noisy Accuracy: {robust_accuracy*100:.1f}% (drop: {robustness_drop*100:.1f}%)[/yellow]")
        
        # ═══════════════════════════════════════════════════════════════════
        # STEP 8: Feature Importance (XAI)
        # ═══════════════════════════════════════════════════════════════════
        console.print("[cyan]⟫ [8/8] Computing feature importance (XAI)...[/cyan]")
        
        self.feature_importance = FeatureImportanceCalculator.calculate_permutation_importance(
            self.category_model, X_test_scaled, y_cat_test_enc, n_repeats=3
        )
        
        console.print("[dim]  Top 10 most important features:[/dim]")
        table = Table(box=ROUNDED, border_style="magenta")
        table.add_column("Feature", style="cyan")
        table.add_column("Importance", style="green")
        
        for name, imp in list(self.feature_importance.items())[:10]:
            bar = "█" * int(imp * 100) if imp > 0 else ""
            table.add_row(name, f"{imp:.4f} {bar}")
        
        console.print(table)
        
        # Store evaluation
        self.evaluation = EvaluationMetrics(
            category_accuracy=cat_accuracy,
            category_report=cat_report,
            capability_mae=cap_mae,
            capability_r2=cap_r2,
            cv_scores=cv_scores.tolist(),
            cv_mean=float(cv_scores.mean()),
            cv_std=float(cv_scores.std()),
            robustness_accuracy=robust_accuracy,
            robustness_drop=robustness_drop,
            feature_importance=self.feature_importance,
            dataset_balance=dict(category_counts),
            train_size=len(X_train),
            test_size=len(X_test)
        )
        
        # Save everything
        self._save()
        
        # Display final summary
        self._display_summary()
        
        self.is_trained = True
        return True
    
    def _display_summary(self):
        """Display comprehensive training summary"""
        if not self.evaluation:
            return
        
        ev = self.evaluation
        
        console.print()
        console.print(Rule("[bold cyan]TRAINING COMPLETE - RESEARCH METRICS[/bold cyan]", style="cyan"))
        
        table = Table(title="[cyan]Model Performance Summary[/cyan]", box=DOUBLE, border_style="cyan")
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="green")
        table.add_column("Status", style="yellow")
        
        # Category accuracy
        acc = ev.category_accuracy
        status = "✓ Excellent" if acc > 0.9 else "✓ Good" if acc > 0.8 else "○ Fair" if acc > 0.7 else "✗ Improve"
        table.add_row("Category Accuracy", f"{acc*100:.1f}%", status)
        
        # Cross-validation
        table.add_row("CV Mean ± Std", f"{ev.cv_mean:.3f} ± {ev.cv_std:.3f}", 
                     "✓ Stable" if ev.cv_std < 0.05 else "○ Variable")
        
        # Capability metrics
        status = "✓ Excellent" if ev.capability_mae < 0.5 else "✓ Good" if ev.capability_mae < 1.0 else "○ Fair"
        table.add_row("Capability MAE", f"{ev.capability_mae:.2f}", status)
        
        status = "✓ Excellent" if ev.capability_r2 > 0.9 else "✓ Good" if ev.capability_r2 > 0.8 else "○ Fair"
        table.add_row("Capability R²", f"{ev.capability_r2:.3f}", status)
        
        # Robustness
        status = "✓ Robust" if ev.robustness_drop < 0.03 else "✓ Stable" if ev.robustness_drop < 0.08 else "○ Sensitive"
        table.add_row("Robustness Drop", f"{ev.robustness_drop*100:.1f}%", status)
        
        console.print(table)
        
        console.print(Panel.fit(
            f"[bold green]✓ MODEL SAVED SUCCESSFULLY[/bold green]\n\n"
            f"[dim]Location: {MODEL_FILE}[/dim]\n"
            f"[dim]Evaluation: {EVAL_FILE}[/dim]\n"
            f"[dim]Feature Importance: {IMPORTANCE_FILE}[/dim]",
            border_style="green"
        ))
    
    def _save(self):
        """Save all model components"""
        # Save PyTorch models
        torch.save({
            'category_model': self.category_model.state_dict(),
            'capability_model': self.capability_model.state_dict(),
            'n_features': self.n_features,
            'n_classes': self.n_classes
        }, MODEL_FILE)
        
        # Save scaler and encoder
        with open(SCALER_FILE, 'wb') as f:
            pickle.dump(self.scaler, f)
        with open(ENCODER_FILE, 'wb') as f:
            pickle.dump(self.label_encoder, f)
        
        # Save evaluation
        if self.evaluation:
            eval_data = {
                'category_accuracy': self.evaluation.category_accuracy,
                'category_report': self.evaluation.category_report,
                'capability_mae': self.evaluation.capability_mae,
                'capability_r2': self.evaluation.capability_r2,
                'cv_scores': self.evaluation.cv_scores,
                'cv_mean': self.evaluation.cv_mean,
                'cv_std': self.evaluation.cv_std,
                'robustness_accuracy': self.evaluation.robustness_accuracy,
                'robustness_drop': self.evaluation.robustness_drop,
                'dataset_balance': self.evaluation.dataset_balance,
                'train_size': self.evaluation.train_size,
                'test_size': self.evaluation.test_size
            }
            with open(EVAL_FILE, 'w') as f:
                json.dump(eval_data, f, indent=2)
        
        # Save feature importance
        if self.feature_importance:
            with open(IMPORTANCE_FILE, 'w') as f:
                json.dump(self.feature_importance, f, indent=2)
    
    def load(self) -> bool:
        """Load trained model"""
        if not MODEL_FILE.exists():
            return False
        
        try:
            console.print("[cyan]⟫ Loading Deep Learning model...[/cyan]", end=" ")
            
            # Load scaler and encoder
            with open(SCALER_FILE, 'rb') as f:
                self.scaler = pickle.load(f)
            with open(ENCODER_FILE, 'rb') as f:
                self.label_encoder = pickle.load(f)
            
            # Load PyTorch models
            checkpoint = torch.load(MODEL_FILE, map_location=DEVICE)
            self.n_features = checkpoint['n_features']
            self.n_classes = checkpoint['n_classes']
            
            self.category_model = CategoryClassifierDL(self.n_features, self.n_classes).to(DEVICE)
            self.capability_model = CapabilityRegressorDL(self.n_features, 16).to(DEVICE)
            
            self.category_model.load_state_dict(checkpoint['category_model'])
            self.capability_model.load_state_dict(checkpoint['capability_model'])
            
            self.category_model.eval()
            self.capability_model.eval()
            
            # Load feature importance
            if IMPORTANCE_FILE.exists():
                with open(IMPORTANCE_FILE) as f:
                    self.feature_importance = json.load(f)
            
            self.is_trained = True
            console.print("[green]✓[/green]")
            
            # Show metrics
            if EVAL_FILE.exists():
                with open(EVAL_FILE) as f:
                    ev = json.load(f)
                console.print(f"[dim]  Acc: {ev.get('category_accuracy', 0)*100:.1f}% | "
                            f"CV: {ev.get('cv_mean', 0):.3f}±{ev.get('cv_std', 0):.3f} | "
                            f"R²: {ev.get('capability_r2', 0):.3f}[/dim]")
            
            return True
        except Exception as e:
            console.print(f"[red]✗ {e}[/red]")
            return False
    
    def predict(self, obj: SCObject) -> AnalysisResult:
        """Make prediction with uncertainty and feature contributions"""
        
        # Extract and scale features
        features = self.feature_extractor.extract(obj)
        if len(features) != self.n_features:
            features = np.pad(features, (0, max(0, self.n_features - len(features))))[:self.n_features]
        
        features_scaled = self.scaler.transform(features.reshape(1, -1))
        features_tensor = torch.FloatTensor(features_scaled).to(DEVICE)
        
        # Predictions
        self.category_model.eval()
        self.capability_model.eval()
        
        with torch.no_grad():
            # Category with probabilities
            cat_probs = self.category_model.predict_proba(features_tensor)[0]
            cat_idx = cat_probs.argmax().item()
            dl_category = self.label_encoder.inverse_transform([cat_idx])[0]
            dl_confidence = float(cat_probs[cat_idx])
            
            # All probabilities
            dl_proba_dict = {
                self.label_encoder.inverse_transform([i])[0]: float(p)
                for i, p in enumerate(cat_probs.cpu().numpy()) if p > 0.01
            }
            
            # Capability scores
            cap_scores = self.capability_model(features_tensor)[0].cpu().numpy()
            dl_capability_scores = {c: float(cap_scores[i]) for i, c in enumerate(CAPABILITIES)}
        
        # Feature contributions for this prediction
        feature_contributions = {}
        for i, name in enumerate(FEATURE_NAMES[:len(features)]):
            if name in self.feature_importance:
                feature_contributions[name] = self.feature_importance[name] * float(features_scaled[0, i])
        
        # Rule validation
        rule_category = PhysicsEngine.get_rule_category(obj)
        agreement = dl_category == rule_category or rule_category == obj.category
        disagreement_note = "" if agreement else f"DL: {dl_category} vs Rules: {rule_category}"
        
        # Final output (DL-driven)
        ranked = sorted(dl_capability_scores.items(), key=lambda x: -x[1])
        
        return AnalysisResult(
            obj=obj,
            dl_category=dl_category,
            dl_confidence=dl_confidence,
            dl_probabilities=dl_proba_dict,
            dl_capability_scores=dl_capability_scores,
            rule_category=rule_category,
            agreement=agreement,
            disagreement_note=disagreement_note,
            final_category=dl_category,
            final_confidence=dl_confidence,
            final_scores=dl_capability_scores,
            ranked_capabilities=ranked,
            top_capabilities=ranked[:5],
            feature_contributions=dict(sorted(feature_contributions.items(), key=lambda x: -abs(x[1]))[:10]),
            hazards=PhysicsEngine.get_hazards(obj),
            recommendations=self._get_recommendations(ranked[0][0] if ranked else ""),
            analysis_time=datetime.datetime.now().isoformat()
        )
    
    def _get_recommendations(self, top_cap: str) -> List[str]:
        recs = {
            "Resource Extraction": ["Deploy mining survey", "Analyze minerals", "Plan extraction"],
            "Life Prediction": ["Deploy biosensors", "Sample for organics", "Search water"],
            "Energy Generation": ["Position solar arrays", "Harness thermal", "Setup power grid"],
            "Space Exploration": ["Deploy rovers", "Map terrain", "Establish outposts"]
        }
        for key, rec in recs.items():
            if key in top_cap:
                return rec
        return ["Conduct analysis", "Deploy probes", "Gather data"]


# ═══════════════════════════════════════════════════════════════════════════════
# REPORT GENERATOR
# ═══════════════════════════════════════════════════════════════════════════════

class ReportGenerator:
    
    def __init__(self):
        REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    
    def _filename(self, name: str, ext: str) -> Path:
        safe = re.sub(r'[^\w\-]', '_', name)
        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        return REPORTS_DIR / f"{safe}_{ts}.{ext}"
    
    def generate_txt(self, r: AnalysisResult) -> Path:
        path = self._filename(r.obj.name, "txt")
        w = 80
        fmt = lambda v, u="": f"{v:.3e}{u}" if (v > 1e6 or (0 < v < 1e-3)) else f"{v:.4g}{u}" if v > 0 else "—"
        
        lines = [
            "═" * w, "STELLAR INTELLIGENCE v7.0 - DEEP LEARNING REPORT".center(w), "═" * w, "",
            f"Generated: {r.analysis_time}", f"Source: {r.obj.source_file}", "",
            "─" * w, "DL PREDICTION".center(w), "─" * w,
            f"  Category:    {r.dl_category}",
            f"  Confidence:  {r.dl_confidence * 100:.1f}%", "",
            "  Probability Distribution:"
        ]
        
        for cat, prob in sorted(r.dl_probabilities.items(), key=lambda x: -x[1])[:5]:
            lines.append(f"    • {cat}: {prob*100:.1f}%")
        
        lines.extend(["", "─" * w, "FEATURE CONTRIBUTIONS (XAI)".center(w), "─" * w])
        for feat, contrib in list(r.feature_contributions.items())[:8]:
            lines.append(f"    {feat}: {contrib:+.4f}")
        
        lines.extend(["", "─" * w, "PHYSICAL PROPERTIES".center(w), "─" * w,
            f"  Mass:   {fmt(r.obj.mass)} M⊕  |  Radius: {fmt(r.obj.radius)} km",
            f"  Temp:   {fmt(r.obj.temperature)} K  |  Density: {fmt(r.obj.density)} kg/m³",
            f"  Gravity: {fmt(r.obj.surface_gravity_g)}g", "",
            "═" * w, "DL CAPABILITY PREDICTIONS".center(w), "═" * w, ""])
        
        for cap, score in r.ranked_capabilities:
            bar = "█" * int(score / 10 * 20) + "░" * (20 - int(score / 10 * 20))
            lines.extend([f"  {CAPABILITY_ICONS.get(cap, '◆')} {cap}", f"     [{bar}] {score:.1f}/10", ""])
        
        lines.extend(["─" * w, "HAZARDS".center(w), "─" * w] + [f"  {h}" for h in r.hazards])
        lines.extend(["", "─" * w, "RECOMMENDATIONS".center(w), "─" * w])
        lines.extend([f"  {i}. {rec}" for i, rec in enumerate(r.recommendations, 1)])
        lines.extend(["", "═" * w, "END".center(w), "═" * w, "", "Stellar Intelligence v7.0 - DL Research Edition"])
        
        path.write_text('\n'.join(lines), encoding='utf-8')
        return path
    
    def generate_json(self, r: AnalysisResult) -> Path:
        path = self._filename(r.obj.name, "json")
        report = {
            'version': '7.0', 'deep_learning': True, 'bias_breaking': True,
            'generated': r.analysis_time,
            'dl_prediction': {
                'category': r.dl_category, 'confidence': r.dl_confidence,
                'probabilities': r.dl_probabilities, 'capabilities': r.dl_capability_scores
            },
            'xai': {'feature_contributions': r.feature_contributions},
            'validation': {'rule_category': r.rule_category, 'agreement': r.agreement},
            'hazards': r.hazards, 'recommendations': r.recommendations
        }
        path.write_text(json.dumps(report, indent=2, default=str), encoding='utf-8')
        return path
    
    def generate_html(self, r: AnalysisResult) -> Path:
        path = self._filename(r.obj.name, "html")
        fmt = lambda v, u="": f"{v:.3e}{u}" if (v > 1e6 or (0 < v < 1e-3)) else f"{v:.4g}{u}" if v > 0 else "—"
        
        conf_color = "#0f8" if r.final_confidence > 0.8 else "#fc0" if r.final_confidence > 0.5 else "#f44"
        
        proba_html = ""
        for cat, prob in sorted(r.dl_probabilities.items(), key=lambda x: -x[1])[:5]:
            proba_html += f'<div class="proba"><span>{cat}</span><div class="pbar"><div style="width:{prob*100}%"></div></div><span>{prob*100:.1f}%</span></div>'
        
        xai_html = ""
        for feat, contrib in list(r.feature_contributions.items())[:8]:
            color = "#0f8" if contrib > 0 else "#f44"
            xai_html += f'<div class="xai-item"><span>{feat}</span><span style="color:{color}">{contrib:+.4f}</span></div>'
        
        caps_html = ""
        for cap, score in r.ranked_capabilities:
            color = "#0f8" if score >= 7 else "#fc0" if score >= 4 else "#f44"
            caps_html += f'''<div class="cap">
                <div class="cap-h"><span>{CAPABILITY_ICONS.get(cap, '◆')}</span><span class="cap-n">{cap}</span><span style="color:{color}">{score:.1f}</span></div>
                <div class="bar"><div style="width:{score*10}%;background:{color}"></div></div>
            </div>'''
        
        hazards_html = "".join([f"<li>{h}</li>" for h in r.hazards])
        recs_html = "".join([f"<li>{rec}</li>" for rec in r.recommendations])
        
        html = f'''<!DOCTYPE html>
<html><head><meta charset="UTF-8"><title>{r.obj.name} - DL Analysis</title>
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{font-family:'Segoe UI',system-ui;background:linear-gradient(135deg,#0a0a1a,#1a1a3a);color:#e0e0e0;min-height:100vh;padding:20px}}
.container{{max-width:1000px;margin:auto}}
header{{text-align:center;padding:25px;background:rgba(0,150,100,0.15);border:2px solid #0f8;border-radius:15px;margin-bottom:20px}}
h1{{color:#0f8;font-size:1.6em}}
.objname{{font-size:2em;color:#fff;margin:15px 0}}
.badges{{display:flex;justify-content:center;gap:10px;flex-wrap:wrap}}
.badge{{padding:8px 16px;border-radius:15px;font-weight:bold;font-size:0.85em}}
.section{{background:rgba(30,30,60,0.5);border:1px solid #336;border-radius:12px;padding:18px;margin-bottom:15px}}
.section h2{{color:#0cf;border-bottom:2px solid #0af;padding-bottom:8px;margin-bottom:12px;font-size:1.1em}}
.dl-box{{background:rgba(0,150,100,0.15);border:2px solid #0f8;border-radius:12px;padding:18px;margin-bottom:15px}}
.dl-box h2{{color:#0f8;border-color:#0f8}}
.xai-box{{background:rgba(150,0,150,0.15);border:2px solid #a0f;border-radius:12px;padding:18px;margin-bottom:15px}}
.xai-box h2{{color:#a0f;border-color:#a0f}}
.proba{{display:flex;align-items:center;gap:10px;margin:8px 0;font-size:0.9em}}
.proba span:first-child{{width:150px}}
.pbar{{flex:1;height:8px;background:rgba(255,255,255,0.1);border-radius:4px;overflow:hidden}}
.pbar div{{height:100%;background:#0f8}}
.proba span:last-child{{width:50px;text-align:right}}
.xai-item{{display:flex;justify-content:space-between;padding:5px 0;border-bottom:1px solid rgba(255,255,255,0.1)}}
.grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(150px,1fr));gap:10px}}
.prop{{background:rgba(0,50,80,0.3);padding:10px;border-radius:8px;border-left:3px solid #0af}}
.prop-l{{color:#888;font-size:0.75em}}
.prop-v{{color:#fff;font-weight:bold}}
.cap{{background:rgba(20,20,40,0.5);padding:10px;border-radius:8px;margin-bottom:6px}}
.cap-h{{display:flex;align-items:center;gap:8px;margin-bottom:5px}}
.cap-n{{flex:1;font-size:0.85em}}
.bar{{height:5px;background:rgba(255,255,255,0.1);border-radius:3px;overflow:hidden}}
.bar div{{height:100%;border-radius:3px}}
.hazards li{{padding:5px 0;color:#fc0;font-size:0.9em}}
.recs li{{padding:5px 0;color:#0f8;font-size:0.9em}}
footer{{text-align:center;padding:15px;color:#666}}
</style></head>
<body><div class="container">
<header>
<h1>🧠 STELLAR INTELLIGENCE v7.0 - DEEP LEARNING</h1>
<div class="objname">{r.obj.name}</div>
<div class="badges">
<span class="badge" style="background:linear-gradient(135deg,#0f8,#0a6)">{r.final_category}</span>
<span class="badge" style="border:2px solid {conf_color};color:{conf_color}">DL Confidence: {r.final_confidence*100:.1f}%</span>
<span class="badge" style="border:2px solid #a0f;color:#a0f">PyTorch DNN</span>
</div>
</header>

<div class="dl-box">
<h2>🧠 Deep Learning Prediction</h2>
<p style="margin-bottom:12px">Category: <strong>{r.dl_category}</strong> — Confidence: <strong>{r.dl_confidence*100:.1f}%</strong></p>
<h4 style="color:#888;margin-bottom:8px">Probability Distribution:</h4>
{proba_html}
</div>

<div class="xai-box">
<h2>🔍 Explainability (XAI) - Feature Contributions</h2>
{xai_html}
</div>

<div class="section">
<h2>📊 Physical Properties</h2>
<div class="grid">
<div class="prop"><div class="prop-l">Mass</div><div class="prop-v">{fmt(r.obj.mass)} M⊕</div></div>
<div class="prop"><div class="prop-l">Radius</div><div class="prop-v">{fmt(r.obj.radius)} km</div></div>
<div class="prop"><div class="prop-l">Temperature</div><div class="prop-v">{fmt(r.obj.temperature)} K</div></div>
<div class="prop"><div class="prop-l">Density</div><div class="prop-v">{fmt(r.obj.density)} kg/m³</div></div>
<div class="prop"><div class="prop-l">Gravity</div><div class="prop-v">{fmt(r.obj.surface_gravity_g)}g</div></div>
<div class="prop"><div class="prop-l">Escape Vel</div><div class="prop-v">{fmt(r.obj.escape_velocity/1000)} km/s</div></div>
</div>
</div>

<div class="section">
<h2>📈 DL Capability Predictions</h2>
{caps_html}
</div>

<div class="section">
<h2>⚠️ Hazards</h2>
<ul class="hazards">{hazards_html}</ul>
</div>

<div class="section">
<h2>🚀 Recommendations</h2>
<ol class="recs">{recs_html}</ol>
</div>

<footer>Stellar Intelligence v7.0 - Deep Learning Research Edition | Bias-Breaking Training | K-Fold CV | XAI</footer>
</div></body></html>'''
        
        path.write_text(html, encoding='utf-8')
        return path
    
    def generate_all(self, r: AnalysisResult) -> List[Path]:
        return [self.generate_txt(r), self.generate_json(r), self.generate_html(r)]


# ═══════════════════════════════════════════════════════════════════════════════
# UI
# ═══════════════════════════════════════════════════════════════════════════════

class UI:
    
    def show(self, r: AnalysisResult):
        console.print()
        
        # Header
        txt = Text()
        txt.append(f"◈ {r.obj.name.upper()}\n", style="bold white")
        console.print(Panel(txt, title="[bold green]🧠 DEEP LEARNING ANALYSIS[/bold green]", border_style="green", box=DOUBLE))
        
        # DL Prediction
        dl_txt = Text()
        dl_txt.append("\n  Category: ", style="dim")
        dl_txt.append(f"{r.dl_category}\n", style="bold cyan")
        dl_txt.append("  Confidence: ", style="dim")
        conf_style = "bold green" if r.dl_confidence > 0.8 else "bold yellow" if r.dl_confidence > 0.5 else "bold red"
        dl_txt.append(f"{r.dl_confidence * 100:.1f}%\n\n", style=conf_style)
        
        dl_txt.append("  Probabilities:\n", style="dim")
        for cat, prob in sorted(r.dl_probabilities.items(), key=lambda x: -x[1])[:5]:
            bar_len = int(prob * 20)
            dl_txt.append(f"    {cat[:25]:<25} ", style="white")
            dl_txt.append("█" * bar_len, style="green")
            dl_txt.append("░" * (20 - bar_len), style="dim")
            dl_txt.append(f" {prob*100:.1f}%\n", style="green")
        
        console.print(Panel(dl_txt, title="[green]🧠 DL PREDICTION[/green]", border_style="green", box=ROUNDED))
        
        # XAI - Feature Contributions
        xai_txt = Text()
        xai_txt.append("\n  Top contributing features:\n", style="dim")
        for feat, contrib in list(r.feature_contributions.items())[:6]:
            style = "green" if contrib > 0 else "red"
            xai_txt.append(f"    {feat:<25} ", style="white")
            xai_txt.append(f"{contrib:+.4f}\n", style=style)
        
        console.print(Panel(xai_txt, title="[magenta]🔍 EXPLAINABILITY (XAI)[/magenta]", border_style="magenta", box=ROUNDED))
        
        # Properties
        fmt = lambda v, u="": f"{v:.2e}{u}" if (v > 1e6 or (0 < v < 1e-3)) else f"{v:.4g}{u}" if v > 0 else "[dim]—[/dim]"
        
        table = Table(title="[cyan]Properties[/cyan]", box=ROUNDED, border_style="blue", show_header=False)
        table.add_column(width=18)
        table.add_column(width=14)
        table.add_column(width=18)
        table.add_column(width=14)
        
        table.add_row("Mass", f"{fmt(r.obj.mass)} M⊕", "Density", f"{fmt(r.obj.density)} kg/m³")
        table.add_row("Radius", f"{fmt(r.obj.radius)} km", "Gravity", f"{fmt(r.obj.surface_gravity_g)}g")
        table.add_row("Temp", f"{fmt(r.obj.temperature)} K", "Escape V", f"{fmt(r.obj.escape_velocity/1000)} km/s")
        
        console.print(table)
        
        # Capabilities
        console.print()
        console.print(Rule("[cyan]🧠 DL CAPABILITY PREDICTIONS[/cyan]", style="cyan"))
        
        cap_table = Table(box=ROUNDED, border_style="blue", show_header=False)
        cap_table.add_column(width=45)
        cap_table.add_column(width=45)
        
        for i in range(0, 16, 2):
            def make_bar(cap, score):
                color = "green" if score >= 7 else "yellow" if score >= 4 else "red"
                t = Text()
                t.append(f"{CAPABILITY_ICONS.get(cap, '◆')} ", style="bold")
                t.append(f"{cap[:22]:<22} ", style="dim")
                t.append("█" * int(score), style=color)
                t.append("░" * (10 - int(score)), style="dim")
                t.append(f" {score:.1f}", style=f"bold {color}")
                return t
            
            cap_table.add_row(
                make_bar(CAPABILITIES[i], r.final_scores[CAPABILITIES[i]]),
                make_bar(CAPABILITIES[i+1], r.final_scores[CAPABILITIES[i+1]])
            )
        
        console.print(cap_table)
        
        # Top 5
        console.print()
        top_txt = Text()
        medals = ["🥇", "🥈", "🥉", "🏅", "🏅"]
        for i, (cap, score) in enumerate(r.top_capabilities):
            top_txt.append(f"\n  {medals[i]} ", style="bold")
            top_txt.append(f"{CAPABILITY_ICONS.get(cap, '◆')} {cap}", style="white")
            top_txt.append(f" — {score:.1f}/10", style="dim")
        
        console.print(Panel(top_txt, title="[green]🏆 TOP CAPABILITIES[/green]", border_style="green", box=DOUBLE))
        
        # Hazards & Recommendations
        console.print()
        haz_txt = Text()
        for h in r.hazards:
            style = "bold red" if "🔴" in h else "yellow" if "🟡" in h or "🟠" in h else "green"
            haz_txt.append(f"\n  {h}", style=style)
        console.print(Panel(haz_txt, title="[yellow]⚠️ HAZARDS[/yellow]", border_style="yellow", box=ROUNDED))
        
        console.print()
        rec_txt = Text()
        for i, rec in enumerate(r.recommendations, 1):
            rec_txt.append(f"\n  {i}. ", style="bold green")
            rec_txt.append(rec, style="white")
        console.print(Panel(rec_txt, title="[green]🚀 RECOMMENDATIONS[/green]", border_style="green", box=ROUNDED))


# ═══════════════════════════════════════════════════════════════════════════════
# FILE DIALOGS
# ═══════════════════════════════════════════════════════════════════════════════

def select_folder() -> Optional[Path]:
    if TK_AVAILABLE:
        try:
            root = tk.Tk(); root.withdraw(); root.attributes('-topmost', True)
            folder = filedialog.askdirectory(title="Select Catalog Folder")
            root.destroy()
            if folder: return Path(folder)
        except: pass
    p = input("Enter folder path: ").strip().strip('"')
    return Path(p) if p and Path(p).exists() else None

def select_file() -> Optional[Path]:
    if TK_AVAILABLE:
        try:
            root = tk.Tk(); root.withdraw(); root.attributes('-topmost', True)
            f = filedialog.askopenfilename(title="Select .sc File", filetypes=[("Space Engine", "*.sc"), ("All", "*.*")])
            root.destroy()
            if f: return Path(f)
        except: pass
    p = input("Enter file path: ").strip().strip('"')
    return Path(p) if p and Path(p).exists() else None


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    banner = """
[bold green]
╔══════════════════════════════════════════════════════════════════════════════╗
║                                                                              ║
║   ███████╗████████╗███████╗██╗     ██╗      █████╗ ██████╗                   ║
║   ██╔════╝╚══██╔══╝██╔════╝██║     ██║     ██╔══██╗██╔══██╗                  ║
║   ███████╗   ██║   █████╗  ██║     ██║     ███████║██████╔╝                  ║
║   ╚════██║   ██║   ██╔══╝  ██║     ██║     ██╔══██║██╔══██╗                  ║
║   ███████║   ██║   ███████╗███████╗███████╗██║  ██║██║  ██║                  ║
║   ╚══════╝   ╚═╝   ╚══════╝╚══════╝╚══════╝╚═╝  ╚═╝╚═╝  ╚═╝                  ║
║                                                                              ║
║          ██╗███╗   ██╗████████╗███████╗██╗     ██╗     ██╗ ██████╗ ███████╗  ║
║          ██║████╗  ██║╚══██╔══╝██╔════╝██║     ██║     ██║██╔════╝ ██╔════╝  ║
║          ██║██╔██╗ ██║   ██║   █████╗  ██║     ██║     ██║██║ ███╗ █████╗    ║
║          ██║██║╚██╗██║   ██║   ██╔══╝  ██║     ██║     ██║██║  ██║ ██╔══╝    ║
║          ██║██║ ╚████║   ██║   ███████╗███████╗███████╗██║╚██████╔╝ ███████╗ ║
║          ╚═╝╚═╝  ╚═══╝   ╚═╝   ╚══════╝╚══════╝╚══════╝╚═╝ ╚═════╝  ╚══════╝ ║
║                                                                              ║
║               ◈ DEEP LEARNING RESEARCH EDITION v7.0 ◈                        ║
║                                                                              ║
║   ✓ PyTorch Deep Neural Networks   ✓ Bias-Breaking Training                 ║
║   ✓ K-Fold Cross Validation        ✓ Feature Importance (XAI)               ║
║   ✓ Dataset Balance Analysis       ✓ Statistical Validation                 ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝
[/bold green]
"""
    console.print(banner)
    console.print(f"[dim]Device: {DEVICE}[/dim]\n")
    
    model = StellarDLModel()
    parser = SCParser()
    ui = UI()
    report_gen = ReportGenerator()
    
    if StellarDLModel.model_exists():
        console.print("[bold green]◈ TRAINED DL MODEL FOUND[/bold green]\n")
        console.print("[cyan]Options:[/cyan]")
        console.print("  [1] Analyze .sc files (DL prediction)")
        console.print("  [2] Retrain model")
        console.print("  [3] Show evaluation & XAI")
        console.print("  [4] Exit\n")
        
        choice = input("Select [1/2/3/4]: ").strip()
        
        if choice == "2":
            if input("Retrain? [y/N]: ").lower() == 'y':
                folder = select_folder()
                if folder: model.train(folder)
        elif choice == "3":
            if EVAL_FILE.exists() and IMPORTANCE_FILE.exists():
                with open(EVAL_FILE) as f:
                    ev = json.load(f)
                with open(IMPORTANCE_FILE) as f:
                    imp = json.load(f)
                
                console.print(Panel.fit(
                    f"[cyan]Category Accuracy:[/cyan] {ev.get('category_accuracy', 0)*100:.1f}%\n"
                    f"[cyan]CV Mean ± Std:[/cyan] {ev.get('cv_mean', 0):.3f} ± {ev.get('cv_std', 0):.3f}\n"
                    f"[cyan]Capability MAE:[/cyan] {ev.get('capability_mae', 0):.2f}\n"
                    f"[cyan]Capability R²:[/cyan] {ev.get('capability_r2', 0):.3f}\n"
                    f"[cyan]Robustness:[/cyan] {ev.get('robustness_accuracy', 0)*100:.1f}%",
                    title="[cyan]Evaluation Metrics[/cyan]",
                    border_style="cyan"
                ))
                
                console.print("\n[magenta]Top 10 Feature Importance (XAI):[/magenta]")
                for name, val in list(imp.items())[:10]:
                    console.print(f"  {name}: {val:.4f}")
            
            input("\nPress Enter to continue...")
        elif choice == "4":
            return
        
        if choice != "2" and not model.load():
            folder = select_folder()
            if folder: model.train(folder)
    else:
        console.print("[yellow]◈ FIRST RUN - Training required[/yellow]\n")
        console.print(Panel.fit(
            "[bold]Deep Learning Research Edition[/bold]\n\n"
            "This version includes:\n"
            "• [green]PyTorch Deep Neural Networks[/green]\n"
            "• [cyan]Bias-breaking noise injection[/cyan]\n"
            "• [cyan]K-Fold Cross Validation[/cyan]\n"
            "• [magenta]Feature Importance (XAI)[/magenta]\n"
            "• [cyan]Dataset balance analysis[/cyan]",
            border_style="yellow"
        ))
        
        folder = select_folder()
        if folder:
            if not model.train(folder):
                return
        else:
            return
    
    # Analysis mode
    console.print(Panel.fit(
        "[bold green]ANALYSIS MODE[/bold green]\n[dim]Deep Learning Prediction[/dim]",
        border_style="green"
    ))
    
    while True:
        sc_file = select_file()
        if not sc_file:
            break
        
        console.print(f"\n[cyan]⟫ Analyzing: {sc_file.name}[/cyan]")
        
        objects = parser.parse_file(sc_file)
        console.print(f"[green]  Found {len(objects)} object(s)[/green]")
        
        for obj in objects:
            console.print(f"\n[cyan]⟫ DL Prediction: {obj.name}[/cyan]")
            result = model.predict(obj)
            ui.show(result)
            
            console.print()
            console.print("[cyan]Export: [1]TXT [2]JSON [3]HTML [4]ALL [0]Skip[/cyan]")
            exp = input("Choice: ").strip()
            
            if exp == "1":
                console.print(f"[green]✓ {report_gen.generate_txt(result)}[/green]")
            elif exp == "2":
                console.print(f"[green]✓ {report_gen.generate_json(result)}[/green]")
            elif exp == "3":
                p = report_gen.generate_html(result)
                console.print(f"[green]✓ {p}[/green]")
                try: webbrowser.open(f"file://{p.absolute()}")
                except: pass
            elif exp == "4":
                for p in report_gen.generate_all(result):
                    console.print(f"[green]✓ {p}[/green]")
        
        if input("\nAnalyze another? [Y/n]: ").lower() == 'n':
            break
    
    console.print(f"\n[dim]Reports: {REPORTS_DIR.absolute()}[/dim]")
    console.print("\n[bold green]◈ Deep Learning Analysis Complete ◈[/bold green]")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        console.print("\n[yellow]Interrupted.[/yellow]")
    except Exception as e:
        console.print(f"\n[red]Error: {e}[/red]")
        import traceback
        console.print(f"[dim]{traceback.format_exc()}[/dim]")