# -*- coding: utf-8 -*-
import sys
import json
import subprocess
import shutil
import datetime
from pathlib import Path

from PyQt6.QtWidgets import (QApplication, QMainWindow, QDialog, QVBoxLayout, 
                             QHBoxLayout, QGridLayout, QLabel, QPushButton, 
                             QLineEdit, QFileDialog, QMessageBox, QFrame, QWidget)
from PyQt6.QtCore import Qt, QPoint
from PyQt6.QtGui import QFont, QIcon

# Configuration File Location
CONFIG_FILE = Path.home() / ".novacore_config.json"

# Hardcore Grid-Based Terminal Stylesheet
STYLE_SHEET = """
QMainWindow, QDialog, QWidget { 
    background-color: #020202; 
    color: #33ff33; 
    font-family: 'Consolas', 'Courier New', monospace; 
}

/* Base Grid Blocks */
QFrame#TermBox {
    background-color: #050a05;
    border: 1px solid #1a4d1a;
    border-radius: 0px;
}
QFrame#TermBox:hover { 
    border: 1px solid #33ff33; 
    background-color: #081208; 
}

/* Terminal Headers & Text */
QLabel#TermHeader { 
    background-color: #0a290a; 
    color: #33ff33; 
    font-size: 13px; 
    font-weight: bold; 
    padding: 6px; 
    border-bottom: 1px solid #1a4d1a;
    text-transform: uppercase;
}
QLabel#TermDesc { 
    color: #2db32d; 
    font-size: 11px; 
    padding: 4px; 
    line-height: 1.2;
}
QLabel#TermPrompt { 
    color: #00ffff; 
    font-size: 11px; 
    font-weight: bold; 
}

/* Side Panel / Telemetry specifics */
QLabel#SysText { color: #33ff33; font-size: 11px; margin-left: 4px; }
QLabel#SysWarning { color: #ff3333; font-size: 11px; margin-left: 4px; font-weight: bold; }
QLabel#SysHighlight { color: #ffcc00; font-size: 11px; margin-left: 4px; }

/* Command Execution Buttons */
QPushButton {
    background-color: #020202;
    border: 1px solid #1a4d1a;
    color: #33ff33;
    padding: 8px;
    font-weight: bold;
    text-align: left;
}
QPushButton:hover { 
    background-color: #33ff33; 
    color: #020202; 
    border: 1px solid #33ff33;
}
QPushButton#SetupBtn { 
    border: 1px solid #ffcc00; 
    color: #ffcc00; 
    text-align: center; 
}
QPushButton#SetupBtn:hover { 
    background-color: #ffcc00; 
    color: #020202; 
}

/* Custom Terminal Window Controls */
QPushButton#WinBtn {
    border: 1px solid #1a4d1a;
    color: #33ff33;
    padding: 4px 8px;
    font-weight: bold;
    text-align: center;
}
QPushButton#WinBtn:hover { 
    background-color: #33ff33; 
    color: #020202; 
}
QPushButton#WinBtnClose {
    border: 1px solid #4d1a1a;
    color: #ff3333;
    padding: 4px 8px;
    font-weight: bold;
    text-align: center;
}
QPushButton#WinBtnClose:hover {
    background-color: #ff3333;
    color: #020202;
    border: 1px solid #ff3333;
}

/* Dialog Inputs */
QLineEdit {
    background-color: #020202;
    color: #00ffff;
    border: 1px solid #1a4d1a;
    padding: 6px;
    font-size: 11px;
}
QLineEdit:focus { border: 1px solid #00ffff; }
"""

class ConfigManager:
    @staticmethod
    def load():
        if CONFIG_FILE.exists():
            try:
                with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception:
                return {}
        return {}

    @staticmethod
    def save(data):
        with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4)


class SetupDialog(QDialog):
    """Terminal-style Setup Wizard."""
    def __init__(self, current_config, parent=None):
        super().__init__(parent)
        self.setWindowTitle("root@novacore:~# ./configure_paths.sh")
        self.setMinimumSize(650, 420)
        self.config = current_config
        
        layout = QVBoxLayout(self)
        layout.setContentsMargins(10, 10, 10, 10)
        
        term_box = QFrame()
        term_box.setObjectName("TermBox")
        box_layout = QVBoxLayout(term_box)
        
        header = QLabel("[ SYSTEM CONFIGURATION UTILITY ]")
        header.setObjectName("TermHeader")
        box_layout.addWidget(header)
        box_layout.addSpacing(10)

        # Helper for rows
        def add_config_row(label_text, key):
            row_layout = QHBoxLayout()
            lbl = QLabel(label_text)
            lbl.setFixedWidth(150)
            row_layout.addWidget(lbl)
            
            inp = QLineEdit()
            inp.setText(self.config.get(key, ""))
            inp.setReadOnly(True)
            row_layout.addWidget(inp)
            
            btn = QPushButton("[ BROWSE ]")
            btn.setObjectName("SetupBtn")
            btn.setFixedWidth(100)
            btn.clicked.connect(lambda: self.browse_file(inp))
            row_layout.addWidget(btn)
            
            box_layout.addLayout(row_layout)
            return inp

        self.astro_input = add_config_row("export ASTRO_BIN=", "astro_path")
        self.harv_input = add_config_row("export HARVEST_BIN=", "harvester_path")
        self.stellar_input = add_config_row("export INTEL_BIN=", "stellar_intel_path")
        self.scenario_input = add_config_row("export SCENARIO_BIN=", "scenario_engine_path")

        box_layout.addSpacing(20)

        save_btn = QPushButton("root@novacore:~# MAKE INSTALL && REBOOT")
        save_btn.setObjectName("SetupBtn")
        save_btn.clicked.connect(self.save_and_close)
        box_layout.addWidget(save_btn)
        
        layout.addWidget(term_box)

    def browse_file(self, line_edit):
        file_path, _ = QFileDialog.getOpenFileName(
            self, "Select Binary/Script", "", "Python Files (*.py);;Executable (*.exe);;All Files (*.*)"
        )
        if file_path:
            line_edit.setText(file_path)

    def save_and_close(self):
        astro = self.astro_input.text().strip()
        harv = self.harv_input.text().strip()
        stellar = self.stellar_input.text().strip()
        scenario = self.scenario_input.text().strip()
        
        if not astro or not harv or not stellar or not scenario:
            QMessageBox.warning(self, "SYS_ERR", "FATAL: ALL ENVIRONMENT VARIABLES MUST BE SET.")
            return
            
        self.config["astro_path"] = astro
        self.config["harvester_path"] = harv
        self.config["stellar_intel_path"] = stellar
        self.config["scenario_engine_path"] = scenario
        ConfigManager.save(self.config)
        self.accept()


class NovaCoreLauncher(QMainWindow):
    """The Highly Grid-Based Terminal UI."""
    def __init__(self, config):
        super().__init__()
        self.config = config
        self.setWindowTitle("NOVACORE OS // GRID TERMINAL")
        self.setMinimumSize(900, 650)
        
        # Remove standard OS window frame to keep only the custom sci-fi buttons
        self.setWindowFlags(Qt.WindowType.FramelessWindowHint)
        self._drag_pos = None

        self._build_ui()

    def toggle_maximize(self):
        """Custom handler for the terminal maximize button."""
        if self.isMaximized():
            self.showNormal()
        else:
            self.showMaximized()

    # --- Custom Window Dragging Logic ---
    def mousePressEvent(self, event):
        if event.button() == Qt.MouseButton.LeftButton:
            self._drag_pos = event.globalPosition().toPoint()

    def mouseMoveEvent(self, event):
        if event.buttons() == Qt.MouseButton.LeftButton and self._drag_pos is not None:
            delta = event.globalPosition().toPoint() - self._drag_pos
            self.move(self.x() + delta.x(), self.y() + delta.y())
            self._drag_pos = event.globalPosition().toPoint()

    def mouseReleaseEvent(self, event):
        if event.button() == Qt.MouseButton.LeftButton:
            self._drag_pos = None
    # ------------------------------------

    def _build_ui(self):
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        
        # Strict Grid Layout
        grid = QGridLayout(central_widget)
        grid.setSpacing(6)
        grid.setContentsMargins(10, 10, 10, 10)

        # ==========================================
        # ROW 0: MASTER HEADER (Spans all columns)
        # ==========================================
        master_header = QFrame()
        master_header.setObjectName("TermBox")
        master_header.setFixedHeight(60)
        mh_layout = QHBoxLayout(master_header)
        mh_layout.setContentsMargins(15, 0, 15, 0)
        
        title = QLabel("NOVACORE DISTRIBUTED MAINFRAME [VERSION 3.0.1]")
        title.setStyleSheet("color: #00ffff; font-size: 16px; font-weight: 900;")
        mh_layout.addWidget(title)
        
        mh_layout.addStretch()  # Forces the clock and buttons to the right
        
        now = datetime.datetime.now().strftime("%Y-%m-%d // %H:%M:%S UTC")
        time_lbl = QLabel(f"SYS_CLOCK: {now}")
        time_lbl.setStyleSheet("color: #ffcc00; font-weight: bold; margin-right: 20px;")
        mh_layout.addWidget(time_lbl)
        
        # --- Custom Terminal Window Controls ---
        min_btn = QPushButton("[-]")
        min_btn.setObjectName("WinBtn")
        min_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        min_btn.clicked.connect(self.showMinimized)
        
        max_btn = QPushButton("[□]")
        max_btn.setObjectName("WinBtn")
        max_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        max_btn.clicked.connect(self.toggle_maximize)
        
        close_btn = QPushButton("[X]")
        close_btn.setObjectName("WinBtnClose")
        close_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        close_btn.clicked.connect(self.close)
        
        mh_layout.addWidget(min_btn)
        mh_layout.addWidget(max_btn)
        mh_layout.addWidget(close_btn)
        
        grid.addWidget(master_header, 0, 0, 1, 3)

        # ==========================================
        # ROW 1 & 2, COL 0: TELEMETRY (Spans 2 rows)
        # ==========================================
        telem_box = QFrame()
        telem_box.setObjectName("TermBox")
        telem_box.setFixedWidth(260)
        telem_layout = QVBoxLayout(telem_box)
        telem_layout.setContentsMargins(0, 0, 0, 0)
        
        t_head = QLabel(">> /VAR/LOG/SYSLOG")
        t_head.setObjectName("TermHeader")
        telem_layout.addWidget(t_head)
        
        # Telemetry logs wrapping
        log_wrap = QVBoxLayout()
        log_wrap.setContentsMargins(10, 10, 10, 10)
        log_wrap.setSpacing(6)
        
        log_wrap.addWidget(QLabel("[ OK ] KERNEL LOADED", objectName="SysText"))
        log_wrap.addWidget(QLabel("[ OK ] MOUNT /DEV/SDA1", objectName="SysText"))
        log_wrap.addWidget(QLabel("[ OK ] ESTABLISH SECURE LINK", objectName="SysText"))
        log_wrap.addWidget(QLabel("[WARN] NEURAL_NET_GPU_TEMP_HIGH", objectName="SysWarning"))
        log_wrap.addWidget(QLabel("[INFO] AWAITING OPERATOR INPUT", objectName="SysHighlight"))
        log_wrap.addSpacing(20)
        
        log_wrap.addWidget(QLabel(">> ACTIVE DAEMONS", objectName="TermPrompt"))
        log_wrap.addWidget(QLabel("  ├─ se_export_dir.service", objectName="SysText"))
        log_wrap.addWidget(QLabel("  ├─ scenario_db_sync.sh", objectName="SysText"))
        log_wrap.addWidget(QLabel("  └─ astro_cache_mgr.bin", objectName="SysText"))
        
        telem_layout.addLayout(log_wrap)
        telem_layout.addStretch()
        grid.addWidget(telem_box, 1, 0, 2, 1)

        # ==========================================
        # ROW 1, COL 1 & 2: MODULES 1 & 2
        # ==========================================
        grid.addWidget(self._create_term_card(
            title=">> /BIN/ASTRO_CATALOG",
            desc="Executing this binary initializes the high-speed celestial object caching framework. Real-time observatory feed enabled.",
            config_key="astro_path",
            accent="#00ffff"
        ), 1, 1)

        grid.addWidget(self._create_term_card(
            title=">> /BIN/COSMIC_HARVESTER",
            desc="Background daemon for automated workspace extraction. Hooks into live directory pipelines for scientific sorting.",
            config_key="harvester_path",
            accent="#cc33ff"
        ), 1, 2)

        # ==========================================
        # ROW 2, COL 1 & 2: MODULES 3 & 4
        # ==========================================
        grid.addWidget(self._create_term_card(
            title=">> /BIN/STELLAR_INTEL",
            desc="Initializes the PyTorch deep learning tensor processes. Classifies spectral anomalies via neural heuristic networks.",
            config_key="stellar_intel_path",
            accent="#33ff33"
        ), 2, 1)

        grid.addWidget(self._create_term_card(
            title=">> /BIN/SCENARIO_ENGINE",
            desc="NASA-level physics simulation engine. Generates combinatoric astrophysical anomalies and renders PDF incident reports.",
            config_key="scenario_engine_path",
            accent="#ff3333"
        ), 2, 2)

        # ==========================================
        # ROW 3: SYSTEM OVERRIDE / CONFIG (Spans 3 cols)
        # ==========================================
        config_box = QFrame()
        config_box.setObjectName("TermBox")
        config_box.setFixedHeight(90)
        c_layout = QHBoxLayout(config_box)
        c_layout.setContentsMargins(15, 10, 15, 10)
        
        c_info = QVBoxLayout()
        c_lbl = QLabel(">> /ETC/NOVACORE/CONFIG.JSON")
        c_lbl.setObjectName("TermPrompt")
        c_lbl.setStyleSheet("color: #ffcc00;")
        c_desc = QLabel("Modify core binary paths. Restart required on environment variable changes.")
        c_desc.setObjectName("TermDesc")
        c_info.addWidget(c_lbl)
        c_info.addWidget(c_desc)
        c_layout.addLayout(c_info)
        
        cfg_btn = QPushButton("root@novacore:~# nano config.json")
        cfg_btn.setObjectName("SetupBtn")
        cfg_btn.setFixedWidth(300)
        cfg_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        cfg_btn.clicked.connect(self.open_settings)
        c_layout.addWidget(cfg_btn)
        
        grid.addWidget(config_box, 3, 0, 1, 3)

    def _create_term_card(self, title, desc, config_key, accent):
        card = QFrame()
        card.setObjectName("TermBox")
        layout = QVBoxLayout(card)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)

        # Header section
        header = QLabel(title)
        header.setObjectName("TermHeader")
        header.setStyleSheet(f"color: {accent}; border-bottom: 1px solid {accent};")
        layout.addWidget(header)
        
        # Body section
        body = QVBoxLayout()
        body.setContentsMargins(10, 10, 10, 10)
        
        desc_lbl = QLabel(desc)
        desc_lbl.setObjectName("TermDesc")
        desc_lbl.setWordWrap(True)
        body.addWidget(desc_lbl)
        body.addStretch()

        # Execute Command Line Button
        cmd_btn = QPushButton(f"root@novacore:~# ./{config_key.replace('_path','')}.sh")
        cmd_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        cmd_btn.setStyleSheet(f"QPushButton:hover {{ background-color: {accent}; color: #020202; border: 1px solid {accent}; }}")
        cmd_btn.clicked.connect(lambda: self.launch_app(config_key))
        body.addWidget(cmd_btn)
        
        layout.addLayout(body)
        return card

    def open_settings(self):
        dialog = SetupDialog(self.config, self)
        if dialog.exec():
            self.config = ConfigManager.load()

    def launch_app(self, config_key):
        target_path = Path(self.config.get(config_key, ""))
        
        if not target_path.exists() or not target_path.is_file():
            QMessageBox.critical(self, "SYS_ERR", f"bash: {target_path}: No such file or directory\n\nCHECK CONFIG.JSON.")
            self.open_settings()
            return

        try:
            if sys.platform == "win32":
                subprocess.Popen([sys.executable, str(target_path)], cwd=target_path.parent, creationflags=subprocess.CREATE_NEW_CONSOLE)
            elif sys.platform == "darwin":
                subprocess.Popen(["open", "-a", "Terminal", str(target_path)], cwd=target_path.parent)
            else:
                terminals = ['x-terminal-emulator', 'gnome-terminal', 'konsole', 'xfce4-terminal', 'xterm']
                launched = False
                for term in terminals:
                    if shutil.which(term):
                        args = [term, '--', sys.executable, str(target_path)] if term in ['gnome-terminal', 'x-terminal-emulator'] else [term, '-e', sys.executable, str(target_path)]
                        subprocess.Popen(args, cwd=target_path.parent)
                        launched = True
                        break
                if not launched:
                    subprocess.Popen([sys.executable, str(target_path)], cwd=target_path.parent)
        except Exception as e:
            QMessageBox.critical(self, "EXECUTION_ERR", f"SEGFAULT OR EXECUTION FAILURE:\n{e}")


def main():
    app = QApplication(sys.argv)
    app.setStyleSheet(STYLE_SHEET)

    config = ConfigManager.load()

    missing_paths = (
        not config.get("astro_path") or 
        not config.get("harvester_path") or 
        not config.get("stellar_intel_path") or 
        not config.get("scenario_engine_path")
    )
    
    if missing_paths:
        setup = SetupDialog(config)
        if not setup.exec():
            sys.exit(0)
        config = ConfigManager.load()

    paths_exist = (
        Path(config.get("astro_path", "")).exists() and 
        Path(config.get("harvester_path", "")).exists() and
        Path(config.get("stellar_intel_path", "")).exists() and
        Path(config.get("scenario_engine_path", "")).exists()
    )
                   
    if not paths_exist:
        setup = SetupDialog(config)
        if not setup.exec():
            sys.exit(0)
        config = ConfigManager.load()

    launcher = NovaCoreLauncher(config)
    launcher.show()
    sys.exit(app.exec())

if __name__ == "__main__":
    main()