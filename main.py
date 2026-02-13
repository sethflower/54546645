# -*- coding: utf-8 -*-
"""
Реестр ТСД - Программа для ведения учёта терминалов сбора данных
Версия: 1.0
"""

import sys
import json
import os
from datetime import datetime

from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QTabWidget, QTableWidget, QTableWidgetItem, QPushButton, QLabel,
    QLineEdit, QComboBox, QDialog, QFormLayout, QMessageBox,
    QHeaderView, QFrame, QScrollArea, QGridLayout,
    QAbstractItemView, QGroupBox, QSizePolicy, QSpacerItem
)
from PyQt6.QtCore import Qt, QSize
from PyQt6.QtGui import QFont, QColor


# ========================== ХРАНИЛИЩЕ ДАННЫХ ==========================

DATA_FILE = "tsd_registry_data.json"


def load_data():
    default_data = {
        "devices": [],
        "locations": [],
        "statuses": [],
        "registry": {}
    }
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                for key in default_data:
                    if key not in data:
                        data[key] = default_data[key]
                return data
        except (json.JSONDecodeError, IOError):
            return default_data
    return default_data


def save_data(data):
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# ========================== ГЛОБАЛЬНЫЕ СТИЛИ ==========================

MAIN_STYLE = """
QMainWindow {
    background-color: #f0f2f5;
}
QWidget {
    font-family: 'Segoe UI', 'Arial', sans-serif;
}
QTabWidget::pane {
    border: none;
    background-color: #f0f2f5;
    border-radius: 12px;
}
QTabWidget::tab-bar {
    alignment: center;
}
QTabBar::tab {
    background-color: #e1e5eb;
    color: #5a6270;
    padding: 12px 36px;
    margin: 4px 2px;
    border: none;
    border-radius: 10px;
    font-size: 14px;
    font-weight: 600;
    min-width: 140px;
}
QTabBar::tab:selected {
    background-color: #4a90d9;
    color: white;
}
QTabBar::tab:hover:!selected {
    background-color: #c8d0da;
    color: #3a4250;
}
QTableWidget {
    background-color: white;
    border: 1px solid #e1e5eb;
    border-radius: 12px;
    gridline-color: #f0f2f5;
    selection-background-color: #e8f0fe;
    selection-color: #1a1a2e;
    font-size: 13px;
    padding: 4px;
}
QTableWidget::item {
    padding: 10px 14px;
    border-bottom: 1px solid #f0f2f5;
}
QTableWidget::item:selected {
    background-color: #e8f0fe;
    color: #1a1a2e;
}
QTableWidget::item:hover {
    background-color: #f5f8ff;
}
QHeaderView::section {
    background-color: #f7f8fa;
    color: #5a6270;
    padding: 12px 14px;
    border: none;
    border-bottom: 2px solid #e1e5eb;
    font-size: 13px;
    font-weight: 700;
    text-transform: uppercase;
}
QTableWidget QTableCornerButton::section {
    background-color: #f7f8fa;
    border: none;
}
QPushButton {
    border: none;
    border-radius: 10px;
    padding: 11px 28px;
    font-size: 13px;
    font-weight: 600;
    color: white;
    background-color: #4a90d9;
}
QPushButton:hover {
    background-color: #3a7bc8;
}
QPushButton:pressed {
    background-color: #2d6cb5;
}
QPushButton#btnAdd {
    background-color: #4a90d9;
}
QPushButton#btnAdd:hover {
    background-color: #3a7bc8;
}
QPushButton#btnEdit {
    background-color: #6c7ce0;
}
QPushButton#btnEdit:hover {
    background-color: #5a6ad0;
}
QPushButton#btnDelete {
    background-color: #e07474;
}
QPushButton#btnDelete:hover {
    background-color: #d05e5e;
}
QPushButton#btnSuccess {
    background-color: #5cb85c;
}
QPushButton#btnSuccess:hover {
    background-color: #4cae4c;
}
QPushButton#btnCancel {
    background-color: #95a5a6;
}
QPushButton#btnCancel:hover {
    background-color: #839596;
}
QLineEdit {
    border: 2px solid #e1e5eb;
    border-radius: 10px;
    padding: 10px 16px;
    font-size: 14px;
    background-color: white;
    color: #1a1a2e;
}
QLineEdit:focus {
    border-color: #4a90d9;
    background-color: #fafbff;
}
QLineEdit:hover {
    border-color: #c0c8d4;
}
QComboBox {
    border: 2px solid #e1e5eb;
    border-radius: 10px;
    padding: 10px 16px;
    font-size: 14px;
    background-color: white;
    color: #1a1a2e;
    min-width: 200px;
}
QComboBox:focus {
    border-color: #4a90d9;
}
QComboBox:hover {
    border-color: #c0c8d4;
}
QComboBox::drop-down {
    border: none;
    width: 36px;
}
QComboBox QAbstractItemView {
    background-color: white;
    border: 2px solid #e1e5eb;
    border-radius: 8px;
    selection-background-color: #e8f0fe;
    selection-color: #1a1a2e;
    padding: 4px;
}
QGroupBox {
    background-color: white;
    border: 1px solid #e1e5eb;
    border-radius: 12px;
    margin-top: 20px;
    padding-top: 28px;
    font-size: 14px;
    font-weight: 700;
    color: #3a4250;
}
QGroupBox::title {
    subcontrol-origin: margin;
    subcontrol-position: top left;
    padding: 4px 16px;
    color: #4a90d9;
    font-size: 15px;
}
QLabel {
    color: #3a4250;
    font-size: 13px;
}
QLabel#titleLabel {
    font-size: 22px;
    font-weight: 800;
    color: #1a1a2e;
    padding: 8px 0px;
}
QLabel#subtitleLabel {
    font-size: 14px;
    color: #7a8290;
    font-weight: 400;
}
QScrollBar:vertical {
    background: #f0f2f5;
    width: 10px;
    margin: 0;
    border-radius: 5px;
}
QScrollBar::handle:vertical {
    background: #c8d0da;
    min-height: 30px;
    border-radius: 5px;
}
QScrollBar::handle:vertical:hover {
    background: #aab4c2;
}
QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {
    height: 0;
}
QScrollBar:horizontal {
    background: #f0f2f5;
    height: 10px;
    margin: 0;
    border-radius: 5px;
}
QScrollBar::handle:horizontal {
    background: #c8d0da;
    min-width: 30px;
    border-radius: 5px;
}
QScrollBar::handle:horizontal:hover {
    background: #aab4c2;
}
QScrollBar::add-line:horizontal, QScrollBar::sub-line:horizontal {
    width: 0;
}
QDialog {
    background-color: #f0f2f5;
    border-radius: 16px;
}
QFrame#cardFrame {
    background-color: white;
    border: 1px solid #e1e5eb;
    border-radius: 14px;
    padding: 16px;
}
QFrame#statFrame {
    background-color: white;
    border: 1px solid #e1e5eb;
    border-radius: 14px;
}
"""


# ========================== ДИАЛОГ ДОБАВЛЕНИЯ / РЕДАКТИРОВАНИЯ ТСД ==========================

class DeviceDialog(QDialog):
    def __init__(self, parent=None, device=None, statuses=None):
        super().__init__(parent)
        self.setWindowTitle("Добавить ТСД" if device is None else "Редактировать ТСД")
        self.setMinimumWidth(480)
        self.setStyleSheet(MAIN_STYLE)
        self.device = device
        self.statuses = statuses or []
        self.result_data = None

        layout = QVBoxLayout(self)
        layout.setSpacing(16)
        layout.setContentsMargins(28, 28, 28, 28)

        title = QLabel("Добавить ТСД" if device is None else "Редактировать ТСД")
        title.setObjectName("titleLabel")
        title.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(title)

        form_frame = QFrame()
        form_frame.setObjectName("cardFrame")
        form_layout = QFormLayout(form_frame)
        form_layout.setSpacing(14)
        form_layout.setContentsMargins(20, 20, 20, 20)

        self.brand_edit = QLineEdit()
        self.brand_edit.setPlaceholderText("Введите бренд...")
        form_layout.addRow(self._make_label("Бренд:"), self.brand_edit)

        self.model_edit = QLineEdit()
        self.model_edit.setPlaceholderText("Введите модель...")
        form_layout.addRow(self._make_label("Модель:"), self.model_edit)

        self.imei_edit = QLineEdit()
        self.imei_edit.setPlaceholderText("Введите IMEI...")
        form_layout.addRow(self._make_label("IMEI:"), self.imei_edit)

        self.status_combo = QComboBox()
        self.status_combo.addItem("-- Не выбрано --", "")
        for s in self.statuses:
            self.status_combo.addItem(s, s)
        form_layout.addRow(self._make_label("Состояние:"), self.status_combo)

        layout.addWidget(form_frame)

        btn_layout = QHBoxLayout()
        btn_layout.setSpacing(12)
        btn_save = QPushButton("  Сохранить")
        btn_save.setObjectName("btnSuccess")
        btn_save.setCursor(Qt.CursorShape.PointingHandCursor)
        btn_save.clicked.connect(self.accept_data)
        btn_cancel = QPushButton("  Отмена")
        btn_cancel.setObjectName("btnCancel")
        btn_cancel.setCursor(Qt.CursorShape.PointingHandCursor)
        btn_cancel.clicked.connect(self.reject)
        btn_layout.addStretch()
        btn_layout.addWidget(btn_cancel)
        btn_layout.addWidget(btn_save)
        layout.addLayout(btn_layout)

        if device:
            self.brand_edit.setText(device.get("brand", ""))
            self.model_edit.setText(device.get("model", ""))
            self.imei_edit.setText(device.get("imei", ""))
            idx = self.status_combo.findData(device.get("default_status", ""))
            if idx >= 0:
                self.status_combo.setCurrentIndex(idx)

    def _make_label(self, text):
        lbl = QLabel(text)
        lbl.setStyleSheet("font-weight: 600; font-size: 14px; color: #3a4250;")
        return lbl

    def accept_data(self):
        brand = self.brand_edit.text().strip()
        model = self.model_edit.text().strip()
        imei = self.imei_edit.text().strip()
        status = self.status_combo.currentData()
        if not brand:
            QMessageBox.warning(self, "Ошибка", "Укажите бренд!")
            return
        if not model:
            QMessageBox.warning(self, "Ошибка", "Укажите модель!")
            return
        if not imei:
            QMessageBox.warning(self, "Ошибка", "Укажите IMEI!")
            return
        self.result_data = {
            "brand": brand,
            "model": model,
            "imei": imei,
            "default_status": status
        }
        self.accept()


# ========================== ДИАЛОГ ЗАКРЕПЛЕНИЯ ТСД ==========================

class AssignDialog(QDialog):
    def __init__(self, parent=None, device=None, registry_info=None, locations=None, statuses=None):
        super().__init__(parent)
        self.setWindowTitle("Закрепление ТСД")
        self.setMinimumWidth(520)
        self.setStyleSheet(MAIN_STYLE)
        self.result_data = None

        layout = QVBoxLayout(self)
        layout.setSpacing(16)
        layout.setContentsMargins(28, 28, 28, 28)

        title = QLabel("Закрепление ТСД")
        title.setObjectName("titleLabel")
        title.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(title)

        info_frame = QFrame()
        info_frame.setObjectName("cardFrame")
        info_layout = QVBoxLayout(info_frame)
        info_layout.setSpacing(6)
        dev_info = f"{device.get('brand', '')} {device.get('model', '')}  |  IMEI: {device.get('imei', '')}"
        info_label = QLabel(dev_info)
        info_label.setStyleSheet("font-size: 15px; font-weight: 700; color: #4a90d9; padding: 8px;")
        info_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        info_layout.addWidget(info_label)
        layout.addWidget(info_frame)

        form_frame = QFrame()
        form_frame.setObjectName("cardFrame")
        form_layout = QFormLayout(form_frame)
        form_layout.setSpacing(14)
        form_layout.setContentsMargins(20, 20, 20, 20)

        self.employee_edit = QLineEdit()
        self.employee_edit.setPlaceholderText("ФИО сотрудника (пусто = Свободный)...")
        form_layout.addRow(self._make_label("Сотрудник:"), self.employee_edit)

        self.location_combo = QComboBox()
        self.location_combo.addItem("-- Не указана --", "")
        for loc in (locations or []):
            self.location_combo.addItem(loc, loc)
        form_layout.addRow(self._make_label("Локация:"), self.location_combo)

        self.status_combo = QComboBox()
        self.status_combo.addItem("-- Не выбрано --", "")
        for s in (statuses or []):
            self.status_combo.addItem(s, s)
        form_layout.addRow(self._make_label("Состояние:"), self.status_combo)

        layout.addWidget(form_frame)

        if registry_info:
            emp = registry_info.get("employee", "")
            if emp and emp != "Свободный":
                self.employee_edit.setText(emp)
            loc = registry_info.get("location", "")
            idx = self.location_combo.findData(loc)
            if idx >= 0:
                self.location_combo.setCurrentIndex(idx)
            st = registry_info.get("status", "")
            idx2 = self.status_combo.findData(st)
            if idx2 >= 0:
                self.status_combo.setCurrentIndex(idx2)

        btn_layout = QHBoxLayout()
        btn_layout.setSpacing(12)
        btn_save = QPushButton("  Сохранить")
        btn_save.setObjectName("btnSuccess")
        btn_save.setCursor(Qt.CursorShape.PointingHandCursor)
        btn_save.clicked.connect(self.accept_data)
        btn_cancel = QPushButton("  Отмена")
        btn_cancel.setObjectName("btnCancel")
        btn_cancel.setCursor(Qt.CursorShape.PointingHandCursor)
        btn_cancel.clicked.connect(self.reject)
        btn_layout.addStretch()
        btn_layout.addWidget(btn_cancel)
        btn_layout.addWidget(btn_save)
        layout.addLayout(btn_layout)

    def _make_label(self, text):
        lbl = QLabel(text)
        lbl.setStyleSheet("font-weight: 600; font-size: 14px; color: #3a4250;")
        return lbl

    def accept_data(self):
        status = self.status_combo.currentData()
        if not status:
            QMessageBox.warning(self, "Ошибка", "Укажите состояние! Это обязательное поле.")
            return
        employee = self.employee_edit.text().strip()
        if not employee:
            employee = "Свободный"
        location = self.location_combo.currentData()
        self.result_data = {
            "employee": employee,
            "location": location,
            "status": status,
            "last_modified": datetime.now().strftime("%d.%m.%Y %H:%M:%S")
        }
        self.accept()


# ========================== ДИАЛОГ ПРОСТОГО ВВОДА ==========================

class SimpleInputDialog(QDialog):
    def __init__(self, parent=None, title="Добавить", label_text="Название:", current_value=""):
        super().__init__(parent)
        self.setWindowTitle(title)
        self.setMinimumWidth(420)
        self.setStyleSheet(MAIN_STYLE)
        self.result_value = None

        layout = QVBoxLayout(self)
        layout.setSpacing(16)
        layout.setContentsMargins(28, 28, 28, 28)

        title_lbl = QLabel(title)
        title_lbl.setObjectName("titleLabel")
        title_lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(title_lbl)

        form_frame = QFrame()
        form_frame.setObjectName("cardFrame")
        form_layout = QFormLayout(form_frame)
        form_layout.setSpacing(14)
        form_layout.setContentsMargins(20, 20, 20, 20)

        self.input_edit = QLineEdit()
        self.input_edit.setPlaceholderText("Введите значение...")
        self.input_edit.setText(current_value)
        lbl = QLabel(label_text)
        lbl.setStyleSheet("font-weight: 600; font-size: 14px; color: #3a4250;")
        form_layout.addRow(lbl, self.input_edit)
        layout.addWidget(form_frame)

        btn_layout = QHBoxLayout()
        btn_layout.setSpacing(12)
        btn_save = QPushButton("  Сохранить")
        btn_save.setObjectName("btnSuccess")
        btn_save.setCursor(Qt.CursorShape.PointingHandCursor)
        btn_save.clicked.connect(self.accept_data)
        btn_cancel = QPushButton("  Отмена")
        btn_cancel.setObjectName("btnCancel")
        btn_cancel.setCursor(Qt.CursorShape.PointingHandCursor)
        btn_cancel.clicked.connect(self.reject)
        btn_layout.addStretch()
        btn_layout.addWidget(btn_cancel)
        btn_layout.addWidget(btn_save)
        layout.addLayout(btn_layout)

    def accept_data(self):
        val = self.input_edit.text().strip()
        if not val:
            QMessageBox.warning(self, "Ошибка", "Поле не может быть пустым!")
            return
        self.result_value = val
        self.accept()


# ========================== ГЛАВНОЕ ОКНО ==========================

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Реестр ТСД  -  Управление терминалами сбора данных")
        self.setMinimumSize(1100, 700)
        self.resize(1400, 850)
        self.data = load_data()

        central = QWidget()
        self.setCentralWidget(central)
        main_layout = QVBoxLayout(central)
        main_layout.setContentsMargins(16, 16, 16, 16)
        main_layout.setSpacing(0)

        # Заголовок
        header = QFrame()
        header.setFixedHeight(70)
        header.setStyleSheet("""
            QFrame {
                background: qlineargradient(x1:0, y1:0, x2:1, y2:0,
                    stop:0 #4a90d9, stop:1 #6c7ce0);
                border-radius: 14px;
            }
        """)
        header_layout = QHBoxLayout(header)
        header_layout.setContentsMargins(24, 0, 24, 0)
        app_title = QLabel("Реестр ТСД")
        app_title.setStyleSheet("font-size: 24px; font-weight: 800; color: white;")
        header_layout.addWidget(app_title)
        app_subtitle = QLabel("Управление терминалами сбора данных")
        app_subtitle.setStyleSheet("font-size: 13px; color: rgba(255,255,255,0.8); font-weight: 400;")
        app_subtitle.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        header_layout.addWidget(app_subtitle)
        main_layout.addWidget(header)
        main_layout.addSpacing(12)

        # Вкладки
        self.tabs = QTabWidget()
        self.tabs.setDocumentMode(True)
        main_layout.addWidget(self.tabs)

        self.registry_tab = QWidget()
        self.build_registry_tab()
        self.tabs.addTab(self.registry_tab, "  Реестр")

        self.directory_tab = QWidget()
        self.build_directory_tab()
        self.tabs.addTab(self.directory_tab, "  Справочник")

        self.stats_tab = QWidget()
        self.build_stats_tab()
        self.tabs.addTab(self.stats_tab, "  Статистика")

        self.tabs.currentChanged.connect(self.on_tab_changed)
        self.setStyleSheet(MAIN_STYLE)

        self.refresh_registry_table()
        self.refresh_devices_table()
        self.refresh_locations_table()
        self.refresh_statuses_table()

    # ---------- Вспомогательные шрифты ----------

    def _bold_font(self):
        f = QFont()
        f.setBold(True)
        f.setPointSize(10)
        return f

    def _italic_font(self):
        f = QFont()
        f.setItalic(True)
        f.setPointSize(10)
        return f

        # ========================== ВКЛАДКА РЕЕСТР ==========================

    def build_registry_tab(self):
        layout = QVBoxLayout(self.registry_tab)
        layout.setSpacing(12)
        layout.setContentsMargins(8, 12, 8, 8)

        top = QHBoxLayout()
        lbl = QLabel("Реестр ТСД")
        lbl.setObjectName("titleLabel")
        top.addWidget(lbl)
        sub = QLabel("Двойной клик по строке для закрепления / редактирования")
        sub.setObjectName("subtitleLabel")
        top.addStretch()
        top.addWidget(sub)
        layout.addLayout(top)

        self.registry_search = QLineEdit()
        self.registry_search.setPlaceholderText("  Поиск по бренду, модели, IMEI, сотруднику, локации...")
        self.registry_search.setMinimumHeight(42)
        self.registry_search.textChanged.connect(self.filter_registry)
        layout.addWidget(self.registry_search)

        self.registry_table = QTableWidget()
        self.registry_table.setColumnCount(7)
        self.registry_table.setHorizontalHeaderLabels([
            "Бренд", "Модель", "IMEI", "Состояние", "Сотрудник", "Локация", "Последнее изменение"
        ])
        self.registry_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.registry_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.registry_table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self.registry_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.registry_table.setAlternatingRowColors(True)
        self.registry_table.setStyleSheet("alternate-background-color: #fafbfd;")
        self.registry_table.verticalHeader().setVisible(False)
        self.registry_table.doubleClicked.connect(self.on_registry_double_click)
        layout.addWidget(self.registry_table)

    def refresh_registry_table(self):
        devices = self.data.get("devices", [])
        registry = self.data.get("registry", {})
        self.registry_table.setRowCount(len(devices))

        for row, dev in enumerate(devices):
            dev_id = dev.get("id", "")
            reg = registry.get(dev_id, {})

            brand = dev.get("brand", "")
            model = dev.get("model", "")
            imei = dev.get("imei", "")
            status = reg.get("status", dev.get("default_status", ""))
            employee = reg.get("employee", "Свободный")
            location = reg.get("location", "")
            last_mod = reg.get("last_modified", "")

            values = [brand, model, imei, status, employee, location, last_mod]

            for col, val in enumerate(values):
                item = QTableWidgetItem(str(val))
                item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)

                # Цветовая индикация состояния
                if col == 3 and val:
                    item.setFont(self._bold_font())
                    lower_val = val.lower()
                    if "ремонт" in lower_val:
                        item.setForeground(QColor("#e07474"))
                    elif "рабоч" in lower_val or "актив" in lower_val or "используется" in lower_val:
                        item.setForeground(QColor("#27ae60"))
                    elif "склад" in lower_val or "запас" in lower_val:
                        item.setForeground(QColor("#f39c12"))
                    elif "списан" in lower_val:
                        item.setForeground(QColor("#95a5a6"))
                    else:
                        item.setForeground(QColor("#4a90d9"))

                # Цветовая индикация сотрудника
                if col == 4:
                    if val == "Свободный":
                        item.setForeground(QColor("#95a5a6"))
                        item.setFont(self._italic_font())
                    else:
                        item.setForeground(QColor("#27ae60"))
                        item.setFont(self._bold_font())

                # Цвет локации
                if col == 5 and val:
                    item.setForeground(QColor("#6c7ce0"))

                self.registry_table.setItem(row, col, item)

    def filter_registry(self, text):
        text = text.lower().strip()
        for row in range(self.registry_table.rowCount()):
            match = False
            if not text:
                match = True
            else:
                for col in range(self.registry_table.columnCount()):
                    item = self.registry_table.item(row, col)
                    if item and text in item.text().lower():
                        match = True
                        break
            self.registry_table.setRowHidden(row, not match)

    def on_registry_double_click(self, index):
        row = index.row()
        devices = self.data.get("devices", [])
        if row < 0 or row >= len(devices):
            return
        device = devices[row]
        dev_id = device.get("id", "")
        registry = self.data.get("registry", {})
        reg_info = registry.get(dev_id, {})

        dlg = AssignDialog(
            self,
            device=device,
            registry_info=reg_info,
            locations=self.data.get("locations", []),
            statuses=self.data.get("statuses", [])
        )
        if dlg.exec() == QDialog.DialogCode.Accepted and dlg.result_data:
            self.data["registry"][dev_id] = dlg.result_data
            save_data(self.data)
            self.refresh_registry_table()

    # ========================== ВКЛАДКА СПРАВОЧНИК ==========================

    def build_directory_tab(self):
        layout = QVBoxLayout(self.directory_tab)
        layout.setSpacing(16)
        layout.setContentsMargins(8, 12, 8, 8)

        title = QLabel("Справочник")
        title.setObjectName("titleLabel")
        layout.addWidget(title)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setStyleSheet("QScrollArea { border: none; background: transparent; }")
        scroll_widget = QWidget()
        scroll_layout = QVBoxLayout(scroll_widget)
        scroll_layout.setSpacing(20)
        scroll_layout.setContentsMargins(0, 0, 0, 0)

        # -------- Секция ТСД --------
        grp_devices = QGroupBox("Терминалы сбора данных (ТСД)")
        grp_devices_layout = QVBoxLayout(grp_devices)
        grp_devices_layout.setSpacing(10)
        grp_devices_layout.setContentsMargins(16, 16, 16, 16)

        btn_row_dev = QHBoxLayout()

        btn_add_dev = QPushButton("  Добавить ТСД")
        btn_add_dev.setObjectName("btnAdd")
        btn_add_dev.setCursor(Qt.CursorShape.PointingHandCursor)
        btn_add_dev.clicked.connect(self.add_device)

        btn_edit_dev = QPushButton("  Редактировать")
        btn_edit_dev.setObjectName("btnEdit")
        btn_edit_dev.setCursor(Qt.CursorShape.PointingHandCursor)
        btn_edit_dev.clicked.connect(self.edit_device)

        btn_del_dev = QPushButton("  Удалить")
        btn_del_dev.setObjectName("btnDelete")
        btn_del_dev.setCursor(Qt.CursorShape.PointingHandCursor)
        btn_del_dev.clicked.connect(self.delete_device)

        btn_row_dev.addWidget(btn_add_dev)
        btn_row_dev.addWidget(btn_edit_dev)
        btn_row_dev.addWidget(btn_del_dev)
        btn_row_dev.addStretch()
        grp_devices_layout.addLayout(btn_row_dev)

        self.devices_table = QTableWidget()
        self.devices_table.setColumnCount(4)
        self.devices_table.setHorizontalHeaderLabels(["Бренд", "Модель", "IMEI", "Состояние по умолч."])
        self.devices_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.devices_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.devices_table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self.devices_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.devices_table.setAlternatingRowColors(True)
        self.devices_table.setStyleSheet("alternate-background-color: #fafbfd;")
        self.devices_table.verticalHeader().setVisible(False)
        self.devices_table.setMinimumHeight(200)
        grp_devices_layout.addWidget(self.devices_table)
        scroll_layout.addWidget(grp_devices)

        # -------- Секция Локации --------
        grp_locations = QGroupBox("Локации")
        grp_loc_layout = QVBoxLayout(grp_locations)
        grp_loc_layout.setSpacing(10)
        grp_loc_layout.setContentsMargins(16, 16, 16, 16)

        btn_row_loc = QHBoxLayout()

        btn_add_loc = QPushButton("  Добавить локацию")
        btn_add_loc.setObjectName("btnAdd")
        btn_add_loc.setCursor(Qt.CursorShape.PointingHandCursor)
        btn_add_loc.clicked.connect(self.add_location)

        btn_edit_loc = QPushButton("  Редактировать")
        btn_edit_loc.setObjectName("btnEdit")
        btn_edit_loc.setCursor(Qt.CursorShape.PointingHandCursor)
        btn_edit_loc.clicked.connect(self.edit_location)

        btn_del_loc = QPushButton("  Удалить")
        btn_del_loc.setObjectName("btnDelete")
        btn_del_loc.setCursor(Qt.CursorShape.PointingHandCursor)
        btn_del_loc.clicked.connect(self.delete_location)

        btn_row_loc.addWidget(btn_add_loc)
        btn_row_loc.addWidget(btn_edit_loc)
        btn_row_loc.addWidget(btn_del_loc)
        btn_row_loc.addStretch()
        grp_loc_layout.addLayout(btn_row_loc)

        self.locations_table = QTableWidget()
        self.locations_table.setColumnCount(1)
        self.locations_table.setHorizontalHeaderLabels(["Название локации"])
        self.locations_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.locations_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.locations_table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self.locations_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.locations_table.setAlternatingRowColors(True)
        self.locations_table.setStyleSheet("alternate-background-color: #fafbfd;")
        self.locations_table.verticalHeader().setVisible(False)
        self.locations_table.setMinimumHeight(150)
        grp_loc_layout.addWidget(self.locations_table)
        scroll_layout.addWidget(grp_locations)

        # -------- Секция Состояния --------
        grp_statuses = QGroupBox("Состояния")
        grp_st_layout = QVBoxLayout(grp_statuses)
        grp_st_layout.setSpacing(10)
        grp_st_layout.setContentsMargins(16, 16, 16, 16)

        btn_row_st = QHBoxLayout()

        btn_add_st = QPushButton("  Добавить состояние")
        btn_add_st.setObjectName("btnAdd")
        btn_add_st.setCursor(Qt.CursorShape.PointingHandCursor)
        btn_add_st.clicked.connect(self.add_status)

        btn_edit_st = QPushButton("  Редактировать")
        btn_edit_st.setObjectName("btnEdit")
        btn_edit_st.setCursor(Qt.CursorShape.PointingHandCursor)
        btn_edit_st.clicked.connect(self.edit_status)

        btn_del_st = QPushButton("  Удалить")
        btn_del_st.setObjectName("btnDelete")
        btn_del_st.setCursor(Qt.CursorShape.PointingHandCursor)
        btn_del_st.clicked.connect(self.delete_status)

        btn_row_st.addWidget(btn_add_st)
        btn_row_st.addWidget(btn_edit_st)
        btn_row_st.addWidget(btn_del_st)
        btn_row_st.addStretch()
        grp_st_layout.addLayout(btn_row_st)

        self.statuses_table = QTableWidget()
        self.statuses_table.setColumnCount(1)
        self.statuses_table.setHorizontalHeaderLabels(["Название состояния"])
        self.statuses_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.statuses_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.statuses_table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self.statuses_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.statuses_table.setAlternatingRowColors(True)
        self.statuses_table.setStyleSheet("alternate-background-color: #fafbfd;")
        self.statuses_table.verticalHeader().setVisible(False)
        self.statuses_table.setMinimumHeight(150)
        grp_st_layout.addWidget(self.statuses_table)
        scroll_layout.addWidget(grp_statuses)

        scroll_layout.addStretch()
        scroll.setWidget(scroll_widget)
        layout.addWidget(scroll)

    # ---------- Справочник: обновление таблиц ----------

    def refresh_devices_table(self):
        devices = self.data.get("devices", [])
        self.devices_table.setRowCount(len(devices))
        for row, dev in enumerate(devices):
            vals = [
                dev.get("brand", ""),
                dev.get("model", ""),
                dev.get("imei", ""),
                dev.get("default_status", "")
            ]
            for col, val in enumerate(vals):
                item = QTableWidgetItem(str(val))
                item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                self.devices_table.setItem(row, col, item)

    def refresh_locations_table(self):
        locations = self.data.get("locations", [])
        self.locations_table.setRowCount(len(locations))
        for row, loc in enumerate(locations):
            item = QTableWidgetItem(loc)
            item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
            self.locations_table.setItem(row, 0, item)

    def refresh_statuses_table(self):
        statuses = self.data.get("statuses", [])
        self.statuses_table.setRowCount(len(statuses))
        for row, st in enumerate(statuses):
            item = QTableWidgetItem(st)
            item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
            self.statuses_table.setItem(row, 0, item)

    # ---------- Справочник: ТСД CRUD ----------

    def add_device(self):
        dlg = DeviceDialog(self, statuses=self.data.get("statuses", []))
        if dlg.exec() == QDialog.DialogCode.Accepted and dlg.result_data:
            new_id = datetime.now().strftime("%Y%m%d%H%M%S%f")
            device = {
                "id": new_id,
                "brand": dlg.result_data["brand"],
                "model": dlg.result_data["model"],
                "imei": dlg.result_data["imei"],
                "default_status": dlg.result_data["default_status"]
            }
            self.data["devices"].append(device)
            # Создаём запись реестра если указано состояние
            if device["default_status"]:
                self.data["registry"][new_id] = {
                    "employee": "Свободный",
                    "location": "",
                    "status": device["default_status"],
                    "last_modified": datetime.now().strftime("%d.%m.%Y %H:%M:%S")
                }
            save_data(self.data)
            self.refresh_devices_table()
            self.refresh_registry_table()

    def edit_device(self):
        row = self.devices_table.currentRow()
        if row < 0:
            QMessageBox.information(self, "Информация", "Выберите ТСД для редактирования.")
            return
        devices = self.data.get("devices", [])
        if row >= len(devices):
            return
        device = devices[row]
        old_id = device.get("id", "")

        dlg = DeviceDialog(self, device=device, statuses=self.data.get("statuses", []))
        if dlg.exec() == QDialog.DialogCode.Accepted and dlg.result_data:
            device["brand"] = dlg.result_data["brand"]
            device["model"] = dlg.result_data["model"]
            device["imei"] = dlg.result_data["imei"]
            device["default_status"] = dlg.result_data["default_status"]
            save_data(self.data)
            self.refresh_devices_table()
            self.refresh_registry_table()

    def delete_device(self):
        row = self.devices_table.currentRow()
        if row < 0:
            QMessageBox.information(self, "Информация", "Выберите ТСД для удаления.")
            return
        devices = self.data.get("devices", [])
        if row >= len(devices):
            return
        device = devices[row]
        dev_id = device.get("id", "")
        name = f"{device.get('brand', '')} {device.get('model', '')} (IMEI: {device.get('imei', '')})"

        reply = QMessageBox.question(
            self, "Подтверждение удаления",
            f"Вы уверены, что хотите удалить ТСД:\n{name}?\n\nВся связанная информация в реестре будет удалена.",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No
        )
        if reply == QMessageBox.StandardButton.Yes:
            devices.pop(row)
            if dev_id in self.data.get("registry", {}):
                del self.data["registry"][dev_id]
            save_data(self.data)
            self.refresh_devices_table()
            self.refresh_registry_table()

    # ---------- Справочник: Локации CRUD ----------

    def add_location(self):
        dlg = SimpleInputDialog(self, title="Добавить локацию", label_text="Название:")
        if dlg.exec() == QDialog.DialogCode.Accepted and dlg.result_value:
            val = dlg.result_value
            if val in self.data.get("locations", []):
                QMessageBox.warning(self, "Ошибка", "Такая локация уже существует!")
                return
            self.data["locations"].append(val)
            save_data(self.data)
            self.refresh_locations_table()

    def edit_location(self):
        row = self.locations_table.currentRow()
        if row < 0:
            QMessageBox.information(self, "Информация", "Выберите локацию для редактирования.")
            return
        locations = self.data.get("locations", [])
        if row >= len(locations):
            return
        old_val = locations[row]

        dlg = SimpleInputDialog(self, title="Редактировать локацию", label_text="Название:", current_value=old_val)
        if dlg.exec() == QDialog.DialogCode.Accepted and dlg.result_value:
            new_val = dlg.result_value
            locations[row] = new_val
            # Обновляем в реестре все ссылки на старую локацию
            for dev_id, reg in self.data.get("registry", {}).items():
                if reg.get("location", "") == old_val:
                    reg["location"] = new_val
            save_data(self.data)
            self.refresh_locations_table()
            self.refresh_registry_table()

    def delete_location(self):
        row = self.locations_table.currentRow()
        if row < 0:
            QMessageBox.information(self, "Информация", "Выберите локацию для удаления.")
            return
        locations = self.data.get("locations", [])
        if row >= len(locations):
            return
        val = locations[row]

        reply = QMessageBox.question(
            self, "Подтверждение удаления",
            f"Удалить локацию «{val}»?\n\nУ всех ТСД с этой локацией поле будет очищено.",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No
        )
        if reply == QMessageBox.StandardButton.Yes:
            locations.pop(row)
            for dev_id, reg in self.data.get("registry", {}).items():
                if reg.get("location", "") == val:
                    reg["location"] = ""
            save_data(self.data)
            self.refresh_locations_table()
            self.refresh_registry_table()

    # ---------- Справочник: Состояния CRUD ----------

    def add_status(self):
        dlg = SimpleInputDialog(self, title="Добавить состояние", label_text="Название:")
        if dlg.exec() == QDialog.DialogCode.Accepted and dlg.result_value:
            val = dlg.result_value
            if val in self.data.get("statuses", []):
                QMessageBox.warning(self, "Ошибка", "Такое состояние уже существует!")
                return
            self.data["statuses"].append(val)
            save_data(self.data)
            self.refresh_statuses_table()

    def edit_status(self):
        row = self.statuses_table.currentRow()
        if row < 0:
            QMessageBox.information(self, "Информация", "Выберите состояние для редактирования.")
            return
        statuses = self.data.get("statuses", [])
        if row >= len(statuses):
            return
        old_val = statuses[row]

        dlg = SimpleInputDialog(self, title="Редактировать состояние", label_text="Название:", current_value=old_val)
        if dlg.exec() == QDialog.DialogCode.Accepted and dlg.result_value:
            new_val = dlg.result_value
            statuses[row] = new_val
            # Обновляем в реестре и в устройствах
            for dev_id, reg in self.data.get("registry", {}).items():
                if reg.get("status", "") == old_val:
                    reg["status"] = new_val
            for dev in self.data.get("devices", []):
                if dev.get("default_status", "") == old_val:
                    dev["default_status"] = new_val
            save_data(self.data)
            self.refresh_statuses_table()
            self.refresh_devices_table()
            self.refresh_registry_table()

    def delete_status(self):
        row = self.statuses_table.currentRow()
        if row < 0:
            QMessageBox.information(self, "Информация", "Выберите состояние для удаления.")
            return
        statuses = self.data.get("statuses", [])
        if row >= len(statuses):
            return
        val = statuses[row]

        reply = QMessageBox.question(
            self, "Подтверждение удаления",
            f"Удалить состояние «{val}»?\n\nУ всех ТСД с этим состоянием поле будет очищено.",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No
        )
        if reply == QMessageBox.StandardButton.Yes:
            statuses.pop(row)
            for dev_id, reg in self.data.get("registry", {}).items():
                if reg.get("status", "") == val:
                    reg["status"] = ""
            for dev in self.data.get("devices", []):
                if dev.get("default_status", "") == val:
                    dev["default_status"] = ""
            save_data(self.data)
            self.refresh_statuses_table()
            self.refresh_devices_table()
            self.refresh_registry_table()

    # ========================== ВКЛАДКА СТАТИСТИКА ==========================

    def build_stats_tab(self):
        layout = QVBoxLayout(self.stats_tab)
        layout.setSpacing(12)
        layout.setContentsMargins(8, 12, 8, 8)
