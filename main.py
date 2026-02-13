#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Реестр ТСД - Программа для ведения учёта терминалов сбора данных
"""

import tkinter as tk
from tkinter import ttk, messagebox, simpledialog
import json
import os
from datetime import datetime


# ========================== ДАННЫЕ ==========================

DATA_FILE = "tsd_registry_data.json"


def load_data():
    """Загрузка данных из файла"""
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
    """Сохранение данных в файл"""
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# ========================== ЦВЕТА И СТИЛИ ==========================

class Theme:
    # Основные цвета
    BG_PRIMARY = "#F0F4F8"
    BG_SECONDARY = "#FFFFFF"
    BG_CARD = "#FFFFFF"
    BG_SIDEBAR = "#1E293B"
    BG_SIDEBAR_HOVER = "#334155"
    BG_SIDEBAR_ACTIVE = "#3B82F6"

    # Акцентные цвета
    ACCENT_PRIMARY = "#3B82F6"
    ACCENT_PRIMARY_HOVER = "#2563EB"
    ACCENT_SUCCESS = "#10B981"
    ACCENT_SUCCESS_HOVER = "#059669"
    ACCENT_WARNING = "#F59E0B"
    ACCENT_DANGER = "#EF4444"
    ACCENT_DANGER_HOVER = "#DC2626"
    ACCENT_INFO = "#6366F1"

    # Текст
    TEXT_PRIMARY = "#1E293B"
    TEXT_SECONDARY = "#64748B"
    TEXT_LIGHT = "#FFFFFF"
    TEXT_MUTED = "#94A3B8"

    # Границы
    BORDER = "#E2E8F0"
    BORDER_FOCUS = "#3B82F6"

    # Тени и прочее
    SHADOW = "#CBD5E1"
    TABLE_ROW_ALT = "#F8FAFC"
    TABLE_ROW_HOVER = "#EFF6FF"
    TABLE_HEADER = "#F1F5F9"

    # Шрифты
    FONT_FAMILY = "Segoe UI"
    FONT_TITLE = ("Segoe UI", 18, "bold")
    FONT_SUBTITLE = ("Segoe UI", 14, "bold")
    FONT_BODY = ("Segoe UI", 11)
    FONT_BODY_BOLD = ("Segoe UI", 11, "bold")
    FONT_SMALL = ("Segoe UI", 10)
    FONT_SMALL_BOLD = ("Segoe UI", 10, "bold")
    FONT_SIDEBAR = ("Segoe UI", 12)
    FONT_SIDEBAR_ACTIVE = ("Segoe UI", 12, "bold")
    FONT_STAT_NUMBER = ("Segoe UI", 28, "bold")
    FONT_STAT_LABEL = ("Segoe UI", 11)
    FONT_BUTTON = ("Segoe UI", 11)
    FONT_TABLE_HEADER = ("Segoe UI", 10, "bold")
    FONT_TABLE_BODY = ("Segoe UI", 10)


# ========================== ВИДЖЕТЫ ==========================

class RoundedButton(tk.Canvas):
    """Кнопка с закруглёнными углами"""

    def __init__(self, parent, text="", command=None, bg_color=Theme.ACCENT_PRIMARY,
                 hover_color=Theme.ACCENT_PRIMARY_HOVER, text_color=Theme.TEXT_LIGHT,
                 width=160, height=40, radius=8, font=None, **kwargs):
        super().__init__(parent, width=width, height=height,
                         bg=parent.cget("bg") if hasattr(parent, 'cget') else Theme.BG_PRIMARY,
                         highlightthickness=0, **kwargs)

        self.command = command
        self.bg_color = bg_color
        self.hover_color = hover_color
        self.text_color = text_color
        self.btn_width = width
        self.btn_height = height
        self.radius = radius
        self.text = text
        self.font = font or Theme.FONT_BUTTON
        self._is_hovered = False

        self._draw(self.bg_color)

        self.bind("<Enter>", self._on_enter)
        self.bind("<Leave>", self._on_leave)
        self.bind("<Button-1>", self._on_click)

    def _draw(self, color):
        self.delete("all")
        r = self.radius
        w = self.btn_width
        h = self.btn_height

        # Рисуем закруглённый прямоугольник
        self.create_arc(0, 0, 2 * r, 2 * r, start=90, extent=90, fill=color, outline=color)
        self.create_arc(w - 2 * r, 0, w, 2 * r, start=0, extent=90, fill=color, outline=color)
        self.create_arc(0, h - 2 * r, 2 * r, h, start=180, extent=90, fill=color, outline=color)
        self.create_arc(w - 2 * r, h - 2 * r, w, h, start=270, extent=90, fill=color, outline=color)

        self.create_rectangle(r, 0, w - r, h, fill=color, outline=color)
        self.create_rectangle(0, r, w, h - r, fill=color, outline=color)

        self.create_text(w / 2, h / 2, text=self.text, fill=self.text_color, font=self.font)

    def _on_enter(self, event):
        self._is_hovered = True
        self._draw(self.hover_color)

    def _on_leave(self, event):
        self._is_hovered = False
        self._draw(self.bg_color)

    def _on_click(self, event):
        if self.command:
            self.command()

    def configure_bg(self, parent_bg):
        self.configure(bg=parent_bg)


class StyledEntry(tk.Frame):
    """Стилизованное поле ввода"""

    def __init__(self, parent, label_text="", placeholder="", width=300, **kwargs):
        super().__init__(parent, bg=parent.cget("bg") if hasattr(parent, 'cget') else Theme.BG_SECONDARY)

        if label_text:
            label = tk.Label(self, text=label_text, font=Theme.FONT_SMALL_BOLD,
                             fg=Theme.TEXT_PRIMARY, bg=self.cget("bg"))
            label.pack(anchor="w", pady=(0, 4))

        self.entry_frame = tk.Frame(self, bg=Theme.BORDER, padx=1, pady=1)
        self.entry_frame.pack(fill="x")

        self.inner_frame = tk.Frame(self.entry_frame, bg=Theme.BG_SECONDARY, padx=10, pady=6)
        self.inner_frame.pack(fill="x")

        self.entry = tk.Entry(self.inner_frame, font=Theme.FONT_BODY,
                              bg=Theme.BG_SECONDARY, fg=Theme.TEXT_PRIMARY,
                              insertbackground=Theme.TEXT_PRIMARY,
                              relief="flat", width=width // 8, **kwargs)
        self.entry.pack(fill="x")

        self.entry.bind("<FocusIn>", self._on_focus_in)
        self.entry.bind("<FocusOut>", self._on_focus_out)

    def _on_focus_in(self, event):
        self.entry_frame.configure(bg=Theme.BORDER_FOCUS)

    def _on_focus_out(self, event):
        self.entry_frame.configure(bg=Theme.BORDER)

    def get(self):
        return self.entry.get().strip()

    def set(self, value):
        self.entry.delete(0, "end")
        self.entry.insert(0, value)

    def clear(self):
        self.entry.delete(0, "end")


class StyledCombobox(tk.Frame):
    """Стилизованный выпадающий список"""

    def __init__(self, parent, label_text="", values=None, width=300, **kwargs):
        super().__init__(parent, bg=parent.cget("bg") if hasattr(parent, 'cget') else Theme.BG_SECONDARY)

        if label_text:
            label = tk.Label(self, text=label_text, font=Theme.FONT_SMALL_BOLD,
                             fg=Theme.TEXT_PRIMARY, bg=self.cget("bg"))
            label.pack(anchor="w", pady=(0, 4))

        self.combo_frame = tk.Frame(self, bg=Theme.BORDER, padx=1, pady=1)
        self.combo_frame.pack(fill="x")

        style = ttk.Style()
        style.configure("Custom.TCombobox",
                         fieldbackground=Theme.BG_SECONDARY,
                         background=Theme.BG_SECONDARY)

        self.combo = ttk.Combobox(self.combo_frame, values=values or [],
                                   font=Theme.FONT_BODY, state="readonly",
                                   width=width // 10)
        self.combo.pack(fill="x", padx=2, pady=2)

    def get(self):
        return self.combo.get().strip()

    def set(self, value):
        self.combo.set(value)

    def update_values(self, values):
        self.combo["values"] = values

    def clear(self):
        self.combo.set("")


# ========================== ГЛАВНОЕ ПРИЛОЖЕНИЕ ==========================

class TSDRegistryApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Реестр ТСД")
        self.root.geometry("1280x800")
        self.root.minsize(1024, 600)

        # Данные
        self.data = load_data()

        # Настройка стилей
        self._setup_styles()

        # Основной контейнер
        self.root.configure(bg=Theme.BG_PRIMARY)

        # Боковая панель + основная область
        self.main_container = tk.Frame(self.root, bg=Theme.BG_PRIMARY)
        self.main_container.pack(fill="both", expand=True)

        # Боковая панель (sidebar)
        self._create_sidebar()

        # Основная область
        self.content_area = tk.Frame(self.main_container, bg=Theme.BG_PRIMARY)
        self.content_area.pack(side="left", fill="both", expand=True)

        # Текущая вкладка
        self.current_tab = None
        self.tabs = {}

        # Создание фреймов для каждой вкладки
        self._create_registry_tab()
        self._create_directory_tab()
        self._create_statistics_tab()

        # Показать реестр по умолчанию
        self.show_tab("registry")

    def _setup_styles(self):
        """Настройка стилей ttk"""
        style = ttk.Style()
        style.theme_use("clam")

        # Стиль для Treeview
        style.configure("Custom.Treeview",
                         background=Theme.BG_SECONDARY,
                         foreground=Theme.TEXT_PRIMARY,
                         fieldbackground=Theme.BG_SECONDARY,
                         font=Theme.FONT_TABLE_BODY,
                         rowheight=36,
                         borderwidth=0)

        style.configure("Custom.Treeview.Heading",
                         background=Theme.TABLE_HEADER,
                         foreground=Theme.TEXT_PRIMARY,
                         font=Theme.FONT_TABLE_HEADER,
                         borderwidth=0,
                         relief="flat")

        style.map("Custom.Treeview.Heading",
                   background=[("active", Theme.BORDER)])

        style.map("Custom.Treeview",
                   background=[("selected", Theme.ACCENT_PRIMARY)],
                   foreground=[("selected", Theme.TEXT_LIGHT)])

        # Стиль для Combobox
        style.configure("TCombobox",
                         fieldbackground=Theme.BG_SECONDARY,
                         background=Theme.BG_SECONDARY,
                         foreground=Theme.TEXT_PRIMARY,
                         arrowcolor=Theme.TEXT_SECONDARY)

        # Скроллбар
        style.configure("Custom.Vertical.TScrollbar",
                         background=Theme.BORDER,
                         troughcolor=Theme.BG_SECONDARY,
                         borderwidth=0,
                         arrowsize=0)

    def _create_sidebar(self):
        """Создание боковой панели"""
        self.sidebar = tk.Frame(self.main_container, bg=Theme.BG_SIDEBAR, width=240)
        self.sidebar.pack(side="left", fill="y")
        self.sidebar.pack_propagate(False)

        # Логотип / заголовок
        logo_frame = tk.Frame(self.sidebar, bg=Theme.BG_SIDEBAR, pady=20)
        logo_frame.pack(fill="x")

        # Иконка ТСД
        icon_label = tk.Label(logo_frame, text="📱", font=("Segoe UI", 32),
                              bg=Theme.BG_SIDEBAR, fg=Theme.TEXT_LIGHT)
        icon_label.pack()

        title_label = tk.Label(logo_frame, text="Реестр ТСД",
                               font=("Segoe UI", 16, "bold"),
                               bg=Theme.BG_SIDEBAR, fg=Theme.TEXT_LIGHT)
        title_label.pack(pady=(5, 0))

        subtitle_label = tk.Label(logo_frame, text="Система учёта",
                                  font=Theme.FONT_SMALL,
                                  bg=Theme.BG_SIDEBAR, fg=Theme.TEXT_MUTED)
        subtitle_label.pack()

        # Разделитель
        separator = tk.Frame(self.sidebar, bg=Theme.BG_SIDEBAR_HOVER, height=1)
        separator.pack(fill="x", padx=20, pady=10)

        # Меню навигации
        self.nav_buttons = {}
        nav_items = [
            ("registry", "📋  Реестр"),
            ("directory", "📁  Справочник"),
            ("statistics", "📊  Статистика"),
        ]

        for tab_id, text in nav_items:
            btn = tk.Label(self.sidebar, text=text, font=Theme.FONT_SIDEBAR,
                           bg=Theme.BG_SIDEBAR, fg=Theme.TEXT_MUTED,
                           anchor="w", padx=24, pady=12, cursor="hand2")
            btn.pack(fill="x")
            btn.bind("<Button-1>", lambda e, tid=tab_id: self.show_tab(tid))
            btn.bind("<Enter>", lambda e, b=btn, tid=tab_id: self._sidebar_hover(b, tid, True))
            btn.bind("<Leave>", lambda e, b=btn, tid=tab_id: self._sidebar_hover(b, tid, False))
            self.nav_buttons[tab_id] = btn

        # Нижняя часть sidebar
        bottom_frame = tk.Frame(self.sidebar, bg=Theme.BG_SIDEBAR)
        bottom_frame.pack(side="bottom", fill="x", pady=20)

        separator2 = tk.Frame(bottom_frame, bg=Theme.BG_SIDEBAR_HOVER, height=1)
        separator2.pack(fill="x", padx=20, pady=(0, 10))

        version_label = tk.Label(bottom_frame, text="Версия 1.0",
                                 font=Theme.FONT_SMALL,
                                 bg=Theme.BG_SIDEBAR, fg=Theme.TEXT_MUTED)
        version_label.pack()

    def _sidebar_hover(self, button, tab_id, entering):
        """Обработка hover на боковой панели"""
        if self.current_tab == tab_id:
            return
        if entering:
            button.configure(bg=Theme.BG_SIDEBAR_HOVER, fg=Theme.TEXT_LIGHT)
        else:
            button.configure(bg=Theme.BG_SIDEBAR, fg=Theme.TEXT_MUTED)

    def _update_sidebar_active(self, active_tab):
        """Обновление активной кнопки на sidebar"""
        for tab_id, btn in self.nav_buttons.items():
            if tab_id == active_tab:
                btn.configure(bg=Theme.BG_SIDEBAR_ACTIVE, fg=Theme.TEXT_LIGHT,
                              font=Theme.FONT_SIDEBAR_ACTIVE)
            else:
                btn.configure(bg=Theme.BG_SIDEBAR, fg=Theme.TEXT_MUTED,
                              font=Theme.FONT_SIDEBAR)

    def show_tab(self, tab_id):
        """Показать вкладку"""
        if self.current_tab == tab_id:
            return

        # Скрыть текущую вкладку
        for tid, frame in self.tabs.items():
            frame.pack_forget()

        # Показать нужную вкладку
        self.tabs[tab_id].pack(fill="both", expand=True)
        self.current_tab = tab_id
        self._update_sidebar_active(tab_id)

        # Обновить данные
        if tab_id == "registry":
            self._refresh_registry()
        elif tab_id == "directory":
            self._refresh_directory()
        elif tab_id == "statistics":
            self._refresh_statistics()

        # ========================== ВКЛАДКА РЕЕСТР ==========================

    def _create_registry_tab(self):
        """Создание вкладки Реестр"""
        frame = tk.Frame(self.content_area, bg=Theme.BG_PRIMARY)
        self.tabs["registry"] = frame

        # Заголовок
        header = tk.Frame(frame, bg=Theme.BG_PRIMARY, pady=20, padx=30)
        header.pack(fill="x")

        title = tk.Label(header, text="Реестр ТСД", font=Theme.FONT_TITLE,
                         bg=Theme.BG_PRIMARY, fg=Theme.TEXT_PRIMARY)
        title.pack(side="left")

        subtitle = tk.Label(header, text="Управление закреплением устройств",
                            font=Theme.FONT_BODY, bg=Theme.BG_PRIMARY, fg=Theme.TEXT_SECONDARY)
        subtitle.pack(side="left", padx=(15, 0), pady=(5, 0))

        # Поиск
        search_frame = tk.Frame(frame, bg=Theme.BG_PRIMARY, padx=30)
        search_frame.pack(fill="x")

        search_card = tk.Frame(search_frame, bg=Theme.BG_CARD, padx=15, pady=10)
        search_card.pack(fill="x")

        tk.Label(search_card, text="🔍", font=("Segoe UI", 14),
                 bg=Theme.BG_CARD, fg=Theme.TEXT_SECONDARY).pack(side="left")

        self.registry_search_var = tk.StringVar()
        self.registry_search_var.trace("w", lambda *args: self._refresh_registry())
        search_entry = tk.Entry(search_card, textvariable=self.registry_search_var,
                                font=Theme.FONT_BODY, bg=Theme.BG_CARD,
                                fg=Theme.TEXT_PRIMARY, relief="flat",
                                insertbackground=Theme.TEXT_PRIMARY)
        search_entry.pack(side="left", fill="x", expand=True, padx=(10, 0))

        # Таблица
        table_frame = tk.Frame(frame, bg=Theme.BG_PRIMARY, padx=30, pady=(15, 30))
        table_frame.pack(fill="both", expand=True)

        table_card = tk.Frame(table_frame, bg=Theme.BG_CARD, padx=2, pady=2)
        table_card.pack(fill="both", expand=True)

        table_border = tk.Frame(table_card, bg=Theme.BORDER, padx=1, pady=1)
        table_border.pack(fill="both", expand=True)

        table_inner = tk.Frame(table_border, bg=Theme.BG_SECONDARY)
        table_inner.pack(fill="both", expand=True)

        columns = ("brand", "model", "imei", "status", "employee", "location", "last_edit")
        self.registry_tree = ttk.Treeview(table_inner, columns=columns, show="headings",
                                           style="Custom.Treeview", selectmode="browse")

        headers = {
            "brand": ("Бренд", 120),
            "model": ("Модель", 140),
            "imei": ("IMEI", 160),
            "status": ("Состояние", 130),
            "employee": ("Сотрудник", 160),
            "location": ("Локация", 140),
            "last_edit": ("Последнее изменение", 180),
        }

        for col, (heading, width) in headers.items():
            self.registry_tree.heading(col, text=heading, anchor="w")
            self.registry_tree.column(col, width=width, minwidth=80, anchor="w")

        scrollbar = ttk.Scrollbar(table_inner, orient="vertical",
                                   command=self.registry_tree.yview,
                                   style="Custom.Vertical.TScrollbar")
        self.registry_tree.configure(yscrollcommand=scrollbar.set)

        self.registry_tree.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        self.registry_tree.bind("<Double-1>", self._on_registry_double_click)

        # Подсказка внизу
        hint_frame = tk.Frame(frame, bg=Theme.BG_PRIMARY, padx=30, pady=(0, 15))
        hint_frame.pack(fill="x")
        tk.Label(hint_frame,
                 text="💡 Дважды щёлкните по строке для закрепления ТСД за сотрудником",
                 font=Theme.FONT_SMALL, bg=Theme.BG_PRIMARY, fg=Theme.TEXT_SECONDARY
                 ).pack(side="left")

    def _refresh_registry(self):
        """Обновление таблицы реестра"""
        for item in self.registry_tree.get_children():
            self.registry_tree.delete(item)

        search = self.registry_search_var.get().lower().strip() if hasattr(self, 'registry_search_var') else ""

        self.registry_tree.tag_configure("even", background=Theme.BG_SECONDARY)
        self.registry_tree.tag_configure("odd", background=Theme.TABLE_ROW_ALT)
        self.registry_tree.tag_configure("free", foreground=Theme.ACCENT_SUCCESS)
        self.registry_tree.tag_configure("busy", foreground=Theme.ACCENT_PRIMARY)

        row_index = 0
        for device in self.data["devices"]:
            dev_id = str(device.get("id", ""))
            reg_info = self.data["registry"].get(dev_id, {})

            brand = device.get("brand", "")
            model = device.get("model", "")
            imei = device.get("imei", "")
            status = reg_info.get("status", device.get("status", ""))
            employee = reg_info.get("employee", "Свободный")
            location = reg_info.get("location", "")
            last_edit = reg_info.get("last_edit", "")

            if not employee:
                employee = "Свободный"

            if search:
                combined = f"{brand} {model} {imei} {status} {employee} {location}".lower()
                if search not in combined:
                    continue

            values = (brand, model, imei, status, employee, location, last_edit)
            tag = "even" if row_index % 2 == 0 else "odd"
            self.registry_tree.insert("", "end", values=values, iid=dev_id, tags=(tag,))
            row_index += 1

    def _on_registry_double_click(self, event):
        """Обработка двойного клика по записи реестра"""
        selected = self.registry_tree.selection()
        if not selected:
            return

        dev_id = selected[0]
        device = None
        for d in self.data["devices"]:
            if str(d.get("id", "")) == str(dev_id):
                device = d
                break

        if not device:
            return

        self._open_assignment_dialog(device)

    def _open_assignment_dialog(self, device):
        """Открыть диалог закрепления ТСД"""
        dialog = tk.Toplevel(self.root)
        dialog.title("Закрепление ТСД")
        dialog.geometry("500x540")
        dialog.resizable(False, False)
        dialog.configure(bg=Theme.BG_SECONDARY)
        dialog.transient(self.root)
        dialog.grab_set()

        dialog.update_idletasks()
        x = (dialog.winfo_screenwidth() - 500) // 2
        y = (dialog.winfo_screenheight() - 540) // 2
        dialog.geometry(f"500x540+{x}+{y}")

        dev_id = str(device.get("id", ""))
        reg_info = self.data["registry"].get(dev_id, {})

        # Заголовок
        header_frame = tk.Frame(dialog, bg=Theme.ACCENT_PRIMARY, pady=20)
        header_frame.pack(fill="x")

        tk.Label(header_frame, text="📱 Закрепление ТСД",
                 font=("Segoe UI", 16, "bold"),
                 bg=Theme.ACCENT_PRIMARY, fg=Theme.TEXT_LIGHT).pack()

        device_info = f"{device.get('brand', '')} {device.get('model', '')} | IMEI: {device.get('imei', '')}"
        tk.Label(header_frame, text=device_info,
                 font=Theme.FONT_SMALL,
                 bg=Theme.ACCENT_PRIMARY, fg="#BFDBFE").pack(pady=(5, 0))

        # Форма
        form_frame = tk.Frame(dialog, bg=Theme.BG_SECONDARY, padx=30, pady=20)
        form_frame.pack(fill="both", expand=True)

        # Сотрудник
        emp_entry = StyledEntry(form_frame, label_text="👤  Сотрудник", width=350)
        emp_entry.pack(fill="x", pady=(0, 15))
        current_emp = reg_info.get("employee", "")
        if current_emp and current_emp != "Свободный":
            emp_entry.set(current_emp)

        # Локация
        loc_combo = StyledCombobox(form_frame, label_text="📍  Локация",
                                    values=self.data["locations"], width=350)
        loc_combo.pack(fill="x", pady=(0, 15))
        current_loc = reg_info.get("location", "")
        if current_loc:
            loc_combo.set(current_loc)

        # Состояние
        status_combo = StyledCombobox(form_frame, label_text="⚙️  Состояние *",
                                       values=self.data["statuses"], width=350)
        status_combo.pack(fill="x", pady=(0, 15))
        current_status = reg_info.get("status", device.get("status", ""))
        if current_status:
            status_combo.set(current_status)

        # Информация
        info_frame = tk.Frame(form_frame, bg="#FEF3C7", padx=12, pady=8)
        info_frame.pack(fill="x", pady=(5, 15))
        tk.Label(info_frame,
                 text="ℹ️  Если сотрудник не указан, устройство считается свободным.\n"
                      "    Состояние обязательно для заполнения.",
                 font=Theme.FONT_SMALL, bg="#FEF3C7", fg="#92400E",
                 wraplength=380, justify="left").pack(anchor="w")

        # Кнопки
        btn_frame = tk.Frame(dialog, bg=Theme.BG_SECONDARY, pady=15, padx=30)
        btn_frame.pack(fill="x")

        def save_assignment():
            status_val = status_combo.get()
            if not status_val:
                messagebox.showwarning("Внимание", "Состояние обязательно для заполнения!",
                                       parent=dialog)
                return

            employee_val = emp_entry.get()
            if not employee_val:
                employee_val = "Свободный"

            location_val = loc_combo.get()
            now = datetime.now().strftime("%d.%m.%Y %H:%M:%S")

            self.data["registry"][dev_id] = {
                "employee": employee_val,
                "location": location_val,
                "status": status_val,
                "last_edit": now
            }

            save_data(self.data)
            self._refresh_registry()
            dialog.destroy()

        def clear_assignment():
            """Освободить ТСД"""
            now = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
            current_reg = self.data["registry"].get(dev_id, {})
            self.data["registry"][dev_id] = {
                "employee": "Свободный",
                "location": "",
                "status": current_reg.get("status", device.get("status", "")),
                "last_edit": now
            }
            save_data(self.data)
            self._refresh_registry()
            dialog.destroy()

        save_btn = RoundedButton(btn_frame, text="💾  Сохранить", command=save_assignment,
                                  bg_color=Theme.ACCENT_PRIMARY,
                                  hover_color=Theme.ACCENT_PRIMARY_HOVER,
                                  width=180, height=42)
        save_btn.configure_bg(Theme.BG_SECONDARY)
        save_btn.pack(side="left", padx=(0, 10))

        clear_btn = RoundedButton(btn_frame, text="🔓  Освободить", command=clear_assignment,
                                   bg_color=Theme.ACCENT_WARNING,
                                   hover_color="#D97706",
                                   width=150, height=42)
        clear_btn.configure_bg(Theme.BG_SECONDARY)
        clear_btn.pack(side="left", padx=(0, 10))

        cancel_btn = RoundedButton(btn_frame, text="Отмена", command=dialog.destroy,
                                    bg_color=Theme.TEXT_SECONDARY,
                                    hover_color="#475569",
                                    width=120, height=42)
        cancel_btn.configure_bg(Theme.BG_SECONDARY)
        cancel_btn.pack(side="right")

        # ========================== ВКЛАДКА СПРАВОЧНИК ==========================

    def _create_directory_tab(self):
        """Создание вкладки Справочник"""
        frame = tk.Frame(self.content_area, bg=Theme.BG_PRIMARY)
        self.tabs["directory"] = frame

        # Заголовок
        header = tk.Frame(frame, bg=Theme.BG_PRIMARY, pady=20, padx=30)
        header.pack(fill="x")

        title = tk.Label(header, text="Справочник", font=Theme.FONT_TITLE,
                         bg=Theme.BG_PRIMARY, fg=Theme.TEXT_PRIMARY)
        title.pack(side="left")

        subtitle = tk.Label(header, text="Управление устройствами, локациями и состояниями",
                            font=Theme.FONT_BODY, bg=Theme.BG_PRIMARY, fg=Theme.TEXT_SECONDARY)
        subtitle.pack(side="left", padx=(15, 0), pady=(5, 0))

        # Основная область со скроллом
        canvas = tk.Canvas(frame, bg=Theme.BG_PRIMARY, highlightthickness=0)
        scrollbar = ttk.Scrollbar(frame, orient="vertical", command=canvas.yview)
        scrollable_frame = tk.Frame(canvas, bg=Theme.BG_PRIMARY)

        scrollable_frame.bind("<Configure>",
                               lambda e: canvas.configure(scrollregion=canvas.bbox("all")))

        canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)

        canvas.pack(side="left", fill="both", expand=True, padx=30)
        scrollbar.pack(side="right", fill="y")

        def _on_mousewheel(event):
            canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")

        def bind_wheel():
            canvas.bind_all("<MouseWheel>", _on_mousewheel)

        def unbind_wheel():
            canvas.unbind_all("<MouseWheel>")

        canvas.bind("<Enter>", lambda e: bind_wheel())
        canvas.bind("<Leave>", lambda e: unbind_wheel())

        self.dir_scrollable = scrollable_frame
        self.dir_canvas = canvas

        # === Секция ТСД ===
        self._create_device_section(scrollable_frame)

        # === Секция Локации ===
        self._create_location_section(scrollable_frame)

        # === Секция Состояния ===
        self._create_status_section(scrollable_frame)

    def _create_device_section(self, parent):
        """Секция управления ТСД"""
        section = tk.Frame(parent, bg=Theme.BG_CARD, padx=20, pady=20)
        section.pack(fill="x", pady=(0, 20))

        section_header = tk.Frame(section, bg=Theme.BG_CARD)
        section_header.pack(fill="x", pady=(0, 15))

        tk.Label(section_header, text="📱  Устройства ТСД", font=Theme.FONT_SUBTITLE,
                 bg=Theme.BG_CARD, fg=Theme.TEXT_PRIMARY).pack(side="left")

        add_btn = RoundedButton(section_header, text="➕ Добавить ТСД",
                                 command=self._add_device_dialog,
                                 bg_color=Theme.ACCENT_SUCCESS,
                                 hover_color=Theme.ACCENT_SUCCESS_HOVER,
                                 width=170, height=36, font=Theme.FONT_SMALL_BOLD)
        add_btn.configure_bg(Theme.BG_CARD)
        add_btn.pack(side="right")

        tree_frame = tk.Frame(section, bg=Theme.BORDER, padx=1, pady=1)
        tree_frame.pack(fill="x")

        columns = ("brand", "model", "imei", "status")
        self.device_tree = ttk.Treeview(tree_frame, columns=columns, show="headings",
                                         style="Custom.Treeview", height=6, selectmode="browse")

        dev_headers = {
            "brand": ("Бренд", 150),
            "model": ("Модель", 180),
            "imei": ("IMEI", 200),
            "status": ("Состояние", 150),
        }

        for col, (heading, width) in dev_headers.items():
            self.device_tree.heading(col, text=heading, anchor="w")
            self.device_tree.column(col, width=width, minwidth=80, anchor="w")

        self.device_tree.pack(fill="x")

        dev_btn_frame = tk.Frame(section, bg=Theme.BG_CARD, pady=10)
        dev_btn_frame.pack(fill="x")

        edit_btn = RoundedButton(dev_btn_frame, text="✏️ Редактировать",
                                  command=self._edit_device_dialog,
                                  bg_color=Theme.ACCENT_PRIMARY,
                                  hover_color=Theme.ACCENT_PRIMARY_HOVER,
                                  width=160, height=36, font=Theme.FONT_SMALL_BOLD)
        edit_btn.configure_bg(Theme.BG_CARD)
        edit_btn.pack(side="left", padx=(0, 10))

        del_btn = RoundedButton(dev_btn_frame, text="🗑️ Удалить",
                                 command=self._delete_device,
                                 bg_color=Theme.ACCENT_DANGER,
                                 hover_color=Theme.ACCENT_DANGER_HOVER,
                                 width=140, height=36, font=Theme.FONT_SMALL_BOLD)
        del_btn.configure_bg(Theme.BG_CARD)
        del_btn.pack(side="left")

    def _create_location_section(self, parent):
        """Секция управления локациями"""
        section = tk.Frame(parent, bg=Theme.BG_CARD, padx=20, pady=20)
        section.pack(fill="x", pady=(0, 20))

        section_header = tk.Frame(section, bg=Theme.BG_CARD)
        section_header.pack(fill="x", pady=(0, 15))

        tk.Label(section_header, text="📍  Локации", font=Theme.FONT_SUBTITLE,
                 bg=Theme.BG_CARD, fg=Theme.TEXT_PRIMARY).pack(side="left")

        add_btn = RoundedButton(section_header, text="➕ Добавить",
                                 command=self._add_location_dialog,
                                 bg_color=Theme.ACCENT_SUCCESS,
                                 hover_color=Theme.ACCENT_SUCCESS_HOVER,
                                 width=140, height=36, font=Theme.FONT_SMALL_BOLD)
        add_btn.configure_bg(Theme.BG_CARD)
        add_btn.pack(side="right")

        tree_frame = tk.Frame(section, bg=Theme.BORDER, padx=1, pady=1)
        tree_frame.pack(fill="x")

        columns = ("location",)
        self.location_tree = ttk.Treeview(tree_frame, columns=columns, show="headings",
                                           style="Custom.Treeview", height=4, selectmode="browse")

        self.location_tree.heading("location", text="Название локации", anchor="w")
        self.location_tree.column("location", width=400, minwidth=200, anchor="w")
        self.location_tree.pack(fill="x")

        loc_btn_frame = tk.Frame(section, bg=Theme.BG_CARD, pady=10)
        loc_btn_frame.pack(fill="x")

        edit_btn = RoundedButton(loc_btn_frame, text="✏️ Редактировать",
                                  command=self._edit_location_dialog,
                                  bg_color=Theme.ACCENT_PRIMARY,
                                  hover_color=Theme.ACCENT_PRIMARY_HOVER,
                                  width=160, height=36, font=Theme.FONT_SMALL_BOLD)
        edit_btn.configure_bg(Theme.BG_CARD)
        edit_btn.pack(side="left", padx=(0, 10))

        del_btn = RoundedButton(loc_btn_frame, text="🗑️ Удалить",
                                 command=self._delete_location,
                                 bg_color=Theme.ACCENT_DANGER,
                                 hover_color=Theme.ACCENT_DANGER_HOVER,
                                 width=140, height=36, font=Theme.FONT_SMALL_BOLD)
        del_btn.configure_bg(Theme.BG_CARD)
        del_btn.pack(side="left")

    def _create_status_section(self, parent):
        """Секция управления состояниями"""
        section = tk.Frame(parent, bg=Theme.BG_CARD, padx=20, pady=20)
        section.pack(fill="x", pady=(0, 20))

        section_header = tk.Frame(section, bg=Theme.BG_CARD)
        section_header.pack(fill="x", pady=(0, 15))

        tk.Label(section_header, text="⚙️  Состояния", font=Theme.FONT_SUBTITLE,
                 bg=Theme.BG_CARD, fg=Theme.TEXT_PRIMARY).pack(side="left")

        add_btn = RoundedButton(section_header, text="➕ Добавить",
                                 command=self._add_status_dialog,
                                 bg_color=Theme.ACCENT_SUCCESS,
                                 hover_color=Theme.ACCENT_SUCCESS_HOVER,
                                 width=140, height=36, font=Theme.FONT_SMALL_BOLD)
        add_btn.configure_bg(Theme.BG_CARD)
        add_btn.pack(side="right")

        tree_frame = tk.Frame(section, bg=Theme.BORDER, padx=1, pady=1)
        tree_frame.pack(fill="x")

        columns = ("status",)
        self.status_tree = ttk.Treeview(tree_frame, columns=columns, show="headings",
                                         style="Custom.Treeview", height=4, selectmode="browse")

        self.status_tree.heading("status", text="Название состояния", anchor="w")
        self.status_tree.column("status", width=400, minwidth=200, anchor="w")
        self.status_tree.pack(fill="x")

        st_btn_frame = tk.Frame(section, bg=Theme.BG_CARD, pady=10)
        st_btn_frame.pack(fill="x")

        edit_btn = RoundedButton(st_btn_frame, text="✏️ Редактировать",
                                  command=self._edit_status_dialog,
                                  bg_color=Theme.ACCENT_PRIMARY,
                                  hover_color=Theme.ACCENT_PRIMARY_HOVER,
                                  width=160, height=36, font=Theme.FONT_SMALL_BOLD)
        edit_btn.configure_bg(Theme.BG_CARD)
        edit_btn.pack(side="left", padx=(0, 10))

        del_btn = RoundedButton(st_btn_frame, text="🗑️ Удалить",
                                 command=self._delete_status,
                                 bg_color=Theme.ACCENT_DANGER,
                                 hover_color=Theme.ACCENT_DANGER_HOVER,
                                 width=140, height=36, font=Theme.FONT_SMALL_BOLD)
        del_btn.configure_bg(Theme.BG_CARD)
        del_btn.pack(side="left")

    def _refresh_directory(self):
        """Обновление всех таблиц справочника"""
        # Устройства
        for item in self.device_tree.get_children():
            self.device_tree.delete(item)

        self.device_tree.tag_configure("even", background=Theme.BG_SECONDARY)
        self.device_tree.tag_configure("odd", background=Theme.TABLE_ROW_ALT)

        for i, device in enumerate(self.data["devices"]):
            tag = "even" if i % 2 == 0 else "odd"
            self.device_tree.insert("", "end", iid=str(device["id"]),
                                     values=(device["brand"], device["model"],
                                             device["imei"], device.get("status", "")),
                                     tags=(tag,))

        # Локации
        for item in self.location_tree.get_children():
            self.location_tree.delete(item)

        self.location_tree.tag_configure("even", background=Theme.BG_SECONDARY)
        self.location_tree.tag_configure("odd", background=Theme.TABLE_ROW_ALT)

        for i, loc in enumerate(self.data["locations"]):
            tag = "even" if i % 2 == 0 else "odd"
            self.location_tree.insert("", "end", iid=str(i), values=(loc,), tags=(tag,))

        # Состояния
        for item in self.status_tree.get_children():
            self.status_tree.delete(item)

        self.status_tree.tag_configure("even", background=Theme.BG_SECONDARY)
        self.status_tree.tag_configure("odd", background=Theme.TABLE_ROW_ALT)

        for i, st in enumerate(self.data["statuses"]):
            tag = "even" if i % 2 == 0 else "odd"
            self.status_tree.insert("", "end", iid=str(i), values=(st,), tags=(tag,))

    # --- Диалоги устройств ---

    def _add_device_dialog(self):
        """Диалог добавления нового ТСД"""
        self._device_dialog("Добавить ТСД", None)

    def _edit_device_dialog(self):
        """Диалог редактирования ТСД"""
        selected = self.device_tree.selection()
        if not selected:
            messagebox.showinfo("Информация", "Выберите устройство для редактирования")
            return

        dev_id = selected[0]
        device = None
        for d in self.data["devices"]:
            if str(d["id"]) == str(dev_id):
                device = d
                break

        if device:
            self._device_dialog("Редактировать ТСД", device)

    def _device_dialog(self, title_text, device):
        """Универсальный диалог для добавления/редактирования ТСД"""
        dialog = tk.Toplevel(self.root)
        dialog.title(title_text)
        dialog.geometry("480x500")
        dialog.resizable(False, False)
        dialog.configure(bg=Theme.BG_SECONDARY)
        dialog.transient(self.root)
        dialog.grab_set()

        dialog.update_idletasks()
        x = (dialog.winfo_screenwidth() - 480) // 2
        y = (dialog.winfo_screenheight() - 500) // 2
        dialog.geometry(f"480x500+{x}+{y}")

        # Заголовок
        header = tk.Frame(dialog, bg=Theme.ACCENT_PRIMARY, pady=18)
        header.pack(fill="x")

        icon = "📱" if device is None else "✏️"
        tk.Label(header, text=f"{icon} {title_text}",
                 font=("Segoe UI", 15, "bold"),
                 bg=Theme.ACCENT_PRIMARY, fg=Theme.TEXT_LIGHT).pack()

        # Форма
        form = tk.Frame(dialog, bg=Theme.BG_SECONDARY, padx=30, pady=20)
        form.pack(fill="both", expand=True)

        brand_entry = StyledEntry(form, label_text="Бренд *")
        brand_entry.pack(fill="x", pady=(0, 12))

        model_entry = StyledEntry(form, label_text="Модель *")
        model_entry.pack(fill="x", pady=(0, 12))

        imei_entry = StyledEntry(form, label_text="IMEI *")
        imei_entry.pack(fill="x", pady=(0, 12))

        status_combo = StyledCombobox(form, label_text="Состояние *",
                                       values=self.data["statuses"])
        status_combo.pack(fill="x", pady=(0, 12))

        if device:
            brand_entry.set(device.get("brand", ""))
            model_entry.set(device.get("model", ""))
            imei_entry.set(device.get("imei", ""))
            status_combo.set(device.get("status", ""))

        # Кнопки
        btn_frame = tk.Frame(dialog, bg=Theme.BG_SECONDARY, pady=15, padx=30)
        btn_frame.pack(fill="x")

        def save_device():
            brand = brand_entry.get()
            model = model_entry.get()
            imei = imei_entry.get()
            status = status_combo.get()

            if not brand or not model or not imei or not status:
                messagebox.showwarning("Внимание", "Все поля обязательны для заполнения!",
                                       parent=dialog)
                return

            # Проверка уникальности IMEI
            for d in self.data["devices"]:
                if d["imei"] == imei and (device is None or str(d["id"]) != str(device["id"])):
                    messagebox.showwarning("Внимание", "Устройство с таким IMEI уже существует!",
                                           parent=dialog)
                    return

            if device is None:
                # Добавление нового
                new_id = max([d["id"] for d in self.data["devices"]], default=0) + 1
                new_device = {
                    "id": new_id,
                    "brand": brand,
                    "model": model,
                    "imei": imei,
                    "status": status
                }
                self.data["devices"].append(new_device)
            else:
                # Редактирование существующего
                for d in self.data["devices"]:
                    if str(d["id"]) == str(device["id"]):
                        d["brand"] = brand
                        d["model"] = model
                        d["imei"] = imei
                        d["status"] = status
                        break

            save_data(self.data)
            self._refresh_directory()
            dialog.destroy()

        save_btn = RoundedButton(btn_frame, text="💾  Сохранить", command=save_device,
                                  bg_color=Theme.ACCENT_PRIMARY,
                                  hover_color=Theme.ACCENT_PRIMARY_HOVER,
                                  width=160, height=42)
        save_btn.configure_bg(Theme.BG_SECONDARY)
        save_btn.pack(side="left", padx=(0, 10))

        cancel_btn = RoundedButton(btn_frame, text="Отмена", command=dialog.destroy,
                                    bg_color=Theme.TEXT_SECONDARY,
                                    hover_color="#475569",
                                    width=120, height=42)
        cancel_btn.configure_bg(Theme.BG_SECONDARY)
        cancel_btn.pack(side="right")

    def _delete_device(self):
        """Удаление ТСД"""
        selected = self.device_tree.selection()
        if not selected:
            messagebox.showinfo("Информация", "Выберите устройство для удаления")
            return

        dev_id = selected[0]
        device = None
        for d in self.data["devices"]:
            if str(d["id"]) == str(dev_id):
                device = d
                break

        if device:
            confirm = messagebox.askyesno(
                "Подтверждение",
                f"Удалить устройство {device['brand']} {device['model']} (IMEI: {device['imei']})?\n\n"
                f"Также будет удалена информация из реестра."
            )
            if confirm:
                self.data["devices"] = [d for d in self.data["devices"]
                                         if str(d["id"]) != str(dev_id)]
                if str(dev_id) in self.data["registry"]:
                    del self.data["registry"][str(dev_id)]
                save_data(self.data)
                self._refresh_directory()

    # --- Диалоги локаций ---

    def _add_location_dialog(self):
        """Диалог добавления локации"""
        self._simple_dialog("Добавить локацию", "📍", "Название локации *",
                             self._save_new_location)

    def _edit_location_dialog(self):
        """Диалог редактирования локации"""
        selected = self.location_tree.selection()
        if not selected:
            messagebox.showinfo("Информация", "Выберите локацию для редактирования")
            return

        idx = int(selected[0])
        if idx < len(self.data["locations"]):
            old_value = self.data["locations"][idx]
            self._simple_dialog("Редактировать локацию", "📍", "Название локации *",
                                 lambda val: self._save_edit_location(idx, old_value, val),
                                 default_value=old_value)

    def _save_new_location(self, value):
        """Сохранить новую локацию"""
        if value in self.data["locations"]:
            messagebox.showwarning("Внимание", "Такая локация уже существует!")
            return False
        self.data["locations"].append(value)
        save_data(self.data)
        self._refresh_directory()
        return True

    def _save_edit_location(self, idx, old_value, new_value):
        """Сохранить изменённую локацию"""
        if new_value != old_value and new_value in self.data["locations"]:
            messagebox.showwarning("Внимание", "Такая локация уже существует!")
            return False
        self.data["locations"][idx] = new_value
        # Обновить в реестре
        for dev_id, reg in self.data["registry"].items():
            if reg.get("location", "") == old_value:
                reg["location"] = new_value
        save_data(self.data)
        self._refresh_directory()
        return True

    def _delete_location(self):
        """Удаление локации"""
        selected = self.location_tree.selection()
        if not selected:
            messagebox.showinfo("Информация", "Выберите локацию для удаления")
            return

        idx = int(selected[0])
        if idx < len(self.data["locations"]):
            loc_name = self.data["locations"][idx]
            # Проверить использование
            used_count = sum(1 for reg in self.data["registry"].values()
                              if reg.get("location", "") == loc_name)

            msg = f"Удалить локацию \"{loc_name}\"?"
            if used_count > 0:
                msg += f"\n\nЭта локация используется в {used_count} записях реестра.\n" \
                       f"Локация будет очищена в этих записях."

            if messagebox.askyesno("Подтверждение", msg):
                self.data["locations"].pop(idx)
                for dev_id, reg in self.data["registry"].items():
                    if reg.get("location", "") == loc_name:
                        reg["location"] = ""
                save_data(self.data)
                self._refresh_directory()

    # --- Диалоги состояний ---

    def _add_status_dialog(self):
        """Диалог добавления состояния"""
        self._simple_dialog("Добавить состояние", "⚙️", "Название состояния *",
                             self._save_new_status)

    def _edit_status_dialog(self):
        """Диалог редактирования состояния"""
        selected = self.status_tree.selection()
        if not selected:
            messagebox.showinfo("Информация", "Выберите состояние для редактирования")
            return

        idx = int(selected[0])
        if idx < len(self.data["statuses"]):
            old_value = self.data["statuses"][idx]
            self._simple_dialog("Редактировать состояние", "⚙️", "Название состояния *",
                                 lambda val: self._save_edit_status(idx, old_value, val),
                                 default_value=old_value)

    def _save_new_status(self, value):
        """Сохранить новое состояние"""
        if value in self.data["statuses"]:
            messagebox.showwarning("Внимание", "Такое состояние уже существует!")
            return False
        self.data["statuses"].append(value)
        save_data(self.data)
        self._refresh_directory()
        return True

    def _save_edit_status(self, idx, old_value, new_value):
        """Сохранить изменённое состояние"""
        if new_value != old_value and new_value in self.data["statuses"]:
            messagebox.showwarning("Внимание", "Такое состояние уже существует!")
            return False
        self.data["statuses"][idx] = new_value
        # Обновить в устройствах
        for device in self.data["devices"]:
            if device.get("status", "") == old_value:
                device["status"] = new_value
        # Обновить в реестре
        for dev_id, reg in self.data["registry"].items():
            if reg.get("status", "") == old_value:
                reg["status"] = new_value
        save_data(self.data)
        self._refresh_directory()
        return True

    def _delete_status(self):
        """Удаление состояния"""
        selected = self.status_tree.selection()
        if not selected:
            messagebox.showinfo("Информация", "Выберите состояние для удаления")
            return

        idx = int(selected[0])
        if idx < len(self.data["statuses"]):
            st_name = self.data["statuses"][idx]

            used_in_devices = sum(1 for d in self.data["devices"]
                                   if d.get("status", "") == st_name)
            used_in_registry = sum(1 for reg in self.data["registry"].values()
                                    if reg.get("status", "") == st_name)

            msg = f"Удалить состояние \"{st_name}\"?"
            if used_in_devices > 0 or used_in_registry > 0:
                msg += f"\n\nЭто состояние используется:\n"
                if used_in_devices > 0:
                    msg += f"  - в {used_in_devices} устройствах\n"
                if used_in_registry > 0:
                    msg += f"  - в {used_in_registry} записях реестра\n"
                msg += "\nСостояние будет очищено в этих записях."

            if messagebox.askyesno("Подтверждение", msg):
                self.data["statuses"].pop(idx)
                for device in self.data["devices"]:
                    if device.get("status", "") == st_name:
                        device["status"] = ""
                for dev_id, reg in self.data["registry"].items():
                    if reg.get("status", "") == st_name:
                        reg["status"] = ""
                save_data(self.data)
                self._refresh_directory()

        # --- Универсальный простой диалог ---

    def _simple_dialog(self, title_text, icon, label_text, save_callback, default_value=""):
        """Универсальный диалог для добавления/редактирования строкового значения"""
        dialog = tk.Toplevel(self.root)
        dialog.title(title_text)
        dialog.geometry("420x280")
        dialog.resizable(False, False)
        dialog.configure(bg=Theme.BG_SECONDARY)
        dialog.transient(self.root)
        dialog.grab_set()

        dialog.update_idletasks()
        x = (dialog.winfo_screenwidth() - 420) // 2
        y = (dialog.winfo_screenheight() - 280) // 2
        dialog.geometry(f"420x280+{x}+{y}")

        # Заголовок
        header = tk.Frame(dialog, bg=Theme.ACCENT_PRIMARY, pady=18)
        header.pack(fill="x")

        tk.Label(header, text=f"{icon} {title_text}",
                 font=("Segoe UI", 15, "bold"),
                 bg=Theme.ACCENT_PRIMARY, fg=Theme.TEXT_LIGHT).pack()

        # Форма
        form = tk.Frame(dialog, bg=Theme.BG_SECONDARY, padx=30, pady=25)
        form.pack(fill="both", expand=True)

        entry = StyledEntry(form, label_text=label_text)
        entry.pack(fill="x", pady=(0, 15))

        if default_value:
            entry.set(default_value)

        # Кнопки
        btn_frame = tk.Frame(dialog, bg=Theme.BG_SECONDARY, pady=15, padx=30)
        btn_frame.pack(fill="x")

        def on_save():
            value = entry.get()
            if not value:
                messagebox.showwarning("Внимание", "Поле не может быть пустым!", parent=dialog)
                return
            result = save_callback(value)
            if result is not False:
                dialog.destroy()

        save_btn = RoundedButton(btn_frame, text="💾  Сохранить", command=on_save,
                                  bg_color=Theme.ACCENT_PRIMARY,
                                  hover_color=Theme.ACCENT_PRIMARY_HOVER,
                                  width=160, height=42)
        save_btn.configure_bg(Theme.BG_SECONDARY)
        save_btn.pack(side="left", padx=(0, 10))

        cancel_btn = RoundedButton(btn_frame, text="Отмена", command=dialog.destroy,
                                    bg_color=Theme.TEXT_SECONDARY,
                                    hover_color="#475569",
                                    width=120, height=42)
        cancel_btn.configure_bg(Theme.BG_SECONDARY)
        cancel_btn.pack(side="right")

        # Фокус на поле ввода
        entry.entry.focus_set()

        # Enter для сохранения, Escape для отмены
        dialog.bind("<Return>", lambda e: on_save())
        dialog.bind("<Escape>", lambda e: dialog.destroy())

    # ========================== ВКЛАДКА СТАТИСТИКА ==========================

    def _create_statistics_tab(self):
        """Создание вкладки Статистика"""
        frame = tk.Frame(self.content_area, bg=Theme.BG_PRIMARY)
        self.tabs["statistics"] = frame

        # Заголовок
        header = tk.Frame(frame, bg=Theme.BG_PRIMARY, pady=20, padx=30)
        header.pack(fill="x")

        title = tk.Label(header, text="Статистика", font=Theme.FONT_TITLE,
                         bg=Theme.BG_PRIMARY, fg=Theme.TEXT_PRIMARY)
        title.pack(side="left")

        subtitle = tk.Label(header, text="Аналитика по устройствам",
                            font=Theme.FONT_BODY, bg=Theme.BG_PRIMARY, fg=Theme.TEXT_SECONDARY)
        subtitle.pack(side="left", padx=(15, 0), pady=(5, 0))

        # Кнопка обновления
        refresh_btn = RoundedButton(header, text="🔄 Обновить",
                                     command=self._refresh_statistics,
                                     bg_color=Theme.ACCENT_PRIMARY,
                                     hover_color=Theme.ACCENT_PRIMARY_HOVER,
                                     width=140, height=36, font=Theme.FONT_SMALL_BOLD)
        refresh_btn.configure_bg(Theme.BG_PRIMARY)
        refresh_btn.pack(side="right")

        # Основная область со скроллом
        canvas = tk.Canvas(frame, bg=Theme.BG_PRIMARY, highlightthickness=0)
        scrollbar = ttk.Scrollbar(frame, orient="vertical", command=canvas.yview)
        self.stats_scrollable = tk.Frame(canvas, bg=Theme.BG_PRIMARY)

        self.stats_scrollable.bind("<Configure>",
                                    lambda e: canvas.configure(scrollregion=canvas.bbox("all")))

        canvas.create_window((0, 0), window=self.stats_scrollable, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)

        canvas.pack(side="left", fill="both", expand=True, padx=30)
        scrollbar.pack(side="right", fill="y")

        # Привязка колеса мыши
        def _on_mousewheel(event):
            canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")

        def bind_wheel():
            canvas.bind_all("<MouseWheel>", _on_mousewheel)

        def unbind_wheel():
            canvas.unbind_all("<MouseWheel>")

        canvas.bind("<Enter>", lambda e: bind_wheel())
        canvas.bind("<Leave>", lambda e: unbind_wheel())

        self.stats_canvas = canvas

    def _refresh_statistics(self):
        """Обновление статистики"""
        # Очистить старые виджеты
        for widget in self.stats_scrollable.winfo_children():
            widget.destroy()

        # Сбор данных
        total_devices = len(self.data["devices"])

        # Данные из реестра
        assigned_count = 0
        free_count = 0
        location_counts = {}
        status_counts = {}
        location_status_counts = {}

        for device in self.data["devices"]:
            dev_id = str(device.get("id", ""))
            reg_info = self.data["registry"].get(dev_id, {})

            employee = reg_info.get("employee", "Свободный")
            location = reg_info.get("location", "")
            status = reg_info.get("status", device.get("status", ""))

            if not employee or employee == "Свободный":
                free_count += 1
            else:
                assigned_count += 1

            # Подсчёт по локациям
            loc_key = location if location else "Без локации"
            location_counts[loc_key] = location_counts.get(loc_key, 0) + 1

            # Подсчёт по состояниям
            st_key = status if status else "Не указано"
            status_counts[st_key] = status_counts.get(st_key, 0) + 1

            # Подсчёт по локациям и состояниям
            if loc_key not in location_status_counts:
                location_status_counts[loc_key] = {}
            location_status_counts[loc_key][st_key] = \
                location_status_counts[loc_key].get(st_key, 0) + 1

        parent = self.stats_scrollable

        # === Основные карточки ===
        cards_frame = tk.Frame(parent, bg=Theme.BG_PRIMARY)
        cards_frame.pack(fill="x", pady=(0, 20))

        # Настройка колонок для равномерного распределения
        cards_frame.columnconfigure(0, weight=1)
        cards_frame.columnconfigure(1, weight=1)
        cards_frame.columnconfigure(2, weight=1)

        # Карточка: Всего ТСД
        self._create_stat_card(cards_frame, "📱", "Всего ТСД в системе",
                                str(total_devices), Theme.ACCENT_PRIMARY, 0, 0)

        # Карточка: Закреплено
        self._create_stat_card(cards_frame, "👤", "Закреплено за сотрудниками",
                                str(assigned_count), Theme.ACCENT_SUCCESS, 0, 1)

        # Карточка: Свободно
        self._create_stat_card(cards_frame, "🔓", "Свободных ТСД",
                                str(free_count), Theme.ACCENT_WARNING, 0, 2)

        # === Состояния - общая таблица ===
        if status_counts:
            status_section = tk.Frame(parent, bg=Theme.BG_CARD, padx=20, pady=20)
            status_section.pack(fill="x", pady=(0, 20))

            tk.Label(status_section, text="⚙️  Распределение по состояниям (общее)",
                     font=Theme.FONT_SUBTITLE,
                     bg=Theme.BG_CARD, fg=Theme.TEXT_PRIMARY).pack(anchor="w", pady=(0, 15))

            # Заголовок общей сводки
            summary_text = f"Всего ТСД: {total_devices}"
            for st_name, st_count in sorted(status_counts.items(), key=lambda x: -x[1]):
                pct = (st_count / total_devices * 100) if total_devices > 0 else 0
                summary_text += f"   |   {st_name}: {st_count} ({pct:.0f}%)"

            tk.Label(status_section, text=summary_text,
                     font=Theme.FONT_BODY, bg=Theme.BG_CARD,
                     fg=Theme.TEXT_SECONDARY, wraplength=800,
                     justify="left").pack(anchor="w", pady=(0, 10))

            # Прогресс-бары для каждого состояния
            colors_cycle = [Theme.ACCENT_PRIMARY, Theme.ACCENT_SUCCESS,
                            Theme.ACCENT_WARNING, Theme.ACCENT_DANGER,
                            Theme.ACCENT_INFO, "#8B5CF6", "#EC4899", "#14B8A6"]

            for i, (st_name, st_count) in enumerate(
                    sorted(status_counts.items(), key=lambda x: -x[1])):
                self._create_progress_bar(status_section, st_name, st_count,
                                           total_devices, colors_cycle[i % len(colors_cycle)])

        # === Статистика по локациям ===
        if location_counts:
            loc_section = tk.Frame(parent, bg=Theme.BG_CARD, padx=20, pady=20)
            loc_section.pack(fill="x", pady=(0, 20))

            tk.Label(loc_section, text="📍  Количество ТСД по локациям",
                     font=Theme.FONT_SUBTITLE,
                     bg=Theme.BG_CARD, fg=Theme.TEXT_PRIMARY).pack(anchor="w", pady=(0, 15))

            colors_cycle_loc = [Theme.ACCENT_INFO, Theme.ACCENT_PRIMARY,
                                Theme.ACCENT_SUCCESS, Theme.ACCENT_WARNING,
                                "#8B5CF6", "#EC4899", "#14B8A6", Theme.ACCENT_DANGER]

            for i, (loc_name, loc_count) in enumerate(
                    sorted(location_counts.items(), key=lambda x: -x[1])):
                self._create_progress_bar(loc_section, loc_name, loc_count,
                                           total_devices,
                                           colors_cycle_loc[i % len(colors_cycle_loc)])

        # === Детальная статистика по каждой локации ===
        if location_status_counts:
            detail_section = tk.Frame(parent, bg=Theme.BG_CARD, padx=20, pady=20)
            detail_section.pack(fill="x", pady=(0, 20))

            tk.Label(detail_section,
                     text="📊  Детальная статистика по локациям (состояния)",
                     font=Theme.FONT_SUBTITLE,
                     bg=Theme.BG_CARD, fg=Theme.TEXT_PRIMARY).pack(anchor="w", pady=(0, 15))

            for loc_name in sorted(location_status_counts.keys()):
                statuses_in_loc = location_status_counts[loc_name]
                loc_total = sum(statuses_in_loc.values())

                # Подзаголовок локации
                loc_header = tk.Frame(detail_section, bg=Theme.BG_CARD)
                loc_header.pack(fill="x", pady=(10, 5))

                tk.Label(loc_header, text=f"📍 {loc_name}",
                         font=Theme.FONT_BODY_BOLD,
                         bg=Theme.BG_CARD, fg=Theme.TEXT_PRIMARY).pack(side="left")

                tk.Label(loc_header, text=f"  —  Всего: {loc_total} ТСД",
                         font=Theme.FONT_BODY,
                         bg=Theme.BG_CARD, fg=Theme.TEXT_SECONDARY).pack(side="left")

                # Состояния в этой локации
                detail_inner = tk.Frame(detail_section, bg=Theme.TABLE_ROW_ALT,
                                         padx=15, pady=10)
                detail_inner.pack(fill="x", pady=(0, 5))

                for j, (st_name, st_count) in enumerate(
                        sorted(statuses_in_loc.items(), key=lambda x: -x[1])):
                    pct = (st_count / loc_total * 100) if loc_total > 0 else 0

                    row = tk.Frame(detail_inner, bg=Theme.TABLE_ROW_ALT)
                    row.pack(fill="x", pady=2)

                    tk.Label(row, text=f"  ⚙️ {st_name}:",
                             font=Theme.FONT_SMALL,
                             bg=Theme.TABLE_ROW_ALT, fg=Theme.TEXT_PRIMARY,
                             width=25, anchor="w").pack(side="left")

                    tk.Label(row, text=f"{st_count} шт. ({pct:.0f}%)",
                             font=Theme.FONT_SMALL_BOLD,
                             bg=Theme.TABLE_ROW_ALT,
                             fg=Theme.ACCENT_PRIMARY).pack(side="left")

            # Разделитель внизу
            tk.Frame(detail_section, bg=Theme.BORDER, height=1).pack(fill="x", pady=(15, 0))

            # Дата формирования
            now_str = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
            tk.Label(detail_section,
                     text=f"Отчёт сформирован: {now_str}",
                     font=Theme.FONT_SMALL,
                     bg=Theme.BG_CARD, fg=Theme.TEXT_MUTED).pack(anchor="e", pady=(10, 0))

        # Если данных нет
        if total_devices == 0:
            empty_frame = tk.Frame(parent, bg=Theme.BG_CARD, padx=40, pady=60)
            empty_frame.pack(fill="x", pady=(0, 20))

            tk.Label(empty_frame, text="📭", font=("Segoe UI", 48),
                     bg=Theme.BG_CARD, fg=Theme.TEXT_MUTED).pack()

            tk.Label(empty_frame,
                     text="Нет данных для отображения",
                     font=Theme.FONT_SUBTITLE,
                     bg=Theme.BG_CARD, fg=Theme.TEXT_SECONDARY).pack(pady=(10, 5))

            tk.Label(empty_frame,
                     text="Добавьте устройства в Справочнике, чтобы увидеть статистику",
                     font=Theme.FONT_BODY,
                     bg=Theme.BG_CARD, fg=Theme.TEXT_MUTED).pack()

    def _create_stat_card(self, parent, icon, label, value, color, row, col):
        """Создание карточки статистики"""
        card = tk.Frame(parent, bg=Theme.BG_CARD, padx=20, pady=20)
        card.grid(row=row, column=col, padx=(0 if col == 0 else 10, 0),
                  pady=5, sticky="nsew")

        # Верхняя цветная полоска
        color_bar = tk.Frame(card, bg=color, height=4)
        color_bar.pack(fill="x", pady=(0, 15))

        # Иконка
        tk.Label(card, text=icon, font=("Segoe UI", 24),
                 bg=Theme.BG_CARD, fg=color).pack(anchor="w")

        # Значение
        tk.Label(card, text=value, font=Theme.FONT_STAT_NUMBER,
                 bg=Theme.BG_CARD, fg=Theme.TEXT_PRIMARY).pack(anchor="w", pady=(5, 0))

        # Подпись
        tk.Label(card, text=label, font=Theme.FONT_STAT_LABEL,
                 bg=Theme.BG_CARD, fg=Theme.TEXT_SECONDARY).pack(anchor="w", pady=(2, 0))

    def _create_progress_bar(self, parent, label, value, total, color):
        """Создание прогресс-бара"""
        bar_frame = tk.Frame(parent, bg=Theme.BG_CARD, pady=5)
        bar_frame.pack(fill="x")

        # Текст сверху
        info_row = tk.Frame(bar_frame, bg=Theme.BG_CARD)
        info_row.pack(fill="x")

        tk.Label(info_row, text=label, font=Theme.FONT_SMALL_BOLD,
                 bg=Theme.BG_CARD, fg=Theme.TEXT_PRIMARY).pack(side="left")

        pct = (value / total * 100) if total > 0 else 0
        tk.Label(info_row, text=f"{value} шт. ({pct:.0f}%)",
                 font=Theme.FONT_SMALL,
                 bg=Theme.BG_CARD, fg=Theme.TEXT_SECONDARY).pack(side="right")

        # Прогресс-бар
        bar_bg = tk.Frame(bar_frame, bg=Theme.BORDER, height=10)
        bar_bg.pack(fill="x", pady=(4, 0))

        bar_width = pct / 100.0
        if bar_width > 0:
            bar_fill = tk.Frame(bar_bg, bg=color, height=10)
            bar_fill.place(relwidth=bar_width, relheight=1.0)


# ========================== ЗАПУСК ПРИЛОЖЕНИЯ ==========================

def main():
    root = tk.Tk()

    # Иконка окна (если доступна)
    try:
        root.iconbitmap(default="")
    except Exception:
        pass

    # DPI awareness для Windows
    try:
        from ctypes import windll
        windll.shcore.SetProcessDpiAwareness(1)
    except Exception:
        pass

    app = TSDRegistryApp(root)

    # Центрирование окна при запуске
    root.update_idletasks()
    screen_w = root.winfo_screenwidth()
    screen_h = root.winfo_screenheight()
    win_w = 1280
    win_h = 800
    x = (screen_w - win_w) // 2
    y = (screen_h - win_h) // 2
    root.geometry(f"{win_w}x{win_h}+{x}+{y}")

    root.mainloop()


if __name__ == "__main__":
    main()
