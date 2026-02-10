import getpass
import re
import sqlite3
from contextlib import closing
from datetime import datetime
import calendar
import tkinter as tk
from tkinter import messagebox, simpledialog, ttk

DB_FILE = "wms_3pl.db"


class Database:
    """Класс для работы с базой данных SQLite"""

    def __init__(self, db_file: str):
        self.conn = sqlite3.connect(db_file)
        self.conn.execute("PRAGMA foreign_keys = ON")
        self._create_schema()
        self._migrate_products_table()
        self._migrate_subcategories_table()
        self._migrate_suppliers_table()
        self._migrate_clients_table()
        self._migrate_inbound_tables()
        self._migrate_inbound_orders_table()
        self._migrate_outbound_tables()
        self._seed_reference_data()

    def _create_schema(self):
        with closing(self.conn.cursor()) as cur:
            cur.executescript("""
                CREATE TABLE IF NOT EXISTS clients (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    code TEXT NOT NULL UNIQUE,
                    name TEXT NOT NULL,
                    contact TEXT,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS suppliers (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL UNIQUE
                );
                CREATE TABLE IF NOT EXISTS warehouses (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL UNIQUE
                );
                CREATE TABLE IF NOT EXISTS categories (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL UNIQUE
                );
                CREATE TABLE IF NOT EXISTS subcategories (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    category_id INTEGER NOT NULL,
                    name TEXT NOT NULL,
                    UNIQUE(category_id, name),
                    FOREIGN KEY(category_id) REFERENCES categories(id) ON DELETE RESTRICT
                );
                CREATE TABLE IF NOT EXISTS products (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    sku TEXT UNIQUE,
                    name TEXT,
                    client_id INTEGER,
                    unit TEXT NOT NULL DEFAULT 'pcs',
                    min_stock INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(client_id) REFERENCES clients(id) ON DELETE CASCADE
                );
                CREATE TABLE IF NOT EXISTS movements (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    product_id INTEGER NOT NULL,
                    movement_type TEXT NOT NULL CHECK(movement_type IN ('IN','OUT')),
                    quantity INTEGER NOT NULL CHECK(quantity > 0),
                    reference TEXT,
                    moved_at TEXT NOT NULL,
                    note TEXT,
                    FOREIGN KEY(product_id) REFERENCES products(id) ON DELETE CASCADE
                );
                CREATE TABLE IF NOT EXISTS inbound_orders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    order_number TEXT NOT NULL UNIQUE,
                    created_at TEXT NOT NULL,
                    received_at TEXT,
                    created_by TEXT NOT NULL,
                    supplier_id INTEGER NOT NULL,
                    client_id INTEGER NOT NULL,
                    warehouse_id INTEGER NOT NULL,
                    status TEXT NOT NULL CHECK(status IN ('Новый','Принят')),
                    FOREIGN KEY(supplier_id) REFERENCES suppliers(id) ON DELETE RESTRICT,
                    FOREIGN KEY(client_id) REFERENCES clients(id) ON DELETE RESTRICT,
                    FOREIGN KEY(warehouse_id) REFERENCES warehouses(id) ON DELETE RESTRICT
                );
                CREATE TABLE IF NOT EXISTS inbound_order_items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    order_id INTEGER NOT NULL,
                    category_id INTEGER,
                    subcategory_id INTEGER,
                    product_id INTEGER NOT NULL,
                    planned_qty REAL NOT NULL CHECK(planned_qty > 0),
                    actual_qty REAL NOT NULL DEFAULT 0,
                    actual_filled INTEGER NOT NULL DEFAULT 0,
                    planned_weight REAL,
                    planned_volume REAL,
                    serial_numbers TEXT,
                    FOREIGN KEY(order_id) REFERENCES inbound_orders(id) ON DELETE CASCADE,
                    FOREIGN KEY(category_id) REFERENCES categories(id) ON DELETE SET NULL,
                    FOREIGN KEY(subcategory_id) REFERENCES subcategories(id) ON DELETE SET NULL,
                    FOREIGN KEY(product_id) REFERENCES products(id) ON DELETE RESTRICT
                );
                CREATE TABLE IF NOT EXISTS warehouse_cells (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL UNIQUE,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS unplaced_stock (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    product_id INTEGER NOT NULL UNIQUE,
                    quantity REAL NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY(product_id) REFERENCES products(id) ON DELETE CASCADE
                );
                CREATE TABLE IF NOT EXISTS cell_stock (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    product_id INTEGER NOT NULL,
                    cell_id INTEGER NOT NULL,
                    quantity REAL NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL,
                    UNIQUE(product_id, cell_id),
                    FOREIGN KEY(product_id) REFERENCES products(id) ON DELETE CASCADE,
                    FOREIGN KEY(cell_id) REFERENCES warehouse_cells(id) ON DELETE CASCADE
                );
                CREATE TABLE IF NOT EXISTS outbound_orders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    order_number TEXT NOT NULL UNIQUE,
                    created_at TEXT NOT NULL,
                    shipped_at TEXT,
                    status TEXT NOT NULL CHECK(status IN ('Новый','Отгружен'))
                );
                CREATE TABLE IF NOT EXISTS outbound_order_items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    order_id INTEGER NOT NULL,
                    product_id INTEGER NOT NULL,
                    planned_qty REAL NOT NULL CHECK(planned_qty > 0),
                    actual_qty REAL NOT NULL DEFAULT 0,
                    FOREIGN KEY(order_id) REFERENCES outbound_orders(id) ON DELETE CASCADE,
                    FOREIGN KEY(product_id) REFERENCES products(id) ON DELETE RESTRICT
                );
            """)
            self.conn.commit()

    def _add_column_if_missing(self, table, column, definition):
        existing = {row[1] for row in self.query(f"PRAGMA table_info({table})")}
        if column not in existing:
            self.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")

    def _add_unique_index_if_missing(self, table, column):
        index_name = f"idx_{table}_{column}_unique"
        indexes = self.query(f"PRAGMA index_list({table})")
        if not any(row[1] == index_name for row in indexes):
            self.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS {index_name} ON {table}({column})")

    def _migrate_products_table(self):
        self._add_column_if_missing("products", "brand", "TEXT")
        self._add_column_if_missing("products", "supplier_id",
                                    "INTEGER REFERENCES suppliers(id) ON DELETE SET NULL")
        self._add_column_if_missing("products", "volume", "REAL")
        self._add_column_if_missing("products", "weight", "REAL")
        self._add_column_if_missing("products", "barcode", "TEXT")
        self._add_column_if_missing("products", "serial_tracking", "TEXT NOT NULL DEFAULT 'Нет'")
        self._add_column_if_missing("products", "article", "TEXT")
        self._add_column_if_missing("products", "category_id",
                                    "INTEGER REFERENCES categories(id) ON DELETE RESTRICT")
        self._add_column_if_missing("products", "subcategory_id",
                                    "INTEGER REFERENCES subcategories(id) ON DELETE RESTRICT")
        self._add_column_if_missing("products", "product_owner", "TEXT")
        self._add_unique_index_if_missing("products", "article")

    def _migrate_subcategories_table(self):
        self.execute("DELETE FROM subcategories WHERE category_id IS NULL")

    def _migrate_inbound_orders_table(self):
        self._add_column_if_missing("inbound_orders", "accepted_by", "TEXT")

    def _migrate_suppliers_table(self):
        self._add_column_if_missing("suppliers", "phone", "TEXT")
        self._add_column_if_missing("suppliers", "created_at", "TEXT")
        self.execute(
            "UPDATE suppliers SET created_at=? WHERE created_at IS NULL OR TRIM(created_at)=''",
            (datetime.now().strftime("%Y-%m-%d %H:%M:%S"),))

    def _migrate_clients_table(self):
        self._add_column_if_missing("clients", "code", "TEXT")
        self._add_column_if_missing("clients", "name", "TEXT")
        self._add_column_if_missing("clients", "contact", "TEXT")
        self._add_column_if_missing("clients", "created_at", "TEXT")
        self.execute(
            "UPDATE clients SET created_at=? WHERE created_at IS NULL OR TRIM(created_at)=''",
            (datetime.now().strftime("%Y-%m-%d %H:%M:%S"),))

    def _migrate_inbound_tables(self):
        self._add_column_if_missing("inbound_order_items", "actual_filled",
                                    "INTEGER NOT NULL DEFAULT 0")
        self._add_column_if_missing("inbound_order_items", "serial_numbers", "TEXT")

    def _migrate_outbound_tables(self):
        self._add_column_if_missing("outbound_orders", "shipped_at", "TEXT")

    def _seed_reference_data(self):
        if not self.query("SELECT id FROM suppliers LIMIT 1"):
            self.execute("INSERT INTO suppliers(name,phone,created_at) VALUES(?,?,?)",
                         ("Default Supplier", "", datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        if not self.query("SELECT id FROM warehouses LIMIT 1"):
            self.execute("INSERT INTO warehouses(name) VALUES(?)", ("Основной склад",))

    def execute(self, query, params=()):
        with closing(self.conn.cursor()) as cur:
            cur.execute(query, params)
            self.conn.commit()
            return cur.lastrowid

    def query(self, query, params=()):
        with closing(self.conn.cursor()) as cur:
            cur.execute(query, params)
            return cur.fetchall()

    def close(self):
        self.conn.close()


# ─────────────────────── CUSTOM WIDGETS ───────────────────────

class SidebarButton(tk.Canvas):
    """Кнопка бокового меню с иконкой и подсветкой"""

    def __init__(self, parent, text, icon="", command=None, **kw):
        super().__init__(parent, highlightthickness=0, cursor="hand2", **kw)
        self.text = text
        self.icon = icon
        self.command = command
        self._active = False
        self._hover = False

        self.configure(height=42)
        self.bind("<Enter>", self._on_enter)
        self.bind("<Leave>", self._on_leave)
        self.bind("<ButtonRelease-1>", self._on_click)
        self.bind("<Configure>", self._draw)

    def _draw(self, event=None):
        self.delete("all")
        w = self.winfo_width()
        h = self.winfo_height()

        if self._active:
            self.configure(bg="#ffffff")
            self.create_rectangle(0, 0, 4, h, fill="#1565C0", outline="")
            text_color = "#1565C0"
            icon_color = "#1565C0"
        elif self._hover:
            self.configure(bg="#f5f5f5")
            text_color = "#333333"
            icon_color = "#555555"
        else:
            self.configure(bg="#fafafa")
            text_color = "#555555"
            icon_color = "#888888"

        self.create_text(38, h // 2, text=self.icon, font=("Segoe UI", 13),
                         fill=icon_color, anchor="w")
        self.create_text(62, h // 2, text=self.text, font=("Segoe UI", 10),
                         fill=text_color, anchor="w")

    def set_active(self, val):
        self._active = val
        self._draw()

    def _on_enter(self, e):
        self._hover = True
        self._draw()

    def _on_leave(self, e):
        self._hover = False
        self._draw()

    def _on_click(self, e):
        if self.command:
            self.command()


class StatusDot(tk.Canvas):
    """Цветная точка-индикатор статуса"""

    COLORS_MAP = {
        "Новый": "#FFA000",
        "Принят": "#4CAF50",
        "Отменен": "#F44336",
    }

    def __init__(self, parent, status="", size=10, **kw):
        super().__init__(parent, width=size + 4, height=size + 4,
                         highlightthickness=0, **kw)
        color = self.COLORS_MAP.get(status, "#9E9E9E")
        pad = 2
        self.create_oval(pad, pad, size + pad, size + pad, fill=color, outline="")


# ─────────────────────── MAIN APPLICATION ───────────────────────

class WMSApp(tk.Tk):
    """WMS 3PL — Современный корпоративный интерфейс"""

    # ── Палитра ──
    C = {
        "sidebar_bg": "#fafafa",
        "sidebar_border": "#e0e0e0",
        "topbar_bg": "#ffffff",
        "topbar_border": "#e0e0e0",
        "content_bg": "#f4f6f8",
        "card_bg": "#ffffff",
        "card_border": "#e8eaed",
        "primary": "#1565C0",
        "primary_light": "#1976D2",
        "primary_dark": "#0D47A1",
        "accent": "#2196F3",
        "success": "#4CAF50",
        "success_bg": "#E8F5E9",
        "warning": "#FF9800",
        "warning_bg": "#FFF3E0",
        "error": "#F44336",
        "error_bg": "#FFEBEE",
        "text": "#212121",
        "text_secondary": "#757575",
        "text_hint": "#9E9E9E",
        "text_white": "#ffffff",
        "divider": "#EEEEEE",
        "hover": "#E3F2FD",
        "selected": "#BBDEFB",
        "row_alt": "#FAFAFA",
        "badge_new": "#FFA000",
        "badge_done": "#4CAF50",
    }

    F = {
        "title": ("Segoe UI Semibold", 16),
        "subtitle": ("Segoe UI", 11),
        "heading": ("Segoe UI Semibold", 13),
        "body": ("Segoe UI", 10),
        "body_bold": ("Segoe UI Semibold", 10),
        "small": ("Segoe UI", 9),
        "small_bold": ("Segoe UI Semibold", 9),
        "button": ("Segoe UI Semibold", 9),
        "metric_val": ("Segoe UI Semibold", 22),
        "metric_lbl": ("Segoe UI", 9),
        "tree": ("Segoe UI", 9),
        "tree_head": ("Segoe UI Semibold", 9),
        "sidebar": ("Segoe UI", 10),
        "topbar_title": ("Segoe UI Semibold", 14),
    }

    def __init__(self):
        super().__init__()
        self.title("WMS 3PL — Система управления складом")

        sw = self.winfo_screenwidth()
        sh = self.winfo_screenheight()
        w = min(1600, int(sw * 0.92))
        h = min(950, int(sh * 0.90))
        x = (sw - w) // 2
        y = max(0, (sh - h) // 2 - 20)
        self.geometry(f"{w}x{h}+{x}+{y}")
        self.minsize(1100, 650)
        self.configure(bg=self.C["content_bg"])

        self.db = Database(DB_FILE)
        self.current_user = getpass.getuser()
        self.current_page = None
        self.sidebar_buttons = {}

        self._configure_styles()
        self._init_variables()
        self._build_layout()
        self._navigate("suppliers")
        self.refresh_all()

        self.protocol("WM_DELETE_WINDOW", self._on_close)
        self.after(50, self._try_maximize)

    def _try_maximize(self):
        try:
            self.state("zoomed")
        except tk.TclError:
            pass

    # ─────────── Variables ───────────

    def _init_variables(self):
        self.selected_copy_value = ""

        self.suppliers_search_var = tk.StringVar()
        self.suppliers_search_var.trace_add("write", lambda *_: self.refresh_suppliers())

        self.clients_search_var = tk.StringVar()
        self.clients_search_var.trace_add("write", lambda *_: self.refresh_clients())

        self.categories_filter_var = tk.StringVar()
        self.categories_filter_var.trace_add("write", lambda *_: self.refresh_categories())
        self.new_category_var = tk.StringVar()
        self.new_subcategory_var = tk.StringVar()
        self.subcategory_parent_var = tk.StringVar()

        self.nom_brand_var = tk.StringVar()
        self.nom_article_var = tk.StringVar()
        self.nom_supplier_var = tk.StringVar()
        self.nom_client_var = tk.StringVar()
        self.nom_searched = False

        self.inb_search_var = tk.StringVar()
        self.inb_status_var = tk.StringVar(value="Все")
        self.inb_from_var = tk.StringVar()
        self.inb_to_var = tk.StringVar()
        self.inb_created_by_var = tk.StringVar()
        self.inb_accepted_by_var = tk.StringVar()
        self.inb_supplier_var = tk.StringVar()
        self.inb_client_var = tk.StringVar()
        self.inb_searched = True

        self.mov_product_var = tk.StringVar()
        self.mov_type_var = tk.StringVar(value="IN")
        self.mov_qty_var = tk.StringVar(value="1")
        self.mov_ref_var = tk.StringVar()
        self.mov_note_var = tk.StringVar()

        self.place_qty_var = tk.StringVar(value="1")
        self.place_cell_var = tk.StringVar()
        self.placement_product_id = None

        self.cell_name_var = tk.StringVar()

        self.search_name_var = tk.StringVar()
        self.search_article_var = tk.StringVar()
        self.search_barcode_var = tk.StringVar()
        self.search_category_var = tk.StringVar(value="Все")
        self.search_subcategory_var = tk.StringVar(value="Все")
        self.search_client_var = tk.StringVar(value="Все")
        self.search_only_in_stock_var = tk.BooleanVar(value=False)

    # ─────────── Styles ───────────

    def _configure_styles(self):
        self.style = ttk.Style(self)
        self.style.theme_use("clam")

        # Treeview
        self.style.configure("WMS.Treeview",
                             font=self.F["tree"],
                             rowheight=30,
                             background=self.C["card_bg"],
                             fieldbackground=self.C["card_bg"],
                             foreground=self.C["text"],
                             borderwidth=0)
        self.style.configure("WMS.Treeview.Heading",
                             font=self.F["tree_head"],
                             background="#F5F5F5",
                             foreground=self.C["text_secondary"],
                             borderwidth=0,
                             relief="flat",
                             padding=(8, 6))
        self.style.map("WMS.Treeview",
                       background=[("selected", self.C["selected"])],
                       foreground=[("selected", self.C["text"])])
        self.style.layout("WMS.Treeview", [
            ('Treeview.treearea', {'sticky': 'nswe'})])

        # Entry
        self.style.configure("WMS.TEntry",
                             font=self.F["body"],
                             padding=(8, 5),
                             borderwidth=1,
                             relief="solid")

        # Combobox
        self.style.configure("WMS.TCombobox",
                             font=self.F["body"],
                             padding=(8, 5))

        # Separator
        self.style.configure("WMS.TSeparator", background=self.C["divider"])

    # ─────────── Layout ───────────

    def _build_layout(self):
        # Root grid: sidebar | main
        self.grid_columnconfigure(1, weight=1)
        self.grid_rowconfigure(0, weight=1)

        self._build_sidebar()
        self._build_main_area()

    def _build_sidebar(self):
        sidebar = tk.Frame(self, bg=self.C["sidebar_bg"], width=220)
        sidebar.grid(row=0, column=0, sticky="ns")
        sidebar.grid_propagate(False)

        # Border right
        border = tk.Frame(sidebar, bg=self.C["sidebar_border"], width=1)
        border.pack(side="right", fill="y")

        inner = tk.Frame(sidebar, bg=self.C["sidebar_bg"])
        inner.pack(side="left", fill="both", expand=True)

        # Logo area
        logo_frame = tk.Frame(inner, bg=self.C["sidebar_bg"], height=60)
        logo_frame.pack(fill="x")
        logo_frame.pack_propagate(False)

        logo_inner = tk.Frame(logo_frame, bg=self.C["sidebar_bg"])
        logo_inner.pack(expand=True)

        tk.Label(logo_inner, text="📦", font=("Segoe UI", 20),
                 bg=self.C["sidebar_bg"], fg=self.C["primary"]).pack(side="left", padx=(0, 8))
        tk.Label(logo_inner, text="WMS 3PL", font=("Segoe UI Semibold", 15),
                 bg=self.C["sidebar_bg"], fg=self.C["primary"]).pack(side="left")

        # Divider
        tk.Frame(inner, bg=self.C["divider"], height=1).pack(fill="x", padx=16, pady=(0, 8))

        # Navigation items
        nav_items = [
            ("suppliers", "📋", "Поставщики"),
            ("clients", "👥", "3PL Клиенты"),
            ("categories", "📁", "Категории"),
            ("nomenclature", "📦", "Номенклатура"),
            ("inbound", "📥", "Приходы"),
            ("movements", "🔄", "Движения"),
            ("stock", "📊", "Остатки"),
            ("placement", "🧭", "Размещение"),
            ("cells_ref", "🗂️", "Справочник ячеек"),
            ("product_search", "🔎", "Поиск товара"),
        ]

        for key, icon, label in nav_items:
            btn = SidebarButton(inner, text=label, icon=icon,
                                command=lambda k=key: self._navigate(k),
                                bg=self.C["sidebar_bg"])
            btn.pack(fill="x", padx=8, pady=1)
            self.sidebar_buttons[key] = btn

        # Bottom user info
        bottom = tk.Frame(inner, bg=self.C["sidebar_bg"])
        bottom.pack(side="bottom", fill="x", padx=16, pady=16)

        tk.Frame(bottom, bg=self.C["divider"], height=1).pack(fill="x", pady=(0, 12))
        tk.Label(bottom, text=f"👤 {self.current_user}",
                 font=self.F["small"], bg=self.C["sidebar_bg"],
                 fg=self.C["text_secondary"]).pack(anchor="w")
        tk.Label(bottom, text=datetime.now().strftime("%d.%m.%Y"),
                 font=self.F["small"], bg=self.C["sidebar_bg"],
                 fg=self.C["text_hint"]).pack(anchor="w")

    def _build_main_area(self):
        self.main_area = tk.Frame(self, bg=self.C["content_bg"])
        self.main_area.grid(row=0, column=1, sticky="nsew")
        self.main_area.grid_columnconfigure(0, weight=1)
        self.main_area.grid_rowconfigure(1, weight=1)

        # Topbar
        self.topbar = tk.Frame(self.main_area, bg=self.C["topbar_bg"], height=52)
        self.topbar.grid(row=0, column=0, sticky="ew")
        self.topbar.grid_propagate(False)

        tk.Frame(self.topbar, bg=self.C["topbar_border"], height=1).pack(side="bottom", fill="x")

        self.topbar_inner = tk.Frame(self.topbar, bg=self.C["topbar_bg"])
        self.topbar_inner.pack(fill="both", expand=True, padx=20)

        self.topbar_title_lbl = tk.Label(self.topbar_inner, text="",
                                         font=self.F["topbar_title"],
                                         bg=self.C["topbar_bg"],
                                         fg=self.C["text"])
        self.topbar_title_lbl.pack(side="left", pady=12)

        # Metrics row
        self.metrics_frame = tk.Frame(self.main_area, bg=self.C["content_bg"])
        # Will be managed in content area

        # Content area
        self.content_frame = tk.Frame(self.main_area, bg=self.C["content_bg"])
        self.content_frame.grid(row=1, column=0, sticky="nsew")
        self.content_frame.grid_columnconfigure(0, weight=1)
        self.content_frame.grid_rowconfigure(0, weight=1)

        # Pages
        self.pages = {}
        self._build_all_pages()

    def _build_all_pages(self):
        for key in ["suppliers", "clients", "categories", "nomenclature",
                     "inbound", "movements", "stock", "placement",
                     "cells_ref", "product_search"]:
            page = tk.Frame(self.content_frame, bg=self.C["content_bg"])
            page.grid(row=0, column=0, sticky="nsew")
            page.grid_columnconfigure(0, weight=1)
            page.grid_rowconfigure(1, weight=1)  # tree area expands
            self.pages[key] = page

        self._build_suppliers_page()
        self._build_clients_page()
        self._build_categories_page()
        self._build_nomenclature_page()
        self._build_inbound_page()
        self._build_movements_page()
        self._build_stock_page()
        self._build_placement_page()
        self._build_cells_ref_page()
        self._build_product_search_page()

    # ─────────── Navigation ───────────

    def _navigate(self, page_key):
        if self.current_page == page_key:
            return

        titles = {
            "suppliers": "Поставщики",
            "clients": "3PL Клиенты",
            "categories": "Категории",
            "nomenclature": "Номенклатура",
            "inbound": "Приходные заказы",
            "movements": "Движения товаров",
            "stock": "Остатки на складе",
            "placement": "Размещение товара",
            "cells_ref": "Справочник ячеек",
            "product_search": "Поиск товара",
        }

        self.current_page = page_key
        self.topbar_title_lbl.configure(text=titles.get(page_key, ""))

        for k, btn in self.sidebar_buttons.items():
            btn.set_active(k == page_key)

        for k, page in self.pages.items():
            if k == page_key:
                page.tkraise()

        # ─────────── Helper: create card ───────────

    def _make_card(self, parent, **pack_kw):
        """Создать белую карточку с тенью-бордером"""
        expand = pack_kw.pop("expand", False)
        outer = tk.Frame(parent, bg=self.C["card_border"], padx=1, pady=1)
        outer.pack(fill="both", expand=expand, **pack_kw)
        card = tk.Frame(outer, bg=self.C["card_bg"])
        card.pack(fill="both", expand=True)
        return card

    def _make_toolbar(self, parent):
        """Панель инструментов поверх карточки"""
        tb = tk.Frame(parent, bg=self.C["card_bg"])
        tb.pack(fill="x", padx=16, pady=(12, 4))
        return tb

    def _make_flat_btn(self, parent, text, color=None, command=None, icon=""):
        """Плоская кнопка в стиле Material"""
        if color is None:
            color = self.C["primary"]
        full = f"{icon} {text}".strip()
        bg = parent.cget("bg") if isinstance(parent, tk.Frame) else self.C["card_bg"]
        btn = tk.Label(parent, text=full, font=self.F["button"],
                       fg=color, bg=bg, cursor="hand2", padx=12, pady=5)
        hover_bg = self.C["hover"]
        btn.bind("<Enter>", lambda e, b=btn, h=hover_bg: b.configure(bg=h))
        btn.bind("<Leave>", lambda e, b=btn, o=bg: b.configure(bg=o))
        if command:
            btn.bind("<ButtonRelease-1>", lambda e, c=command: c())
        return btn

    def _make_raised_btn(self, parent, text, bg_color=None, fg_color=None,
                         command=None, icon=""):
        """Приподнятая цветная кнопка"""
        if bg_color is None:
            bg_color = self.C["primary"]
        if fg_color is None:
            fg_color = self.C["text_white"]
        full = f"{icon} {text}".strip()
        btn = tk.Label(parent, text=full, font=self.F["button"],
                       fg=fg_color, bg=bg_color, cursor="hand2",
                       padx=14, pady=6, relief="flat", bd=0)
        darker = self._darken(bg_color, 0.15)
        btn.bind("<Enter>", lambda e, b=btn, d=darker: b.configure(bg=d))
        btn.bind("<Leave>", lambda e, b=btn, o=bg_color: b.configure(bg=o))
        if command:
            btn.bind("<ButtonRelease-1>", lambda e, c=command: c())
        return btn

    @staticmethod
    def _darken(hex_color, factor=0.1):
        """Затемнить hex-цвет"""
        hex_color = hex_color.lstrip("#")
        r, g, b = int(hex_color[:2], 16), int(hex_color[2:4], 16), int(hex_color[4:6], 16)
        r = max(0, int(r * (1 - factor)))
        g = max(0, int(g * (1 - factor)))
        b = max(0, int(b * (1 - factor)))
        return f"#{r:02x}{g:02x}{b:02x}"

    def _make_search_entry(self, parent, var, placeholder="Поиск...", width=28):
        """Поле поиска с иконкой"""
        bg = parent.cget("bg") if isinstance(parent, tk.Frame) else "#F5F5F5"
        frame = tk.Frame(parent, bg="#F5F5F5", padx=8, pady=4,
                         highlightbackground=self.C["card_border"],
                         highlightthickness=1)
        tk.Label(frame, text="🔍", font=("Segoe UI", 9),
                 bg="#F5F5F5", fg=self.C["text_hint"]).pack(side="left", padx=(0, 4))
        entry = tk.Entry(frame, textvariable=var, font=self.F["body"],
                         bg="#F5F5F5", fg=self.C["text"],
                         relief="flat", width=width,
                         insertbackground=self.C["text"])
        entry.pack(side="left", fill="x", expand=True)
        return frame

    def _make_labeled_entry(self, parent, label, var, width=18, readonly=False):
        """Подпись + поле ввода"""
        bg = parent.cget("bg") if isinstance(parent, tk.Frame) else self.C["card_bg"]
        frame = tk.Frame(parent, bg=bg)
        tk.Label(frame, text=label, font=self.F["small"],
                 bg=bg, fg=self.C["text_secondary"]).pack(anchor="w")
        state = "readonly" if readonly else "normal"
        entry = tk.Entry(frame, textvariable=var, font=self.F["body"],
                         width=width, state=state, relief="solid", bd=1,
                         highlightthickness=0)
        entry.pack(fill="x", pady=(2, 0))
        return frame

    def _make_labeled_combo(self, parent, label, var, values, width=18):
        """Подпись + комбобокс"""
        bg = parent.cget("bg") if isinstance(parent, tk.Frame) else self.C["card_bg"]
        frame = tk.Frame(parent, bg=bg)
        tk.Label(frame, text=label, font=self.F["small"],
                 bg=bg, fg=self.C["text_secondary"]).pack(anchor="w")
        combo = ttk.Combobox(frame, textvariable=var, values=values,
                             state="readonly", width=width, font=self.F["body"])
        combo.pack(fill="x", pady=(2, 0))
        frame._combo = combo
        return frame

    def _make_tree(self, parent, columns, widths_map, height=20):
        """Создать Treeview с скроллбарами внутри карточки"""
        tree_outer = tk.Frame(parent, bg=self.C["card_bg"])
        tree_outer.pack(fill="both", expand=True, padx=1, pady=(0, 1))

        tree = ttk.Treeview(tree_outer, columns=columns, show="headings",
                            height=height, style="WMS.Treeview")

        vsb = ttk.Scrollbar(tree_outer, orient="vertical", command=tree.yview)
        hsb = ttk.Scrollbar(tree_outer, orient="horizontal", command=tree.xview)
        tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

        tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")

        tree_outer.grid_rowconfigure(0, weight=1)
        tree_outer.grid_columnconfigure(0, weight=1)

        for col, (title, w) in widths_map.items():
            tree.heading(col, text=title, anchor="w")
            tree.column(col, width=w, minwidth=40, anchor="w")

        # Альтернативная подсветка строк через теги
        tree.tag_configure("evenrow", background=self.C["row_alt"])
        tree.tag_configure("oddrow", background=self.C["card_bg"])
        tree.tag_configure("new", background=self.C["warning_bg"])
        tree.tag_configure("accepted", background=self.C["success_bg"])
        tree.tag_configure("in", background=self.C["success_bg"])
        tree.tag_configure("out", background=self.C["error_bg"])
        tree.tag_configure("zero", background=self.C["error_bg"])
        tree.tag_configure("low", background=self.C["warning_bg"])
        tree.tag_configure("ok", background=self.C["success_bg"])
        tree.tag_configure("empty", background="#f5f5f5")
        tree.tag_configure("less", background=self.C["warning_bg"])
        tree.tag_configure("more", background=self.C["error_bg"])

        return tree, tree_outer

    def _make_metric_card(self, parent, icon, label, key):
        """Карточка метрики"""
        card = tk.Frame(parent, bg=self.C["card_bg"],
                        highlightbackground=self.C["card_border"],
                        highlightthickness=1, padx=18, pady=10)

        top_row = tk.Frame(card, bg=self.C["card_bg"])
        top_row.pack(fill="x")

        tk.Label(top_row, text=icon, font=("Segoe UI", 12),
                 bg=self.C["card_bg"], fg=self.C["primary"]).pack(side="left")
        tk.Label(top_row, text=label, font=self.F["metric_lbl"],
                 bg=self.C["card_bg"], fg=self.C["text_secondary"]).pack(
            side="left", padx=(6, 0))

        val_lbl = tk.Label(card, text="0", font=self.F["metric_val"],
                           bg=self.C["card_bg"], fg=self.C["text"])
        val_lbl.pack(anchor="w", pady=(4, 0))

        self.metric_labels[key] = val_lbl
        return card

    # ─────────── Utility ───────────

    def _center_dialog(self, dialog, w, h):
        dialog.update_idletasks()
        sw = dialog.winfo_screenwidth()
        sh = dialog.winfo_screenheight()
        x = (sw - w) // 2
        y = max(0, (sh - h) // 2 - 30)
        dialog.geometry(f"{w}x{h}+{x}+{y}")

    def _create_dialog(self, title, width, height):
        dialog = tk.Toplevel(self)
        dialog.title(title)
        dialog.configure(bg=self.C["card_bg"])
        dialog.transient(self)
        dialog.grab_set()
        dialog.resizable(False, False)
        self._center_dialog(dialog, width, height)
        return dialog

    def _create_fullscreen_dialog(self, title, width=1200, height=750):
        dialog = tk.Toplevel(self)
        dialog.title(title)
        dialog.configure(bg=self.C["content_bg"])
        dialog.transient(self)
        dialog.grab_set()
        self._center_dialog(dialog, width, height)
        return dialog

    def _copy_value(self):
        self.clipboard_clear()
        self.clipboard_append(str(self.selected_copy_value))

    def _get_id(self, token):
        if token and "|" in token:
            try:
                return int(token.split(" | ")[0])
            except ValueError:
                return None
        return None

    def _load_dict(self, table):
        rows = self.db.query(f"SELECT id, name FROM {table} ORDER BY name")
        return {f"{r[0]} | {r[1]}": r[0] for r in rows}

    def _normalize_decimal(self, raw):
        val = (raw or "").strip().replace(",", ".")
        if not val:
            return None
        if not re.fullmatch(r"\d+(?:\.\d{1,4})?", val):
            raise ValueError
        return float(val)

    def _pick_date(self, target_var):
        picker = self._create_dialog("📅 Выбор даты", 320, 340)
        today = datetime.now()
        year_var = tk.IntVar(value=today.year)
        month_var = tk.IntVar(value=today.month)

        main = tk.Frame(picker, bg=self.C["card_bg"])
        main.pack(fill="both", expand=True, padx=15, pady=15)

        month_names = ["", "Январь", "Февраль", "Март", "Апрель", "Май", "Июнь",
                       "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь"]

        nav = tk.Frame(main, bg=self.C["card_bg"])
        nav.pack(fill="x", pady=(0, 12))

        def shift(delta):
            m = month_var.get() + delta
            y = year_var.get()
            if m < 1:
                m, y = 12, y - 1
            elif m > 12:
                m, y = 1, y + 1
            month_var.set(m)
            year_var.set(y)
            build()

        btn_prev = tk.Label(nav, text="◀", font=self.F["body_bold"],
                            bg=self.C["card_bg"], fg=self.C["primary"],
                            cursor="hand2", padx=8)
        btn_prev.pack(side="left")
        btn_prev.bind("<ButtonRelease-1>", lambda e: shift(-1))

        month_lbl = tk.Label(nav, text="", font=self.F["heading"],
                             bg=self.C["card_bg"], fg=self.C["text"])
        month_lbl.pack(side="left", expand=True)

        btn_next = tk.Label(nav, text="▶", font=self.F["body_bold"],
                            bg=self.C["card_bg"], fg=self.C["primary"],
                            cursor="hand2", padx=8)
        btn_next.pack(side="right")
        btn_next.bind("<ButtonRelease-1>", lambda e: shift(1))

        days_frame = tk.Frame(main, bg=self.C["card_bg"])
        days_frame.pack(fill="both", expand=True)

        def select(day):
            target_var.set(f"{year_var.get():04d}-{month_var.get():02d}-{day:02d}")
            picker.destroy()

        def build():
            for w in days_frame.winfo_children():
                w.destroy()
            y, m = year_var.get(), month_var.get()
            month_lbl.configure(text=f"{month_names[m]} {y}")

            days = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
            for c, d in enumerate(days):
                tk.Label(days_frame, text=d, font=self.F["small_bold"],
                         bg=self.C["card_bg"], fg=self.C["text_secondary"],
                         width=4).grid(row=0, column=c, padx=2, pady=4)

            for r, week in enumerate(calendar.monthcalendar(y, m)):
                for c, day in enumerate(week):
                    if day == 0:
                        tk.Label(days_frame, text="", width=4,
                                 bg=self.C["card_bg"]).grid(row=r + 1, column=c)
                    else:
                        is_today = (day == today.day and m == today.month
                                    and y == today.year)
                        bg = self.C["primary"] if is_today else self.C["card_bg"]
                        fg = self.C["text_white"] if is_today else self.C["text"]
                        lbl = tk.Label(days_frame, text=str(day), width=4,
                                       font=self.F["small"], bg=bg, fg=fg,
                                       cursor="hand2")
                        lbl.grid(row=r + 1, column=c, padx=2, pady=2)
                        lbl.bind("<ButtonRelease-1>",
                                 lambda e, d=day: select(d))

        btns = tk.Frame(main, bg=self.C["card_bg"])
        btns.pack(fill="x", pady=(12, 0))

        b1 = self._make_flat_btn(btns, "Очистить", color=self.C["text_secondary"],
                                 command=lambda: [target_var.set(""), picker.destroy()],
                                 icon="✖")
        b1.pack(side="left")
        b2 = self._make_flat_btn(btns, "Сегодня", command=lambda: [
            target_var.set(today.strftime("%Y-%m-%d")), picker.destroy()], icon="📅")
        b2.pack(side="right")

        build()

    # ─────────── BUILD PAGES ───────────

    # ==================== ПОСТАВЩИКИ ====================

    def _build_suppliers_page(self):
        page = self.pages["suppliers"]

        # Metrics row
        metrics = tk.Frame(page, bg=self.C["content_bg"])
        metrics.pack(fill="x", padx=20, pady=(16, 10))

        self.metric_labels = {}

        configs = [
            ("suppliers", "📋", "Поставщики"),
            ("clients", "👥", "Клиенты"),
            ("products", "📦", "Товары"),
            ("inbound", "📥", "Приходы"),
            ("movements", "🔄", "Движения"),
        ]
        for key, icon, label in configs:
            mc = self._make_metric_card(metrics, icon, label, key)
            mc.pack(side="left", padx=(0, 10))

        # Card
        card = self._make_card(page, padx=20, pady=(0, 16), expand=True)

        # Toolbar
        tb = self._make_toolbar(card)

        sf = self._make_search_entry(tb, self.suppliers_search_var, "Поиск поставщика...")
        sf.pack(side="left")

        b3 = self._make_raised_btn(tb, "Удалить", bg_color=self.C["error"],
                                    command=self.delete_supplier, icon="🗑️")
        b3.pack(side="right", padx=(8, 0))
        b2 = self._make_raised_btn(tb, "Редактировать",
                                    bg_color=self.C["primary_light"],
                                    command=self.edit_supplier, icon="✏️")
        b2.pack(side="right", padx=(8, 0))
        b1 = self._make_raised_btn(tb, "Создать", bg_color=self.C["success"],
                                    command=self.create_supplier, icon="➕")
        b1.pack(side="right")

        # Separator
        tk.Frame(card, bg=self.C["divider"], height=1).pack(fill="x", padx=16)

        # Tree
        cols = ("id", "name", "phone", "created")
        wmap = {
            "id": ("ID", 60),
            "name": ("Название поставщика", 350),
            "phone": ("Телефон", 160),
            "created": ("Дата создания", 160),
        }
        self.suppliers_tree, _ = self._make_tree(card, cols, wmap)
        self.suppliers_tree.bind("<Double-1>", lambda e: self.edit_supplier())

        # Context menu
        self.suppliers_menu = tk.Menu(self, tearoff=0, font=self.F["body"])
        self.suppliers_menu.add_command(label="📋 Копировать", command=self._copy_value)
        self.suppliers_tree.bind("<Button-3>", self._show_suppliers_menu)

    def _show_suppliers_menu(self, event):
        row = self.suppliers_tree.identify_row(event.y)
        col = self.suppliers_tree.identify_column(event.x)
        if row:
            self.suppliers_tree.selection_set(row)
            vals = self.suppliers_tree.item(row, "values")
            idx = int(col.replace("#", "")) - 1 if col else 0
            self.selected_copy_value = vals[idx] if 0 <= idx < len(vals) else ""
            self.suppliers_menu.tk_popup(event.x_root, event.y_root)

    # ==================== КЛИЕНТЫ ====================

    def _build_clients_page(self):
        page = self.pages["clients"]

        card = self._make_card(page, padx=20, pady=16, expand=True)

        tb = self._make_toolbar(card)
        sf = self._make_search_entry(tb, self.clients_search_var, "Поиск клиента...")
        sf.pack(side="left")

        b3 = self._make_raised_btn(tb, "Удалить", bg_color=self.C["error"],
                                    command=self.delete_client, icon="🗑️")
        b3.pack(side="right", padx=(8, 0))
        b2 = self._make_raised_btn(tb, "Редактировать",
                                    bg_color=self.C["primary_light"],
                                    command=self.edit_client, icon="✏️")
        b2.pack(side="right", padx=(8, 0))
        b1 = self._make_raised_btn(tb, "Создать", bg_color=self.C["success"],
                                    command=self.create_client, icon="➕")
        b1.pack(side="right")

        tk.Frame(card, bg=self.C["divider"], height=1).pack(fill="x", padx=16)

        cols = ("id", "name", "contact", "created")
        wmap = {
            "id": ("ID", 60),
            "name": ("Название клиента", 350),
            "contact": ("Контакт", 160),
            "created": ("Дата создания", 160),
        }
        self.clients_tree, _ = self._make_tree(card, cols, wmap)
        self.clients_tree.bind("<Double-1>", lambda e: self.edit_client())

    # ==================== КАТЕГОРИИ ====================

    def _build_categories_page(self):
        page = self.pages["categories"]

        # Creation area
        create_card = self._make_card(page, padx=20, pady=(16, 8))

        tb = tk.Frame(create_card, bg=self.C["card_bg"])
        tb.pack(fill="x", padx=16, pady=12)

        tk.Label(tb, text="Создание категорий и подкатегорий",
                 font=self.F["heading"], bg=self.C["card_bg"],
                 fg=self.C["text"]).pack(anchor="w", pady=(0, 10))

        # Category row
        cat_row = tk.Frame(tb, bg=self.C["card_bg"])
        cat_row.pack(fill="x", pady=4)

        tk.Label(cat_row, text="Категория:", font=self.F["body"],
                 bg=self.C["card_bg"], fg=self.C["text_secondary"]).pack(
            side="left", padx=(0, 8))
        tk.Entry(cat_row, textvariable=self.new_category_var, font=self.F["body"],
                 width=22, relief="solid", bd=1).pack(side="left", padx=(0, 8))
        bc = self._make_raised_btn(cat_row, "Создать", bg_color=self.C["success"],
                                    command=self.create_category, icon="➕")
        bc.pack(side="left")

        # Subcategory row
        sub_row = tk.Frame(tb, bg=self.C["card_bg"])
        sub_row.pack(fill="x", pady=4)

        tk.Label(sub_row, text="Подкатегория:", font=self.F["body"],
                 bg=self.C["card_bg"], fg=self.C["text_secondary"]).pack(
            side="left", padx=(0, 8))
        tk.Entry(sub_row, textvariable=self.new_subcategory_var, font=self.F["body"],
                 width=18, relief="solid", bd=1).pack(side="left", padx=(0, 8))
        tk.Label(sub_row, text="в:", font=self.F["body"],
                 bg=self.C["card_bg"], fg=self.C["text_secondary"]).pack(
            side="left", padx=(0, 4))
        self.subcat_parent_box = ttk.Combobox(sub_row,
                                               textvariable=self.subcategory_parent_var,
                                               state="readonly", width=22,
                                               font=self.F["body"])
        self.subcat_parent_box.pack(side="left", padx=(0, 8))
        bs = self._make_raised_btn(sub_row, "Создать", bg_color=self.C["success"],
                                    command=self.create_subcategory, icon="➕")
        bs.pack(side="left")

        # Search
        search_row = tk.Frame(tb, bg=self.C["card_bg"])
        search_row.pack(fill="x", pady=(8, 0))
        sf = self._make_search_entry(search_row, self.categories_filter_var,
                                     "Фильтр по названию...")
        sf.pack(side="left")

        # Trees side by side
        trees_card = self._make_card(page, padx=20, pady=(0, 16), expand=True)

        panes = tk.Frame(trees_card, bg=self.C["card_bg"])
        panes.pack(fill="both", expand=True, padx=16, pady=12)

        # Left: categories
        left = tk.Frame(panes, bg=self.C["card_bg"])
        left.pack(side="left", fill="both", expand=True, padx=(0, 8))

        tk.Label(left, text="📁 Категории", font=self.F["heading"],
                 bg=self.C["card_bg"], fg=self.C["text"]).pack(anchor="w", pady=(0, 6))

        cat_cols = ("id", "name")
        cat_wmap = {"id": ("ID", 60), "name": ("Название", 220)}
        self.categories_tree, _ = self._make_tree(left, cat_cols, cat_wmap, height=14)

        # Right: subcategories
        right = tk.Frame(panes, bg=self.C["card_bg"])
        right.pack(side="left", fill="both", expand=True, padx=(8, 0))

        tk.Label(right, text="📂 Подкатегории", font=self.F["heading"],
                 bg=self.C["card_bg"], fg=self.C["text"]).pack(anchor="w", pady=(0, 6))

        sub_cols = ("id", "name", "category")
        sub_wmap = {"id": ("ID", 60), "name": ("Подкатегория", 160),
                    "category": ("Категория", 160)}
        self.subcategories_tree, _ = self._make_tree(right, sub_cols, sub_wmap, height=14)

        # ==================== НОМЕНКЛАТУРА ====================

    def _build_nomenclature_page(self):
        page = self.pages["nomenclature"]

        card = self._make_card(page, padx=20, pady=16, expand=True)

        # Toolbar
        tb = self._make_toolbar(card)

        # Filters
        filters_frame = tk.Frame(tb, bg=self.C["card_bg"])
        filters_frame.pack(side="left", fill="x")

        for lbl_text, var, w in [
            ("Марка", self.nom_brand_var, 10),
            ("Артикул", self.nom_article_var, 8),
            ("Поставщик", self.nom_supplier_var, 10),
            ("Клиент", self.nom_client_var, 10),
        ]:
            tk.Label(filters_frame, text=f"{lbl_text}:", font=self.F["small"],
                     bg=self.C["card_bg"], fg=self.C["text_secondary"]).pack(
                side="left", padx=(0, 3))
            tk.Entry(filters_frame, textvariable=var, font=self.F["body"],
                     width=w, relief="solid", bd=1).pack(side="left", padx=(0, 10))

        search_btn = self._make_raised_btn(filters_frame, "Поиск",
                                           command=self.search_nomenclature, icon="🔍")
        search_btn.pack(side="left", padx=(4, 0))

        # Action buttons
        btns_frame = tk.Frame(tb, bg=self.C["card_bg"])
        btns_frame.pack(side="right")

        b3 = self._make_raised_btn(btns_frame, "Удалить", bg_color=self.C["error"],
                                   command=self.delete_product, icon="🗑️")
        b3.pack(side="right", padx=(8, 0))
        b2 = self._make_raised_btn(btns_frame, "Редактировать",
                                   bg_color=self.C["primary_light"],
                                   command=self.edit_product, icon="✏️")
        b2.pack(side="right", padx=(8, 0))
        b1 = self._make_raised_btn(btns_frame, "Создать", bg_color=self.C["success"],
                                   command=self.create_product, icon="➕")
        b1.pack(side="right")

        tk.Frame(card, bg=self.C["divider"], height=1).pack(fill="x", padx=16)

        # Tree
        cols = ("id", "article", "brand", "supplier", "client", "unit",
                "volume", "weight", "barcode", "serial", "category",
                "subcategory", "owner")
        wmap = {
            "id": ("ID", 45), "article": ("Артикул", 85), "brand": ("Марка", 140),
            "supplier": ("Поставщик", 110), "client": ("Клиент", 110),
            "unit": ("Ед.", 45), "volume": ("Объём", 55), "weight": ("Вес", 55),
            "barcode": ("Штрих-код", 95), "serial": ("Серийный", 65),
            "category": ("Категория", 95), "subcategory": ("Подкатегория", 95),
            "owner": ("Владелец", 75),
        }
        self.nomenclature_tree, _ = self._make_tree(card, cols, wmap, height=18)
        self.nomenclature_tree.bind("<Double-1>", lambda e: self.edit_product())

        # Context menu
        self.nom_menu = tk.Menu(self, tearoff=0, font=self.F["body"])
        self.nom_menu.add_command(label="📋 Копировать", command=self._copy_value)
        self.nomenclature_tree.bind("<Button-3>", self._show_nom_menu)

    def _show_nom_menu(self, event):
        row = self.nomenclature_tree.identify_row(event.y)
        col = self.nomenclature_tree.identify_column(event.x)
        if row:
            self.nomenclature_tree.selection_set(row)
            vals = self.nomenclature_tree.item(row, "values")
            idx = int(col.replace("#", "")) - 1 if col else 0
            self.selected_copy_value = vals[idx] if 0 <= idx < len(vals) else ""
            self.nom_menu.tk_popup(event.x_root, event.y_root)

    # ==================== ПРИХОДЫ ====================

    def _build_inbound_page(self):
        page = self.pages["inbound"]

        card = self._make_card(page, padx=20, pady=16, expand=True)

        # Toolbar row 1 — filters
        tb1 = tk.Frame(card, bg=self.C["card_bg"])
        tb1.pack(fill="x", padx=16, pady=(12, 4))

        for lbl_text, var, w in [
            ("№ заказа", self.inb_search_var, 8),
            ("Создал", self.inb_created_by_var, 8),
            ("Принял", self.inb_accepted_by_var, 8),
            ("Поставщик", self.inb_supplier_var, 10),
            ("Клиент", self.inb_client_var, 10),
        ]:
            tk.Label(tb1, text=f"{lbl_text}:", font=self.F["small"],
                     bg=self.C["card_bg"], fg=self.C["text_secondary"]).pack(
                side="left", padx=(0, 3))
            tk.Entry(tb1, textvariable=var, font=self.F["body"],
                     width=w, relief="solid", bd=1).pack(side="left", padx=(0, 10))

        # Toolbar row 2 — status, dates, search button
        tb2 = tk.Frame(card, bg=self.C["card_bg"])
        tb2.pack(fill="x", padx=16, pady=(0, 4))

        tk.Label(tb2, text="Статус:", font=self.F["small"],
                 bg=self.C["card_bg"], fg=self.C["text_secondary"]).pack(
            side="left", padx=(0, 3))
        ttk.Combobox(tb2, textvariable=self.inb_status_var,
                     values=["Все", "Новый", "Принят"], state="readonly",
                     width=8, font=self.F["body"]).pack(side="left", padx=(0, 10))

        tk.Label(tb2, text="От:", font=self.F["small"],
                 bg=self.C["card_bg"], fg=self.C["text_secondary"]).pack(
            side="left", padx=(0, 3))
        tk.Entry(tb2, textvariable=self.inb_from_var, font=self.F["body"],
                 width=10, relief="solid", bd=1, state="readonly").pack(
            side="left", padx=(0, 3))
        date_btn1 = self._make_flat_btn(tb2, "", command=lambda: self._pick_date(self.inb_from_var),
                                        icon="📅")
        date_btn1.pack(side="left", padx=(0, 10))

        tk.Label(tb2, text="До:", font=self.F["small"],
                 bg=self.C["card_bg"], fg=self.C["text_secondary"]).pack(
            side="left", padx=(0, 3))
        tk.Entry(tb2, textvariable=self.inb_to_var, font=self.F["body"],
                 width=10, relief="solid", bd=1, state="readonly").pack(
            side="left", padx=(0, 3))
        date_btn2 = self._make_flat_btn(tb2, "", command=lambda: self._pick_date(self.inb_to_var),
                                        icon="📅")
        date_btn2.pack(side="left", padx=(0, 10))

        search_btn = self._make_raised_btn(tb2, "Поиск",
                                           command=self.search_inbound, icon="🔍")
        search_btn.pack(side="left", padx=(4, 0))

        # Toolbar row 3 — actions
        tb3 = tk.Frame(card, bg=self.C["card_bg"])
        tb3.pack(fill="x", padx=16, pady=(0, 4))

        b1 = self._make_raised_btn(tb3, "Создать заказ", bg_color=self.C["success"],
                                   command=self.create_inbound_order, icon="➕")
        b1.pack(side="left")

        tk.Label(tb3, text="💡 Двойной клик для открытия заказа",
                 font=self.F["small"], bg=self.C["card_bg"],
                 fg=self.C["text_hint"]).pack(side="right")

        tk.Frame(card, bg=self.C["divider"], height=1).pack(fill="x", padx=16)

        # Tree
        cols = ("order_number", "created_at", "received_at", "created_by",
                "accepted_by", "supplier", "client", "warehouse", "status",
                "planned", "actual")
        wmap = {
            "order_number": ("№ Заказа", 95), "created_at": ("Создан", 125),
            "received_at": ("Принят", 125), "created_by": ("Создал", 75),
            "accepted_by": ("Принял", 75), "supplier": ("Поставщик", 115),
            "client": ("Клиент", 115), "warehouse": ("Склад", 95),
            "status": ("Статус", 75), "planned": ("План", 65),
            "actual": ("Факт", 65),
        }
        self.inbound_tree, _ = self._make_tree(card, cols, wmap, height=16)
        self.inbound_tree.bind("<Double-1>", self._open_inbound_order)

    # ==================== ДВИЖЕНИЯ ====================

    def _build_movements_page(self):
        page = self.pages["movements"]

        # Input card
        input_card = self._make_card(page, padx=20, pady=(16, 8))

        inp_inner = tk.Frame(input_card, bg=self.C["card_bg"])
        inp_inner.pack(fill="x", padx=16, pady=12)

        tk.Label(inp_inner, text="Добавить движение", font=self.F["heading"],
                 bg=self.C["card_bg"], fg=self.C["text"]).pack(anchor="w", pady=(0, 10))

        row = tk.Frame(inp_inner, bg=self.C["card_bg"])
        row.pack(fill="x")

        tk.Label(row, text="Товар:", font=self.F["small"],
                 bg=self.C["card_bg"], fg=self.C["text_secondary"]).pack(
            side="left", padx=(0, 4))
        self.mov_product_box = ttk.Combobox(row, textvariable=self.mov_product_var,
                                            width=28, state="readonly",
                                            font=self.F["body"])
        self.mov_product_box.pack(side="left", padx=(0, 10))

        tk.Label(row, text="Тип:", font=self.F["small"],
                 bg=self.C["card_bg"], fg=self.C["text_secondary"]).pack(
            side="left", padx=(0, 4))
        ttk.Combobox(row, textvariable=self.mov_type_var, values=["IN", "OUT"],
                     width=5, state="readonly", font=self.F["body"]).pack(
            side="left", padx=(0, 10))

        tk.Label(row, text="Кол-во:", font=self.F["small"],
                 bg=self.C["card_bg"], fg=self.C["text_secondary"]).pack(
            side="left", padx=(0, 4))
        tk.Entry(row, textvariable=self.mov_qty_var, font=self.F["body"],
                 width=7, relief="solid", bd=1).pack(side="left", padx=(0, 10))

        tk.Label(row, text="Документ:", font=self.F["small"],
                 bg=self.C["card_bg"], fg=self.C["text_secondary"]).pack(
            side="left", padx=(0, 4))
        tk.Entry(row, textvariable=self.mov_ref_var, font=self.F["body"],
                 width=10, relief="solid", bd=1).pack(side="left", padx=(0, 10))

        tk.Label(row, text="Коммент.:", font=self.F["small"],
                 bg=self.C["card_bg"], fg=self.C["text_secondary"]).pack(
            side="left", padx=(0, 4))
        tk.Entry(row, textvariable=self.mov_note_var, font=self.F["body"],
                 width=12, relief="solid", bd=1).pack(side="left", padx=(0, 10))

        b_add = self._make_raised_btn(row, "Провести", bg_color=self.C["success"],
                                      command=self.add_movement, icon="✅")
        b_add.pack(side="left")

        # History card
        hist_card = self._make_card(page, padx=20, pady=(0, 16), expand=True)

        tk.Label(hist_card, text="  История движений", font=self.F["heading"],
                 bg=self.C["card_bg"], fg=self.C["text"]).pack(
            anchor="w", padx=16, pady=(12, 4))

        tk.Frame(hist_card, bg=self.C["divider"], height=1).pack(fill="x", padx=16)

        cols = ("id", "brand", "type", "qty", "reference", "date", "note")
        wmap = {
            "id": ("ID", 55), "brand": ("Товар", 240), "type": ("Тип", 55),
            "qty": ("Кол-во", 75), "reference": ("Документ", 115),
            "date": ("Дата", 135), "note": ("Комментарий", 190),
        }
        self.movements_tree, _ = self._make_tree(hist_card, cols, wmap, height=16)

    # ==================== ОСТАТКИ ====================

    def _build_stock_page(self):
        page = self.pages["stock"]

        card = self._make_card(page, padx=20, pady=16, expand=True)

        tb = self._make_toolbar(card)

        tk.Label(tb, text="📊 Текущие остатки", font=self.F["heading"],
                 bg=self.C["card_bg"], fg=self.C["text"]).pack(side="left")

        b_ref = self._make_raised_btn(tb, "Обновить", command=self.refresh_stock, icon="🔄")
        b_ref.pack(side="right")

        tk.Frame(card, bg=self.C["divider"], height=1).pack(fill="x", padx=16)

        cols = ("brand", "client", "unit", "stock")
        wmap = {
            "brand": ("Товар", 300), "client": ("3PL клиент", 200),
            "unit": ("Ед.изм.", 80), "stock": ("Остаток", 100),
        }
        self.stock_tree, _ = self._make_tree(card, cols, wmap, height=20)

    # ══════════════════════════════════════════════════
    #             ДИАЛОГИ — ПОСТАВЩИКИ
    # ══════════════════════════════════════════════════

    def _next_supplier_id(self):
        row = self.db.query("SELECT COALESCE(MAX(id),0)+1 FROM suppliers")
        return str(row[0][0])

    def create_supplier(self):
        self._supplier_dialog("create")

    def edit_supplier(self):
        sel = self.suppliers_tree.selection()
        if not sel:
            messagebox.showwarning("Внимание", "Выберите поставщика")
            return
        sid = int(self.suppliers_tree.item(sel[0], "values")[0])
        self._supplier_dialog("edit", sid)

    def _supplier_dialog(self, mode, supplier_id=None):
        dialog = self._create_dialog("Карточка поставщика", 500, 320)

        main = tk.Frame(dialog, bg=self.C["card_bg"])
        main.pack(fill="both", expand=True, padx=30, pady=25)

        title = "Создание поставщика" if mode == "create" else "Редактирование поставщика"
        tk.Label(main, text=title, font=self.F["heading"],
                 bg=self.C["card_bg"], fg=self.C["text"]).pack(anchor="w", pady=(0, 20))

        name_var = tk.StringVar()
        id_var = tk.StringVar(value=self._next_supplier_id())
        phone_var = tk.StringVar()
        created_var = tk.StringVar(value=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

        if mode == "edit" and supplier_id:
            row = self.db.query(
                "SELECT id, name, phone, created_at FROM suppliers WHERE id=?",
                (supplier_id,))
            if row:
                id_var.set(str(row[0][0]))
                name_var.set(row[0][1] or "")
                phone_var.set(row[0][2] or "")
                created_var.set(row[0][3] or "")

        form = tk.Frame(main, bg=self.C["card_bg"])
        form.pack(fill="x")

        fields = [
            ("Название *", name_var, False, 30),
            ("ID", id_var, True, 15),
            ("Телефон", phone_var, False, 20),
            ("Дата создания", created_var, True, 20),
        ]
        for i, (lbl, var, ro, w) in enumerate(fields):
            tk.Label(form, text=lbl, font=self.F["body"],
                     bg=self.C["card_bg"], fg=self.C["text_secondary"]).grid(
                row=i, column=0, sticky="w", pady=6)
            state = "readonly" if ro else "normal"
            tk.Entry(form, textvariable=var, font=self.F["body"],
                     width=w, state=state, relief="solid", bd=1).grid(
                row=i, column=1, sticky="w", pady=6, padx=(12, 0))

        def save():
            name = name_var.get().strip()
            if not name:
                messagebox.showwarning("Внимание", "Название обязательно", parent=dialog)
                return
            try:
                if mode == "create":
                    self.db.execute(
                        "INSERT INTO suppliers(name,phone,created_at) VALUES(?,?,?)",
                        (name, phone_var.get().strip(), created_var.get().strip()))
                else:
                    self.db.execute(
                        "UPDATE suppliers SET name=?, phone=? WHERE id=?",
                        (name, phone_var.get().strip(), supplier_id))
            except sqlite3.IntegrityError:
                messagebox.showerror("Ошибка", "Поставщик уже существует", parent=dialog)
                return
            self.refresh_all()
            dialog.destroy()

        btns = tk.Frame(main, bg=self.C["card_bg"])
        btns.pack(fill="x", pady=(20, 0))

        bc = self._make_flat_btn(btns, "Отмена", color=self.C["text_secondary"],
                                 command=dialog.destroy, icon="✖")
        bc.pack(side="left")
        bs = self._make_raised_btn(btns, "Сохранить", bg_color=self.C["success"],
                                   command=save, icon="✅")
        bs.pack(side="right")

    def delete_supplier(self):
        sel = self.suppliers_tree.selection()
        if not sel:
            messagebox.showwarning("Внимание", "Выберите поставщика")
            return
        vals = self.suppliers_tree.item(sel[0], "values")
        if messagebox.askyesno("Удаление", f"Удалить «{vals[1]}»?"):
            self.db.execute("DELETE FROM suppliers WHERE id=?", (int(vals[0]),))
            self.refresh_all()

    # ══════════════════════════════════════════════════
    #             ДИАЛОГИ — КЛИЕНТЫ
    # ══════════════════════════════════════════════════

    def _next_client_code(self):
        row = self.db.query(
            "SELECT code FROM clients WHERE code LIKE 'C%' ORDER BY id DESC LIMIT 1")
        if not row or not row[0][0]:
            return "C00001"
        try:
            return f"C{int(row[0][0].replace('C', '')) + 1:05d}"
        except Exception:
            cnt = self.db.query("SELECT COUNT(*) FROM clients")[0][0] + 1
            return f"C{cnt:05d}"

    def create_client(self):
        self._client_dialog("create")

    def edit_client(self):
        sel = self.clients_tree.selection()
        if not sel:
            messagebox.showwarning("Внимание", "Выберите клиента")
            return
        cid = int(self.clients_tree.item(sel[0], "values")[0])
        self._client_dialog("edit", cid)

    def _client_dialog(self, mode, client_id=None):
        dialog = self._create_dialog("Карточка 3PL клиента", 500, 320)

        main = tk.Frame(dialog, bg=self.C["card_bg"])
        main.pack(fill="both", expand=True, padx=30, pady=25)

        title = "Создание клиента" if mode == "create" else "Редактирование клиента"
        tk.Label(main, text=title, font=self.F["heading"],
                 bg=self.C["card_bg"], fg=self.C["text"]).pack(anchor="w", pady=(0, 20))

        name_var = tk.StringVar()
        id_var = tk.StringVar(
            value=str(self.db.query("SELECT COALESCE(MAX(id),0)+1 FROM clients")[0][0]))
        contact_var = tk.StringVar()
        created_var = tk.StringVar(value=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

        if mode == "edit" and client_id:
            row = self.db.query(
                "SELECT id, name, contact, created_at FROM clients WHERE id=?",
                (client_id,))
            if row:
                id_var.set(str(row[0][0]))
                name_var.set(row[0][1] or "")
                contact_var.set(row[0][2] or "")
                created_var.set(row[0][3] or "")

        form = tk.Frame(main, bg=self.C["card_bg"])
        form.pack(fill="x")

        fields = [
            ("Название *", name_var, False, 30),
            ("ID", id_var, True, 15),
            ("Контакт", contact_var, False, 20),
            ("Дата создания", created_var, True, 20),
        ]
        for i, (lbl, var, ro, w) in enumerate(fields):
            tk.Label(form, text=lbl, font=self.F["body"],
                     bg=self.C["card_bg"], fg=self.C["text_secondary"]).grid(
                row=i, column=0, sticky="w", pady=6)
            state = "readonly" if ro else "normal"
            tk.Entry(form, textvariable=var, font=self.F["body"],
                     width=w, state=state, relief="solid", bd=1).grid(
                row=i, column=1, sticky="w", pady=6, padx=(12, 0))

        def save():
            name = name_var.get().strip()
            if not name:
                messagebox.showwarning("Внимание", "Название обязательно", parent=dialog)
                return
            try:
                if mode == "create":
                    self.db.execute(
                        "INSERT INTO clients(code,name,contact,created_at) VALUES(?,?,?,?)",
                        (self._next_client_code(), name,
                         contact_var.get().strip(), created_var.get().strip()))
                else:
                    self.db.execute(
                        "UPDATE clients SET name=?, contact=? WHERE id=?",
                        (name, contact_var.get().strip(), client_id))
            except sqlite3.IntegrityError:
                messagebox.showerror("Ошибка", "Клиент уже существует", parent=dialog)
                return
            self.refresh_all()
            dialog.destroy()

        btns = tk.Frame(main, bg=self.C["card_bg"])
        btns.pack(fill="x", pady=(20, 0))

        bc = self._make_flat_btn(btns, "Отмена", color=self.C["text_secondary"],
                                 command=dialog.destroy, icon="✖")
        bc.pack(side="left")
        bs = self._make_raised_btn(btns, "Сохранить", bg_color=self.C["success"],
                                   command=save, icon="✅")
        bs.pack(side="right")

    def delete_client(self):
        sel = self.clients_tree.selection()
        if not sel:
            messagebox.showwarning("Внимание", "Выберите клиента")
            return
        vals = self.clients_tree.item(sel[0], "values")
        if messagebox.askyesno("Удаление", f"Удалить «{vals[1]}»?"):
            self.db.execute("DELETE FROM clients WHERE id=?", (int(vals[0]),))
            self.refresh_all()

    # ══════════════════════════════════════════════════
    #             ДИАЛОГИ — КАТЕГОРИИ
    # ══════════════════════════════════════════════════

    def create_category(self):
        name = self.new_category_var.get().strip()
        if not name:
            messagebox.showwarning("Внимание", "Введите название")
            return
        try:
            self.db.execute("INSERT INTO categories(name) VALUES(?)", (name,))
        except sqlite3.IntegrityError:
            messagebox.showerror("Ошибка", "Категория уже существует")
            return
        self.new_category_var.set("")
        self.refresh_all()

    def create_subcategory(self):
        name = self.new_subcategory_var.get().strip()
        cat = self.subcategory_parent_var.get().strip()
        if not name or not cat:
            messagebox.showwarning("Внимание", "Укажите название и категорию")
            return
        cat_id = self._get_id(cat)
        try:
            self.db.execute(
                "INSERT INTO subcategories(category_id,name) VALUES(?,?)", (cat_id, name))
        except sqlite3.IntegrityError:
            messagebox.showerror("Ошибка", "Подкатегория уже существует")
            return
        self.new_subcategory_var.set("")
        self.refresh_all()

        # ══════════════════════════════════════════════════
    #             ДИАЛОГИ — НОМЕНКЛАТУРА
    # ══════════════════════════════════════════════════

    def _next_article(self):
        row = self.db.query(
            "SELECT article FROM products WHERE article LIKE 'ART-%' ORDER BY id DESC LIMIT 1")
        if not row or not row[0][0]:
            return "ART-00001"
        try:
            nxt = int(str(row[0][0]).split('-')[-1]) + 1
        except Exception:
            nxt = self.db.query("SELECT COUNT(*) FROM products")[0][0] + 1
        return f"ART-{nxt:05d}"

    def search_nomenclature(self):
        self.nom_searched = True
        self.refresh_nomenclature()

    def create_product(self):
        self._product_dialog("create")

    def edit_product(self):
        sel = self.nomenclature_tree.selection()
        if not sel:
            messagebox.showwarning("Внимание", "Выберите товар")
            return
        pid = int(self.nomenclature_tree.item(sel[0], "values")[0])
        self._product_dialog("edit", pid)

    def _product_dialog(self, mode, product_id=None):
        dialog = self._create_fullscreen_dialog("Карточка товара", 720, 620)

        main = tk.Frame(dialog, bg=self.C["card_bg"])
        main.pack(fill="both", expand=True, padx=30, pady=25)

        title = "Создание карточки товара" if mode == "create" else "Редактирование"
        tk.Label(main, text=title, font=self.F["heading"],
                 bg=self.C["card_bg"], fg=self.C["text"]).pack(anchor="w", pady=(0, 18))

        brand_var = tk.StringVar()
        article_var = tk.StringVar(value=self._next_article())
        supplier_var = tk.StringVar()
        client_var = tk.StringVar()
        unit_var = tk.StringVar(value="Шт")
        volume_var = tk.StringVar()
        weight_var = tk.StringVar()
        barcode_var = tk.StringVar()
        serial_var = tk.StringVar(value="Нет")
        category_var = tk.StringVar()
        subcategory_var = tk.StringVar()
        owner_var = tk.StringVar(value=self.current_user)

        suppliers = self._load_dict("suppliers")
        clients = self._load_dict("clients")
        categories = self._load_dict("categories")

        # Two columns
        cols_frame = tk.Frame(main, bg=self.C["card_bg"])
        cols_frame.pack(fill="both", expand=True)

        left = tk.Frame(cols_frame, bg=self.C["card_bg"])
        left.pack(side="left", fill="both", expand=True, padx=(0, 15))

        right = tk.Frame(cols_frame, bg=self.C["card_bg"])
        right.pack(side="left", fill="both", expand=True)

        def add_field(parent, label_text, widget_or_var, readonly=False,
                      is_combo=False, values=None, width=28):
            frame = tk.Frame(parent, bg=self.C["card_bg"])
            frame.pack(fill="x", pady=5)
            tk.Label(frame, text=label_text, font=self.F["small"],
                     bg=self.C["card_bg"], fg=self.C["text_secondary"]).pack(anchor="w")
            if is_combo:
                combo = ttk.Combobox(frame, textvariable=widget_or_var,
                                     values=values or [], state="readonly",
                                     width=width, font=self.F["body"])
                combo.pack(fill="x", pady=(2, 0))
                return combo
            else:
                state = "readonly" if readonly else "normal"
                ent = tk.Entry(frame, textvariable=widget_or_var, font=self.F["body"],
                               width=width, state=state, relief="solid", bd=1)
                ent.pack(fill="x", pady=(2, 0))
                return ent

        # Left column fields
        add_field(left, "Артикул", article_var, readonly=True)
        add_field(left, "Марка / Название *", brand_var)
        add_field(left, "Поставщик *", supplier_var, is_combo=True,
                  values=list(suppliers.keys()))
        add_field(left, "3PL клиент *", client_var, is_combo=True,
                  values=list(clients.keys()))
        add_field(left, "Единица измерения", unit_var, is_combo=True,
                  values=["Шт", "Палета"])
        add_field(left, "Серийный учёт", serial_var, is_combo=True,
                  values=["Да", "Нет"])

        # Right column fields
        category_combo = add_field(right, "Категория *", category_var, is_combo=True,
                                   values=list(categories.keys()))
        subcategory_combo = add_field(right, "Подкатегория *", subcategory_var,
                                      is_combo=True, values=[])
        add_field(right, "Объём (м³)", volume_var)
        add_field(right, "Вес (кг)", weight_var)
        add_field(right, "Штрих-код", barcode_var)
        add_field(right, "Владелец", owner_var, readonly=True)

        def load_subcategories(*_):
            cat = category_var.get().strip()
            if not cat:
                subcategory_combo["values"] = []
                return
            cat_id = self._get_id(cat)
            rows = self.db.query(
                "SELECT id, name FROM subcategories WHERE category_id=? ORDER BY name",
                (cat_id,))
            vals = [f"{r[0]} | {r[1]}" for r in rows]
            subcategory_combo["values"] = vals
            if vals:
                subcategory_var.set(vals[0])

        category_var.trace_add("write", load_subcategories)

        # Load data for edit mode
        if mode == "edit" and product_id:
            row = self.db.query(
                """SELECT article, brand, supplier_id, client_id, unit, volume, weight,
                          barcode, serial_tracking, category_id, subcategory_id, product_owner
                   FROM products WHERE id=?""", (product_id,))
            if row:
                r = row[0]
                article_var.set(r[0] or "")
                brand_var.set(r[1] or "")
                supplier_var.set(
                    next((k for k, v in suppliers.items() if v == r[2]), ""))
                client_var.set(
                    next((k for k, v in clients.items() if v == r[3]), ""))
                unit_var.set(r[4] or "Шт")
                volume_var.set("" if r[5] is None else str(r[5]))
                weight_var.set("" if r[6] is None else str(r[6]))
                barcode_var.set(r[7] or "")
                serial_var.set(r[8] or "Нет")
                category_var.set(
                    next((k for k, v in categories.items() if v == r[9]), ""))
                load_subcategories()
                subcats = {f"{x[0]} | {x[1]}": x[0]
                           for x in self.db.query("SELECT id, name FROM subcategories")}
                subcategory_var.set(
                    next((k for k, v in subcats.items() if v == r[10]), ""))
                owner_var.set(r[11] or self.current_user)
        else:
            load_subcategories()

        def save():
            brand = brand_var.get().strip()
            if not brand:
                messagebox.showwarning("Внимание", "Марка обязательна", parent=dialog)
                return

            s_id = self._get_id(supplier_var.get())
            c_id = self._get_id(client_var.get())
            if not s_id or not c_id:
                messagebox.showwarning("Внимание",
                                       "Укажите поставщика и клиента", parent=dialog)
                return

            cat_id = self._get_id(category_var.get())
            sub_id = self._get_id(subcategory_var.get())
            if not cat_id or not sub_id:
                messagebox.showwarning("Внимание",
                                       "Выберите категорию и подкатегорию", parent=dialog)
                return

            try:
                volume = self._normalize_decimal(volume_var.get())
                weight = self._normalize_decimal(weight_var.get())
            except ValueError:
                messagebox.showwarning("Внимание",
                                       "Некорректный формат числа", parent=dialog)
                return

            check = self.db.query(
                "SELECT id FROM subcategories WHERE id=? AND category_id=?",
                (sub_id, cat_id))
            if not check:
                messagebox.showwarning("Внимание",
                                       "Подкатегория не соответствует категории",
                                       parent=dialog)
                return

            try:
                if mode == "create":
                    self.db.execute(
                        """INSERT INTO products(name, article, brand, supplier_id,
                           client_id, unit, volume, weight, barcode, serial_tracking,
                           category_id, subcategory_id, product_owner, created_at)
                           VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                        (brand, article_var.get().strip(), brand, s_id, c_id,
                         unit_var.get().strip(), volume, weight,
                         barcode_var.get().strip(), serial_var.get().strip(),
                         cat_id, sub_id, owner_var.get().strip(),
                         datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
                else:
                    self.db.execute(
                        """UPDATE products SET name=?, article=?, brand=?, supplier_id=?,
                           client_id=?, unit=?, volume=?, weight=?, barcode=?,
                           serial_tracking=?, category_id=?, subcategory_id=?,
                           product_owner=? WHERE id=?""",
                        (brand, article_var.get().strip(), brand, s_id, c_id,
                         unit_var.get().strip(), volume, weight,
                         barcode_var.get().strip(), serial_var.get().strip(),
                         cat_id, sub_id, owner_var.get().strip(), product_id))
            except sqlite3.IntegrityError as e:
                messagebox.showerror("Ошибка", f"Ошибка сохранения: {e}",
                                     parent=dialog)
                return

            self.refresh_all()
            dialog.destroy()

        btns = tk.Frame(main, bg=self.C["card_bg"])
        btns.pack(fill="x", pady=(18, 0))

        bc = self._make_flat_btn(btns, "Отмена", color=self.C["text_secondary"],
                                 command=dialog.destroy, icon="✖")
        bc.pack(side="left")
        bs = self._make_raised_btn(btns, "Сохранить", bg_color=self.C["success"],
                                   command=save, icon="✅")
        bs.pack(side="right")

    def delete_product(self):
        sel = self.nomenclature_tree.selection()
        if not sel:
            messagebox.showwarning("Внимание", "Выберите товар")
            return
        vals = self.nomenclature_tree.item(sel[0], "values")
        name = vals[2] or vals[1] or f"ID: {vals[0]}"
        if messagebox.askyesno("Удаление", f"Удалить «{name}»?"):
            self.db.execute("DELETE FROM products WHERE id=?", (int(vals[0]),))
            self.refresh_all()

    # ══════════════════════════════════════════════════
    #             ДИАЛОГИ — ПРИХОДЫ
    # ══════════════════════════════════════════════════

    def search_inbound(self):
        self.inb_searched = True
        self.refresh_inbound()

    def _next_order_number(self):
        row = self.db.query(
            "SELECT order_number FROM inbound_orders ORDER BY id DESC LIMIT 1")
        if not row or not row[0][0]:
            return "IN-00001"
        try:
            nxt = int(row[0][0].split("-")[-1]) + 1
        except Exception:
            nxt = self.db.query("SELECT COUNT(*) FROM inbound_orders")[0][0] + 1
        return f"IN-{nxt:05d}"

    def create_inbound_order(self):
        dialog = self._create_fullscreen_dialog("Создание приходного заказа", 1150, 720)

        main = tk.Frame(dialog, bg=self.C["card_bg"])
        main.pack(fill="both", expand=True, padx=25, pady=20)

        tk.Label(main, text="Новый приходный заказ", font=self.F["heading"],
                 bg=self.C["card_bg"], fg=self.C["text"]).pack(anchor="w", pady=(0, 12))

        order_num_var = tk.StringVar(value=self._next_order_number())
        created_var = tk.StringVar(value=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        status_var = tk.StringVar(value="Новый")
        created_by_var = tk.StringVar(value=self.current_user)
        supplier_var = tk.StringVar()
        client_var = tk.StringVar()
        warehouse_var = tk.StringVar()

        suppliers = self._load_dict("suppliers")
        clients = self._load_dict("clients")
        warehouses = self._load_dict("warehouses")
        categories = self._load_dict("categories")

        # Header form
        hdr = tk.Frame(main, bg=self.C["card_bg"])
        hdr.pack(fill="x", pady=(0, 8))

        r1 = tk.Frame(hdr, bg=self.C["card_bg"])
        r1.pack(fill="x", pady=3)

        for lbl_text, var, w, ro in [
            ("№ заказа", order_num_var, 10, True),
            ("Дата", created_var, 16, True),
            ("Статус", status_var, 8, True),
            ("Создал", created_by_var, 10, True),
        ]:
            tk.Label(r1, text=f"{lbl_text}:", font=self.F["small"],
                     bg=self.C["card_bg"], fg=self.C["text_secondary"]).pack(
                side="left", padx=(0, 3))
            tk.Entry(r1, textvariable=var, font=self.F["body"], width=w,
                     state="readonly" if ro else "normal",
                     relief="solid", bd=1).pack(side="left", padx=(0, 12))

        r2 = tk.Frame(hdr, bg=self.C["card_bg"])
        r2.pack(fill="x", pady=3)

        for lbl_text, var, vals, w in [
            ("Поставщик *", supplier_var, list(suppliers.keys()), 22),
            ("3PL клиент *", client_var, list(clients.keys()), 22),
            ("Склад *", warehouse_var, list(warehouses.keys()), 18),
        ]:
            tk.Label(r2, text=f"{lbl_text}:", font=self.F["small"],
                     bg=self.C["card_bg"], fg=self.C["text_secondary"]).pack(
                side="left", padx=(0, 3))
            ttk.Combobox(r2, textvariable=var, values=vals, state="readonly",
                         width=w, font=self.F["body"]).pack(
                side="left", padx=(0, 12))

        tk.Frame(main, bg=self.C["divider"], height=1).pack(fill="x", pady=8)

        # Add items section
        tk.Label(main, text="Добавление позиций", font=self.F["body_bold"],
                 bg=self.C["card_bg"], fg=self.C["text"]).pack(anchor="w", pady=(0, 6))

        line_cat_var = tk.StringVar()
        line_sub_var = tk.StringVar()
        line_prod_var = tk.StringVar()
        line_qty_var = tk.StringVar(value="1")
        line_unit_var = tk.StringVar()

        add_row = tk.Frame(main, bg=self.C["card_bg"])
        add_row.pack(fill="x", pady=(0, 8))

        tk.Label(add_row, text="Категория:", font=self.F["small"],
                 bg=self.C["card_bg"], fg=self.C["text_secondary"]).pack(
            side="left", padx=(0, 3))
        line_cat_box = ttk.Combobox(add_row, textvariable=line_cat_var,
                                    values=list(categories.keys()),
                                    state="readonly", width=16, font=self.F["body"])
        line_cat_box.pack(side="left", padx=(0, 8))

        tk.Label(add_row, text="Подкатег.:", font=self.F["small"],
                 bg=self.C["card_bg"], fg=self.C["text_secondary"]).pack(
            side="left", padx=(0, 3))
        line_sub_box = ttk.Combobox(add_row, textvariable=line_sub_var,
                                    state="readonly", width=16, font=self.F["body"])
        line_sub_box.pack(side="left", padx=(0, 8))

        tk.Label(add_row, text="Товар:", font=self.F["small"],
                 bg=self.C["card_bg"], fg=self.C["text_secondary"]).pack(
            side="left", padx=(0, 3))
        line_prod_box = ttk.Combobox(add_row, textvariable=line_prod_var,
                                     state="readonly", width=20, font=self.F["body"])
        line_prod_box.pack(side="left", padx=(0, 8))

        tk.Label(add_row, text="Кол-во:", font=self.F["small"],
                 bg=self.C["card_bg"], fg=self.C["text_secondary"]).pack(
            side="left", padx=(0, 3))
        tk.Entry(add_row, textvariable=line_qty_var, font=self.F["body"],
                 width=6, relief="solid", bd=1).pack(side="left", padx=(0, 3))
        tk.Label(add_row, textvariable=line_unit_var, font=self.F["small"],
                 bg=self.C["card_bg"], fg=self.C["text_hint"]).pack(
            side="left", padx=(0, 8))

        add_btn = self._make_raised_btn(add_row, "Добавить",
                                        bg_color=self.C["primary"],
                                        command=lambda: add_line(), icon="➕")
        add_btn.pack(side="left")

        # Items storage
        order_items = []
        product_catalog = {}

        # Items tree
        tree_frame = tk.Frame(main, bg=self.C["card_bg"])
        tree_frame.pack(fill="both", expand=True, pady=(0, 8))

        it_cols = ("category", "subcategory", "article", "product",
                   "unit", "qty", "weight", "volume")
        it_wmap = {
            "category": ("Категория", 125), "subcategory": ("Подкатегория", 135),
            "article": ("Артикул", 95), "product": ("Товар", 170),
            "unit": ("Ед.", 50), "qty": ("Кол-во", 70),
            "weight": ("Вес", 75), "volume": ("Объём", 75),
        }
        items_tree, _ = self._make_tree(tree_frame, it_cols, it_wmap, height=10)

        def refresh_subcats(*_):
            cat = line_cat_var.get().strip()
            if not cat:
                line_sub_box["values"] = []
                line_prod_box["values"] = []
                return
            cat_id = self._get_id(cat)
            rows = self.db.query(
                "SELECT id, name FROM subcategories WHERE category_id=? ORDER BY name",
                (cat_id,))
            vals = [f"{r[0]} | {r[1]}" for r in rows]
            line_sub_box["values"] = vals
            line_sub_var.set(vals[0] if vals else "")
            refresh_products()

        def refresh_products(*_):
            sub = line_sub_var.get().strip()
            cat = line_cat_var.get().strip()
            if not sub or not cat:
                line_prod_box["values"] = []
                return
            sub_id = self._get_id(sub)
            cat_id = self._get_id(cat)
            rows = self.db.query(
                """SELECT id, article, brand, unit, COALESCE(weight,0),
                          COALESCE(volume,0)
                   FROM products WHERE subcategory_id=? AND category_id=?
                   ORDER BY brand""", (sub_id, cat_id))
            product_catalog.clear()
            vals = []
            for pid, art, brand, unit, wt, vol in rows:
                token = f"{pid} | {art or ''} | {brand or ''}"
                vals.append(token)
                product_catalog[token] = {
                    "id": pid, "article": art or "", "brand": brand or "",
                    "unit": unit or "Шт", "weight": float(wt), "volume": float(vol)}
            line_prod_box["values"] = vals
            if vals:
                line_prod_var.set(vals[0])
                update_unit()

        def update_unit(*_):
            info = product_catalog.get(line_prod_var.get().strip())
            line_unit_var.set(info["unit"] if info else "")

        line_cat_var.trace_add("write", refresh_subcats)
        line_sub_var.trace_add("write", refresh_products)
        line_prod_var.trace_add("write", update_unit)

        def add_line():
            cat = line_cat_var.get().strip()
            sub = line_sub_var.get().strip()
            prod = line_prod_var.get().strip()
            if not cat or not sub or not prod:
                messagebox.showwarning("Внимание",
                                       "Выберите категорию, подкатегорию и товар",
                                       parent=dialog)
                return
            try:
                qty = float(line_qty_var.get())
                if qty <= 0:
                    raise ValueError
            except ValueError:
                messagebox.showwarning("Внимание", "Некорректное количество",
                                       parent=dialog)
                return

            info = product_catalog.get(prod)
            if not info:
                return

            item = {
                "category_id": self._get_id(cat),
                "subcategory_id": self._get_id(sub),
                "product_id": info["id"],
                "category_name": cat.split(" | ", 1)[1],
                "subcategory_name": sub.split(" | ", 1)[1],
                "article": info["article"],
                "brand": info["brand"],
                "unit": info["unit"],
                "planned_qty": qty,
                "planned_weight": qty * info["weight"],
                "planned_volume": qty * info["volume"],
            }
            order_items.append(item)
            items_tree.insert("", "end", values=(
                item["category_name"], item["subcategory_name"],
                item["article"], item["brand"], item["unit"],
                f"{qty:.2f}", f"{item['planned_weight']:.3f}",
                f"{item['planned_volume']:.4f}"))

        def save_order():
            sup_id = self._get_id(supplier_var.get())
            cli_id = self._get_id(client_var.get())
            wh_id = self._get_id(warehouse_var.get())
            if not sup_id or not cli_id or not wh_id:
                messagebox.showwarning("Внимание",
                                       "Заполните поставщика, клиента и склад",
                                       parent=dialog)
                return
            if not order_items:
                messagebox.showwarning("Внимание", "Добавьте позиции", parent=dialog)
                return
            try:
                oid = self.db.execute(
                    """INSERT INTO inbound_orders(order_number, created_at,
                       received_at, created_by, supplier_id, client_id,
                       warehouse_id, status) VALUES(?,?,NULL,?,?,?,?,?)""",
                    (order_num_var.get().strip(), created_var.get().strip(),
                     created_by_var.get().strip(), sup_id, cli_id, wh_id, "Новый"))
                for it in order_items:
                    self.db.execute(
                        """INSERT INTO inbound_order_items(order_id, category_id,
                           subcategory_id, product_id, planned_qty, actual_qty,
                           actual_filled, planned_weight, planned_volume,
                           serial_numbers) VALUES(?,?,?,?,?,0,0,?,?,'')""",
                        (oid, it["category_id"], it["subcategory_id"],
                         it["product_id"], it["planned_qty"],
                         it["planned_weight"], it["planned_volume"]))
            except sqlite3.IntegrityError as e:
                messagebox.showerror("Ошибка", f"Ошибка: {e}", parent=dialog)
                return
            self.refresh_all()
            dialog.destroy()
            messagebox.showinfo("Успешно", f"Заказ {order_num_var.get()} создан")

        btns = tk.Frame(main, bg=self.C["card_bg"])
        btns.pack(fill="x")

        bc = self._make_flat_btn(btns, "Отмена", color=self.C["text_secondary"],
                                 command=dialog.destroy, icon="✖")
        bc.pack(side="left")
        bs = self._make_raised_btn(btns, "Сохранить заказ",
                                   bg_color=self.C["success"],
                                   command=save_order, icon="✅")
        bs.pack(side="right")

        # ──────────── Открытие / Приёмка заказа ────────────

    def _open_inbound_order(self, event=None):
        sel = self.inbound_tree.selection()
        if not sel:
            return
        order_num = self.inbound_tree.item(sel[0], "values")[0]
        self._inbound_order_dialog(order_num)

    def _inbound_order_dialog(self, order_number):
        rows = self.db.query(
            """SELECT o.id, o.order_number, o.created_at,
                      COALESCE(o.received_at,''), s.name, c.name, w.name,
                      o.status, o.created_by, COALESCE(o.accepted_by,'')
               FROM inbound_orders o
               JOIN suppliers s ON s.id=o.supplier_id
               JOIN clients c ON c.id=o.client_id
               JOIN warehouses w ON w.id=o.warehouse_id
               WHERE o.order_number=?""", (order_number,))
        if not rows:
            messagebox.showerror("Ошибка", "Заказ не найден")
            return

        (order_id, order_no, created_at, received_at, supplier, client,
         warehouse, status, created_by, accepted_by) = rows[0]

        dialog = self._create_fullscreen_dialog(
            f"Приёмка заказа {order_no}", 1250, 720)

        main = tk.Frame(dialog, bg=self.C["card_bg"])
        main.pack(fill="both", expand=True, padx=25, pady=20)

        # ── Title row with status badge ──
        title_row = tk.Frame(main, bg=self.C["card_bg"])
        title_row.pack(fill="x", pady=(0, 10))

        tk.Label(title_row, text=f"Заказ {order_no}", font=self.F["heading"],
                 bg=self.C["card_bg"], fg=self.C["text"]).pack(side="left")

        status_colors = {"Новый": self.C["warning"], "Принят": self.C["success"]}
        st_bg = status_colors.get(status, self.C["text_hint"])
        tk.Label(title_row, text=f"  {status}  ", font=self.F["small_bold"],
                 bg=st_bg, fg="white", padx=8, pady=2).pack(side="right")

        # ── Info grid ──
        info_frame = tk.Frame(main, bg=self.C["card_bg"])
        info_frame.pack(fill="x", pady=(0, 8))

        info_data = [
            ("№ заказа:", order_no), ("Создан:", created_at),
            ("Поставщик:", supplier), ("Клиент:", client),
            ("Склад:", warehouse), ("Создал:", created_by),
            ("Принял:", accepted_by or "—"), ("Дата приёма:", received_at or "—"),
        ]
        for i, (lbl, val) in enumerate(info_data):
            r, c = i // 4, (i % 4) * 2
            tk.Label(info_frame, text=lbl, font=self.F["small"],
                     bg=self.C["card_bg"], fg=self.C["text_secondary"]).grid(
                row=r, column=c, sticky="w",
                padx=(0 if c == 0 else 15, 4), pady=2)
            tk.Label(info_frame, text=val, font=self.F["body_bold"],
                     bg=self.C["card_bg"], fg=self.C["text"]).grid(
                row=r, column=c + 1, sticky="w", pady=2)

        tk.Frame(main, bg=self.C["divider"], height=1).pack(fill="x", pady=8)

        # ── Action toolbar ──
        toolbar = tk.Frame(main, bg=self.C["card_bg"])
        toolbar.pack(fill="x", pady=(0, 8))

        # ── Items tree ──
        tree_frame = tk.Frame(main, bg=self.C["card_bg"])
        tree_frame.pack(fill="both", expand=True)

        it_cols = ("line_id", "product", "article", "category", "subcategory",
                   "unit", "planned", "actual", "weight", "volume", "barcode",
                   "serials", "serial_tracking")
        it_wmap = {
            "line_id": ("ID", 40), "product": ("Товар", 170),
            "article": ("Артикул", 85), "category": ("Категория", 105),
            "subcategory": ("Подкатегория", 115), "unit": ("Ед.", 45),
            "planned": ("План", 65), "actual": ("Факт", 65),
            "weight": ("Вес", 65), "volume": ("Объём", 65),
            "barcode": ("Штрих-код", 95), "serials": ("Серий", 55),
            "serial_tracking": ("Серийный", 65),
        }
        items_tree, _ = self._make_tree(tree_frame, it_cols, it_wmap, height=14)

        # ── Helper: serial count ──
        def serial_count(s):
            return str(len([x for x in (s or "").split(",") if x.strip()]))

        def get_tag(planned, actual, filled):
            if filled == 0:
                return "empty"
            if actual < planned:
                return "less"
            if actual > planned:
                return "more"
            return "ok"

        # ── Load items into tree ──
        def load_items():
            for child in items_tree.get_children():
                items_tree.delete(child)

            item_rows = self.db.query(
                """SELECT i.id, COALESCE(p.name, p.brand, ''),
                          COALESCE(p.article,''),
                          COALESCE(cat.name,''), COALESCE(sub.name,''),
                          COALESCE(p.unit,''), i.planned_qty, i.actual_qty,
                          COALESCE(p.weight,0)*i.actual_qty,
                          COALESCE(p.volume,0)*i.actual_qty,
                          COALESCE(p.barcode,''),
                          COALESCE(i.serial_numbers,''),
                          COALESCE(p.serial_tracking,'Нет'),
                          i.actual_filled
                   FROM inbound_order_items i
                   JOIN products p ON p.id=i.product_id
                   LEFT JOIN categories cat ON cat.id=i.category_id
                   LEFT JOIN subcategories sub ON sub.id=i.subcategory_id
                   WHERE i.order_id=? ORDER BY i.id""", (order_id,))

            for r in item_rows:
                tag = get_tag(float(r[6]), float(r[7]), int(r[13]))
                items_tree.insert("", "end", values=(
                    r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7],
                    f"{r[8]:.2f}", f"{r[9]:.3f}", r[10],
                    serial_count(r[11]), r[12]
                ), tags=(tag,))

        # ── Set actual qty helper ──
        def set_actual(line_id, qty, serials=None):
            if serials is None:
                self.db.execute(
                    "UPDATE inbound_order_items SET actual_qty=?, actual_filled=1 WHERE id=?",
                    (qty, line_id))
            else:
                self.db.execute(
                    """UPDATE inbound_order_items SET actual_qty=?,
                       actual_filled=1, serial_numbers=? WHERE id=?""",
                    (qty, serials, line_id))

        # ── Serial numbers dialog ──
        def edit_serial(line_id, product_name, current):
            ser_dialog = self._create_dialog(
                f"Серийные номера: {product_name}", 550, 470)

            ser_main = tk.Frame(ser_dialog, bg=self.C["card_bg"])
            ser_main.pack(fill="both", expand=True, padx=20, pady=20)

            tk.Label(ser_main, text=f"Товар: {product_name}",
                     font=self.F["heading"], bg=self.C["card_bg"],
                     fg=self.C["text"]).pack(anchor="w", pady=(0, 12))

            # Input row
            inp_frame = tk.Frame(ser_main, bg=self.C["card_bg"])
            inp_frame.pack(fill="x", pady=(0, 10))

            tk.Label(inp_frame, text="Серийный номер:",
                     font=self.F["body"], bg=self.C["card_bg"],
                     fg=self.C["text_secondary"]).pack(side="left", padx=(0, 8))
            ser_input = tk.StringVar()
            ser_entry = tk.Entry(inp_frame, textvariable=ser_input,
                                 font=self.F["body"], width=22,
                                 relief="solid", bd=1)
            ser_entry.pack(side="left", padx=(0, 8))
            ser_entry.focus_set()

            def add_ser(e=None):
                val = ser_input.get().strip()
                if not val:
                    return
                existing = [listbox.get(i) for i in range(listbox.size())]
                if val in existing:
                    messagebox.showwarning("Внимание", "Уже добавлено",
                                           parent=ser_dialog)
                    return
                listbox.insert("end", val)
                ser_input.set("")
                ser_entry.focus_set()
                refresh_cnt()

            add_ser_btn = self._make_raised_btn(
                inp_frame, "Добавить", bg_color=self.C["primary"],
                command=add_ser, icon="➕")
            add_ser_btn.pack(side="left")

            ser_entry.bind("<Return>", add_ser)

            # Listbox
            serials = [x.strip() for x in (current or "").split(",") if x.strip()]

            list_frame = tk.Frame(ser_main, bg=self.C["card_bg"])
            list_frame.pack(fill="both", expand=True, pady=8)

            listbox = tk.Listbox(list_frame, font=self.F["body"], height=10,
                                 relief="solid", bd=1,
                                 selectbackground=self.C["selected"],
                                 selectforeground=self.C["text"])
            list_vsb = ttk.Scrollbar(list_frame, orient="vertical",
                                     command=listbox.yview)
            listbox.configure(yscrollcommand=list_vsb.set)
            listbox.pack(side="left", fill="both", expand=True)
            list_vsb.pack(side="right", fill="y")

            for s in serials:
                listbox.insert("end", s)

            count_var = tk.StringVar(value=f"Отсканировано: {len(serials)}")
            tk.Label(ser_main, textvariable=count_var, font=self.F["body"],
                     bg=self.C["card_bg"], fg=self.C["text_secondary"]).pack(
                anchor="w", pady=(4, 0))

            def refresh_cnt():
                count_var.set(f"Отсканировано: {listbox.size()}")

            def remove_ser():
                sel = listbox.curselection()
                if sel:
                    listbox.delete(sel[0])
                    refresh_cnt()

            def finish():
                out = [listbox.get(i) for i in range(listbox.size())]
                set_actual(line_id, float(len(out)), ", ".join(out))
                ser_dialog.destroy()
                load_items()
                self.refresh_inbound()

            btn_frame = tk.Frame(ser_main, bg=self.C["card_bg"])
            btn_frame.pack(fill="x", pady=(12, 0))

            del_btn = self._make_raised_btn(btn_frame, "Удалить выбранный",
                                            bg_color=self.C["error"],
                                            command=remove_ser, icon="🗑️")
            del_btn.pack(side="left")

            fin_btn = self._make_raised_btn(btn_frame, "Завершить",
                                            bg_color=self.C["success"],
                                            command=finish, icon="✅")
            fin_btn.pack(side="right")

        # ── Edit line (add to actual) ──
        def edit_line():
            sel = items_tree.selection()
            if not sel:
                messagebox.showwarning("Внимание", "Выберите позицию",
                                       parent=dialog)
                return

            vals = items_tree.item(sel[0], "values")
            line_id = int(vals[0])
            product_name = vals[1]

            row = self.db.query(
                """SELECT i.actual_qty, COALESCE(i.serial_numbers,''),
                          COALESCE(p.serial_tracking,'Нет')
                   FROM inbound_order_items i
                   JOIN products p ON p.id=i.product_id WHERE i.id=?""",
                (line_id,))[0]
            current_actual = float(row[0])
            current_serial = row[1]
            serial_tracking = row[2]

            if serial_tracking == "Да":
                edit_serial(line_id, product_name, current_serial)
                return

            qty_text = simpledialog.askstring(
                "Ввод количества",
                f"Товар: {product_name}\n"
                f"Текущее: {current_actual}\n\n"
                f"Введите количество для добавления:",
                initialvalue="1", parent=dialog)
            if qty_text is None:
                return

            try:
                delta = float(qty_text)
                if delta <= 0:
                    raise ValueError
            except ValueError:
                messagebox.showwarning("Внимание", "Некорректное количество",
                                       parent=dialog)
                return

            set_actual(line_id, current_actual + delta)
            load_items()
            self.refresh_inbound()

        # ── Edit qty (replace actual) ──
        def edit_qty():
            sel = items_tree.selection()
            if not sel:
                messagebox.showwarning("Внимание", "Выберите позицию",
                                       parent=dialog)
                return

            vals = items_tree.item(sel[0], "values")
            line_id = int(vals[0])
            product_name = vals[1]

            row = self.db.query(
                """SELECT i.actual_qty, COALESCE(i.serial_numbers,''),
                          COALESCE(p.serial_tracking,'Нет')
                   FROM inbound_order_items i
                   JOIN products p ON p.id=i.product_id WHERE i.id=?""",
                (line_id,))[0]
            current_actual = float(row[0])
            current_serial = row[1]
            serial_tracking = row[2]

            if serial_tracking == "Да":
                edit_serial(line_id, product_name, current_serial)
                return

            qty_text = simpledialog.askstring(
                "Редактирование количества",
                f"Товар: {product_name}\n\n"
                f"Введите новое количество:",
                initialvalue=str(current_actual), parent=dialog)
            if qty_text is None:
                return

            try:
                new_qty = float(qty_text)
                if new_qty < 0:
                    raise ValueError
            except ValueError:
                messagebox.showwarning("Внимание", "Некорректное количество",
                                       parent=dialog)
                return

            set_actual(line_id, new_qty)
            load_items()
            self.refresh_inbound()

        # ── Accept order ──
        def accept_order():
            state = self.db.query(
                "SELECT status FROM inbound_orders WHERE id=?",
                (order_id,))[0][0]
            if state == "Принят":
                messagebox.showinfo("Информация", "Заказ уже принят",
                                    parent=dialog)
                return

            check_rows = self.db.query(
                """SELECT i.id, i.planned_qty, i.actual_qty, i.actual_filled,
                          i.product_id, COALESCE(i.serial_numbers,''),
                          COALESCE(p.serial_tracking,'Нет')
                   FROM inbound_order_items i
                   JOIN products p ON p.id=i.product_id
                   WHERE i.order_id=?""", (order_id,))

            if not check_rows:
                messagebox.showwarning("Внимание", "Нет позиций", parent=dialog)
                return

            not_filled = [r for r in check_rows if int(r[3]) != 1]
            if not_filled:
                messagebox.showwarning(
                    "Внимание",
                    f"Заполните фактическое количество.\n"
                    f"Не заполнено: {len(not_filled)} позиций",
                    parent=dialog)
                return

            # Check discrepancies
            discrepancies = []
            for _, planned, actual, _, _, serials, ser_track in check_rows:
                p, a = float(planned), float(actual)
                if a > p:
                    discrepancies.append(f"Излишек: план {p}, факт {a}")
                elif a < p:
                    discrepancies.append(f"Недостача: план {p}, факт {a}")

                if ser_track == "Да":
                    sc = len([x for x in (serials or "").split(",")
                              if x.strip()])
                    if int(a) != sc:
                        messagebox.showwarning(
                            "Внимание",
                            "Количество серийных номеров должно "
                            "совпадать с фактом",
                            parent=dialog)
                        return

            if discrepancies:
                msg = "Обнаружены расхождения:\n\n"
                msg += "\n".join(discrepancies[:5])
                if len(discrepancies) > 5:
                    msg += "\n..."
                msg += "\n\nВсё равно принять?"
                if not messagebox.askyesno("Подтверждение", msg,
                                           parent=dialog):
                    return

            # Create movements + add to unplaced area
            now_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            for _, _, actual, _, product_id, _, _ in check_rows:
                qty = float(actual)
                if qty > 0:
                    if abs(qty - int(qty)) > 1e-9:
                        messagebox.showwarning(
                            "Внимание",
                            "Количество должно быть целым числом",
                            parent=dialog)
                        return
                    int_qty = int(qty)
                    self.db.execute(
                        """INSERT INTO movements(product_id, movement_type,
                           quantity, reference, moved_at, note)
                           VALUES(?, 'IN', ?, ?, ?, ?)""",
                        (product_id, int_qty, order_no,
                         now_ts,
                         "Приём по заказу"))
                    self.db.execute(
                        """INSERT INTO unplaced_stock(product_id, quantity, updated_at)
                           VALUES(?,?,?)
                           ON CONFLICT(product_id) DO UPDATE SET
                           quantity=quantity+excluded.quantity,
                           updated_at=excluded.updated_at""",
                        (product_id, qty, now_ts))

            self.db.execute(
                """UPDATE inbound_orders SET status='Принят',
                   received_at=?, accepted_by=? WHERE id=?""",
                (datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                 self.current_user, order_id))

            messagebox.showinfo("Успешно", "Заказ принят", parent=dialog)
            self.refresh_all()
            dialog.destroy()

        # ── Toolbar buttons ──
        b_enter = self._make_raised_btn(toolbar, "Ввести фактическое",
                                        bg_color=self.C["primary"],
                                        command=edit_line, icon="📝")
        b_enter.pack(side="left", padx=(0, 8))

        b_edit = self._make_raised_btn(toolbar, "Редактировать кол-во",
                                       bg_color=self.C["primary_light"],
                                       command=edit_qty, icon="✏️")
        b_edit.pack(side="left", padx=(0, 8))

        if status == "Новый":
            b_accept = self._make_raised_btn(toolbar, "Принять заказ",
                                             bg_color=self.C["success"],
                                             command=accept_order, icon="✅")
            b_accept.pack(side="left")

        # Initial load
        load_items()

    # ══════════════════════════════════════════════════
    #             ДИАЛОГИ — ДВИЖЕНИЯ
    # ══════════════════════════════════════════════════

    def add_movement(self):
        token = self.mov_product_var.get().strip()
        if not token:
            messagebox.showwarning("Внимание", "Выберите товар")
            return

        product_id = self._get_id(token)
        movement_type = self.mov_type_var.get().strip()

        try:
            qty = int(self.mov_qty_var.get())
            if qty <= 0:
                raise ValueError
        except ValueError:
            messagebox.showwarning("Внимание",
                                   "Количество должно быть целым положительным числом")
            return

        if movement_type == "OUT":
            stock = self._get_stock_by_product()
            available = stock.get(product_id, 0)
            if qty > available:
                messagebox.showerror(
                    "Ошибка",
                    f"Недостаточно остатка.\n"
                    f"Доступно: {available}\nТребуется: {qty}")
                return

        self.db.execute(
            """INSERT INTO movements(product_id, movement_type, quantity,
               reference, moved_at, note) VALUES(?,?,?,?,?,?)""",
            (product_id, movement_type, qty,
             self.mov_ref_var.get().strip(),
             datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
             self.mov_note_var.get().strip()))

        self.mov_qty_var.set("1")
        self.mov_ref_var.set("")
        self.mov_note_var.set("")
        self.refresh_all()
        messagebox.showinfo("Успешно",
                            f"Движение {movement_type} на {qty} ед. проведено")

    def _get_stock_by_product(self):
        rows = self.db.query(
            """SELECT p.id,
                      COALESCE(SUM(CASE WHEN m.movement_type='IN'
                                        THEN m.quantity ELSE -m.quantity END), 0)
               FROM products p
               LEFT JOIN movements m ON m.product_id=p.id
               GROUP BY p.id""")
        return {pid: stock for pid, stock in rows}

        # ══════════════════════════════════════════════════
    #             ОБНОВЛЕНИЕ ДАННЫХ
    # ══════════════════════════════════════════════════

    # ==================== РАЗМЕЩЕНИЕ ====================

    def _build_placement_page(self):
        page = self.pages["placement"]
        page.grid_rowconfigure(0, weight=1)
        page.grid_columnconfigure(0, weight=2)
        page.grid_columnconfigure(1, weight=1)

        left = self._make_card(page, padx=(20, 8), pady=16, expand=True)
        tk.Label(left, text="Неразмещённый участок", font=self.F["heading"],
                 bg=self.C["card_bg"], fg=self.C["text"]).pack(anchor="w", padx=16, pady=(12, 6))

        cols = ("product_id", "name", "article", "unit", "qty")
        wmap = {
            "product_id": ("ID", 55), "name": ("Товар", 220),
            "article": ("Артикул", 120), "unit": ("Ед.", 60),
            "qty": ("Неразмещено", 110),
        }
        self.unplaced_tree, _ = self._make_tree(left, cols, wmap, height=18)
        self.unplaced_tree.bind("<Double-1>", self._on_unplaced_double_click)

        right = self._make_card(page, padx=(8, 20), pady=16, expand=True)
        tk.Label(right, text="Размещение товара", font=self.F["heading"],
                 bg=self.C["card_bg"], fg=self.C["text"]).pack(anchor="w", padx=16, pady=(12, 8))

        self.place_selected_lbl = tk.Label(
            right, text="Выберите товар двойным кликом слева",
            font=self.F["body"], bg=self.C["card_bg"], fg=self.C["text_secondary"], wraplength=280, justify="left")
        self.place_selected_lbl.pack(anchor="w", padx=16)

        f1 = self._make_labeled_entry(right, "Количество (шт/палет)", self.place_qty_var, width=20)
        f1.pack(fill="x", padx=16, pady=(12, 6))
        f2 = self._make_labeled_combo(right, "Складская ячейка", self.place_cell_var, [], width=28)
        f2.pack(fill="x", padx=16, pady=(0, 6))
        self.place_cell_combo = f2._combo

        btn = self._make_raised_btn(right, "Привязать", bg_color=self.C["success"],
                                    command=self.bind_product_to_cell, icon="🔗")
        btn.pack(anchor="w", padx=16, pady=(8, 6))

    def _on_unplaced_double_click(self, event=None):
        sel = self.unplaced_tree.selection()
        if not sel:
            return
        vals = self.unplaced_tree.item(sel[0], "values")
        self.placement_product_id = int(vals[0])
        self.place_selected_lbl.configure(
            text=f"Товар: {vals[1]} (Артикул: {vals[2] or '—'})\nДоступно на неразмещённом участке: {vals[4]}")

    def bind_product_to_cell(self):
        if not self.placement_product_id:
            messagebox.showwarning("Внимание", "Сначала выберите товар двойным кликом")
            return
        cell_token = self.place_cell_var.get().strip()
        cell_id = self._get_id(cell_token)
        if not cell_id:
            messagebox.showwarning("Внимание", "Выберите складскую ячейку")
            return
        try:
            qty = float(self.place_qty_var.get())
            if qty <= 0:
                raise ValueError
        except ValueError:
            messagebox.showwarning("Внимание", "Введите корректное количество")
            return

        row = self.db.query("SELECT quantity FROM unplaced_stock WHERE product_id=?", (self.placement_product_id,))
        available = float(row[0][0]) if row else 0.0
        if qty > available:
            messagebox.showwarning("Внимание", f"Недостаточно товара на неразмещённом участке. Доступно: {available:g}")
            return

        now_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.db.execute(
            """UPDATE unplaced_stock SET quantity=quantity-?, updated_at=? WHERE product_id=?""",
            (qty, now_ts, self.placement_product_id))
        self.db.execute("DELETE FROM unplaced_stock WHERE product_id=? AND quantity<=0", (self.placement_product_id,))
        self.db.execute(
            """INSERT INTO cell_stock(product_id, cell_id, quantity, updated_at)
               VALUES(?,?,?,?)
               ON CONFLICT(product_id, cell_id) DO UPDATE SET
               quantity=quantity+excluded.quantity,
               updated_at=excluded.updated_at""",
            (self.placement_product_id, cell_id, qty, now_ts))

        p = self.db.query("SELECT COALESCE(name, brand, ''), COALESCE(article,'') FROM products WHERE id=?", (self.placement_product_id,))[0]
        c = self.db.query("SELECT name FROM warehouse_cells WHERE id=?", (cell_id,))[0][0]
        messagebox.showinfo("Успешно", f"Товар '{p[0] or p[1]}' привязан к ячейке '{c}' в количестве {qty:g}")
        self.refresh_all()

    # ==================== СПРАВОЧНИК ЯЧЕЕК ====================

    def _build_cells_ref_page(self):
        page = self.pages["cells_ref"]
        card = self._make_card(page, padx=20, pady=16, expand=True)
        tb = tk.Frame(card, bg=self.C["card_bg"])
        tb.pack(fill="x", padx=16, pady=(12, 8))
        create_btn = self._make_raised_btn(tb, "Создать ячейку", bg_color=self.C["success"],
                                           command=self.create_cell_dialog, icon="➕")
        create_btn.pack(side="left")

        cols = ("id", "name", "created_at")
        wmap = {
            "id": ("ID", 60), "name": ("Название ячейки", 260), "created_at": ("Создана", 180)
        }
        self.cells_tree, _ = self._make_tree(card, cols, wmap, height=18)

    def create_cell_dialog(self):
        name = simpledialog.askstring("Создать ячейку", "Введите название ячейки:", parent=self)
        if name is None:
            return
        name = name.strip()
        if not name:
            messagebox.showwarning("Внимание", "Название ячейки не может быть пустым")
            return
        try:
            self.db.execute("INSERT INTO warehouse_cells(name, created_at) VALUES(?,?)",
                            (name, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        except sqlite3.IntegrityError:
            messagebox.showwarning("Внимание", "Ячейка с таким названием уже существует")
            return
        self.refresh_cells_ref()
        self._refresh_cells_combo()

    # ==================== ПОИСК ТОВАРА ====================

    def _build_product_search_page(self):
        page = self.pages["product_search"]
        card = self._make_card(page, padx=20, pady=16, expand=True)

        filters = tk.Frame(card, bg=self.C["card_bg"])
        filters.pack(fill="x", padx=16, pady=(12, 6))
        self._make_labeled_entry(filters, "Название", self.search_name_var, width=20).pack(side="left", padx=(0, 8))
        self._make_labeled_entry(filters, "Артикул", self.search_article_var, width=16).pack(side="left", padx=(0, 8))
        self._make_labeled_entry(filters, "Штрихкод", self.search_barcode_var, width=16).pack(side="left", padx=(0, 8))

        self.search_cat_combo_wrap = self._make_labeled_combo(filters, "Категория", self.search_category_var, ["Все"], width=16)
        self.search_cat_combo_wrap.pack(side="left", padx=(0, 8))
        self.search_cat_combo = self.search_cat_combo_wrap._combo
        self.search_sub_combo_wrap = self._make_labeled_combo(filters, "Подкатегория", self.search_subcategory_var, ["Все"], width=16)
        self.search_sub_combo_wrap.pack(side="left", padx=(0, 8))
        self.search_sub_combo = self.search_sub_combo_wrap._combo
        self.search_client_combo_wrap = self._make_labeled_combo(filters, "3PL клиент", self.search_client_var, ["Все"], width=16)
        self.search_client_combo_wrap.pack(side="left", padx=(0, 8))
        self.search_client_combo = self.search_client_combo_wrap._combo

        row2 = tk.Frame(card, bg=self.C["card_bg"])
        row2.pack(fill="x", padx=16, pady=(0, 8))
        tk.Checkbutton(row2, text="Только в наличии", variable=self.search_only_in_stock_var,
                       bg=self.C["card_bg"], fg=self.C["text"]).pack(side="left")
        self._make_raised_btn(row2, "Поиск", command=self.search_products, icon="🔍").pack(side="left", padx=(10, 0))
        tk.Label(row2, text="Кликните товар 2 раза ЛКМ для действий", bg=self.C["card_bg"],
                 fg=self.C["text_hint"], font=self.F["small"]).pack(side="right")

        cols = ("id", "name", "article", "category", "subcategory", "client", "unplaced", "placed", "total", "reserved", "waiting")
        wmap = {
            "id": ("ID", 50), "name": ("Название", 180), "article": ("Артикул", 100),
            "category": ("Категория", 100), "subcategory": ("Подкатегория", 110), "client": ("3PL клиент", 120),
            "unplaced": ("Неразмещ.", 90), "placed": ("Размещ.", 85), "total": ("Общий остаток", 95),
            "reserved": ("В резерве", 80), "waiting": ("В ожидании", 80),
        }
        self.product_search_tree, _ = self._make_tree(card, cols, wmap, height=16)
        self.product_search_tree.bind("<Double-1>", self._on_product_search_double_click)
        self.search_category_var.trace_add("write", lambda *_: self._refresh_search_subcategories())

    def _refresh_cells_combo(self):
        rows = self.db.query("SELECT id, name FROM warehouse_cells ORDER BY name")
        vals = [f"{r[0]} | {r[1]}" for r in rows]
        if hasattr(self, "place_cell_combo"):
            self.place_cell_combo["values"] = vals
        if vals and not self.place_cell_var.get():
            self.place_cell_var.set(vals[0])

    def _refresh_search_filters(self):
        cats = [f"{r[0]} | {r[1]}" for r in self.db.query("SELECT id,name FROM categories ORDER BY name")]
        clients = [f"{r[0]} | {r[1]}" for r in self.db.query("SELECT id,name FROM clients ORDER BY name")]
        if hasattr(self, "search_cat_combo"):
            self.search_cat_combo["values"] = ["Все", *cats]
            self.search_client_combo["values"] = ["Все", *clients]
        self._refresh_search_subcategories()

    def _refresh_search_subcategories(self):
        if not hasattr(self, "search_sub_combo"):
            return
        token = self.search_category_var.get().strip()
        if token == "Все" or not token:
            self.search_sub_combo["values"] = ["Все"]
            self.search_subcategory_var.set("Все")
            return
        cat_id = self._get_id(token)
        rows = self.db.query("SELECT id,name FROM subcategories WHERE category_id=? ORDER BY name", (cat_id,))
        vals = ["Все", *[f"{r[0]} | {r[1]}" for r in rows]]
        self.search_sub_combo["values"] = vals
        if self.search_subcategory_var.get() not in vals:
            self.search_subcategory_var.set("Все")

    def _on_product_search_double_click(self, event=None):
        sel = self.product_search_tree.selection()
        if not sel:
            return
        pid = int(self.product_search_tree.item(sel[0], "values")[0])
        menu = tk.Menu(self, tearoff=0)
        menu.add_command(label="Посмотреть движение товара", command=lambda p=pid: self.show_product_movements(p))
        menu.add_command(label="Посмотреть остаток на ячейках", command=lambda p=pid: self.show_product_cells(p))
        menu.tk_popup(self.winfo_pointerx(), self.winfo_pointery())

    def show_product_movements(self, product_id):
        rows = self.db.query(
            """SELECT 'Приход', o.order_number, o.created_at, i.actual_qty
               FROM inbound_order_items i
               JOIN inbound_orders o ON o.id=i.order_id
               WHERE i.product_id=? AND o.status='Принят'
               UNION ALL
               SELECT 'Отгрузка', o.order_number, COALESCE(o.shipped_at,o.created_at), i.actual_qty
               FROM outbound_order_items i
               JOIN outbound_orders o ON o.id=i.order_id
               WHERE i.product_id=? AND o.status='Отгружен'
               ORDER BY 3 DESC""", (product_id, product_id))
        d = self._create_dialog("Движение товара", 760, 520)
        box = tk.Frame(d, bg=self.C["card_bg"])
        box.pack(fill="both", expand=True, padx=14, pady=14)
        cols = ("type", "order", "date", "qty")
        wmap = {"type": ("Тип", 110), "order": ("Заказ", 140), "date": ("Дата", 170), "qty": ("Факт", 80)}
        tree, _ = self._make_tree(box, cols, wmap, height=14)
        for r in rows:
            tree.insert("", "end", values=r)

        def open_order(_=None):
            sel = tree.selection()
            if not sel:
                return
            typ, order_no, _, _ = tree.item(sel[0], "values")
            if typ == "Приход":
                d.destroy()
                self._inbound_order_dialog(order_no)
            else:
                messagebox.showinfo("Информация", "Переход к отгрузке не реализован в текущем интерфейсе", parent=d)

        tree.bind("<Double-1>", open_order)

    def show_product_cells(self, product_id):
        d = self._create_dialog("Остаток по ячейкам", 700, 520)
        box = tk.Frame(d, bg=self.C["card_bg"])
        box.pack(fill="both", expand=True, padx=14, pady=14)
        cols = ("cell", "qty")
        wmap = {"cell": ("Ячейка", 260), "qty": ("Количество", 120)}
        tree, _ = self._make_tree(box, cols, wmap, height=14)
        rows = self.db.query(
            """SELECT c.name, s.quantity
               FROM cell_stock s
               JOIN warehouse_cells c ON c.id=s.cell_id
               WHERE s.product_id=? AND s.quantity>0
               ORDER BY c.name""", (product_id,))
        for r in rows:
            tree.insert("", "end", values=(r[0], f"{float(r[1]):g}"))
        unplaced = self.db.query("SELECT COALESCE(quantity,0) FROM unplaced_stock WHERE product_id=?", (product_id,))
        u = float(unplaced[0][0]) if unplaced else 0.0
        tree.insert("", "end", values=("Неразмещённый участок", f"{u:g}"), tags=("warning",))

    def search_products(self):
        self.refresh_product_search()

    def refresh_cells_ref(self):
        if not hasattr(self, "cells_tree"):
            return
        for i in self.cells_tree.get_children():
            self.cells_tree.delete(i)
        rows = self.db.query("SELECT id, name, created_at FROM warehouse_cells ORDER BY id DESC")
        for idx, r in enumerate(rows):
            tag = "evenrow" if idx % 2 == 0 else "oddrow"
            self.cells_tree.insert("", "end", values=r, tags=(tag,))

    def refresh_placement(self):
        if not hasattr(self, "unplaced_tree"):
            return
        for i in self.unplaced_tree.get_children():
            self.unplaced_tree.delete(i)
        rows = self.db.query(
            """SELECT p.id, COALESCE(p.name, p.brand, ''), COALESCE(p.article,''),
                      COALESCE(p.unit,'шт'), u.quantity
               FROM unplaced_stock u
               JOIN products p ON p.id=u.product_id
               WHERE u.quantity>0
               ORDER BY p.id DESC""")
        for idx, r in enumerate(rows):
            tag = "evenrow" if idx % 2 == 0 else "oddrow"
            self.unplaced_tree.insert("", "end", values=(r[0], r[1], r[2], r[3], f"{float(r[4]):g}"), tags=(tag,))
        self._refresh_cells_combo()

    def refresh_product_search(self):
        if not hasattr(self, "product_search_tree"):
            return
        for i in self.product_search_tree.get_children():
            self.product_search_tree.delete(i)

        f_name = self.search_name_var.get().strip().lower()
        f_article = self.search_article_var.get().strip().lower()
        f_barcode = self.search_barcode_var.get().strip().lower()
        cat_token = self.search_category_var.get().strip()
        sub_token = self.search_subcategory_var.get().strip()
        client_token = self.search_client_var.get().strip()
        only_stock = self.search_only_in_stock_var.get()

        rows = self.db.query(
            """SELECT p.id, COALESCE(p.name, p.brand, ''), COALESCE(p.article,''),
                      COALESCE(p.barcode,''), COALESCE(cat.name,''), COALESCE(sub.name,''),
                      COALESCE(cl.name,''),
                      COALESCE((SELECT SUM(quantity) FROM unplaced_stock u WHERE u.product_id=p.id),0),
                      COALESCE((SELECT SUM(quantity) FROM cell_stock cs WHERE cs.product_id=p.id),0),
                      COALESCE((SELECT SUM(i.planned_qty)
                                FROM outbound_order_items i
                                JOIN outbound_orders o ON o.id=i.order_id
                                WHERE i.product_id=p.id AND o.status='Новый'),0),
                      COALESCE((SELECT SUM(i.planned_qty)
                                FROM inbound_order_items i
                                JOIN inbound_orders o ON o.id=i.order_id
                                WHERE i.product_id=p.id AND o.status='Новый'),0)
               FROM products p
               LEFT JOIN categories cat ON cat.id=p.category_id
               LEFT JOIN subcategories sub ON sub.id=p.subcategory_id
               LEFT JOIN clients cl ON cl.id=p.client_id
               ORDER BY p.id DESC""")

        cat_id = self._get_id(cat_token) if cat_token and cat_token != "Все" else None
        sub_id = self._get_id(sub_token) if sub_token and sub_token != "Все" else None
        client_id = self._get_id(client_token) if client_token and client_token != "Все" else None

        for idx, r in enumerate(rows):
            pid, name, article, barcode, cat, sub, client, unplaced, placed, reserved, waiting = r
            total = float(unplaced) + float(placed)
            if f_name and f_name not in (name or "").lower():
                continue
            if f_article and f_article not in (article or "").lower():
                continue
            if f_barcode and f_barcode not in (barcode or "").lower():
                continue
            if cat_id and self.db.query("SELECT category_id FROM products WHERE id=?", (pid,))[0][0] != cat_id:
                continue
            if sub_id and self.db.query("SELECT subcategory_id FROM products WHERE id=?", (pid,))[0][0] != sub_id:
                continue
            if client_id and self.db.query("SELECT client_id FROM products WHERE id=?", (pid,))[0][0] != client_id:
                continue
            if only_stock and total <= 0:
                continue

            tag = "evenrow" if idx % 2 == 0 else "oddrow"
            self.product_search_tree.insert("", "end", values=(
                pid, name, article, cat, sub, client,
                f"{float(unplaced):g}", f"{float(placed):g}", f"{total:g}",
                f"{float(reserved):g}", f"{float(waiting):g}"), tags=(tag,))


    def refresh_all(self):
        self.refresh_suppliers()
        self.refresh_clients()
        self.refresh_categories()
        self.refresh_nomenclature()
        self.refresh_inbound()
        self.refresh_movements()
        self.refresh_stock()
        self.refresh_placement()
        self.refresh_cells_ref()
        self._refresh_search_filters()
        self.refresh_product_search()
        self.refresh_metrics()

    def refresh_suppliers(self):
        for item in self.suppliers_tree.get_children():
            self.suppliers_tree.delete(item)
        rows = self.db.query(
            """SELECT id, name, COALESCE(phone,''), COALESCE(created_at,'')
               FROM suppliers ORDER BY id DESC""")
        term = self.suppliers_search_var.get().strip().lower()
        for i, row in enumerate(rows):
            if term and term not in (row[1] or "").lower():
                continue
            tag = "evenrow" if i % 2 == 0 else "oddrow"
            self.suppliers_tree.insert("", "end", values=row, tags=(tag,))

    def refresh_clients(self):
        for item in self.clients_tree.get_children():
            self.clients_tree.delete(item)
        rows = self.db.query(
            """SELECT id, name, COALESCE(contact,''), COALESCE(created_at,'')
               FROM clients ORDER BY id DESC""")
        term = self.clients_search_var.get().strip().lower()
        for i, row in enumerate(rows):
            if term and term not in (row[1] or "").lower():
                continue
            tag = "evenrow" if i % 2 == 0 else "oddrow"
            self.clients_tree.insert("", "end", values=row, tags=(tag,))

    def refresh_categories(self):
        for item in self.categories_tree.get_children():
            self.categories_tree.delete(item)
        for item in self.subcategories_tree.get_children():
            self.subcategories_tree.delete(item)

        term = self.categories_filter_var.get().strip().lower()

        cat_rows = self.db.query("SELECT id, name FROM categories ORDER BY name")
        for i, row in enumerate(cat_rows):
            if term and term not in (row[1] or "").lower():
                continue
            tag = "evenrow" if i % 2 == 0 else "oddrow"
            self.categories_tree.insert("", "end", values=row, tags=(tag,))

        sub_rows = self.db.query(
            """SELECT s.id, s.name, COALESCE(c.name,'')
               FROM subcategories s
               LEFT JOIN categories c ON c.id=s.category_id
               ORDER BY c.name, s.name""")
        for i, row in enumerate(sub_rows):
            full = f"{row[1]} {row[2]}".lower()
            if term and term not in full:
                continue
            tag = "evenrow" if i % 2 == 0 else "oddrow"
            self.subcategories_tree.insert("", "end", values=row, tags=(tag,))

        vals = [f"{r[0]} | {r[1]}" for r in cat_rows]
        self.subcat_parent_box["values"] = vals
        if vals and not self.subcategory_parent_var.get():
            self.subcategory_parent_var.set(vals[0])

    def refresh_nomenclature(self):
        for item in self.nomenclature_tree.get_children():
            self.nomenclature_tree.delete(item)

        if not self.nom_searched:
            return

        rows = self.db.query(
            """SELECT p.id, COALESCE(p.article,''), p.brand, s.name, c.name,
                      p.unit, p.volume, p.weight, p.barcode, p.serial_tracking,
                      cat.name, sub.name, p.product_owner
               FROM products p
               LEFT JOIN suppliers s ON s.id=p.supplier_id
               LEFT JOIN clients c ON c.id=p.client_id
               LEFT JOIN categories cat ON cat.id=p.category_id
               LEFT JOIN subcategories sub ON sub.id=p.subcategory_id
               ORDER BY p.id DESC""")

        f_brand = self.nom_brand_var.get().strip().lower()
        f_article = self.nom_article_var.get().strip().lower()
        f_supplier = self.nom_supplier_var.get().strip().lower()
        f_client = self.nom_client_var.get().strip().lower()

        idx = 0
        for row in rows:
            if f_brand and f_brand not in (row[2] or "").lower():
                continue
            if f_article and f_article not in (row[1] or "").lower():
                continue
            if f_supplier and f_supplier not in (row[3] or "").lower():
                continue
            if f_client and f_client not in (row[4] or "").lower():
                continue

            tag = "evenrow" if idx % 2 == 0 else "oddrow"
            self.nomenclature_tree.insert("", "end", values=row, tags=(tag,))
            idx += 1

        # Update movements product combobox
        prod_vals = [
            f"{r[0]} | {r[1] or ''} | {r[2] or ''}"
            for r in self.db.query(
                "SELECT id, article, brand FROM products ORDER BY id DESC")]
        self.mov_product_box["values"] = prod_vals
        if prod_vals and not self.mov_product_var.get():
            self.mov_product_var.set(prod_vals[0])

    def refresh_inbound(self):
        for item in self.inbound_tree.get_children():
            self.inbound_tree.delete(item)

        if not self.inb_searched:
            return

        rows = self.db.query(
            """SELECT o.order_number, o.created_at,
                      COALESCE(o.received_at,''), o.created_by,
                      COALESCE(o.accepted_by,''), s.name, c.name, w.name,
                      o.status, COALESCE(SUM(i.planned_qty),0),
                      COALESCE(SUM(i.actual_qty),0)
               FROM inbound_orders o
               JOIN suppliers s ON s.id=o.supplier_id
               JOIN clients c ON c.id=o.client_id
               JOIN warehouses w ON w.id=o.warehouse_id
               LEFT JOIN inbound_order_items i ON i.order_id=o.id
               GROUP BY o.id ORDER BY o.id DESC""")

        f_search = self.inb_search_var.get().strip().lower()
        f_status = self.inb_status_var.get().strip()
        f_from = self.inb_from_var.get().strip()
        f_to = self.inb_to_var.get().strip()
        f_created = self.inb_created_by_var.get().strip().lower()
        f_accepted = self.inb_accepted_by_var.get().strip().lower()
        f_supplier = self.inb_supplier_var.get().strip().lower()
        f_client = self.inb_client_var.get().strip().lower()

        for row in rows:
            order_num = (row[0] or "").lower()
            created_day = (row[1] or "")[:10]
            st = row[8] or ""

            if f_search and f_search not in order_num:
                continue
            if f_status and f_status != "Все" and st != f_status:
                continue
            if f_from and created_day < f_from:
                continue
            if f_to and created_day > f_to:
                continue
            if f_created and f_created not in (row[3] or "").lower():
                continue
            if f_accepted and f_accepted not in (row[4] or "").lower():
                continue
            if f_supplier and f_supplier not in (row[5] or "").lower():
                continue
            if f_client and f_client not in (row[6] or "").lower():
                continue

            tag = "new" if st == "Новый" else "accepted"
            self.inbound_tree.insert("", "end", values=row, tags=(tag,))

    def refresh_movements(self):
        for item in self.movements_tree.get_children():
            self.movements_tree.delete(item)

        rows = self.db.query(
            """SELECT m.id, p.brand, m.movement_type, m.quantity,
                      m.reference, m.moved_at, m.note
               FROM movements m
               JOIN products p ON p.id=m.product_id
               ORDER BY m.id DESC""")

        for row in rows:
            tag = "in" if row[2] == "IN" else "out"
            self.movements_tree.insert("", "end", values=row, tags=(tag,))

    def refresh_stock(self):
        for item in self.stock_tree.get_children():
            self.stock_tree.delete(item)

        rows = self.db.query(
            """SELECT p.brand, c.name, p.unit,
                      COALESCE(SUM(CASE WHEN m.movement_type='IN'
                                        THEN m.quantity
                                        ELSE -m.quantity END), 0)
               FROM products p
               LEFT JOIN clients c ON c.id=p.client_id
               LEFT JOIN movements m ON m.product_id=p.id
               GROUP BY p.id ORDER BY p.id DESC""")

        for row in rows:
            stock = row[3]
            if stock == 0:
                tag = "zero"
            elif stock < 10:
                tag = "low"
            else:
                tag = "ok"
            self.stock_tree.insert("", "end", values=row, tags=(tag,))

    def refresh_metrics(self):
        counts = {
            "suppliers": self.db.query("SELECT COUNT(*) FROM suppliers")[0][0],
            "clients": self.db.query("SELECT COUNT(*) FROM clients")[0][0],
            "products": self.db.query("SELECT COUNT(*) FROM products")[0][0],
            "inbound": self.db.query("SELECT COUNT(*) FROM inbound_orders")[0][0],
            "movements": self.db.query("SELECT COUNT(*) FROM movements")[0][0],
        }
        for key, val in counts.items():
            if key in self.metric_labels:
                self.metric_labels[key].configure(text=str(val))

    # ══════════════════════════════════════════════════
    #             ЗАКРЫТИЕ ПРИЛОЖЕНИЯ
    # ══════════════════════════════════════════════════

    def _on_close(self):
        if messagebox.askyesno("Выход", "Выйти из приложения?"):
            self.db.close()
            self.destroy()


# ══════════════════════════════════════════════════
#                 ТОЧКА ВХОДА
# ══════════════════════════════════════════════════

if __name__ == "__main__":
    app = WMSApp()
    app.mainloop()
