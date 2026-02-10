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
        self._seed_reference_data()

    def _create_schema(self):
        with closing(self.conn.cursor()) as cur:
            cur.executescript(
                """
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
                    movement_type TEXT NOT NULL CHECK(movement_type IN ('IN', 'OUT')),
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
                    status TEXT NOT NULL CHECK(status IN ('Новый', 'Принят')),
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
                """
            )
            self.conn.commit()

    def _add_column_if_missing(self, table: str, column: str, definition: str):
        existing = {row[1] for row in self.query(f"PRAGMA table_info({table})")}
        if column not in existing:
            self.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")

    def _add_unique_index_if_missing(self, table: str, column: str):
        index_name = f"idx_{table}_{column}_unique"
        indexes = self.query(f"PRAGMA index_list({table})")
        if not any(row[1] == index_name for row in indexes):
            self.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS {index_name} ON {table}({column})")

    def _migrate_products_table(self):
        self._add_column_if_missing("products", "brand", "TEXT")
        self._add_column_if_missing("products", "supplier_id", "INTEGER REFERENCES suppliers(id) ON DELETE SET NULL")
        self._add_column_if_missing("products", "volume", "REAL")
        self._add_column_if_missing("products", "weight", "REAL")
        self._add_column_if_missing("products", "barcode", "TEXT")
        self._add_column_if_missing("products", "serial_tracking", "TEXT NOT NULL DEFAULT 'Нет'")
        self._add_column_if_missing("products", "article", "TEXT")
        self._add_column_if_missing("products", "category_id", "INTEGER REFERENCES categories(id) ON DELETE RESTRICT")
        self._add_column_if_missing("products", "subcategory_id", "INTEGER REFERENCES subcategories(id) ON DELETE RESTRICT")
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
            "UPDATE suppliers SET created_at = ? WHERE created_at IS NULL OR TRIM(created_at) = ''",
            (datetime.now().strftime("%Y-%m-%d %H:%M:%S"),),
        )

    def _migrate_clients_table(self):
        self._add_column_if_missing("clients", "code", "TEXT")
        self._add_column_if_missing("clients", "name", "TEXT")
        self._add_column_if_missing("clients", "contact", "TEXT")
        self._add_column_if_missing("clients", "created_at", "TEXT")
        self.execute(
            "UPDATE clients SET created_at = ? WHERE created_at IS NULL OR TRIM(created_at) = ''",
            (datetime.now().strftime("%Y-%m-%d %H:%M:%S"),),
        )

    def _migrate_inbound_tables(self):
        self._add_column_if_missing("inbound_order_items", "actual_filled", "INTEGER NOT NULL DEFAULT 0")
        self._add_column_if_missing("inbound_order_items", "serial_numbers", "TEXT")

    def _seed_reference_data(self):
        if not self.query("SELECT id FROM suppliers LIMIT 1"):
            self.execute(
                "INSERT INTO suppliers(name, phone, created_at) VALUES(?, ?, ?)",
                ("Default Supplier", "", datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
            )
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


class WMSApp(tk.Tk):
    """Современное WMS приложение с профессиональным корпоративным дизайном"""
    
    # Цветовая палитра
    COLORS = {
        "primary": "#2c3e50",
        "primary_light": "#34495e",
        "primary_dark": "#1a252f",
        "accent": "#3498db",
        "accent_hover": "#2980b9",
        "success": "#27ae60",
        "success_light": "#d5f4e6",
        "warning": "#f39c12",
        "warning_light": "#fef9e7",
        "error": "#e74c3c",
        "error_light": "#fdedec",
        "bg_main": "#ecf0f1",
        "bg_card": "#ffffff",
        "bg_header": "#2c3e50",
        "text_primary": "#2c3e50",
        "text_secondary": "#7f8c8d",
        "text_light": "#ffffff",
        "border": "#bdc3c7",
        "border_light": "#dfe6e9",
        "hover": "#ebf5fb",
        "selected": "#d4e6f1",
        "row_alt": "#f8f9fa",
    }
    
    # Шрифты
    FONTS = {
        "title": ("Segoe UI", 20, "bold"),
        "subtitle": ("Segoe UI", 11),
        "heading": ("Segoe UI", 14, "bold"),
        "subheading": ("Segoe UI", 12, "bold"),
        "body": ("Segoe UI", 10),
        "body_bold": ("Segoe UI", 10, "bold"),
        "small": ("Segoe UI", 9),
        "button": ("Segoe UI", 10, "bold"),
        "tab": ("Segoe UI", 10, "bold"),
        "metric": ("Segoe UI", 18, "bold"),
        "metric_label": ("Segoe UI", 9),
    }

    def __init__(self):
        super().__init__()
        self.title("WMS 3PL — Система управления складом")
        
        # Размеры окна
        screen_w = self.winfo_screenwidth()
        screen_h = self.winfo_screenheight()
        win_w = min(1600, int(screen_w * 0.92))
        win_h = min(900, int(screen_h * 0.88))
        x = (screen_w - win_w) // 2
        y = (screen_h - win_h) // 2 - 20
        
        self.geometry(f"{win_w}x{win_h}+{x}+{y}")
        self.minsize(1200, 700)
        self.configure(bg=self.COLORS["bg_main"])
        
        self.db = Database(DB_FILE)
        self.current_user = getpass.getuser()
        
        self.style = ttk.Style(self)
        self._configure_styles()
        self._init_variables()
        self._build_ui()
        self.refresh_all()
        
        self.protocol("WM_DELETE_WINDOW", self._on_close)
        
        # Максимизация окна
        self.after(50, self._try_maximize)

    def _try_maximize(self):
        try:
            self.state("zoomed")
        except tk.TclError:
            pass

    def _init_variables(self):
        """Инициализация переменных"""
        self.selected_copy_value = ""
        
        # Поставщики
        self.suppliers_search_var = tk.StringVar()
        self.suppliers_search_var.trace_add("write", lambda *_: self.refresh_suppliers())
        
        # Клиенты
        self.clients_search_var = tk.StringVar()
        self.clients_search_var.trace_add("write", lambda *_: self.refresh_clients())
        
        # Категории
        self.categories_filter_var = tk.StringVar()
        self.categories_filter_var.trace_add("write", lambda *_: self.refresh_categories())
        self.new_category_var = tk.StringVar()
        self.new_subcategory_var = tk.StringVar()
        self.subcategory_parent_var = tk.StringVar()
        
        # Номенклатура
        self.nom_brand_var = tk.StringVar()
        self.nom_article_var = tk.StringVar()
        self.nom_supplier_var = tk.StringVar()
        self.nom_client_var = tk.StringVar()
        self.nom_searched = False
        
        # Приходы
        self.inb_search_var = tk.StringVar()
        self.inb_status_var = tk.StringVar(value="Все")
        self.inb_from_var = tk.StringVar()
        self.inb_to_var = tk.StringVar()
        self.inb_created_by_var = tk.StringVar()
        self.inb_accepted_by_var = tk.StringVar()
        self.inb_supplier_var = tk.StringVar()
        self.inb_client_var = tk.StringVar()
        self.inb_searched = True
        
        # Движения
        self.mov_product_var = tk.StringVar()
        self.mov_type_var = tk.StringVar(value="IN")
        self.mov_qty_var = tk.StringVar(value="1")
        self.mov_ref_var = tk.StringVar()
        self.mov_note_var = tk.StringVar()

    def _configure_styles(self):
        """Настройка стилей ttk"""
        self.style.theme_use("clam")
        
        # Frames
        self.style.configure("Main.TFrame", background=self.COLORS["bg_main"])
        self.style.configure("Card.TFrame", background=self.COLORS["bg_card"])
        self.style.configure("Header.TFrame", background=self.COLORS["bg_header"])
        
        # Labels
        self.style.configure("Title.TLabel",
                            background=self.COLORS["bg_header"],
                            foreground=self.COLORS["text_light"],
                            font=self.FONTS["title"])
        
        self.style.configure("Subtitle.TLabel",
                            background=self.COLORS["bg_header"],
                            foreground="#bdc3c7",
                            font=self.FONTS["subtitle"])
        
        self.style.configure("Heading.TLabel",
                            background=self.COLORS["bg_card"],
                            foreground=self.COLORS["text_primary"],
                            font=self.FONTS["heading"])
        
        self.style.configure("Body.TLabel",
                            background=self.COLORS["bg_card"],
                            foreground=self.COLORS["text_primary"],
                            font=self.FONTS["body"])
        
        self.style.configure("BodyMain.TLabel",
                            background=self.COLORS["bg_main"],
                            foreground=self.COLORS["text_primary"],
                            font=self.FONTS["body"])
        
        self.style.configure("Small.TLabel",
                            background=self.COLORS["bg_main"],
                            foreground=self.COLORS["text_secondary"],
                            font=self.FONTS["small"])
        
        # Buttons
        self.style.configure("Primary.TButton",
                            font=self.FONTS["button"],
                            padding=(16, 8))
        
        self.style.configure("Success.TButton",
                            font=self.FONTS["button"],
                            padding=(16, 8))
        
        self.style.configure("Danger.TButton",
                            font=self.FONTS["button"],
                            padding=(16, 8))
        
        self.style.configure("Secondary.TButton",
                            font=self.FONTS["button"],
                            padding=(12, 6))
        
        self.style.configure("Small.TButton",
                            font=self.FONTS["small"],
                            padding=(8, 4))
        
        # Entry
        self.style.configure("TEntry",
                            font=self.FONTS["body"],
                            padding=6)
        
        # Combobox
        self.style.configure("TCombobox",
                            font=self.FONTS["body"],
                            padding=6)
        
        # Treeview
        self.style.configure("Treeview",
                            font=self.FONTS["body"],
                            rowheight=32,
                            background=self.COLORS["bg_card"],
                            fieldbackground=self.COLORS["bg_card"],
                            foreground=self.COLORS["text_primary"])
        
        self.style.configure("Treeview.Heading",
                            font=self.FONTS["body_bold"],
                            background=self.COLORS["primary"],
                            foreground=self.COLORS["text_light"],
                            padding=(8, 6))
        
        self.style.map("Treeview",
                      background=[("selected", self.COLORS["selected"])],
                      foreground=[("selected", self.COLORS["text_primary"])])
        
        # Notebook
        self.style.configure("TNotebook",
                            background=self.COLORS["bg_main"],
                            borderwidth=0)
        
        self.style.configure("TNotebook.Tab",
                            font=self.FONTS["tab"],
                            padding=(20, 10),
                            background=self.COLORS["bg_card"])
        
        self.style.map("TNotebook.Tab",
                      background=[("selected", self.COLORS["primary"])],
                      foreground=[("selected", self.COLORS["text_light"])])

    def _build_ui(self):
        """Построение интерфейса"""
        # Главный контейнер
        self.main_frame = ttk.Frame(self, style="Main.TFrame")
        self.main_frame.pack(fill="both", expand=True)
        
        # Header
        self._build_header()
        
        # Metrics
        self._build_metrics()
        
        # Notebook
        self._build_notebook()

    def _build_header(self):
        """Построение заголовка"""
        header = ttk.Frame(self.main_frame, style="Header.TFrame")
        header.pack(fill="x")
        
        inner = ttk.Frame(header, style="Header.TFrame")
        inner.pack(fill="x", padx=25, pady=15)
        
        # Logo + Title
        left = ttk.Frame(inner, style="Header.TFrame")
        left.pack(side="left")
        
        logo = tk.Label(left, text="📦", font=("Segoe UI", 28),
                       bg=self.COLORS["bg_header"], fg=self.COLORS["accent"])
        logo.pack(side="left", padx=(0, 12))
        
        title_frame = ttk.Frame(left, style="Header.TFrame")
        title_frame.pack(side="left")
        
        ttk.Label(title_frame, text="WMS 3PL", style="Title.TLabel").pack(anchor="w")
        ttk.Label(title_frame, text="Поставщики • Клиенты • Номенклатура • Приходы • Движения • Остатки",
                 style="Subtitle.TLabel").pack(anchor="w")
        
        # User info
        right = ttk.Frame(inner, style="Header.TFrame")
        right.pack(side="right")
        
        user_lbl = tk.Label(right, text=f"👤 {self.current_user}",
                           font=self.FONTS["body"], bg=self.COLORS["bg_header"],
                           fg=self.COLORS["text_light"])
        user_lbl.pack(side="left", padx=(0, 20))
        
        date_lbl = tk.Label(right, text=f"📅 {datetime.now().strftime('%d.%m.%Y')}",
                           font=self.FONTS["body"], bg=self.COLORS["bg_header"],
                           fg=self.COLORS["text_light"])
        date_lbl.pack(side="left")

    def _build_metrics(self):
        """Построение панели метрик"""
        metrics_outer = tk.Frame(self.main_frame, bg=self.COLORS["primary_light"])
        metrics_outer.pack(fill="x")
        
        metrics_inner = tk.Frame(metrics_outer, bg=self.COLORS["primary_light"])
        metrics_inner.pack(fill="x", padx=25, pady=12)
        
        self.metric_labels = {}
        configs = [
            ("suppliers", "📋", "Поставщики"),
            ("clients", "👥", "3PL Клиенты"),
            ("products", "📦", "Номенклатура"),
            ("inbound", "📥", "Приходы"),
            ("movements", "🔄", "Движения"),
        ]
        
        for key, icon, title in configs:
            card = tk.Frame(metrics_inner, bg=self.COLORS["primary"], padx=18, pady=10)
            card.pack(side="left", padx=(0, 12))
            
            top = tk.Frame(card, bg=self.COLORS["primary"])
            top.pack(fill="x")
            
            tk.Label(top, text=icon, font=("Segoe UI", 14),
                    bg=self.COLORS["primary"], fg=self.COLORS["accent"]).pack(side="left")
            tk.Label(top, text=title, font=self.FONTS["metric_label"],
                    bg=self.COLORS["primary"], fg="#bdc3c7").pack(side="left", padx=(6, 0))
            
            val = tk.Label(card, text="0", font=self.FONTS["metric"],
                          bg=self.COLORS["primary"], fg=self.COLORS["text_light"])
            val.pack(anchor="w")
            self.metric_labels[key] = val

    def _build_notebook(self):
        """Построение вкладок"""
        container = ttk.Frame(self.main_frame, style="Main.TFrame")
        container.pack(fill="both", expand=True, padx=15, pady=15)
        
        self.notebook = ttk.Notebook(container)
        self.notebook.pack(fill="both", expand=True)
        
        # Вкладки
        self.tab_suppliers = ttk.Frame(self.notebook, style="Card.TFrame")
        self.tab_clients = ttk.Frame(self.notebook, style="Card.TFrame")
        self.tab_categories = ttk.Frame(self.notebook, style="Card.TFrame")
        self.tab_nomenclature = ttk.Frame(self.notebook, style="Card.TFrame")
        self.tab_inbound = ttk.Frame(self.notebook, style="Card.TFrame")
        self.tab_movements = ttk.Frame(self.notebook, style="Card.TFrame")
        self.tab_stock = ttk.Frame(self.notebook, style="Card.TFrame")
        
        self.notebook.add(self.tab_suppliers, text="  📋 Поставщики  ")
        self.notebook.add(self.tab_clients, text="  👥 3PL Клиенты  ")
        self.notebook.add(self.tab_categories, text="  📁 Категории  ")
        self.notebook.add(self.tab_nomenclature, text="  📦 Номенклатура  ")
        self.notebook.add(self.tab_inbound, text="  📥 Приходы  ")
        self.notebook.add(self.tab_movements, text="  🔄 Движения  ")
        self.notebook.add(self.tab_stock, text="  📊 Остатки  ")
        
        # Строим содержимое
        self._build_suppliers_tab()
        self._build_clients_tab()
        self._build_categories_tab()
        self._build_nomenclature_tab()
        self._build_inbound_tab()
        self._build_movements_tab()
        self._build_stock_tab()

    # ================== ПОСТАВЩИКИ ==================
    
    def _build_suppliers_tab(self):
        """Вкладка поставщиков"""
        # Header
        header = ttk.Frame(self.tab_suppliers, style="Card.TFrame")
        header.pack(fill="x", padx=20, pady=(20, 10))
        
        ttk.Label(header, text="Управление поставщиками", style="Heading.TLabel").pack(anchor="w")
        ttk.Label(header, text="Создание, редактирование и удаление карточек поставщиков",
                 font=self.FONTS["small"], background=self.COLORS["bg_card"],
                 foreground=self.COLORS["text_secondary"]).pack(anchor="w", pady=(2, 0))
        
        # Toolbar
        toolbar = ttk.Frame(self.tab_suppliers, style="Card.TFrame")
        toolbar.pack(fill="x", padx=20, pady=(0, 10))
        
        # Search
        search_frame = ttk.Frame(toolbar, style="Card.TFrame")
        search_frame.pack(side="left")
        
        ttk.Label(search_frame, text="🔍", style="Body.TLabel").pack(side="left", padx=(0, 5))
        ttk.Entry(search_frame, textvariable=self.suppliers_search_var, width=35).pack(side="left")
        
        # Buttons
        btn_frame = ttk.Frame(toolbar, style="Card.TFrame")
        btn_frame.pack(side="right")
        
        ttk.Button(btn_frame, text="➕ Создать", style="Success.TButton",
                  command=self.create_supplier).pack(side="left", padx=(0, 8))
        ttk.Button(btn_frame, text="✏️ Редактировать", style="Primary.TButton",
                  command=self.edit_supplier).pack(side="left", padx=(0, 8))
        ttk.Button(btn_frame, text="🗑️ Удалить", style="Danger.TButton",
                  command=self.delete_supplier).pack(side="left")
        
        # Tree
        tree_frame = ttk.Frame(self.tab_suppliers, style="Card.TFrame")
        tree_frame.pack(fill="both", expand=True, padx=20, pady=(0, 20))
        
        cols = ("id", "name", "phone", "created")
        self.suppliers_tree = ttk.Treeview(tree_frame, columns=cols, show="headings", height=20)
        
        vsb = ttk.Scrollbar(tree_frame, orient="vertical", command=self.suppliers_tree.yview)
        self.suppliers_tree.configure(yscrollcommand=vsb.set)
        
        self.suppliers_tree.pack(side="left", fill="both", expand=True)
        vsb.pack(side="right", fill="y")
        
        widths = [("id", "ID", 60), ("name", "Название поставщика", 350),
                 ("phone", "Телефон", 150), ("created", "Дата создания", 150)]
        for col, title, w in widths:
            self.suppliers_tree.heading(col, text=title)
            self.suppliers_tree.column(col, width=w, minwidth=50)
        
        self.suppliers_tree.bind("<Double-1>", lambda e: self.edit_supplier())
        
        # Context menu
        self.suppliers_menu = tk.Menu(self, tearoff=0)
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

    # ================== КЛИЕНТЫ ==================
    
    def _build_clients_tab(self):
        """Вкладка клиентов"""
        header = ttk.Frame(self.tab_clients, style="Card.TFrame")
        header.pack(fill="x", padx=20, pady=(20, 10))
        
        ttk.Label(header, text="Управление 3PL клиентами", style="Heading.TLabel").pack(anchor="w")
        ttk.Label(header, text="Создание, редактирование и удаление карточек клиентов",
                 font=self.FONTS["small"], background=self.COLORS["bg_card"],
                 foreground=self.COLORS["text_secondary"]).pack(anchor="w", pady=(2, 0))
        
        toolbar = ttk.Frame(self.tab_clients, style="Card.TFrame")
        toolbar.pack(fill="x", padx=20, pady=(0, 10))
        
        search_frame = ttk.Frame(toolbar, style="Card.TFrame")
        search_frame.pack(side="left")
        
        ttk.Label(search_frame, text="🔍", style="Body.TLabel").pack(side="left", padx=(0, 5))
        ttk.Entry(search_frame, textvariable=self.clients_search_var, width=35).pack(side="left")
        
        btn_frame = ttk.Frame(toolbar, style="Card.TFrame")
        btn_frame.pack(side="right")
        
        ttk.Button(btn_frame, text="➕ Создать", style="Success.TButton",
                  command=self.create_client).pack(side="left", padx=(0, 8))
        ttk.Button(btn_frame, text="✏️ Редактировать", style="Primary.TButton",
                  command=self.edit_client).pack(side="left", padx=(0, 8))
        ttk.Button(btn_frame, text="🗑️ Удалить", style="Danger.TButton",
                  command=self.delete_client).pack(side="left")
        
        tree_frame = ttk.Frame(self.tab_clients, style="Card.TFrame")
        tree_frame.pack(fill="both", expand=True, padx=20, pady=(0, 20))
        
        cols = ("id", "name", "contact", "created")
        self.clients_tree = ttk.Treeview(tree_frame, columns=cols, show="headings", height=20)
        
        vsb = ttk.Scrollbar(tree_frame, orient="vertical", command=self.clients_tree.yview)
        self.clients_tree.configure(yscrollcommand=vsb.set)
        
        self.clients_tree.pack(side="left", fill="both", expand=True)
        vsb.pack(side="right", fill="y")
        
        widths = [("id", "ID", 60), ("name", "Название клиента", 350),
                 ("contact", "Контакт", 150), ("created", "Дата создания", 150)]
        for col, title, w in widths:
            self.clients_tree.heading(col, text=title)
            self.clients_tree.column(col, width=w, minwidth=50)
        
        self.clients_tree.bind("<Double-1>", lambda e: self.edit_client())

    # ================== КАТЕГОРИИ ==================
    
    def _build_categories_tab(self):
        """Вкладка категорий"""
        header = ttk.Frame(self.tab_categories, style="Card.TFrame")
        header.pack(fill="x", padx=20, pady=(20, 10))
        
        ttk.Label(header, text="Управление категориями", style="Heading.TLabel").pack(anchor="w")
        ttk.Label(header, text="Создание категорий и подкатегорий для классификации товаров",
                 font=self.FONTS["small"], background=self.COLORS["bg_card"],
                 foreground=self.COLORS["text_secondary"]).pack(anchor="w", pady=(2, 0))
        
        # Search
        search_frame = ttk.Frame(self.tab_categories, style="Card.TFrame")
        search_frame.pack(fill="x", padx=20, pady=(0, 10))
        
        ttk.Label(search_frame, text="🔍", style="Body.TLabel").pack(side="left", padx=(0, 5))
        ttk.Entry(search_frame, textvariable=self.categories_filter_var, width=40).pack(side="left")
        
        # Create forms
        create_frame = ttk.Frame(self.tab_categories, style="Card.TFrame")
        create_frame.pack(fill="x", padx=20, pady=(0, 10))
        
        # Category
        cat_row = ttk.Frame(create_frame, style="Card.TFrame")
        cat_row.pack(fill="x", pady=5)
        
        ttk.Label(cat_row, text="Новая категория:", style="Body.TLabel").pack(side="left", padx=(0, 8))
        ttk.Entry(cat_row, textvariable=self.new_category_var, width=25).pack(side="left", padx=(0, 8))
        ttk.Button(cat_row, text="➕ Создать", style="Success.TButton",
                  command=self.create_category).pack(side="left")
        
        # Subcategory
        sub_row = ttk.Frame(create_frame, style="Card.TFrame")
        sub_row.pack(fill="x", pady=5)
        
        ttk.Label(sub_row, text="Новая подкатегория:", style="Body.TLabel").pack(side="left", padx=(0, 8))
        ttk.Entry(sub_row, textvariable=self.new_subcategory_var, width=20).pack(side="left", padx=(0, 8))
        ttk.Label(sub_row, text="в категории:", style="Body.TLabel").pack(side="left", padx=(0, 8))
        self.subcat_parent_box = ttk.Combobox(sub_row, textvariable=self.subcategory_parent_var,
                                               state="readonly", width=20)
        self.subcat_parent_box.pack(side="left", padx=(0, 8))
        ttk.Button(sub_row, text="➕ Создать", style="Success.TButton",
                  command=self.create_subcategory).pack(side="left")
        
        # Trees
        trees_frame = ttk.Frame(self.tab_categories, style="Card.TFrame")
        trees_frame.pack(fill="both", expand=True, padx=20, pady=(0, 20))
        
        # Categories tree
        cat_frame = ttk.Frame(trees_frame, style="Card.TFrame")
        cat_frame.pack(side="left", fill="both", expand=True, padx=(0, 10))
        
        ttk.Label(cat_frame, text="📁 Категории", style="Heading.TLabel").pack(anchor="w", pady=(0, 8))
        
        cat_tree_frame = ttk.Frame(cat_frame, style="Card.TFrame")
        cat_tree_frame.pack(fill="both", expand=True)
        
        self.categories_tree = ttk.Treeview(cat_tree_frame, columns=("id", "name"), show="headings", height=15)
        cat_vsb = ttk.Scrollbar(cat_tree_frame, orient="vertical", command=self.categories_tree.yview)
        self.categories_tree.configure(yscrollcommand=cat_vsb.set)
        
        self.categories_tree.pack(side="left", fill="both", expand=True)
        cat_vsb.pack(side="right", fill="y")
        
        self.categories_tree.heading("id", text="ID")
        self.categories_tree.heading("name", text="Название")
        self.categories_tree.column("id", width=60, minwidth=50)
        self.categories_tree.column("name", width=200, minwidth=100)
        
        # Subcategories tree
        sub_frame = ttk.Frame(trees_frame, style="Card.TFrame")
        sub_frame.pack(side="left", fill="both", expand=True, padx=(10, 0))
        
        ttk.Label(sub_frame, text="📂 Подкатегории", style="Heading.TLabel").pack(anchor="w", pady=(0, 8))
        
        sub_tree_frame = ttk.Frame(sub_frame, style="Card.TFrame")
        sub_tree_frame.pack(fill="both", expand=True)
        
        self.subcategories_tree = ttk.Treeview(sub_tree_frame, columns=("id", "name", "category"),
                                                show="headings", height=15)
        sub_vsb = ttk.Scrollbar(sub_tree_frame, orient="vertical", command=self.subcategories_tree.yview)
        self.subcategories_tree.configure(yscrollcommand=sub_vsb.set)
        
        self.subcategories_tree.pack(side="left", fill="both", expand=True)
        sub_vsb.pack(side="right", fill="y")
        
        self.subcategories_tree.heading("id", text="ID")
        self.subcategories_tree.heading("name", text="Подкатегория")
        self.subcategories_tree.heading("category", text="Категория")
        self.subcategories_tree.column("id", width=60, minwidth=50)
        self.subcategories_tree.column("name", width=150, minwidth=80)
        self.subcategories_tree.column("category", width=150, minwidth=80)

    # ================== НОМЕНКЛАТУРА ==================
    
    def _build_nomenclature_tab(self):
        """Вкладка номенклатуры"""
        header = ttk.Frame(self.tab_nomenclature, style="Card.TFrame")
        header.pack(fill="x", padx=20, pady=(20, 10))
        
        ttk.Label(header, text="Справочник номенклатуры", style="Heading.TLabel").pack(anchor="w")
        ttk.Label(header, text="Управление карточками товаров: создание, редактирование, поиск",
                 font=self.FONTS["small"], background=self.COLORS["bg_card"],
                 foreground=self.COLORS["text_secondary"]).pack(anchor="w", pady=(2, 0))
        
        # Filters
        filter_frame = ttk.Frame(self.tab_nomenclature, style="Card.TFrame")
        filter_frame.pack(fill="x", padx=20, pady=(0, 10))
        
        filters = [
            ("Марка:", self.nom_brand_var, 12),
            ("Артикул:", self.nom_article_var, 10),
            ("Поставщик:", self.nom_supplier_var, 12),
            ("3PL клиент:", self.nom_client_var, 12),
        ]
        
        for lbl, var, w in filters:
            ttk.Label(filter_frame, text=lbl, style="Body.TLabel").pack(side="left", padx=(0, 4))
            ttk.Entry(filter_frame, textvariable=var, width=w).pack(side="left", padx=(0, 12))
        
        ttk.Button(filter_frame, text="🔍 Поиск", style="Primary.TButton",
                  command=self.search_nomenclature).pack(side="left", padx=(8, 0))
        
        # Toolbar
        toolbar = ttk.Frame(self.tab_nomenclature, style="Card.TFrame")
        toolbar.pack(fill="x", padx=20, pady=(0, 10))
        
        ttk.Button(toolbar, text="➕ Создать карточку", style="Success.TButton",
                  command=self.create_product).pack(side="left", padx=(0, 8))
        ttk.Button(toolbar, text="✏️ Редактировать", style="Primary.TButton",
                  command=self.edit_product).pack(side="left", padx=(0, 8))
        ttk.Button(toolbar, text="🗑️ Удалить", style="Danger.TButton",
                  command=self.delete_product).pack(side="left")
        
        # Tree
        tree_frame = ttk.Frame(self.tab_nomenclature, style="Card.TFrame")
        tree_frame.pack(fill="both", expand=True, padx=20, pady=(0, 20))
        
        cols = ("id", "article", "brand", "supplier", "client", "unit",
               "volume", "weight", "barcode", "serial", "category", "subcategory", "owner")
        
        self.nomenclature_tree = ttk.Treeview(tree_frame, columns=cols, show="headings", height=18)
        
        vsb = ttk.Scrollbar(tree_frame, orient="vertical", command=self.nomenclature_tree.yview)
        hsb = ttk.Scrollbar(tree_frame, orient="horizontal", command=self.nomenclature_tree.xview)
        self.nomenclature_tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        
        self.nomenclature_tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")
        
        tree_frame.grid_rowconfigure(0, weight=1)
        tree_frame.grid_columnconfigure(0, weight=1)
        
        widths = [
            ("id", "ID", 50), ("article", "Артикул", 90), ("brand", "Марка", 150),
            ("supplier", "Поставщик", 120), ("client", "3PL клиент", 120),
            ("unit", "Ед.", 50), ("volume", "Объём", 60), ("weight", "Вес", 60),
            ("barcode", "Штрих-код", 100), ("serial", "Серийный", 70),
            ("category", "Категория", 100), ("subcategory", "Подкатегория", 100),
            ("owner", "Владелец", 80),
        ]
        
        for col, title, w in widths:
            self.nomenclature_tree.heading(col, text=title)
            self.nomenclature_tree.column(col, width=w, minwidth=40)
        
        self.nomenclature_tree.bind("<Double-1>", lambda e: self.edit_product())
        
        # Context menu
        self.nom_menu = tk.Menu(self, tearoff=0)
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

    # ================== ПРИХОДЫ ==================
    
    def _build_inbound_tab(self):
        """Вкладка приходов"""
        header = ttk.Frame(self.tab_inbound, style="Card.TFrame")
        header.pack(fill="x", padx=20, pady=(20, 10))
        
        ttk.Label(header, text="Приходные заказы", style="Heading.TLabel").pack(anchor="w")
        ttk.Label(header, text="Управление заказами на приход товара: создание, приёмка, история",
                 font=self.FONTS["small"], background=self.COLORS["bg_card"],
                 foreground=self.COLORS["text_secondary"]).pack(anchor="w", pady=(2, 0))
        
        # Filters row 1
        f1 = ttk.Frame(self.tab_inbound, style="Card.TFrame")
        f1.pack(fill="x", padx=20, pady=(0, 5))
        
        ttk.Label(f1, text="№ заказа:", style="Body.TLabel").pack(side="left", padx=(0, 4))
        ttk.Entry(f1, textvariable=self.inb_search_var, width=10).pack(side="left", padx=(0, 12))
        
        ttk.Label(f1, text="Статус:", style="Body.TLabel").pack(side="left", padx=(0, 4))
        ttk.Combobox(f1, textvariable=self.inb_status_var,
                    values=["Все", "Новый", "Принят"], state="readonly", width=8).pack(side="left", padx=(0, 12))
        
        ttk.Label(f1, text="От:", style="Body.TLabel").pack(side="left", padx=(0, 4))
        ttk.Entry(f1, textvariable=self.inb_from_var, width=10, state="readonly").pack(side="left", padx=(0, 4))
        ttk.Button(f1, text="📅", style="Small.TButton",
                  command=lambda: self._pick_date(self.inb_from_var)).pack(side="left", padx=(0, 12))
        
        ttk.Label(f1, text="До:", style="Body.TLabel").pack(side="left", padx=(0, 4))
        ttk.Entry(f1, textvariable=self.inb_to_var, width=10, state="readonly").pack(side="left", padx=(0, 4))
        ttk.Button(f1, text="📅", style="Small.TButton",
                  command=lambda: self._pick_date(self.inb_to_var)).pack(side="left")
        
        # Filters row 2
        f2 = ttk.Frame(self.tab_inbound, style="Card.TFrame")
        f2.pack(fill="x", padx=20, pady=(0, 10))
        
        ttk.Label(f2, text="Создал:", style="Body.TLabel").pack(side="left", padx=(0, 4))
        ttk.Entry(f2, textvariable=self.inb_created_by_var, width=10).pack(side="left", padx=(0, 12))
        
        ttk.Label(f2, text="Принял:", style="Body.TLabel").pack(side="left", padx=(0, 4))
        ttk.Entry(f2, textvariable=self.inb_accepted_by_var, width=10).pack(side="left", padx=(0, 12))
        
        ttk.Label(f2, text="Поставщик:", style="Body.TLabel").pack(side="left", padx=(0, 4))
        ttk.Entry(f2, textvariable=self.inb_supplier_var, width=12).pack(side="left", padx=(0, 12))
        
        ttk.Label(f2, text="3PL:", style="Body.TLabel").pack(side="left", padx=(0, 4))
        ttk.Entry(f2, textvariable=self.inb_client_var, width=12).pack(side="left", padx=(0, 12))
        
        ttk.Button(f2, text="🔍 Поиск", style="Primary.TButton",
                  command=self.search_inbound).pack(side="left", padx=(8, 0))
        
        # Toolbar
        toolbar = ttk.Frame(self.tab_inbound, style="Card.TFrame")
        toolbar.pack(fill="x", padx=20, pady=(0, 10))
        
        ttk.Button(toolbar, text="➕ Создать заказ", style="Success.TButton",
                  command=self.create_inbound_order).pack(side="left")
        
        ttk.Label(toolbar, text="💡 Двойной клик для открытия заказа",
                 font=self.FONTS["small"], background=self.COLORS["bg_card"],
                 foreground=self.COLORS["text_secondary"]).pack(side="right")
        
        # Tree
        tree_frame = ttk.Frame(self.tab_inbound, style="Card.TFrame")
        tree_frame.pack(fill="both", expand=True, padx=20, pady=(0, 20))
        
        cols = ("order_number", "created_at", "received_at", "created_by", "accepted_by",
               "supplier", "client", "warehouse", "status", "planned", "actual")
        
        self.inbound_tree = ttk.Treeview(tree_frame, columns=cols, show="headings", height=16)
        
        vsb = ttk.Scrollbar(tree_frame, orient="vertical", command=self.inbound_tree.yview)
        hsb = ttk.Scrollbar(tree_frame, orient="horizontal", command=self.inbound_tree.xview)
        self.inbound_tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        
        self.inbound_tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")
        
        tree_frame.grid_rowconfigure(0, weight=1)
        tree_frame.grid_columnconfigure(0, weight=1)
        
        widths = [
            ("order_number", "№ Заказа", 100), ("created_at", "Создан", 130),
            ("received_at", "Принят", 130), ("created_by", "Создал", 80),
            ("accepted_by", "Принял", 80), ("supplier", "Поставщик", 120),
            ("client", "3PL клиент", 120), ("warehouse", "Склад", 100),
            ("status", "Статус", 80), ("planned", "План", 70), ("actual", "Факт", 70),
        ]
        
        for col, title, w in widths:
            self.inbound_tree.heading(col, text=title)
            self.inbound_tree.column(col, width=w, minwidth=50)
        
        self.inbound_tree.tag_configure("new", background=self.COLORS["warning_light"])
        self.inbound_tree.tag_configure("accepted", background=self.COLORS["success_light"])
        
        self.inbound_tree.bind("<Double-1>", self._open_inbound_order)

    # ================== ДВИЖЕНИЯ ==================
    
    def _build_movements_tab(self):
        """Вкладка движений"""
        header = ttk.Frame(self.tab_movements, style="Card.TFrame")
        header.pack(fill="x", padx=20, pady=(20, 10))
        
        ttk.Label(header, text="Движения товаров", style="Heading.TLabel").pack(anchor="w")
        ttk.Label(header, text="Журнал приходов и расходов товаров",
                 font=self.FONTS["small"], background=self.COLORS["bg_card"],
                 foreground=self.COLORS["text_secondary"]).pack(anchor="w", pady=(2, 0))
        
        # Form
        form_frame = ttk.Frame(self.tab_movements, style="Card.TFrame")
        form_frame.pack(fill="x", padx=20, pady=(0, 10))
        
        ttk.Label(form_frame, text="➕ Добавить движение", style="Heading.TLabel").pack(anchor="w", pady=(0, 10))
        
        row = ttk.Frame(form_frame, style="Card.TFrame")
        row.pack(fill="x")
        
        ttk.Label(row, text="Товар:", style="Body.TLabel").pack(side="left", padx=(0, 4))
        self.mov_product_box = ttk.Combobox(row, textvariable=self.mov_product_var, width=30, state="readonly")
        self.mov_product_box.pack(side="left", padx=(0, 12))
        
        ttk.Label(row, text="Тип:", style="Body.TLabel").pack(side="left", padx=(0, 4))
        ttk.Combobox(row, textvariable=self.mov_type_var, values=["IN", "OUT"],
                    width=6, state="readonly").pack(side="left", padx=(0, 12))
        
        ttk.Label(row, text="Кол-во:", style="Body.TLabel").pack(side="left", padx=(0, 4))
        ttk.Entry(row, textvariable=self.mov_qty_var, width=8).pack(side="left", padx=(0, 12))
        
        ttk.Label(row, text="Документ:", style="Body.TLabel").pack(side="left", padx=(0, 4))
        ttk.Entry(row, textvariable=self.mov_ref_var, width=12).pack(side="left", padx=(0, 12))
        
        ttk.Label(row, text="Комментарий:", style="Body.TLabel").pack(side="left", padx=(0, 4))
        ttk.Entry(row, textvariable=self.mov_note_var, width=15).pack(side="left", padx=(0, 12))
        
        ttk.Button(row, text="✅ Провести", style="Success.TButton",
                  command=self.add_movement).pack(side="left")
        
        # Tree
        tree_frame = ttk.Frame(self.tab_movements, style="Card.TFrame")
        tree_frame.pack(fill="both", expand=True, padx=20, pady=(0, 20))
        
        cols = ("id", "brand", "type", "qty", "reference", "date", "note")
        self.movements_tree = ttk.Treeview(tree_frame, columns=cols, show="headings", height=18)
        
        vsb = ttk.Scrollbar(tree_frame, orient="vertical", command=self.movements_tree.yview)
        self.movements_tree.configure(yscrollcommand=vsb.set)
        
        self.movements_tree.pack(side="left", fill="both", expand=True)
        vsb.pack(side="right", fill="y")
        
        widths = [
            ("id", "ID", 60), ("brand", "Товар", 250), ("type", "Тип", 60),
            ("qty", "Кол-во", 80), ("reference", "Документ", 120),
            ("date", "Дата", 140), ("note", "Комментарий", 200),
        ]
        
        for col, title, w in widths:
            self.movements_tree.heading(col, text=title)
            self.movements_tree.column(col, width=w, minwidth=40)
        
        self.movements_tree.tag_configure("in", background=self.COLORS["success_light"])
        self.movements_tree.tag_configure("out", background=self.COLORS["error_light"])

    # ================== ОСТАТКИ ==================
    
    def _build_stock_tab(self):
        """Вкладка остатков"""
        header = ttk.Frame(self.tab_stock, style="Card.TFrame")
        header.pack(fill="x", padx=20, pady=(20, 10))
        
        title_row = ttk.Frame(header, style="Card.TFrame")
        title_row.pack(fill="x")
        
        ttk.Label(title_row, text="📊 Текущие остатки на складе", style="Heading.TLabel").pack(side="left")
        ttk.Button(title_row, text="🔄 Обновить", style="Primary.TButton",
                  command=self.refresh_stock).pack(side="right")
        
        ttk.Label(header, text="Актуальная информация об остатках товаров",
                 font=self.FONTS["small"], background=self.COLORS["bg_card"],
                 foreground=self.COLORS["text_secondary"]).pack(anchor="w", pady=(2, 0))
        
        # Tree
        tree_frame = ttk.Frame(self.tab_stock, style="Card.TFrame")
        tree_frame.pack(fill="both", expand=True, padx=20, pady=(10, 20))
        
        cols = ("brand", "client", "unit", "stock")
        self.stock_tree = ttk.Treeview(tree_frame, columns=cols, show="headings", height=20)
        
        vsb = ttk.Scrollbar(tree_frame, orient="vertical", command=self.stock_tree.yview)
        self.stock_tree.configure(yscrollcommand=vsb.set)
        
        self.stock_tree.pack(side="left", fill="both", expand=True)
        vsb.pack(side="right", fill="y")
        
        widths = [("brand", "Товар", 300), ("client", "3PL клиент", 200),
                 ("unit", "Ед.изм.", 80), ("stock", "Остаток", 100)]
        
        for col, title, w in widths:
            self.stock_tree.heading(col, text=title)
            self.stock_tree.column(col, width=w, minwidth=50)
        
        self.stock_tree.tag_configure("zero", background=self.COLORS["error_light"])
        self.stock_tree.tag_configure("low", background=self.COLORS["warning_light"])
        self.stock_tree.tag_configure("ok", background=self.COLORS["success_light"])

        # ================== ДИАЛОГИ ==================
    
    def _center_dialog(self, dialog, w, h):
        """Центрирование диалога"""
        dialog.update_idletasks()
        sw = dialog.winfo_screenwidth()
        sh = dialog.winfo_screenheight()
        x = (sw - w) // 2
        y = (sh - h) // 2 - 30
        dialog.geometry(f"{w}x{h}+{x}+{y}")

    def _create_dialog(self, title, width, height):
        """Создание диалогового окна"""
        dialog = tk.Toplevel(self)
        dialog.title(title)
        dialog.configure(bg=self.COLORS["bg_card"])
        dialog.transient(self)
        dialog.grab_set()
        dialog.resizable(False, False)
        self._center_dialog(dialog, width, height)
        return dialog

    def _create_fullscreen_dialog(self, title, width=1200, height=750):
        """Создание большого диалогового окна"""
        dialog = tk.Toplevel(self)
        dialog.title(title)
        dialog.configure(bg=self.COLORS["bg_main"])
        dialog.transient(self)
        dialog.grab_set()
        self._center_dialog(dialog, width, height)
        return dialog

    def _pick_date(self, target_var):
        """Выбор даты"""
        picker = tk.Toplevel(self)
        picker.title("📅 Выбор даты")
        picker.configure(bg=self.COLORS["bg_card"])
        picker.transient(self)
        picker.grab_set()
        picker.resizable(False, False)
        self._center_dialog(picker, 320, 340)
        
        today = datetime.now()
        year_var = tk.IntVar(value=today.year)
        month_var = tk.IntVar(value=today.month)
        
        main = ttk.Frame(picker, style="Card.TFrame")
        main.pack(fill="both", expand=True, padx=15, pady=15)
        
        month_names = ["", "Январь", "Февраль", "Март", "Апрель", "Май", "Июнь",
                      "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь"]
        
        # Navigation
        nav = ttk.Frame(main, style="Card.TFrame")
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
        
        ttk.Button(nav, text="◀", style="Secondary.TButton", width=3,
                  command=lambda: shift(-1)).pack(side="left")
        
        month_lbl = ttk.Label(nav, text="", style="Heading.TLabel")
        month_lbl.pack(side="left", expand=True)
        
        ttk.Button(nav, text="▶", style="Secondary.TButton", width=3,
                  command=lambda: shift(1)).pack(side="right")
        
        # Days
        days_frame = ttk.Frame(main, style="Card.TFrame")
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
                lbl = ttk.Label(days_frame, text=d, style="Body.TLabel", width=4, anchor="center")
                lbl.grid(row=0, column=c, padx=2, pady=4)
            
            for r, week in enumerate(calendar.monthcalendar(y, m)):
                for c, day in enumerate(week):
                    if day == 0:
                        ttk.Label(days_frame, text="", width=4).grid(row=r+1, column=c, padx=2, pady=2)
                    else:
                        is_today = (day == today.day and m == today.month and y == today.year)
                        style = "Primary.TButton" if is_today else "Secondary.TButton"
                        btn = ttk.Button(days_frame, text=str(day), width=4, style=style,
                                        command=lambda d=day: select(d))
                        btn.grid(row=r+1, column=c, padx=2, pady=2)
        
        # Bottom buttons
        btns = ttk.Frame(main, style="Card.TFrame")
        btns.pack(fill="x", pady=(12, 0))
        
        ttk.Button(btns, text="✖ Очистить", style="Secondary.TButton",
                  command=lambda: [target_var.set(""), picker.destroy()]).pack(side="left")
        ttk.Button(btns, text="📅 Сегодня", style="Primary.TButton",
                  command=lambda: [target_var.set(today.strftime("%Y-%m-%d")), picker.destroy()]).pack(side="right")
        
        build()

    def _copy_value(self):
        """Копирование значения"""
        self.clipboard_clear()
        self.clipboard_append(str(self.selected_copy_value))

    def _get_id(self, token):
        """Извлечение ID из токена"""
        if token and "|" in token:
            return int(token.split(" | ")[0])
        return None

    def _load_dict(self, table):
        """Загрузка справочника"""
        rows = self.db.query(f"SELECT id, name FROM {table} ORDER BY name")
        return {f"{r[0]} | {r[1]}": r[0] for r in rows}

    def _normalize_decimal(self, raw):
        """Нормализация числа"""
        val = (raw or "").strip().replace(",", ".")
        if not val:
            return None
        if not re.fullmatch(r"\d+(?:\.\d{1,4})?", val):
            raise ValueError
        return float(val)

    # ================== ПОСТАВЩИКИ - ОПЕРАЦИИ ==================
    
    def _next_supplier_id(self):
        row = self.db.query("SELECT COALESCE(MAX(id), 0) + 1 FROM suppliers")
        return str(row[0][0])

    def create_supplier(self):
        """Создание поставщика"""
        self._supplier_dialog("create")

    def edit_supplier(self):
        """Редактирование поставщика"""
        sel = self.suppliers_tree.selection()
        if not sel:
            messagebox.showwarning("⚠️ Внимание", "Выберите поставщика")
            return
        sid = int(self.suppliers_tree.item(sel[0], "values")[0])
        self._supplier_dialog("edit", sid)

    def _supplier_dialog(self, mode, supplier_id=None):
        """Диалог поставщика"""
        dialog = self._create_dialog("📋 Карточка поставщика", 480, 300)
        
        main = ttk.Frame(dialog, style="Card.TFrame")
        main.pack(fill="both", expand=True, padx=25, pady=25)
        
        title = "Создание поставщика" if mode == "create" else "Редактирование поставщика"
        ttk.Label(main, text=title, style="Heading.TLabel").pack(anchor="w", pady=(0, 20))
        
        # Variables
        name_var = tk.StringVar()
        id_var = tk.StringVar(value=self._next_supplier_id())
        phone_var = tk.StringVar()
        created_var = tk.StringVar(value=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        
        if mode == "edit" and supplier_id:
            row = self.db.query("SELECT id, name, phone, created_at FROM suppliers WHERE id=?", (supplier_id,))
            if row:
                id_var.set(str(row[0][0]))
                name_var.set(row[0][1] or "")
                phone_var.set(row[0][2] or "")
                created_var.set(row[0][3] or "")
        
        # Form
        form = ttk.Frame(main, style="Card.TFrame")
        form.pack(fill="x")
        
        fields = [
            ("Название *", name_var, False, 30),
            ("ID", id_var, True, 15),
            ("Телефон", phone_var, False, 20),
            ("Дата создания", created_var, True, 20),
        ]
        
        for i, (lbl, var, readonly, w) in enumerate(fields):
            ttk.Label(form, text=lbl, style="Body.TLabel").grid(row=i, column=0, sticky="w", pady=6)
            state = "readonly" if readonly else "normal"
            ttk.Entry(form, textvariable=var, width=w, state=state).grid(row=i, column=1, sticky="w", pady=6, padx=(12, 0))
        
        def save():
            name = name_var.get().strip()
            if not name:
                messagebox.showwarning("⚠️ Внимание", "Название обязательно", parent=dialog)
                return
            
            try:
                if mode == "create":
                    self.db.execute(
                        "INSERT INTO suppliers(name, phone, created_at) VALUES(?,?,?)",
                        (name, phone_var.get().strip(), created_var.get().strip())
                    )
                else:
                    self.db.execute(
                        "UPDATE suppliers SET name=?, phone=? WHERE id=?",
                        (name, phone_var.get().strip(), supplier_id)
                    )
            except sqlite3.IntegrityError:
                messagebox.showerror("❌ Ошибка", "Поставщик уже существует", parent=dialog)
                return
            
            self.refresh_all()
            dialog.destroy()
        
        # Buttons
        btns = ttk.Frame(main, style="Card.TFrame")
        btns.pack(fill="x", pady=(20, 0))
        
        ttk.Button(btns, text="❌ Отмена", style="Secondary.TButton",
                  command=dialog.destroy).pack(side="left")
        ttk.Button(btns, text="✅ Сохранить", style="Success.TButton",
                  command=save).pack(side="right")

    def delete_supplier(self):
        """Удаление поставщика"""
        sel = self.suppliers_tree.selection()
        if not sel:
            messagebox.showwarning("⚠️ Внимание", "Выберите поставщика")
            return
        vals = self.suppliers_tree.item(sel[0], "values")
        if messagebox.askyesno("🗑️ Удаление", f"Удалить «{vals[1]}»?"):
            self.db.execute("DELETE FROM suppliers WHERE id=?", (int(vals[0]),))
            self.refresh_all()

    # ================== КЛИЕНТЫ - ОПЕРАЦИИ ==================
    
    def _next_client_code(self):
        row = self.db.query("SELECT code FROM clients WHERE code LIKE 'C%' ORDER BY id DESC LIMIT 1")
        if not row or not row[0][0]:
            return "C00001"
        try:
            return f"C{int(row[0][0].replace('C', '')) + 1:05d}"
        except:
            cnt = self.db.query("SELECT COUNT(*) FROM clients")[0][0] + 1
            return f"C{cnt:05d}"

    def create_client(self):
        """Создание клиента"""
        self._client_dialog("create")

    def edit_client(self):
        """Редактирование клиента"""
        sel = self.clients_tree.selection()
        if not sel:
            messagebox.showwarning("⚠️ Внимание", "Выберите клиента")
            return
        cid = int(self.clients_tree.item(sel[0], "values")[0])
        self._client_dialog("edit", cid)

    def _client_dialog(self, mode, client_id=None):
        """Диалог клиента"""
        dialog = self._create_dialog("👥 Карточка 3PL клиента", 480, 300)
        
        main = ttk.Frame(dialog, style="Card.TFrame")
        main.pack(fill="both", expand=True, padx=25, pady=25)
        
        title = "Создание клиента" if mode == "create" else "Редактирование клиента"
        ttk.Label(main, text=title, style="Heading.TLabel").pack(anchor="w", pady=(0, 20))
        
        name_var = tk.StringVar()
        id_var = tk.StringVar(value=str(self.db.query("SELECT COALESCE(MAX(id),0)+1 FROM clients")[0][0]))
        contact_var = tk.StringVar()
        created_var = tk.StringVar(value=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        
        if mode == "edit" and client_id:
            row = self.db.query("SELECT id, name, contact, created_at FROM clients WHERE id=?", (client_id,))
            if row:
                id_var.set(str(row[0][0]))
                name_var.set(row[0][1] or "")
                contact_var.set(row[0][2] or "")
                created_var.set(row[0][3] or "")
        
        form = ttk.Frame(main, style="Card.TFrame")
        form.pack(fill="x")
        
        fields = [
            ("Название *", name_var, False, 30),
            ("ID", id_var, True, 15),
            ("Контакт", contact_var, False, 20),
            ("Дата создания", created_var, True, 20),
        ]
        
        for i, (lbl, var, readonly, w) in enumerate(fields):
            ttk.Label(form, text=lbl, style="Body.TLabel").grid(row=i, column=0, sticky="w", pady=6)
            state = "readonly" if readonly else "normal"
            ttk.Entry(form, textvariable=var, width=w, state=state).grid(row=i, column=1, sticky="w", pady=6, padx=(12, 0))
        
        def save():
            name = name_var.get().strip()
            if not name:
                messagebox.showwarning("⚠️ Внимание", "Название обязательно", parent=dialog)
                return
            
            try:
                if mode == "create":
                    self.db.execute(
                        "INSERT INTO clients(code, name, contact, created_at) VALUES(?,?,?,?)",
                        (self._next_client_code(), name, contact_var.get().strip(), created_var.get().strip())
                    )
                else:
                    self.db.execute(
                        "UPDATE clients SET name=?, contact=? WHERE id=?",
                        (name, contact_var.get().strip(), client_id)
                    )
            except sqlite3.IntegrityError:
                messagebox.showerror("❌ Ошибка", "Клиент уже существует", parent=dialog)
                return
            
            self.refresh_all()
            dialog.destroy()
        
        btns = ttk.Frame(main, style="Card.TFrame")
        btns.pack(fill="x", pady=(20, 0))
        
        ttk.Button(btns, text="❌ Отмена", style="Secondary.TButton",
                  command=dialog.destroy).pack(side="left")
        ttk.Button(btns, text="✅ Сохранить", style="Success.TButton",
                  command=save).pack(side="right")

    def delete_client(self):
        """Удаление клиента"""
        sel = self.clients_tree.selection()
        if not sel:
            messagebox.showwarning("⚠️ Внимание", "Выберите клиента")
            return
        vals = self.clients_tree.item(sel[0], "values")
        if messagebox.askyesno("🗑️ Удаление", f"Удалить «{vals[1]}»?"):
            self.db.execute("DELETE FROM clients WHERE id=?", (int(vals[0]),))
            self.refresh_all()

    # ================== КАТЕГОРИИ - ОПЕРАЦИИ ==================
    
    def create_category(self):
        """Создание категории"""
        name = self.new_category_var.get().strip()
        if not name:
            messagebox.showwarning("⚠️ Внимание", "Введите название")
            return
        try:
            self.db.execute("INSERT INTO categories(name) VALUES(?)", (name,))
        except sqlite3.IntegrityError:
            messagebox.showerror("❌ Ошибка", "Категория уже существует")
            return
        self.new_category_var.set("")
        self.refresh_all()

    def create_subcategory(self):
        """Создание подкатегории"""
        name = self.new_subcategory_var.get().strip()
        cat = self.subcategory_parent_var.get().strip()
        if not name or not cat:
            messagebox.showwarning("⚠️ Внимание", "Укажите название и категорию")
            return
        cat_id = self._get_id(cat)
        try:
            self.db.execute("INSERT INTO subcategories(category_id, name) VALUES(?,?)", (cat_id, name))
        except sqlite3.IntegrityError:
            messagebox.showerror("❌ Ошибка", "Подкатегория уже существует")
            return
        self.new_subcategory_var.set("")
        self.refresh_all()

    # ================== НОМЕНКЛАТУРА - ОПЕРАЦИИ ==================
    
    def _next_article(self):
        row = self.db.query("SELECT article FROM products WHERE article LIKE 'ART-%' ORDER BY id DESC LIMIT 1")
        if not row or not row[0][0]:
            return "ART-00001"
        try:
            nxt = int(str(row[0][0]).split('-')[-1]) + 1
        except:
            nxt = self.db.query("SELECT COUNT(*) FROM products")[0][0] + 1
        return f"ART-{nxt:05d}"

    def search_nomenclature(self):
        """Поиск номенклатуры"""
        self.nom_searched = True
        self.refresh_nomenclature()

    def create_product(self):
        """Создание товара"""
        self._product_dialog("create")

    def edit_product(self):
        """Редактирование товара"""
        sel = self.nomenclature_tree.selection()
        if not sel:
            messagebox.showwarning("⚠️ Внимание", "Выберите товар")
            return
        pid = int(self.nomenclature_tree.item(sel[0], "values")[0])
        self._product_dialog("edit", pid)

    def _product_dialog(self, mode, product_id=None):
        """Диалог товара"""
        dialog = self._create_fullscreen_dialog("📦 Карточка товара", 700, 580)
        
        main = ttk.Frame(dialog, style="Card.TFrame")
        main.pack(fill="both", expand=True, padx=30, pady=25)
        
        title = "Создание карточки товара" if mode == "create" else "Редактирование карточки товара"
        ttk.Label(main, text=title, style="Heading.TLabel").pack(anchor="w", pady=(0, 20))
        
        # Variables
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
        
        # Form
        form = ttk.Frame(main, style="Card.TFrame")
        form.pack(fill="both", expand=True)
        
        left = ttk.Frame(form, style="Card.TFrame")
        left.pack(side="left", fill="both", expand=True, padx=(0, 15))
        
        right = ttk.Frame(form, style="Card.TFrame")
        right.pack(side="left", fill="both", expand=True)
        
        def add_field(parent, label, widget):
            frame = ttk.Frame(parent, style="Card.TFrame")
            frame.pack(fill="x", pady=6)
            ttk.Label(frame, text=label, style="Body.TLabel").pack(anchor="w")
            widget.pack(fill="x", pady=(4, 0))
        
        # Left column
        add_field(left, "Артикул", ttk.Entry(left, textvariable=article_var, state="readonly"))
        add_field(left, "Марка / Название *", ttk.Entry(left, textvariable=brand_var))
        add_field(left, "Поставщик *", ttk.Combobox(left, textvariable=supplier_var,
                                                    values=list(suppliers.keys()), state="readonly"))
        add_field(left, "3PL клиент *", ttk.Combobox(left, textvariable=client_var,
                                                     values=list(clients.keys()), state="readonly"))
        add_field(left, "Единица измерения", ttk.Combobox(left, textvariable=unit_var,
                                                          values=["Шт", "Палета"], state="readonly"))
        add_field(left, "Серийный учёт", ttk.Combobox(left, textvariable=serial_var,
                                                      values=["Да", "Нет"], state="readonly"))
        
        # Right column
        category_box = ttk.Combobox(right, textvariable=category_var,
                                    values=list(categories.keys()), state="readonly")
        add_field(right, "Категория *", category_box)
        
        subcategory_box = ttk.Combobox(right, textvariable=subcategory_var, state="readonly")
        add_field(right, "Подкатегория *", subcategory_box)
        
        add_field(right, "Объём (м³)", ttk.Entry(right, textvariable=volume_var))
        add_field(right, "Вес (кг)", ttk.Entry(right, textvariable=weight_var))
        add_field(right, "Штрих-код", ttk.Entry(right, textvariable=barcode_var))
        add_field(right, "Владелец", ttk.Entry(right, textvariable=owner_var, state="readonly"))
        
        def load_subcategories(*_):
            cat = category_var.get().strip()
            if not cat:
                subcategory_box["values"] = []
                return
            cat_id = self._get_id(cat)
            rows = self.db.query("SELECT id, name FROM subcategories WHERE category_id=? ORDER BY name", (cat_id,))
            vals = [f"{r[0]} | {r[1]}" for r in rows]
            subcategory_box["values"] = vals
            if vals:
                subcategory_var.set(vals[0])
        
        category_var.trace_add("write", load_subcategories)
        
        # Load data for edit
        if mode == "edit" and product_id:
            row = self.db.query(
                """SELECT article, brand, supplier_id, client_id, unit, volume, weight, 
                          barcode, serial_tracking, category_id, subcategory_id, product_owner
                   FROM products WHERE id=?""", (product_id,)
            )
            if row:
                r = row[0]
                article_var.set(r[0] or "")
                brand_var.set(r[1] or "")
                supplier_var.set(next((k for k, v in suppliers.items() if v == r[2]), ""))
                client_var.set(next((k for k, v in clients.items() if v == r[3]), ""))
                unit_var.set(r[4] or "Шт")
                volume_var.set("" if r[5] is None else str(r[5]))
                weight_var.set("" if r[6] is None else str(r[6]))
                barcode_var.set(r[7] or "")
                serial_var.set(r[8] or "Нет")
                category_var.set(next((k for k, v in categories.items() if v == r[9]), ""))
                load_subcategories()
                subcats = {f"{x[0]} | {x[1]}": x[0] for x in self.db.query("SELECT id, name FROM subcategories")}
                subcategory_var.set(next((k for k, v in subcats.items() if v == r[10]), ""))
                owner_var.set(r[11] or self.current_user)
        else:
            load_subcategories()
        
        def save():
            brand = brand_var.get().strip()
            if not brand:
                messagebox.showwarning("⚠️ Внимание", "Марка обязательна", parent=dialog)
                return
            
            supplier_id = self._get_id(supplier_var.get())
            client_id = self._get_id(client_var.get())
            if not supplier_id or not client_id:
                messagebox.showwarning("⚠️ Внимание", "Укажите поставщика и клиента", parent=dialog)
                return
            
            category_id = self._get_id(category_var.get())
            subcategory_id = self._get_id(subcategory_var.get())
            if not category_id or not subcategory_id:
                messagebox.showwarning("⚠️ Внимание", "Выберите категорию и подкатегорию", parent=dialog)
                return
            
            try:
                volume = self._normalize_decimal(volume_var.get())
                weight = self._normalize_decimal(weight_var.get())
            except ValueError:
                messagebox.showwarning("⚠️ Внимание", "Некорректный формат числа", parent=dialog)
                return
            
            # Check subcategory belongs to category
            check = self.db.query("SELECT id FROM subcategories WHERE id=? AND category_id=?",
                                  (subcategory_id, category_id))
            if not check:
                messagebox.showwarning("⚠️ Внимание", "Подкатегория не соответствует категории", parent=dialog)
                return
            
            try:
                if mode == "create":
                    self.db.execute(
                        """INSERT INTO products(name, article, brand, supplier_id, client_id, unit,
                           volume, weight, barcode, serial_tracking, category_id, subcategory_id,
                           product_owner, created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                        (brand, article_var.get().strip(), brand, supplier_id, client_id,
                         unit_var.get().strip(), volume, weight, barcode_var.get().strip(),
                         serial_var.get().strip(), category_id, subcategory_id,
                         owner_var.get().strip(), datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                    )
                else:
                    self.db.execute(
                        """UPDATE products SET name=?, article=?, brand=?, supplier_id=?, client_id=?,
                           unit=?, volume=?, weight=?, barcode=?, serial_tracking=?, category_id=?,
                           subcategory_id=?, product_owner=? WHERE id=?""",
                        (brand, article_var.get().strip(), brand, supplier_id, client_id,
                         unit_var.get().strip(), volume, weight, barcode_var.get().strip(),
                         serial_var.get().strip(), category_id, subcategory_id,
                         owner_var.get().strip(), product_id)
                    )
            except sqlite3.IntegrityError as e:
                messagebox.showerror("❌ Ошибка", f"Ошибка сохранения: {e}", parent=dialog)
                return
            
            self.refresh_all()
            dialog.destroy()
        
        btns = ttk.Frame(main, style="Card.TFrame")
        btns.pack(fill="x", pady=(20, 0))
        
        ttk.Button(btns, text="❌ Отмена", style="Secondary.TButton",
                  command=dialog.destroy).pack(side="left")
        ttk.Button(btns, text="✅ Сохранить", style="Success.TButton",
                  command=save).pack(side="right")

    def delete_product(self):
        """Удаление товара"""
        sel = self.nomenclature_tree.selection()
        if not sel:
            messagebox.showwarning("⚠️ Внимание", "Выберите товар")
            return
        vals = self.nomenclature_tree.item(sel[0], "values")
        name = vals[2] or vals[1] or f"ID: {vals[0]}"
        if messagebox.askyesno("🗑️ Удаление", f"Удалить «{name}»?"):
            self.db.execute("DELETE FROM products WHERE id=?", (int(vals[0]),))
            self.refresh_all()

        # ================== ПРИХОДЫ - ОПЕРАЦИИ ==================
    
    def search_inbound(self):
        """Поиск приходов"""
        self.inb_searched = True
        self.refresh_inbound()

    def _next_order_number(self):
        row = self.db.query("SELECT order_number FROM inbound_orders ORDER BY id DESC LIMIT 1")
        if not row or not row[0][0]:
            return "IN-00001"
        try:
            nxt = int(row[0][0].split("-")[-1]) + 1
        except:
            nxt = self.db.query("SELECT COUNT(*) FROM inbound_orders")[0][0] + 1
        return f"IN-{nxt:05d}"

    def create_inbound_order(self):
        """Создание приходного заказа"""
        dialog = self._create_fullscreen_dialog("📥 Создание приходного заказа", 1150, 700)
        
        main = ttk.Frame(dialog, style="Card.TFrame")
        main.pack(fill="both", expand=True, padx=25, pady=20)
        
        ttk.Label(main, text="Создание нового приходного заказа", style="Heading.TLabel").pack(anchor="w", pady=(0, 15))
        
        # Header variables
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
        header = ttk.Frame(main, style="Card.TFrame")
        header.pack(fill="x", pady=(0, 10))
        
        row1 = ttk.Frame(header, style="Card.TFrame")
        row1.pack(fill="x", pady=4)
        
        ttk.Label(row1, text="№ заказа:", style="Body.TLabel").pack(side="left", padx=(0, 4))
        ttk.Entry(row1, textvariable=order_num_var, state="readonly", width=12).pack(side="left", padx=(0, 15))
        
        ttk.Label(row1, text="Дата:", style="Body.TLabel").pack(side="left", padx=(0, 4))
        ttk.Entry(row1, textvariable=created_var, state="readonly", width=18).pack(side="left", padx=(0, 15))
        
        ttk.Label(row1, text="Статус:", style="Body.TLabel").pack(side="left", padx=(0, 4))
        ttk.Entry(row1, textvariable=status_var, state="readonly", width=10).pack(side="left", padx=(0, 15))
        
        ttk.Label(row1, text="Создал:", style="Body.TLabel").pack(side="left", padx=(0, 4))
        ttk.Entry(row1, textvariable=created_by_var, state="readonly", width=12).pack(side="left")
        
        row2 = ttk.Frame(header, style="Card.TFrame")
        row2.pack(fill="x", pady=4)
        
        ttk.Label(row2, text="Поставщик *:", style="Body.TLabel").pack(side="left", padx=(0, 4))
        ttk.Combobox(row2, textvariable=supplier_var, values=list(suppliers.keys()),
                    state="readonly", width=22).pack(side="left", padx=(0, 15))
        
        ttk.Label(row2, text="3PL клиент *:", style="Body.TLabel").pack(side="left", padx=(0, 4))
        ttk.Combobox(row2, textvariable=client_var, values=list(clients.keys()),
                    state="readonly", width=22).pack(side="left", padx=(0, 15))
        
        ttk.Label(row2, text="Склад *:", style="Body.TLabel").pack(side="left", padx=(0, 4))
        ttk.Combobox(row2, textvariable=warehouse_var, values=list(warehouses.keys()),
                    state="readonly", width=18).pack(side="left")
        
        ttk.Separator(main).pack(fill="x", pady=10)
        
        # Add items section
        ttk.Label(main, text="➕ Добавление позиций", style="Heading.TLabel").pack(anchor="w", pady=(0, 8))
        
        line_cat_var = tk.StringVar()
        line_sub_var = tk.StringVar()
        line_prod_var = tk.StringVar()
        line_qty_var = tk.StringVar(value="1")
        line_unit_var = tk.StringVar()
        
        add_row = ttk.Frame(main, style="Card.TFrame")
        add_row.pack(fill="x", pady=(0, 8))
        
        ttk.Label(add_row, text="Категория:", style="Body.TLabel").pack(side="left", padx=(0, 4))
        line_cat_box = ttk.Combobox(add_row, textvariable=line_cat_var,
                                    values=list(categories.keys()), state="readonly", width=18)
        line_cat_box.pack(side="left", padx=(0, 12))
        
        ttk.Label(add_row, text="Подкатегория:", style="Body.TLabel").pack(side="left", padx=(0, 4))
        line_sub_box = ttk.Combobox(add_row, textvariable=line_sub_var, state="readonly", width=18)
        line_sub_box.pack(side="left", padx=(0, 12))
        
        ttk.Label(add_row, text="Товар:", style="Body.TLabel").pack(side="left", padx=(0, 4))
        line_prod_box = ttk.Combobox(add_row, textvariable=line_prod_var, state="readonly", width=22)
        line_prod_box.pack(side="left", padx=(0, 12))
        
        ttk.Label(add_row, text="Кол-во:", style="Body.TLabel").pack(side="left", padx=(0, 4))
        ttk.Entry(add_row, textvariable=line_qty_var, width=8).pack(side="left", padx=(0, 4))
        ttk.Label(add_row, textvariable=line_unit_var, style="Body.TLabel").pack(side="left", padx=(0, 12))
        
        ttk.Button(add_row, text="➕ Добавить", style="Success.TButton",
                  command=lambda: add_line()).pack(side="left")
        
        # Items tree
        order_items = []
        product_catalog = {}
        
        tree_frame = ttk.Frame(main, style="Card.TFrame")
        tree_frame.pack(fill="both", expand=True, pady=(0, 10))
        
        cols = ("category", "subcategory", "article", "product", "unit", "qty", "weight", "volume")
        items_tree = ttk.Treeview(tree_frame, columns=cols, show="headings", height=12)
        
        vsb = ttk.Scrollbar(tree_frame, orient="vertical", command=items_tree.yview)
        items_tree.configure(yscrollcommand=vsb.set)
        
        items_tree.pack(side="left", fill="both", expand=True)
        vsb.pack(side="right", fill="y")
        
        for col, title, w in [
            ("category", "Категория", 130), ("subcategory", "Подкатегория", 140),
            ("article", "Артикул", 100), ("product", "Товар", 180),
            ("unit", "Ед.", 60), ("qty", "Кол-во", 80),
            ("weight", "Вес", 80), ("volume", "Объём", 80),
        ]:
            items_tree.heading(col, text=title)
            items_tree.column(col, width=w, minwidth=40)
        
        def refresh_subcats(*_):
            cat = line_cat_var.get().strip()
            if not cat:
                line_sub_box["values"] = []
                line_prod_box["values"] = []
                return
            cat_id = self._get_id(cat)
            rows = self.db.query("SELECT id, name FROM subcategories WHERE category_id=? ORDER BY name", (cat_id,))
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
                """SELECT id, article, brand, unit, COALESCE(weight,0), COALESCE(volume,0)
                   FROM products WHERE subcategory_id=? AND category_id=? ORDER BY brand""",
                (sub_id, cat_id)
            )
            product_catalog.clear()
            vals = []
            for pid, art, brand, unit, w, v in rows:
                token = f"{pid} | {art or ''} | {brand or ''}"
                vals.append(token)
                product_catalog[token] = {
                    "id": pid, "article": art or "", "brand": brand or "",
                    "unit": unit or "Шт", "weight": float(w), "volume": float(v)
                }
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
                messagebox.showwarning("⚠️ Внимание", "Выберите категорию, подкатегорию и товар", parent=dialog)
                return
            
            try:
                qty = float(line_qty_var.get())
                if qty <= 0:
                    raise ValueError
            except ValueError:
                messagebox.showwarning("⚠️ Внимание", "Некорректное количество", parent=dialog)
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
                item["category_name"], item["subcategory_name"], item["article"],
                item["brand"], item["unit"], f"{qty:.2f}",
                f"{item['planned_weight']:.3f}", f"{item['planned_volume']:.4f}"
            ))
        
        def save_order():
            supplier_id = self._get_id(supplier_var.get())
            client_id = self._get_id(client_var.get())
            warehouse_id = self._get_id(warehouse_var.get())
            
            if not supplier_id or not client_id or not warehouse_id:
                messagebox.showwarning("⚠️ Внимание", "Заполните поставщика, клиента и склад", parent=dialog)
                return
            
            if not order_items:
                messagebox.showwarning("⚠️ Внимание", "Добавьте позиции", parent=dialog)
                return
            
            try:
                order_id = self.db.execute(
                    """INSERT INTO inbound_orders(order_number, created_at, received_at, created_by,
                       supplier_id, client_id, warehouse_id, status) VALUES(?,?,NULL,?,?,?,?,?)""",
                    (order_num_var.get().strip(), created_var.get().strip(), created_by_var.get().strip(),
                     supplier_id, client_id, warehouse_id, "Новый")
                )
                
                for item in order_items:
                    self.db.execute(
                        """INSERT INTO inbound_order_items(order_id, category_id, subcategory_id,
                           product_id, planned_qty, actual_qty, actual_filled, planned_weight,
                           planned_volume, serial_numbers) VALUES(?,?,?,?,?,0,0,?,?,'')""",
                        (order_id, item["category_id"], item["subcategory_id"], item["product_id"],
                         item["planned_qty"], item["planned_weight"], item["planned_volume"])
                    )
            except sqlite3.IntegrityError as e:
                messagebox.showerror("❌ Ошибка", f"Ошибка: {e}", parent=dialog)
                return
            
            self.refresh_all()
            dialog.destroy()
            messagebox.showinfo("✅ Успешно", f"Заказ {order_num_var.get()} создан")
        
        btns = ttk.Frame(main, style="Card.TFrame")
        btns.pack(fill="x")
        
        ttk.Button(btns, text="❌ Отмена", style="Secondary.TButton",
                  command=dialog.destroy).pack(side="left")
        ttk.Button(btns, text="✅ Сохранить заказ", style="Success.TButton",
                  command=save_order).pack(side="right")

    def _open_inbound_order(self, event=None):
        """Открытие приходного заказа"""
        sel = self.inbound_tree.selection()
        if not sel:
            return
        order_num = self.inbound_tree.item(sel[0], "values")[0]
        self._inbound_order_dialog(order_num)

    def _inbound_order_dialog(self, order_number):
        """Диалог приёмки заказа"""
        rows = self.db.query(
            """SELECT o.id, o.order_number, o.created_at, COALESCE(o.received_at,''),
                      s.name, c.name, w.name, o.status, o.created_by, COALESCE(o.accepted_by,'')
               FROM inbound_orders o
               JOIN suppliers s ON s.id=o.supplier_id
               JOIN clients c ON c.id=o.client_id
               JOIN warehouses w ON w.id=o.warehouse_id
               WHERE o.order_number=?""",
            (order_number,)
        )
        
        if not rows:
            messagebox.showerror("❌ Ошибка", "Заказ не найден")
            return
        
        order_id, order_no, created_at, received_at, supplier, client, warehouse, status, created_by, accepted_by = rows[0]
        
        dialog = self._create_fullscreen_dialog(f"📥 Приёмка заказа {order_no}", 1250, 700)
        
        main = ttk.Frame(dialog, style="Card.TFrame")
        main.pack(fill="both", expand=True, padx=25, pady=20)
        
        # Title + Status
        title_row = ttk.Frame(main, style="Card.TFrame")
        title_row.pack(fill="x", pady=(0, 12))
        
        ttk.Label(title_row, text=f"Приёмка заказа {order_no}", style="Heading.TLabel").pack(side="left")
        
        status_colors = {"Новый": self.COLORS["warning"], "Принят": self.COLORS["success"]}
        status_lbl = tk.Label(title_row, text=f"  {status}  ", font=self.FONTS["body_bold"],
                             bg=status_colors.get(status, self.COLORS["primary"]), fg="white")
        status_lbl.pack(side="right")
        
        # Info grid
        info_frame = ttk.Frame(main, style="Card.TFrame")
        info_frame.pack(fill="x", pady=(0, 10))
        
        info = [
            ("№ заказа:", order_no), ("Дата создания:", created_at),
            ("Поставщик:", supplier), ("3PL клиент:", client),
            ("Склад:", warehouse), ("Создал:", created_by),
            ("Принял:", accepted_by or "—"), ("Дата приёма:", received_at or "—"),
        ]
        
        for i, (lbl, val) in enumerate(info):
            r, c = i // 4, (i % 4) * 2
            ttk.Label(info_frame, text=lbl, style="Body.TLabel").grid(
                row=r, column=c, sticky="w", padx=(0 if c == 0 else 15, 4), pady=3
            )
            ttk.Label(info_frame, text=val, font=self.FONTS["body_bold"],
                     background=self.COLORS["bg_card"]).grid(row=r, column=c+1, sticky="w", pady=3)
        
        ttk.Separator(main).pack(fill="x", pady=10)
        
        # Toolbar
        toolbar = ttk.Frame(main, style="Card.TFrame")
        toolbar.pack(fill="x", pady=(0, 8))
        
        def edit_line():
            """Ввод фактического количества"""
            sel = items_tree.selection()
            if not sel:
                messagebox.showwarning("⚠️ Внимание", "Выберите позицию", parent=dialog)
                return
            
            vals = items_tree.item(sel[0], "values")
            line_id = int(vals[0])
            product_name = vals[1]
            
            row = self.db.query(
                """SELECT i.actual_qty, COALESCE(i.serial_numbers,''), COALESCE(p.serial_tracking,'Нет')
                   FROM inbound_order_items i
                   JOIN products p ON p.id=i.product_id WHERE i.id=?""",
                (line_id,)
            )[0]
            current_actual, current_serial, serial_tracking = float(row[0]), row[1], row[2]
            
            if serial_tracking == "Да":
                edit_serial(line_id, product_name, current_serial)
                return
            
            qty_text = simpledialog.askstring(
                "📝 Ввод количества",
                f"Введите количество для приёмки:\n\nТовар: {product_name}\nТекущее: {current_actual}",
                initialvalue="1", parent=dialog
            )
            if qty_text is None:
                return
            
            try:
                delta = float(qty_text)
                if delta <= 0:
                    raise ValueError
            except ValueError:
                messagebox.showwarning("⚠️ Внимание", "Некорректное количество", parent=dialog)
                return
            
            set_actual(line_id, current_actual + delta)
            load_items()
            self.refresh_inbound()
        
        def edit_qty():
            """Редактирование фактического количества"""
            sel = items_tree.selection()
            if not sel:
                messagebox.showwarning("⚠️ Внимание", "Выберите позицию", parent=dialog)
                return
            
            vals = items_tree.item(sel[0], "values")
            line_id = int(vals[0])
            product_name = vals[1]
            
            row = self.db.query(
                """SELECT i.actual_qty, COALESCE(i.serial_numbers,''), COALESCE(p.serial_tracking,'Нет')
                   FROM inbound_order_items i
                   JOIN products p ON p.id=i.product_id WHERE i.id=?""",
                (line_id,)
            )[0]
            current_actual, current_serial, serial_tracking = float(row[0]), row[1], row[2]
            
            if serial_tracking == "Да":
                edit_serial(line_id, product_name, current_serial)
                return
            
            qty_text = simpledialog.askstring(
                "✏️ Редактирование количества",
                f"Введите новое количество:\n\nТовар: {product_name}",
                initialvalue=str(current_actual), parent=dialog
            )
            if qty_text is None:
                return
            
            try:
                new_qty = float(qty_text)
                if new_qty < 0:
                    raise ValueError
            except ValueError:
                messagebox.showwarning("⚠️ Внимание", "Некорректное количество", parent=dialog)
                return
            
            set_actual(line_id, new_qty)
            load_items()
            self.refresh_inbound()
        
        def accept_order():
            """Принять заказ"""
            state = self.db.query("SELECT status FROM inbound_orders WHERE id=?", (order_id,))[0][0]
            if state == "Принят":
                messagebox.showinfo("ℹ️ Информация", "Заказ уже принят", parent=dialog)
                return
            
            check_rows = self.db.query(
                """SELECT i.id, i.planned_qty, i.actual_qty, i.actual_filled, i.product_id,
                          COALESCE(i.serial_numbers,''), COALESCE(p.serial_tracking,'Нет')
                   FROM inbound_order_items i
                   JOIN products p ON p.id=i.product_id WHERE i.order_id=?""",
                (order_id,)
            )
            
            if not check_rows:
                messagebox.showwarning("⚠️ Внимание", "Нет позиций", parent=dialog)
                return
            
            not_filled = [r for r in check_rows if int(r[3]) != 1]
            if not_filled:
                messagebox.showwarning("⚠️ Внимание",
                                      f"Заполните фактическое количество.\nНе заполнено: {len(not_filled)} позиций",
                                      parent=dialog)
                return
            
            # Check discrepancies
            discrepancies = []
            for _, planned, actual, _, _, serials, serial_tracking in check_rows:
                p, a = float(planned), float(actual)
                if a > p:
                    discrepancies.append(f"Излишек: план {p}, факт {a}")
                elif a < p:
                    discrepancies.append(f"Недостача: план {p}, факт {a}")
                
                if serial_tracking == "Да":
                    serial_count = len([x for x in (serials or "").split(",") if x.strip()])
                    if int(a) != serial_count:
                        messagebox.showwarning("⚠️ Внимание",
                                              "Количество серийных номеров должно совпадать с фактом",
                                              parent=dialog)
                        return
            
            if discrepancies:
                msg = "Обнаружены расхождения:\n\n" + "\n".join(discrepancies[:5])
                if len(discrepancies) > 5:
                    msg += "\n..."
                msg += "\n\nВсё равно принять?"
                if not messagebox.askyesno("⚠️ Подтверждение", msg, parent=dialog):
                    return
            
            # Create movements
            for _, _, actual, _, product_id, _, _ in check_rows:
                qty = float(actual)
                if qty > 0:
                    if abs(qty - int(qty)) > 1e-9:
                        messagebox.showwarning("⚠️ Внимание",
                                              "Количество должно быть целым числом",
                                              parent=dialog)
                        return
                    self.db.execute(
                        """INSERT INTO movements(product_id, movement_type, quantity, reference, moved_at, note)
                           VALUES(?, 'IN', ?, ?, ?, ?)""",
                        (product_id, int(qty), order_no,
                         datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "Приём по заказу")
                    )
            
            self.db.execute(
                "UPDATE inbound_orders SET status='Принят', received_at=?, accepted_by=? WHERE id=?",
                (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), self.current_user, order_id)
            )
            
            messagebox.showinfo("✅ Успешно", "Заказ принят", parent=dialog)
            self.refresh_all()
            dialog.destroy()
        
        def set_actual(line_id, qty, serials=None):
            if serials is None:
                self.db.execute("UPDATE inbound_order_items SET actual_qty=?, actual_filled=1 WHERE id=?",
                               (qty, line_id))
            else:
                self.db.execute(
                    "UPDATE inbound_order_items SET actual_qty=?, actual_filled=1, serial_numbers=? WHERE id=?",
                    (qty, serials, line_id)
                )
        
        def edit_serial(line_id, product_name, current):
            """Диалог серийных номеров"""
            ser_dialog = tk.Toplevel(dialog)
            ser_dialog.title(f"🔢 Серийные номера: {product_name}")
            ser_dialog.configure(bg=self.COLORS["bg_card"])
            ser_dialog.transient(dialog)
            ser_dialog.grab_set()
            self._center_dialog(ser_dialog, 550, 450)
            
            ser_main = ttk.Frame(ser_dialog, style="Card.TFrame")
            ser_main.pack(fill="both", expand=True, padx=20, pady=20)
            
            ttk.Label(ser_main, text=f"Товар: {product_name}", style="Heading.TLabel").pack(anchor="w", pady=(0, 12))
            
            # Input
            inp_frame = ttk.Frame(ser_main, style="Card.TFrame")
            inp_frame.pack(fill="x", pady=(0, 10))
            
            ttk.Label(inp_frame, text="Серийный номер:", style="Body.TLabel").pack(side="left", padx=(0, 8))
            ser_input = tk.StringVar()
            ser_entry = ttk.Entry(inp_frame, textvariable=ser_input, width=25)
            ser_entry.pack(side="left", padx=(0, 8))
            ser_entry.focus_set()
            
            def add_ser(e=None):
                val = ser_input.get().strip()
                if not val:
                    return
                existing = [listbox.get(i) for i in range(listbox.size())]
                if val in existing:
                    messagebox.showwarning("⚠️ Внимание", "Уже добавлено", parent=ser_dialog)
                    return
                listbox.insert("end", val)
                ser_input.set("")
                ser_entry.focus_set()
                refresh_cnt()
            
            ttk.Button(inp_frame, text="➕ Добавить", style="Success.TButton",
                      command=add_ser).pack(side="left")
            
            # List
            serials = [x.strip() for x in (current or "").split(",") if x.strip()]
            
            list_frame = ttk.Frame(ser_main, style="Card.TFrame")
            list_frame.pack(fill="both", expand=True, pady=8)
            
            listbox = tk.Listbox(list_frame, font=self.FONTS["body"], height=10)
            list_vsb = ttk.Scrollbar(list_frame, orient="vertical", command=listbox.yview)
            listbox.configure(yscrollcommand=list_vsb.set)
            
            listbox.pack(side="left", fill="both", expand=True)
            list_vsb.pack(side="right", fill="y")
            
            for s in serials:
                listbox.insert("end", s)
            
            count_var = tk.StringVar(value=f"📊 Отсканировано: {len(serials)}")
            ttk.Label(ser_main, textvariable=count_var, style="Body.TLabel").pack(anchor="w", pady=(4, 0))
            
            def refresh_cnt():
                count_var.set(f"📊 Отсканировано: {listbox.size()}")
            
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
            
            ser_entry.bind("<Return>", add_ser)
            
            btn_frame = ttk.Frame(ser_main, style="Card.TFrame")
            btn_frame.pack(fill="x", pady=(12, 0))
            
            ttk.Button(btn_frame, text="🗑️ Удалить", style="Danger.TButton",
                      command=remove_ser).pack(side="left")
            ttk.Button(btn_frame, text="✅ Завершить", style="Success.TButton",
                      command=finish).pack(side="right")
        
        ttk.Button(toolbar, text="📝 Ввести фактическое", style="Primary.TButton",
                  command=edit_line).pack(side="left", padx=(0, 8))
        ttk.Button(toolbar, text="✏️ Редактировать кол-во", style="Secondary.TButton",
                  command=edit_qty).pack(side="left", padx=(0, 8))
        
        if status == "Новый":
            ttk.Button(toolbar, text="✅ Принять заказ", style="Success.TButton",
                      command=accept_order).pack(side="left")
        
        # Items tree
        tree_frame = ttk.Frame(main, style="Card.TFrame")
        tree_frame.pack(fill="both", expand=True)
        
        cols = ("line_id", "product", "article", "category", "subcategory", "unit",
               "planned", "actual", "weight", "volume", "barcode", "serials", "serial_tracking")
        
        items_tree = ttk.Treeview(tree_frame, columns=cols, show="headings", height=14)
        
        vsb = ttk.Scrollbar(tree_frame, orient="vertical", command=items_tree.yview)
        hsb = ttk.Scrollbar(tree_frame, orient="horizontal", command=items_tree.xview)
        items_tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        
        items_tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")
        
        tree_frame.grid_rowconfigure(0, weight=1)
        tree_frame.grid_columnconfigure(0, weight=1)
        
        for col, title, w in [
            ("line_id", "ID", 40), ("product", "Товар", 180), ("article", "Артикул", 90),
            ("category", "Категория", 110), ("subcategory", "Подкатегория", 120),
            ("unit", "Ед.", 50), ("planned", "План", 70), ("actual", "Факт", 70),
            ("weight", "Вес", 70), ("volume", "Объём", 70), ("barcode", "Штрих-код", 100),
            ("serials", "Серий", 60), ("serial_tracking", "Серийный", 70),
        ]:
            items_tree.heading(col, text=title)
            items_tree.column(col, width=w, minwidth=35)
        
        items_tree.tag_configure("less", background=self.COLORS["warning_light"])
        items_tree.tag_configure("more", background=self.COLORS["error_light"])
        items_tree.tag_configure("ok", background=self.COLORS["success_light"])
        items_tree.tag_configure("empty", background="#f5f5f5")
        
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
        
        def load_items():
            for i in items_tree.get_children():
                items_tree.delete(i)
            
            rows = self.db.query(
                """SELECT i.id, COALESCE(p.name,p.brand,''), COALESCE(p.article,''),
                          COALESCE(cat.name,''), COALESCE(sub.name,''), COALESCE(p.unit,''),
                          i.planned_qty, i.actual_qty, COALESCE(p.weight,0)*i.actual_qty,
                          COALESCE(p.volume,0)*i.actual_qty, COALESCE(p.barcode,''),
                          COALESCE(i.serial_numbers,''), COALESCE(p.serial_tracking,'Нет'), i.actual_filled
                   FROM inbound_order_items i
                   JOIN products p ON p.id=i.product_id
                   LEFT JOIN categories cat ON cat.id=i.category_id
                   LEFT JOIN subcategories sub ON sub.id=i.subcategory_id
                   WHERE i.order_id=? ORDER BY i.id""",
                (order_id,)
            )
            
            for r in rows:
                tag = get_tag(float(r[6]), float(r[7]), int(r[13]))
                items_tree.insert("", "end", values=(
                    r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7],
                    f"{r[8]:.2f}", f"{r[9]:.3f}", r[10], serial_count(r[11]), r[12]
                ), tags=(tag,))
        
        load_items()

    # ================== ДВИЖЕНИЯ - ОПЕРАЦИИ ==================
    
    def add_movement(self):
        """Добавление движения"""
        token = self.mov_product_var.get().strip()
        if not token:
            messagebox.showwarning("⚠️ Внимание", "Выберите товар")
            return
        
        product_id = self._get_id(token)
        movement_type = self.mov_type_var.get().strip()
        
        try:
            qty = int(self.mov_qty_var.get())
            if qty <= 0:
                raise ValueError
        except ValueError:
            messagebox.showwarning("⚠️ Внимание", "Количество должно быть целым положительным числом")
            return
        
        if movement_type == "OUT":
            stock = self._get_stock_by_product()
            available = stock.get(product_id, 0)
            if qty > available:
                messagebox.showerror("❌ Ошибка", f"Недостаточно остатка.\nДоступно: {available}\nТребуется: {qty}")
                return
        
        self.db.execute(
            """INSERT INTO movements(product_id, movement_type, quantity, reference, moved_at, note)
               VALUES(?,?,?,?,?,?)""",
            (product_id, movement_type, qty, self.mov_ref_var.get().strip(),
             datetime.now().strftime("%Y-%m-%d %H:%M:%S"), self.mov_note_var.get().strip())
        )
        
        self.mov_qty_var.set("1")
        self.mov_ref_var.set("")
        self.mov_note_var.set("")
        self.refresh_all()
        messagebox.showinfo("✅ Успешно", f"Движение {movement_type} на {qty} ед. проведено")

    def _get_stock_by_product(self):
        """Получить остатки по товарам"""
        rows = self.db.query(
            """SELECT p.id, COALESCE(SUM(CASE WHEN m.movement_type='IN' THEN m.quantity ELSE -m.quantity END), 0)
               FROM products p
               LEFT JOIN movements m ON m.product_id=p.id
               GROUP BY p.id"""
        )
        return {pid: stock for pid, stock in rows}

    # ================== ОБНОВЛЕНИЕ ДАННЫХ ==================
    
    def refresh_all(self):
        """Обновление всех данных"""
        self.refresh_suppliers()
        self.refresh_clients()
        self.refresh_categories()
        self.refresh_nomenclature()
        self.refresh_inbound()
        self.refresh_movements()
        self.refresh_stock()
        self.refresh_metrics()

    def refresh_suppliers(self):
        """Обновление списка поставщиков"""
        for item in self.suppliers_tree.get_children():
            self.suppliers_tree.delete(item)
        
        rows = self.db.query(
            "SELECT id, name, COALESCE(phone,''), COALESCE(created_at,'') FROM suppliers ORDER BY id DESC"
        )
        term = self.suppliers_search_var.get().strip().lower()
        
        for row in rows:
            if term and term not in (row[1] or "").lower():
                continue
            self.suppliers_tree.insert("", "end", values=row)

    def refresh_clients(self):
        """Обновление списка клиентов"""
        for item in self.clients_tree.get_children():
            self.clients_tree.delete(item)
        
        rows = self.db.query(
            "SELECT id, name, COALESCE(contact,''), COALESCE(created_at,'') FROM clients ORDER BY id DESC"
        )
        term = self.clients_search_var.get().strip().lower()
        
        for row in rows:
            if term and term not in (row[1] or "").lower():
                continue
            self.clients_tree.insert("", "end", values=row)

    def refresh_categories(self):
        """Обновление категорий"""
        for item in self.categories_tree.get_children():
            self.categories_tree.delete(item)
        for item in self.subcategories_tree.get_children():
            self.subcategories_tree.delete(item)
        
        term = self.categories_filter_var.get().strip().lower()
        
        # Categories
        cat_rows = self.db.query("SELECT id, name FROM categories ORDER BY name")
        for row in cat_rows:
            if term and term not in (row[1] or "").lower():
                continue
            self.categories_tree.insert("", "end", values=row)
        
        # Subcategories
        sub_rows = self.db.query(
            """SELECT s.id, s.name, COALESCE(c.name,'')
               FROM subcategories s
               LEFT JOIN categories c ON c.id=s.category_id
               ORDER BY c.name, s.name"""
        )
        for row in sub_rows:
            full = f"{row[1]} {row[2]}".lower()
            if term and term not in full:
                continue
            self.subcategories_tree.insert("", "end", values=row)
        
        # Update parent combobox
        vals = [f"{r[0]} | {r[1]}" for r in cat_rows]
        self.subcat_parent_box["values"] = vals
        if vals and not self.subcategory_parent_var.get():
            self.subcategory_parent_var.set(vals[0])

    def refresh_nomenclature(self):
        """Обновление номенклатуры"""
        for item in self.nomenclature_tree.get_children():
            self.nomenclature_tree.delete(item)
        
        if not self.nom_searched:
            return
        
        rows = self.db.query(
            """SELECT p.id, COALESCE(p.article,''), p.brand, s.name, c.name, p.unit,
                      p.volume, p.weight, p.barcode, p.serial_tracking, cat.name, sub.name, p.product_owner
               FROM products p
               LEFT JOIN suppliers s ON s.id=p.supplier_id
               LEFT JOIN clients c ON c.id=p.client_id
               LEFT JOIN categories cat ON cat.id=p.category_id
               LEFT JOIN subcategories sub ON sub.id=p.subcategory_id
               ORDER BY p.id DESC"""
        )
        
        f_brand = self.nom_brand_var.get().strip().lower()
        f_article = self.nom_article_var.get().strip().lower()
        f_supplier = self.nom_supplier_var.get().strip().lower()
        f_client = self.nom_client_var.get().strip().lower()
        
        for row in rows:
            brand = (row[2] or "").lower()
            article = (row[1] or "").lower()
            supplier = (row[3] or "").lower()
            client = (row[4] or "").lower()
            
            if f_brand and f_brand not in brand:
                continue
            if f_article and f_article not in article:
                continue
            if f_supplier and f_supplier not in supplier:
                continue
            if f_client and f_client not in client:
                continue
            
            self.nomenclature_tree.insert("", "end", values=row)
        
        # Update movements product combobox
        prod_vals = [f"{r[0]} | {r[1] or ''} | {r[2] or ''}"
                    for r in self.db.query("SELECT id, article, brand FROM products ORDER BY id DESC")]
        self.mov_product_box["values"] = prod_vals
        if prod_vals and not self.mov_product_var.get():
            self.mov_product_var.set(prod_vals[0])

    def refresh_inbound(self):
        """Обновление приходных заказов"""
        for item in self.inbound_tree.get_children():
            self.inbound_tree.delete(item)
        
        if not self.inb_searched:
            return
        
        rows = self.db.query(
            """SELECT o.order_number, o.created_at, COALESCE(o.received_at,''),
                      o.created_by, COALESCE(o.accepted_by,''), s.name, c.name, w.name, o.status,
                      COALESCE(SUM(i.planned_qty),0), COALESCE(SUM(i.actual_qty),0)
               FROM inbound_orders o
               JOIN suppliers s ON s.id=o.supplier_id
               JOIN clients c ON c.id=o.client_id
               JOIN warehouses w ON w.id=o.warehouse_id
               LEFT JOIN inbound_order_items i ON i.order_id=o.id
               GROUP BY o.id ORDER BY o.id DESC"""
        )
        
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
            status = row[8] or ""
            
            if f_search and f_search not in order_num:
                continue
            if f_status and f_status != "Все" and status != f_status:
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
            
            tag = "new" if status == "Новый" else "accepted"
            self.inbound_tree.insert("", "end", values=row, tags=(tag,))

    def refresh_movements(self):
        """Обновление движений"""
        for item in self.movements_tree.get_children():
            self.movements_tree.delete(item)
        
        rows = self.db.query(
            """SELECT m.id, p.brand, m.movement_type, m.quantity, m.reference, m.moved_at, m.note
               FROM movements m
               JOIN products p ON p.id=m.product_id
               ORDER BY m.id DESC"""
        )
        
        for row in rows:
            tag = "in" if row[2] == "IN" else "out"
            self.movements_tree.insert("", "end", values=row, tags=(tag,))

    def refresh_stock(self):
        """Обновление остатков"""
        for item in self.stock_tree.get_children():
            self.stock_tree.delete(item)
        
        rows = self.db.query(
            """SELECT p.brand, c.name, p.unit,
                      COALESCE(SUM(CASE WHEN m.movement_type='IN' THEN m.quantity ELSE -m.quantity END), 0)
               FROM products p
               LEFT JOIN clients c ON c.id=p.client_id
               LEFT JOIN movements m ON m.product_id=p.id
               GROUP BY p.id ORDER BY p.id DESC"""
        )
        
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
        """Обновление метрик"""
        suppliers_cnt = self.db.query("SELECT COUNT(*) FROM suppliers")[0][0]
        clients_cnt = self.db.query("SELECT COUNT(*) FROM clients")[0][0]
        products_cnt = self.db.query("SELECT COUNT(*) FROM products")[0][0]
        inbound_cnt = self.db.query("SELECT COUNT(*) FROM inbound_orders")[0][0]
        movements_cnt = self.db.query("SELECT COUNT(*) FROM movements")[0][0]
        
        self.metric_labels["suppliers"].configure(text=str(suppliers_cnt))
        self.metric_labels["clients"].configure(text=str(clients_cnt))
        self.metric_labels["products"].configure(text=str(products_cnt))
        self.metric_labels["inbound"].configure(text=str(inbound_cnt))
        self.metric_labels["movements"].configure(text=str(movements_cnt))

    # ================== ЗАКРЫТИЕ ПРИЛОЖЕНИЯ ==================
    
    def _on_close(self):
        """Обработчик закрытия"""
        if messagebox.askyesno("🚪 Выход", "Выйти из приложения?"):
            self.db.close()
            self.destroy()


# ================== ТОЧКА ВХОДА ==================

if __name__ == "__main__":
    app = WMSApp()
    app.mainloop()
