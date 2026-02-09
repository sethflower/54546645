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
        "primary": "#1a237e",        # Тёмно-синий (основной)
        "primary_light": "#534bae",  # Светло-синий
        "primary_dark": "#000051",   # Очень тёмно-синий
        "accent": "#00bcd4",         # Бирюзовый акцент
        "accent_hover": "#00acc1",   # Акцент при наведении
        "success": "#4caf50",        # Зелёный успех
        "warning": "#ff9800",        # Оранжевый предупреждение
        "error": "#f44336",          # Красный ошибка
        "bg_main": "#f5f7fa",        # Основной фон
        "bg_card": "#ffffff",        # Фон карточек
        "bg_sidebar": "#1a237e",     # Фон боковой панели
        "text_primary": "#212121",   # Основной текст
        "text_secondary": "#757575", # Второстепенный текст
        "text_light": "#ffffff",     # Светлый текст
        "border": "#e0e0e0",         # Границы
        "hover": "#e3f2fd",          # Подсветка при наведении
        "selected": "#bbdefb",       # Выделенная строка
    }
    
    # Шрифты
    FONTS = {
        "title": ("Segoe UI", 24, "bold"),
        "subtitle": ("Segoe UI", 14),
        "heading": ("Segoe UI", 16, "bold"),
        "body": ("Segoe UI", 11),
        "body_bold": ("Segoe UI", 11, "bold"),
        "small": ("Segoe UI", 10),
        "button": ("Segoe UI", 11, "bold"),
        "tab": ("Segoe UI", 12, "bold"),
    }

    def __init__(self):
        super().__init__()
        self.title("WMS 3PL | Система управления складом")
        
        # Получаем размеры экрана и устанавливаем адаптивный размер окна
        screen_width = self.winfo_screenwidth()
        screen_height = self.winfo_screenheight()
        
        # Окно занимает 90% экрана
        window_width = int(screen_width * 0.9)
        window_height = int(screen_height * 0.9)
        
        # Центрируем окно
        x = (screen_width - window_width) // 2
        y = (screen_height - window_height) // 2
        
        self.geometry(f"{window_width}x{window_height}+{x}+{y}")
        self.minsize(1024, 600)
        
        self.db = Database(DB_FILE)
        self.current_user = getpass.getuser()
        
        self.style = ttk.Style(self)
        self._configure_styles()
        self._init_variables()
        self._build_main_layout()
        self.refresh_all()
        self.protocol("WM_DELETE_WINDOW", self.on_close)
        
        # Развернуть на весь экран после загрузки
        self.after(100, lambda: self._maximize_window(self))

    def _init_variables(self):
        """Инициализация всех переменных"""
        self.selected_copy_value = ""
        self.nomenclature_brand_filter = tk.StringVar()
        self.nomenclature_article_filter = tk.StringVar()
        self.nomenclature_supplier_filter = tk.StringVar()
        self.nomenclature_client_filter = tk.StringVar()
        self.nomenclature_category_filter = tk.StringVar()
        self.nomenclature_subcategory_filter = tk.StringVar()
        self.nomenclature_has_searched = False
        
        self.suppliers_search_var = tk.StringVar()
        self.suppliers_search_var.trace_add("write", lambda *_: self.refresh_suppliers())
        
        self.clients_search_var = tk.StringVar()
        self.clients_search_var.trace_add("write", lambda *_: self.refresh_3pl_clients())
        
        self.categories_filter_var = tk.StringVar()
        self.categories_filter_var.trace_add("write", lambda *_: self.refresh_categories_tab())
        
        self.inbound_order_search_var = tk.StringVar()
        self.inbound_has_searched = True
        self.inbound_status_var = tk.StringVar(value="Все")
        self.inbound_date_filter_var = tk.StringVar()
        self.inbound_from_date_var = tk.StringVar()
        self.inbound_to_date_var = tk.StringVar()
        self.inbound_created_by_filter_var = tk.StringVar()
        self.inbound_accepted_by_filter_var = tk.StringVar()
        self.inbound_supplier_filter_var = tk.StringVar()
        self.inbound_client_filter_var = tk.StringVar()

    def _configure_styles(self):
        """Настройка современных стилей"""
        self.configure(bg=self.COLORS["bg_main"])
        self.style.theme_use("clam")
        
        # Основные фреймы
        self.style.configure("Main.TFrame", background=self.COLORS["bg_main"])
        self.style.configure("Card.TFrame", background=self.COLORS["bg_card"])
        self.style.configure("Sidebar.TFrame", background=self.COLORS["primary"])
        self.style.configure("Header.TFrame", background=self.COLORS["bg_card"])
        self.style.configure("Toolbar.TFrame", background=self.COLORS["bg_card"])
        self.style.configure("Filter.TFrame", background=self.COLORS["bg_main"])
        
        # Метки
        self.style.configure("Title.TLabel", 
                            background=self.COLORS["bg_card"],
                            foreground=self.COLORS["text_primary"],
                            font=self.FONTS["title"])
        
        self.style.configure("Subtitle.TLabel",
                            background=self.COLORS["bg_card"],
                            foreground=self.COLORS["text_secondary"],
                            font=self.FONTS["subtitle"])
        
        self.style.configure("Heading.TLabel",
                            background=self.COLORS["bg_card"],
                            foreground=self.COLORS["text_primary"],
                            font=self.FONTS["heading"])
        
        self.style.configure("Body.TLabel",
                            background=self.COLORS["bg_card"],
                            foreground=self.COLORS["text_primary"],
                            font=self.FONTS["body"])
        
        self.style.configure("FilterLabel.TLabel",
                            background=self.COLORS["bg_main"],
                            foreground=self.COLORS["text_secondary"],
                            font=self.FONTS["small"])
        
        self.style.configure("Metric.TLabel",
                            background=self.COLORS["primary"],
                            foreground=self.COLORS["text_light"],
                            font=self.FONTS["body_bold"])
        
        self.style.configure("MetricValue.TLabel",
                            background=self.COLORS["primary"],
                            foreground=self.COLORS["accent"],
                            font=self.FONTS["heading"])
        
        # Кнопки - основная
        self.style.configure("Primary.TButton",
                            font=self.FONTS["button"],
                            padding=(20, 12),
                            background=self.COLORS["primary"],
                            foreground=self.COLORS["text_light"])
        self.style.map("Primary.TButton",
                      background=[("active", self.COLORS["primary_light"]),
                                 ("pressed", self.COLORS["primary_dark"])])
        
        # Кнопки - акцент
        self.style.configure("Accent.TButton",
                            font=self.FONTS["button"],
                            padding=(20, 12),
                            background=self.COLORS["accent"],
                            foreground=self.COLORS["text_light"])
        self.style.map("Accent.TButton",
                      background=[("active", self.COLORS["accent_hover"])])
        
        # Кнопки - успех
        self.style.configure("Success.TButton",
                            font=self.FONTS["button"],
                            padding=(20, 12),
                            background=self.COLORS["success"],
                            foreground=self.COLORS["text_light"])
        self.style.map("Success.TButton",
                      background=[("active", "#45a049")])
        
        # Кнопки - предупреждение
        self.style.configure("Warning.TButton",
                            font=self.FONTS["button"],
                            padding=(20, 12),
                            background=self.COLORS["warning"],
                            foreground=self.COLORS["text_light"])
        self.style.map("Warning.TButton",
                      background=[("active", "#f57c00")])
        
        # Кнопки - опасность
        self.style.configure("Danger.TButton",
                            font=self.FONTS["button"],
                            padding=(20, 12),
                            background=self.COLORS["error"],
                            foreground=self.COLORS["text_light"])
        self.style.map("Danger.TButton",
                      background=[("active", "#d32f2f")])
        
        # Кнопки - вторичная
        self.style.configure("Secondary.TButton",
                            font=self.FONTS["button"],
                            padding=(16, 10),
                            background=self.COLORS["bg_card"],
                            foreground=self.COLORS["text_primary"],
                            borderwidth=1)
        self.style.map("Secondary.TButton",
                      background=[("active", self.COLORS["hover"])])
        
        # Кнопки - иконки
        self.style.configure("Icon.TButton",
                            font=("Segoe UI", 14),
                            padding=(8, 4),
                            background=self.COLORS["bg_main"],
                            foreground=self.COLORS["text_secondary"])
        self.style.map("Icon.TButton",
                      background=[("active", self.COLORS["hover"])])
        
        # Поля ввода
        self.style.configure("Search.TEntry",
                            font=self.FONTS["body"],
                            padding=10,
                            fieldbackground=self.COLORS["bg_card"])
        
        # Комбобокс
        self.style.configure("TCombobox",
                            font=self.FONTS["body"],
                            padding=8,
                            fieldbackground=self.COLORS["bg_card"])
        
        # Treeview (таблицы)
        self.style.configure("Treeview",
                            font=self.FONTS["body"],
                            rowheight=36,
                            background=self.COLORS["bg_card"],
                            fieldbackground=self.COLORS["bg_card"],
                            foreground=self.COLORS["text_primary"])
        
        self.style.configure("Treeview.Heading",
                            font=self.FONTS["body_bold"],
                            background=self.COLORS["primary"],
                            foreground=self.COLORS["text_light"],
                            padding=12)
        
        self.style.map("Treeview",
                      background=[("selected", self.COLORS["selected"])],
                      foreground=[("selected", self.COLORS["text_primary"])])
        
        # Notebook (вкладки)
        self.style.configure("TNotebook", 
                            background=self.COLORS["bg_main"],
                            borderwidth=0)
        
        self.style.configure("TNotebook.Tab",
                            font=self.FONTS["tab"],
                            padding=(24, 14),
                            background=self.COLORS["bg_card"],
                            foreground=self.COLORS["text_secondary"])
        
        self.style.map("TNotebook.Tab",
                      background=[("selected", self.COLORS["primary"])],
                      foreground=[("selected", self.COLORS["text_light"])],
                      expand=[("selected", [1, 1, 1, 0])])
        
        # Separator
        self.style.configure("TSeparator", background=self.COLORS["border"])
        
        # Scrollbar
        self.style.configure("Vertical.TScrollbar",
                            background=self.COLORS["bg_main"],
                            troughcolor=self.COLORS["bg_card"],
                            borderwidth=0,
                            arrowsize=14)

    def _maximize_window(self, win):
        """Развернуть окно на весь экран"""
        try:
            win.state("zoomed")
        except tk.TclError:
            try:
                win.attributes("-fullscreen", True)
            except:
                pass

    def _build_main_layout(self):
        """Построение основного макета приложения"""
        # Основной контейнер
        self.main_container = ttk.Frame(self, style="Main.TFrame")
        self.main_container.pack(fill="both", expand=True)
        
        # Верхняя панель (Header)
        self._build_header()
        
        # Панель метрик
        self._build_metrics_panel()
        
        # Основная область с вкладками
        self._build_notebook()

    def _build_header(self):
        """Построение заголовка приложения"""
        header_frame = ttk.Frame(self.main_container, style="Header.TFrame")
        header_frame.pack(fill="x", padx=0, pady=0)
        
        # Внутренний контейнер с отступами
        header_inner = ttk.Frame(header_frame, style="Header.TFrame")
        header_inner.pack(fill="x", padx=30, pady=20)
        
        # Левая часть - логотип и название
        left_frame = ttk.Frame(header_inner, style="Header.TFrame")
        left_frame.pack(side="left", fill="y")
        
        # Иконка склада (эмулируем логотип)
        logo_label = tk.Label(left_frame, 
                             text="📦", 
                             font=("Segoe UI", 32),
                             bg=self.COLORS["bg_card"],
                             fg=self.COLORS["primary"])
        logo_label.pack(side="left", padx=(0, 15))
        
        title_frame = ttk.Frame(left_frame, style="Header.TFrame")
        title_frame.pack(side="left", fill="y")
        
        ttk.Label(title_frame, 
                 text="WMS 3PL", 
                 style="Title.TLabel").pack(anchor="w")
        
        ttk.Label(title_frame,
                 text="Система управления складом • Поставщики • Клиенты • Номенклатура • Приходы • Движения",
                 style="Subtitle.TLabel").pack(anchor="w")
        
        # Правая часть - информация о пользователе
        right_frame = ttk.Frame(header_inner, style="Header.TFrame")
        right_frame.pack(side="right", fill="y")
        
        user_frame = ttk.Frame(right_frame, style="Header.TFrame")
        user_frame.pack(side="right")
        
        ttk.Label(user_frame,
                 text=f"👤 {self.current_user}",
                 style="Body.TLabel").pack(side="left", padx=(0, 15))
        
        ttk.Label(user_frame,
                 text=f"📅 {datetime.now().strftime('%d.%m.%Y')}",
                 style="Body.TLabel").pack(side="left")
        
        # Разделитель
        ttk.Separator(self.main_container, orient="horizontal").pack(fill="x")

    def _build_metrics_panel(self):
        """Построение панели с метриками"""
        # Контейнер метрик с тёмным фоном
        metrics_outer = tk.Frame(self.main_container, bg=self.COLORS["primary"])
        metrics_outer.pack(fill="x")
        
        metrics_frame = tk.Frame(metrics_outer, bg=self.COLORS["primary"])
        metrics_frame.pack(fill="x", padx=30, pady=20)
        
        # Создаём метрики
        self.metric_labels = {}
        metrics_config = [
            ("suppliers", "📋 Поставщики", "0"),
            ("clients", "👥 3PL Клиенты", "0"),
            ("products", "📦 Номенклатура", "0"),
            ("inbound", "📥 Приходы", "0"),
            ("movements", "🔄 Движения", "0"),
        ]
        
        for i, (key, title, value) in enumerate(metrics_config):
            metric_card = tk.Frame(metrics_frame, bg=self.COLORS["primary_light"], padx=20, pady=15)
            metric_card.pack(side="left", padx=(0, 15), fill="y")
            
            tk.Label(metric_card, 
                    text=title, 
                    font=self.FONTS["small"],
                    bg=self.COLORS["primary_light"],
                    fg=self.COLORS["text_light"]).pack(anchor="w")
            
            value_label = tk.Label(metric_card, 
                                   text=value, 
                                   font=self.FONTS["heading"],
                                   bg=self.COLORS["primary_light"],
                                   fg=self.COLORS["accent"])
            value_label.pack(anchor="w")
            self.metric_labels[key] = value_label

    def _build_notebook(self):
        """Построение области с вкладками"""
        # Контейнер для вкладок
        notebook_container = ttk.Frame(self.main_container, style="Main.TFrame")
        notebook_container.pack(fill="both", expand=True, padx=20, pady=20)
        
        # Notebook
        self.notebook = ttk.Notebook(notebook_container)
        self.notebook.pack(fill="both", expand=True)
        
        # Создаём вкладки
        self.suppliers_tab = self._create_tab_frame()
        self.clients_tab = self._create_tab_frame()
        self.categories_tab = self._create_tab_frame()
        self.nomenclature_tab = self._create_tab_frame()
        self.inbound_tab = self._create_tab_frame()
        self.movements_tab = self._create_tab_frame()
        self.stock_tab = self._create_tab_frame()
        
        self.notebook.add(self.suppliers_tab, text="  📋 Поставщики  ")
        self.notebook.add(self.clients_tab, text="  👥 3PL Клиенты  ")
        self.notebook.add(self.categories_tab, text="  📁 Категории  ")
        self.notebook.add(self.nomenclature_tab, text="  📦 Номенклатура  ")
        self.notebook.add(self.inbound_tab, text="  📥 Приходы  ")
        self.notebook.add(self.movements_tab, text="  🔄 Движения  ")
        self.notebook.add(self.stock_tab, text="  📊 Остатки  ")
        
        # Строим содержимое вкладок
        self._build_suppliers_tab()
        self._build_clients_tab()
        self._build_categories_tab()
        self._build_nomenclature_tab()
        self._build_inbound_tab()
        self._build_movements_tab()
        self._build_stock_tab()

    def _create_tab_frame(self):
        """Создание фрейма для вкладки"""
        frame = ttk.Frame(self.notebook, style="Card.TFrame")
        return frame

    def _create_card(self, parent, title=None):
        """Создание карточки с тенью"""
        card = ttk.Frame(parent, style="Card.TFrame")
        if title:
            ttk.Label(card, text=title, style="Heading.TLabel").pack(anchor="w", padx=20, pady=(20, 10))
            ttk.Separator(card).pack(fill="x", padx=20)
        return card

    def _create_toolbar(self, parent):
        """Создание панели инструментов"""
        toolbar = ttk.Frame(parent, style="Toolbar.TFrame")
        toolbar.pack(fill="x", padx=20, pady=15)
        return toolbar

    def _create_filter_row(self, parent):
        """Создание строки фильтров"""
        filter_frame = ttk.Frame(parent, style="Filter.TFrame")
        filter_frame.pack(fill="x", padx=20, pady=(0, 15))
        return filter_frame

    def _create_scrollable_tree(self, parent, columns, headings_config):
        """Создание таблицы с прокруткой"""
        # Контейнер для таблицы
        tree_container = ttk.Frame(parent, style="Card.TFrame")
        tree_container.pack(fill="both", expand=True, padx=20, pady=(0, 20))
        
        # Scrollbar
        scrollbar_y = ttk.Scrollbar(tree_container, orient="vertical")
        scrollbar_y.pack(side="right", fill="y")
        
        scrollbar_x = ttk.Scrollbar(tree_container, orient="horizontal")
        scrollbar_x.pack(side="bottom", fill="x")
        
        # Treeview
        tree = ttk.Treeview(tree_container, 
                           columns=columns, 
                           show="headings",
                           yscrollcommand=scrollbar_y.set,
                           xscrollcommand=scrollbar_x.set)
        tree.pack(fill="both", expand=True)
        
        scrollbar_y.config(command=tree.yview)
        scrollbar_x.config(command=tree.xview)
        
        # Настройка колонок
        for col, title, width, anchor in headings_config:
            tree.heading(col, text=title)
            tree.column(col, width=width, anchor=anchor, minwidth=50)
        
        return tree

    # ==================== ПОСТАВЩИКИ ====================
    
    def _build_suppliers_tab(self):
        """Построение вкладки поставщиков"""
        # Заголовок и описание
        header = ttk.Frame(self.suppliers_tab, style="Card.TFrame")
        header.pack(fill="x", padx=20, pady=(20, 0))
        
        ttk.Label(header, text="Управление поставщиками", style="Heading.TLabel").pack(anchor="w")
        ttk.Label(header, text="Создание, редактирование и удаление карточек поставщиков", 
                 style="Subtitle.TLabel").pack(anchor="w", pady=(5, 0))
        
        # Панель инструментов
        toolbar = self._create_toolbar(self.suppliers_tab)
        
        # Поиск
        search_frame = ttk.Frame(toolbar, style="Toolbar.TFrame")
        search_frame.pack(side="left", fill="y")
        
        ttk.Label(search_frame, text="🔍", style="Body.TLabel").pack(side="left", padx=(0, 5))
        search_entry = ttk.Entry(search_frame, textvariable=self.suppliers_search_var, width=30)
        search_entry.pack(side="left")
        search_entry.insert(0, "")
        
        # Кнопки
        btn_frame = ttk.Frame(toolbar, style="Toolbar.TFrame")
        btn_frame.pack(side="right")
        
        ttk.Button(btn_frame, text="➕ Создать", style="Success.TButton",
                  command=self.open_create_supplier_dialog).pack(side="left", padx=(0, 10))
        ttk.Button(btn_frame, text="✏️ Редактировать", style="Primary.TButton",
                  command=self.open_edit_supplier_dialog).pack(side="left", padx=(0, 10))
        ttk.Button(btn_frame, text="🗑️ Удалить", style="Danger.TButton",
                  command=self.delete_selected_supplier).pack(side="left")
        
        # Таблица
        columns = ("id", "name", "phone", "created")
        headings = [
            ("id", "ID", 80, "center"),
            ("name", "Название поставщика", 400, "w"),
            ("phone", "Телефон", 200, "w"),
            ("created", "Дата создания", 180, "center"),
        ]
        
        self.suppliers_tree = self._create_scrollable_tree(self.suppliers_tab, columns, headings)
        
        # Контекстное меню
        self.suppliers_copy_menu = tk.Menu(self, tearoff=0)
        self.suppliers_copy_menu.add_command(label="📋 Скопировать", command=self.copy_selected_value)
        self.suppliers_tree.bind("<Button-3>", self.show_suppliers_copy_menu)
        self.suppliers_tree.bind("<Double-1>", lambda e: self.open_edit_supplier_dialog())

    # ==================== 3PL КЛИЕНТЫ ====================
    
    def _build_clients_tab(self):
        """Построение вкладки 3PL клиентов"""
        # Заголовок
        header = ttk.Frame(self.clients_tab, style="Card.TFrame")
        header.pack(fill="x", padx=20, pady=(20, 0))
        
        ttk.Label(header, text="Управление 3PL клиентами", style="Heading.TLabel").pack(anchor="w")
        ttk.Label(header, text="Создание, редактирование и удаление карточек клиентов", 
                 style="Subtitle.TLabel").pack(anchor="w", pady=(5, 0))
        
        # Панель инструментов
        toolbar = self._create_toolbar(self.clients_tab)
        
        # Поиск
        search_frame = ttk.Frame(toolbar, style="Toolbar.TFrame")
        search_frame.pack(side="left", fill="y")
        
        ttk.Label(search_frame, text="🔍", style="Body.TLabel").pack(side="left", padx=(0, 5))
        ttk.Entry(search_frame, textvariable=self.clients_search_var, width=30).pack(side="left")
        
        # Кнопки
        btn_frame = ttk.Frame(toolbar, style="Toolbar.TFrame")
        btn_frame.pack(side="right")
        
        ttk.Button(btn_frame, text="➕ Создать", style="Success.TButton",
                  command=self.open_create_client_dialog).pack(side="left", padx=(0, 10))
        ttk.Button(btn_frame, text="✏️ Редактировать", style="Primary.TButton",
                  command=self.open_edit_client_dialog).pack(side="left", padx=(0, 10))
        ttk.Button(btn_frame, text="🗑️ Удалить", style="Danger.TButton",
                  command=self.delete_selected_client).pack(side="left")
        
        # Таблица
        columns = ("id", "name", "contact", "created")
        headings = [
            ("id", "ID", 80, "center"),
            ("name", "Название клиента", 400, "w"),
            ("contact", "Контакт", 200, "w"),
            ("created", "Дата создания", 180, "center"),
        ]
        
        self.clients_tree = self._create_scrollable_tree(self.clients_tab, columns, headings)
        
        # Контекстное меню
        self.clients_copy_menu = tk.Menu(self, tearoff=0)
        self.clients_copy_menu.add_command(label="📋 Скопировать", command=self.copy_selected_value)
        self.clients_tree.bind("<Button-3>", self.show_clients_copy_menu)
        self.clients_tree.bind("<Double-1>", lambda e: self.open_edit_client_dialog())

    # ==================== КАТЕГОРИИ ====================
    
    def _build_categories_tab(self):
        """Построение вкладки категорий"""
        # Заголовок
        header = ttk.Frame(self.categories_tab, style="Card.TFrame")
        header.pack(fill="x", padx=20, pady=(20, 0))
        
        ttk.Label(header, text="Управление категориями товаров", style="Heading.TLabel").pack(anchor="w")
        ttk.Label(header, text="Создание категорий и подкатегорий для классификации номенклатуры", 
                 style="Subtitle.TLabel").pack(anchor="w", pady=(5, 0))
        
        # Панель поиска
        search_toolbar = self._create_toolbar(self.categories_tab)
        
        ttk.Label(search_toolbar, text="🔍", style="Body.TLabel").pack(side="left", padx=(0, 5))
        ttk.Entry(search_toolbar, textvariable=self.categories_filter_var, width=40).pack(side="left")
        
        # Панель создания
        create_frame = ttk.Frame(self.categories_tab, style="Card.TFrame")
        create_frame.pack(fill="x", padx=20, pady=(0, 15))
        
        # Создание категории
        cat_frame = ttk.Frame(create_frame, style="Card.TFrame")
        cat_frame.pack(fill="x", pady=10)
        
        ttk.Label(cat_frame, text="Новая категория:", style="Body.TLabel").pack(side="left", padx=(0, 10))
        self.new_category_var = tk.StringVar()
        ttk.Entry(cat_frame, textvariable=self.new_category_var, width=30).pack(side="left", padx=(0, 10))
        ttk.Button(cat_frame, text="➕ Создать категорию", style="Success.TButton",
                  command=self.create_category).pack(side="left")
        
        # Создание подкатегории
        subcat_frame = ttk.Frame(create_frame, style="Card.TFrame")
        subcat_frame.pack(fill="x", pady=10)
        
        ttk.Label(subcat_frame, text="Новая подкатегория:", style="Body.TLabel").pack(side="left", padx=(0, 10))
        self.new_subcategory_var = tk.StringVar()
        ttk.Entry(subcat_frame, textvariable=self.new_subcategory_var, width=25).pack(side="left", padx=(0, 10))
        
        ttk.Label(subcat_frame, text="в категории:", style="Body.TLabel").pack(side="left", padx=(0, 10))
        self.subcategory_parent_var = tk.StringVar()
        self.subcategory_parent_box = ttk.Combobox(subcat_frame, textvariable=self.subcategory_parent_var, 
                                                   state="readonly", width=25)
        self.subcategory_parent_box.pack(side="left", padx=(0, 10))
        ttk.Button(subcat_frame, text="➕ Создать подкатегорию", style="Success.TButton",
                  command=self.create_subcategory).pack(side="left")
        
        # Таблицы категорий и подкатегорий
        trees_frame = ttk.Frame(self.categories_tab, style="Card.TFrame")
        trees_frame.pack(fill="both", expand=True, padx=20, pady=(0, 20))
        
        # Категории
        cat_container = ttk.Frame(trees_frame, style="Card.TFrame")
        cat_container.pack(side="left", fill="both", expand=True, padx=(0, 10))
        
        ttk.Label(cat_container, text="📁 Категории", style="Heading.TLabel").pack(anchor="w", pady=(0, 10))
        
        cat_scroll = ttk.Scrollbar(cat_container, orient="vertical")
        cat_scroll.pack(side="right", fill="y")
        
        self.categories_tree = ttk.Treeview(cat_container, columns=("id", "name"), show="headings",
                                            yscrollcommand=cat_scroll.set, height=15)
        self.categories_tree.pack(fill="both", expand=True)
        cat_scroll.config(command=self.categories_tree.yview)
        
        self.categories_tree.heading("id", text="ID")
        self.categories_tree.heading("name", text="Название категории")
        self.categories_tree.column("id", width=80, anchor="center")
        self.categories_tree.column("name", width=250, anchor="w")
        
        # Подкатегории
        subcat_container = ttk.Frame(trees_frame, style="Card.TFrame")
        subcat_container.pack(side="left", fill="both", expand=True, padx=(10, 0))
        
        ttk.Label(subcat_container, text="📂 Подкатегории", style="Heading.TLabel").pack(anchor="w", pady=(0, 10))
        
        subcat_scroll = ttk.Scrollbar(subcat_container, orient="vertical")
        subcat_scroll.pack(side="right", fill="y")
        
        self.subcategories_tree = ttk.Treeview(subcat_container, columns=("id", "name", "category"), 
                                               show="headings", yscrollcommand=subcat_scroll.set, height=15)
        self.subcategories_tree.pack(fill="both", expand=True)
        subcat_scroll.config(command=self.subcategories_tree.yview)
        
        self.subcategories_tree.heading("id", text="ID")
        self.subcategories_tree.heading("name", text="Подкатегория")
        self.subcategories_tree.heading("category", text="Категория")
        self.subcategories_tree.column("id", width=80, anchor="center")
        self.subcategories_tree.column("name", width=200, anchor="w")
        self.subcategories_tree.column("category", width=200, anchor="w")

    # ==================== НОМЕНКЛАТУРА ====================
    
    def _build_nomenclature_tab(self):
        """Построение вкладки номенклатуры"""
        # Заголовок
        header = ttk.Frame(self.nomenclature_tab, style="Card.TFrame")
        header.pack(fill="x", padx=20, pady=(20, 0))
        
        ttk.Label(header, text="Справочник номенклатуры", style="Heading.TLabel").pack(anchor="w")
        ttk.Label(header, text="Управление карточками товаров: создание, редактирование, поиск", 
                 style="Subtitle.TLabel").pack(anchor="w", pady=(5, 0))
        
        # Панель фильтров
        filter_frame = self._create_filter_row(self.nomenclature_tab)
        
        # Фильтры в одну строку
        filters = [
            ("Марка:", self.nomenclature_brand_filter, 15),
            ("Артикул:", self.nomenclature_article_filter, 12),
            ("Поставщик:", self.nomenclature_supplier_filter, 15),
            ("3PL клиент:", self.nomenclature_client_filter, 15),
        ]
        
        for label_text, var, width in filters:
            ttk.Label(filter_frame, text=label_text, style="FilterLabel.TLabel").pack(side="left", padx=(0, 5))
            ttk.Entry(filter_frame, textvariable=var, width=width).pack(side="left", padx=(0, 15))
        
        ttk.Button(filter_frame, text="🔍 Поиск", style="Primary.TButton",
                  command=self.search_nomenclature).pack(side="left", padx=(10, 0))
        
        # Панель инструментов
        toolbar = self._create_toolbar(self.nomenclature_tab)
        
        ttk.Button(toolbar, text="➕ Создать карточку", style="Success.TButton",
                  command=self.open_create_product_dialog).pack(side="left", padx=(0, 10))
        ttk.Button(toolbar, text="✏️ Редактировать", style="Primary.TButton",
                  command=self.open_edit_product_dialog).pack(side="left", padx=(0, 10))
        ttk.Button(toolbar, text="🗑️ Удалить", style="Danger.TButton",
                  command=self.delete_selected_product).pack(side="left")
        
        # Таблица
        columns = ("id", "article", "brand", "supplier", "client", "unit", 
                  "volume", "weight", "barcode", "serial_tracking", "category", "subcategory", "product_owner")
        headings = [
            ("id", "ID", 60, "center"),
            ("article", "Артикул", 100, "w"),
            ("brand", "Марка", 180, "w"),
            ("supplier", "Поставщик", 150, "w"),
            ("client", "3PL клиент", 150, "w"),
            ("unit", "Ед.изм.", 80, "center"),
            ("volume", "Объём", 80, "center"),
            ("weight", "Вес", 80, "center"),
            ("barcode", "Штрих-код", 120, "w"),
            ("serial_tracking", "Серийный учёт", 100, "center"),
            ("category", "Категория", 140, "w"),
            ("subcategory", "Подкатегория", 140, "w"),
            ("product_owner", "Владелец", 120, "w"),
        ]
        
        self.nomenclature_tree = self._create_scrollable_tree(self.nomenclature_tab, columns, headings)
        
        # Контекстное меню
        self.copy_menu = tk.Menu(self, tearoff=0)
        self.copy_menu.add_command(label="📋 Скопировать", command=self.copy_selected_value)
        self.nomenclature_tree.bind("<Button-3>", self.show_copy_menu)
        self.nomenclature_tree.bind("<Double-1>", lambda e: self.open_edit_product_dialog())

    # ==================== ПРИХОДЫ ====================
    
    def _build_inbound_tab(self):
        """Построение вкладки приходов"""
        # Заголовок
        header = ttk.Frame(self.inbound_tab, style="Card.TFrame")
        header.pack(fill="x", padx=20, pady=(20, 0))
        
        ttk.Label(header, text="Приходные заказы", style="Heading.TLabel").pack(anchor="w")
        ttk.Label(header, text="Управление заказами на приход товара: создание, приёмка, просмотр истории", 
                 style="Subtitle.TLabel").pack(anchor="w", pady=(5, 0))
        
        # Панель фильтров - строка 1
        filter_frame1 = self._create_filter_row(self.inbound_tab)
        
        ttk.Label(filter_frame1, text="№ заказа:", style="FilterLabel.TLabel").pack(side="left", padx=(0, 5))
        ttk.Entry(filter_frame1, textvariable=self.inbound_order_search_var, width=12).pack(side="left", padx=(0, 15))
        
        ttk.Label(filter_frame1, text="Статус:", style="FilterLabel.TLabel").pack(side="left", padx=(0, 5))
        ttk.Combobox(filter_frame1, textvariable=self.inbound_status_var, 
                    values=["Все", "Новый", "Принят"], state="readonly", width=10).pack(side="left", padx=(0, 15))
        
        ttk.Label(filter_frame1, text="От:", style="FilterLabel.TLabel").pack(side="left", padx=(0, 5))
        ttk.Entry(filter_frame1, textvariable=self.inbound_from_date_var, width=12, state="readonly").pack(side="left", padx=(0, 5))
        ttk.Button(filter_frame1, text="📅", style="Icon.TButton",
                  command=lambda: self._open_date_picker(self.inbound_from_date_var)).pack(side="left", padx=(0, 15))
        
        ttk.Label(filter_frame1, text="До:", style="FilterLabel.TLabel").pack(side="left", padx=(0, 5))
        ttk.Entry(filter_frame1, textvariable=self.inbound_to_date_var, width=12, state="readonly").pack(side="left", padx=(0, 5))
        ttk.Button(filter_frame1, text="📅", style="Icon.TButton",
                  command=lambda: self._open_date_picker(self.inbound_to_date_var)).pack(side="left", padx=(0, 15))
        
        # Панель фильтров - строка 2
        filter_frame2 = self._create_filter_row(self.inbound_tab)
        
        ttk.Label(filter_frame2, text="Создал:", style="FilterLabel.TLabel").pack(side="left", padx=(0, 5))
        ttk.Entry(filter_frame2, textvariable=self.inbound_created_by_filter_var, width=12).pack(side="left", padx=(0, 15))
        
        ttk.Label(filter_frame2, text="Принял:", style="FilterLabel.TLabel").pack(side="left", padx=(0, 5))
        ttk.Entry(filter_frame2, textvariable=self.inbound_accepted_by_filter_var, width=12).pack(side="left", padx=(0, 15))
        
        ttk.Label(filter_frame2, text="Поставщик:", style="FilterLabel.TLabel").pack(side="left", padx=(0, 5))
        ttk.Entry(filter_frame2, textvariable=self.inbound_supplier_filter_var, width=15).pack(side="left", padx=(0, 15))
        
        ttk.Label(filter_frame2, text="3PL:", style="FilterLabel.TLabel").pack(side="left", padx=(0, 5))
        ttk.Entry(filter_frame2, textvariable=self.inbound_client_filter_var, width=15).pack(side="left", padx=(0, 15))
        
        ttk.Button(filter_frame2, text="🔍 Поиск", style="Primary.TButton",
                  command=self.search_inbound_orders).pack(side="left", padx=(10, 0))
        
        # Панель инструментов
        toolbar = self._create_toolbar(self.inbound_tab)
        
        ttk.Button(toolbar, text="➕ Создать заказ", style="Success.TButton",
                  command=self.open_create_inbound_order_dialog).pack(side="left")
        
        # Подсказка
        ttk.Label(toolbar, text="💡 Двойной клик для открытия заказа", 
                 style="FilterLabel.TLabel").pack(side="right")
        
        # Таблица
        columns = ("order_number", "created_at", "received_at", "created_by", "accepted_by",
                  "supplier", "client", "warehouse", "status", "planned_qty", "actual_qty")
        headings = [
            ("order_number", "№ Заказа", 120, "w"),
            ("created_at", "Дата создания", 140, "center"),
            ("received_at", "Дата приёма", 140, "center"),
            ("created_by", "Создал", 100, "w"),
            ("accepted_by", "Принял", 100, "w"),
            ("supplier", "Поставщик", 150, "w"),
            ("client", "3PL клиент", 150, "w"),
            ("warehouse", "Склад", 120, "w"),
            ("status", "Статус", 100, "center"),
            ("planned_qty", "План. кол-во", 100, "center"),
            ("actual_qty", "Факт. кол-во", 100, "center"),
        ]
        
        self.inbound_tree = self._create_scrollable_tree(self.inbound_tab, columns, headings)
        self.inbound_tree.bind("<Double-1>", self.open_selected_inbound_order)
        
        # Цветовые теги для статусов
        self.inbound_tree.tag_configure("new", background="#e3f2fd")
        self.inbound_tree.tag_configure("accepted", background="#e8f5e9")

        # ==================== ДВИЖЕНИЯ ====================
    
            # ==================== ДВИЖЕНИЯ ====================
    
    def _build_movements_tab(self):
        """Построение вкладки движений"""
        # Заголовок
        header = ttk.Frame(self.movements_tab, style="Card.TFrame")
        header.pack(fill="x", padx=20, pady=(20, 0))
        
        ttk.Label(header, text="Движения товаров", style="Heading.TLabel").pack(anchor="w")
        ttk.Label(header, text="Журнал приходов и расходов товаров", 
                 style="Subtitle.TLabel").pack(anchor="w", pady=(5, 0))
        
        # Форма добавления движения
        form_card = ttk.Frame(self.movements_tab, style="Card.TFrame")
        form_card.pack(fill="x", padx=20, pady=15)
        
        ttk.Label(form_card, text="➕ Добавить движение", style="Heading.TLabel").pack(anchor="w", pady=(0, 15))
        
        form_row = ttk.Frame(form_card, style="Card.TFrame")
        form_row.pack(fill="x")
        
        # Поля формы
        ttk.Label(form_row, text="Товар:", style="Body.TLabel").pack(side="left", padx=(0, 5))
        self.movement_product = tk.StringVar()
        self.movement_product_box = ttk.Combobox(form_row, textvariable=self.movement_product, width=30, state="readonly")
        self.movement_product_box.pack(side="left", padx=(0, 15))
        
        ttk.Label(form_row, text="Тип:", style="Body.TLabel").pack(side="left", padx=(0, 5))
        self.movement_type = tk.StringVar(value="IN")
        ttk.Combobox(form_row, textvariable=self.movement_type, values=["IN", "OUT"], 
                    width=8, state="readonly").pack(side="left", padx=(0, 15))
        
        ttk.Label(form_row, text="Кол-во:", style="Body.TLabel").pack(side="left", padx=(0, 5))
        self.movement_qty = tk.StringVar(value="1")
        ttk.Entry(form_row, textvariable=self.movement_qty, width=10).pack(side="left", padx=(0, 15))
        
        ttk.Label(form_row, text="Документ:", style="Body.TLabel").pack(side="left", padx=(0, 5))
        self.movement_ref = tk.StringVar()
        ttk.Entry(form_row, textvariable=self.movement_ref, width=15).pack(side="left", padx=(0, 15))
        
        ttk.Label(form_row, text="Комментарий:", style="Body.TLabel").pack(side="left", padx=(0, 5))
        self.movement_note = tk.StringVar()
        ttk.Entry(form_row, textvariable=self.movement_note, width=20).pack(side="left", padx=(0, 15))
        
        ttk.Button(form_row, text="✅ Провести", style="Success.TButton",
                  command=self.add_movement).pack(side="left")
        
        # Таблица движений
        columns = ("id", "brand", "type", "qty", "reference", "date", "note")
        headings = [
            ("id", "ID", 70, "center"),
            ("brand", "Товар", 250, "w"),
            ("type", "Тип", 80, "center"),
            ("qty", "Кол-во", 100, "center"),
            ("reference", "Документ", 150, "w"),
            ("date", "Дата", 160, "center"),
            ("note", "Комментарий", 250, "w"),
        ]
        
        self.movements_tree = self._create_scrollable_tree(self.movements_tab, columns, headings)
        
        # Цветовые теги для типов движений
        self.movements_tree.tag_configure("in", background="#e8f5e9")   # Приход - зелёный
        self.movements_tree.tag_configure("out", background="#ffebee")  # Расход - красный

    # ==================== ОСТАТКИ ====================
    
    def _build_stock_tab(self):
        """Построение вкладки остатков"""
        # Заголовок
        header = ttk.Frame(self.stock_tab, style="Card.TFrame")
        header.pack(fill="x", padx=20, pady=(20, 0))
        
        title_row = ttk.Frame(header, style="Card.TFrame")
        title_row.pack(fill="x")
        
        ttk.Label(title_row, text="📊 Текущие остатки на складе", style="Heading.TLabel").pack(side="left")
        ttk.Button(title_row, text="🔄 Обновить", style="Primary.TButton",
                  command=self.refresh_stock).pack(side="right")
        
        ttk.Label(header, text="Актуальная информация об остатках товаров в разрезе клиентов", 
                 style="Subtitle.TLabel").pack(anchor="w", pady=(5, 15))
        
        # Таблица остатков
        columns = ("brand", "client", "unit", "stock")
        headings = [
            ("brand", "Товар", 350, "w"),
            ("client", "3PL клиент", 250, "w"),
            ("unit", "Ед.изм.", 100, "center"),
            ("stock", "Остаток", 150, "center"),
        ]
        
        self.stock_tree = self._create_scrollable_tree(self.stock_tab, columns, headings)
        
        # Цветовые теги для остатков
        self.stock_tree.tag_configure("low", background="#fff3e0")   # Низкий остаток - оранжевый
        self.stock_tree.tag_configure("ok", background="#e8f5e9")    # Нормальный остаток - зелёный
        self.stock_tree.tag_configure("zero", background="#ffebee")  # Нулевой остаток - красный

    # ==================== ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ ====================

    def _center_window(self, window):
        """Центрировать окно на экране"""
        window.update_idletasks()
        width = window.winfo_width()
        height = window.winfo_height()
        screen_width = window.winfo_screenwidth()
        screen_height = window.winfo_screenheight()
        x = (screen_width - width) // 2
        y = (screen_height - height) // 2
        window.geometry(f"{width}x{height}+{x}+{y}")

    def _open_date_picker(self, target_var):
        """Открытие диалога выбора даты"""
        picker = tk.Toplevel(self)
        picker.title("📅 Выбор даты")
        picker.transient(self)
        picker.grab_set()
        picker.configure(bg=self.COLORS["bg_card"])
        picker.geometry("320x320")
        picker.resizable(False, False)
        
        # Центрируем окно
        self.after(10, lambda: self._center_window(picker))
        
        today = datetime.now()
        year_var = tk.IntVar(value=today.year)
        month_var = tk.IntVar(value=today.month)
        
        main_frame = ttk.Frame(picker, style="Card.TFrame")
        main_frame.pack(fill="both", expand=True, padx=15, pady=15)
        
        # Навигация по месяцам
        nav_frame = ttk.Frame(main_frame, style="Card.TFrame")
        nav_frame.pack(fill="x", pady=(0, 10))
        
        ttk.Button(nav_frame, text="◀", style="Secondary.TButton", width=3,
                  command=lambda: shift_month(-1)).pack(side="left")
        
        month_label = ttk.Label(nav_frame, text="", style="Heading.TLabel")
        month_label.pack(side="left", expand=True)
        
        ttk.Button(nav_frame, text="▶", style="Secondary.TButton", width=3,
                  command=lambda: shift_month(1)).pack(side="right")
        
        # Фрейм для дней
        days_frame = ttk.Frame(main_frame, style="Card.TFrame")
        days_frame.pack(fill="both", expand=True)
        
        def build_days():
            for child in days_frame.winfo_children():
                child.destroy()
            
            y, m = year_var.get(), month_var.get()
            month_names_ru = ["", "Январь", "Февраль", "Март", "Апрель", "Май", "Июнь",
                             "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь"]
            month_label.configure(text=f"{month_names_ru[m]} {y}")
            
            # Заголовки дней недели
            days_of_week = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
            for c, day_name in enumerate(days_of_week):
                lbl = ttk.Label(days_frame, text=day_name, style="Body.TLabel", width=4)
                lbl.grid(row=0, column=c, padx=2, pady=5)
            
            # Дни месяца
            for r, week in enumerate(calendar.monthcalendar(y, m)):
                for c, day in enumerate(week):
                    if day == 0:
                        ttk.Label(days_frame, text="", width=4).grid(row=r+1, column=c, padx=2, pady=2)
                    else:
                        btn = ttk.Button(days_frame, text=str(day), width=4, style="Secondary.TButton",
                                        command=lambda d=day: select_day(d))
                        btn.grid(row=r+1, column=c, padx=2, pady=2)
        
        def select_day(day):
            target_var.set(f"{year_var.get():04d}-{month_var.get():02d}-{day:02d}")
            picker.destroy()
        
        def shift_month(delta):
            m = month_var.get() + delta
            y = year_var.get()
            if m < 1:
                m, y = 12, y - 1
            elif m > 12:
                m, y = 1, y + 1
            month_var.set(m)
            year_var.set(y)
            build_days()
        
        # Кнопки внизу
        btn_frame = ttk.Frame(main_frame, style="Card.TFrame")
        btn_frame.pack(fill="x", pady=(10, 0))
        
        ttk.Button(btn_frame, text="✖ Очистить", style="Secondary.TButton",
                  command=lambda: [target_var.set(""), picker.destroy()]).pack(side="left")
        ttk.Button(btn_frame, text="📅 Сегодня", style="Primary.TButton",
                  command=lambda: [target_var.set(datetime.now().strftime("%Y-%m-%d")), picker.destroy()]).pack(side="right")
        
        build_days()

    def _open_fullscreen_dialog(self, parent, title: str, geometry: str | None = None):
        """Открытие полноэкранного диалога"""
        dialog = tk.Toplevel(parent)
        dialog.title(title)
        dialog.configure(bg=self.COLORS["bg_main"])
        
        if geometry:
            dialog.geometry(geometry)
        
        dialog.transient(parent)
        dialog.grab_set()
        
        # Развернуть на весь экран
        self.after(100, lambda: self._maximize_window(dialog))
        
        return dialog

    def _normalize_decimal(self, raw: str):
        """Нормализация десятичного числа"""
        value = (raw or "").strip().replace(",", ".")
        if not value:
            return None
        if not re.fullmatch(r"\d+(?:\.\d{1,4})?", value):
            raise ValueError
        return float(value)

    def _clear_tree(self, tree):
        """Очистка таблицы"""
        for row in tree.get_children():
            tree.delete(row)

    def _load_reference_dict(self, table_name):
        """Загрузка справочника в словарь"""
        rows = self.db.query(f"SELECT id, name FROM {table_name} ORDER BY name")
        return {f"{r[0]} | {r[1]}": r[0] for r in rows}

    def copy_selected_value(self):
        """Копирование выбранного значения в буфер обмена"""
        self.clipboard_clear()
        self.clipboard_append(str(self.selected_copy_value))

    def show_copy_menu(self, event):
        """Показать контекстное меню копирования для номенклатуры"""
        row = self.nomenclature_tree.identify_row(event.y)
        col = self.nomenclature_tree.identify_column(event.x)
        if not row:
            return
        self.nomenclature_tree.selection_set(row)
        values = self.nomenclature_tree.item(row, "values")
        col_index = int(col.replace("#", "")) - 1 if col else 0
        self.selected_copy_value = values[col_index] if 0 <= col_index < len(values) else ""
        self.copy_menu.tk_popup(event.x_root, event.y_root)

    def show_suppliers_copy_menu(self, event):
        """Показать контекстное меню копирования для поставщиков"""
        row = self.suppliers_tree.identify_row(event.y)
        col = self.suppliers_tree.identify_column(event.x)
        if not row:
            return
        self.suppliers_tree.selection_set(row)
        values = self.suppliers_tree.item(row, "values")
        col_index = int(col.replace("#", "")) - 1 if col else 0
        self.selected_copy_value = values[col_index] if 0 <= col_index < len(values) else ""
        self.suppliers_copy_menu.tk_popup(event.x_root, event.y_root)

    def show_clients_copy_menu(self, event):
        """Показать контекстное меню копирования для клиентов"""
        row = self.clients_tree.identify_row(event.y)
        col = self.clients_tree.identify_column(event.x)
        if not row:
            return
        self.clients_tree.selection_set(row)
        values = self.clients_tree.item(row, "values")
        col_index = int(col.replace("#", "")) - 1 if col else 0
        self.selected_copy_value = values[col_index] if 0 <= col_index < len(values) else ""
        self.clients_copy_menu.tk_popup(event.x_root, event.y_root)

    # ==================== ПОСТАВЩИКИ - ДИАЛОГИ ====================

    def _next_supplier_id(self):
        """Получить следующий ID поставщика"""
        row = self.db.query("SELECT COALESCE(MAX(id), 0) + 1 FROM suppliers")
        return str(row[0][0])

    def open_create_supplier_dialog(self):
        """Открыть диалог создания поставщика"""
        self._open_supplier_dialog(mode="create")

    def open_edit_supplier_dialog(self):
        """Открыть диалог редактирования поставщика"""
        selected = self.suppliers_tree.selection()
        if not selected:
            messagebox.showwarning("⚠️ Внимание", "Выберите поставщика для редактирования")
            return
        supplier_id = int(self.suppliers_tree.item(selected[0], "values")[0])
        self._open_supplier_dialog(mode="edit", supplier_id=supplier_id)

    def _open_supplier_dialog(self, mode: str, supplier_id: int | None = None):
        """Открыть диалог поставщика"""
        dialog = tk.Toplevel(self)
        dialog.title("📋 Карточка поставщика")
        dialog.geometry("500x320")
        dialog.configure(bg=self.COLORS["bg_card"])
        dialog.transient(self)
        dialog.grab_set()
        dialog.resizable(False, False)
        
        self.after(10, lambda: self._center_window(dialog))
        
        main_frame = ttk.Frame(dialog, style="Card.TFrame")
        main_frame.pack(fill="both", expand=True, padx=30, pady=30)
        
        # Заголовок
        title = "Создание поставщика" if mode == "create" else "Редактирование поставщика"
        ttk.Label(main_frame, text=title, style="Heading.TLabel").pack(anchor="w", pady=(0, 20))
        
        # Переменные формы
        supplier_name_var = tk.StringVar()
        supplier_id_var = tk.StringVar(value=self._next_supplier_id())
        supplier_contact_var = tk.StringVar()
        created_at_var = tk.StringVar(value=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        
        if mode == "edit" and supplier_id is not None:
            row = self.db.query("SELECT id, name, phone, created_at FROM suppliers WHERE id = ?", (supplier_id,))
            if not row:
                messagebox.showerror("❌ Ошибка", "Поставщик не найден", parent=dialog)
                dialog.destroy()
                return
            sid, name, phone, created = row[0]
            supplier_id_var.set(str(sid))
            supplier_name_var.set(name or "")
            supplier_contact_var.set(phone or "")
            created_at_var.set(created or "")
        
        # Форма
        form_frame = ttk.Frame(main_frame, style="Card.TFrame")
        form_frame.pack(fill="x")
        
        fields = [
            ("Название поставщика *", supplier_name_var, False),
            ("ID поставщика", supplier_id_var, True),
            ("Контактный телефон", supplier_contact_var, False),
            ("Дата создания", created_at_var, True),
        ]
        
        for i, (label, var, readonly) in enumerate(fields):
            ttk.Label(form_frame, text=label, style="Body.TLabel").grid(row=i, column=0, sticky="w", pady=8)
            state = "readonly" if readonly else "normal"
            entry = ttk.Entry(form_frame, textvariable=var, width=35, state=state)
            entry.grid(row=i, column=1, sticky="w", pady=8, padx=(15, 0))
        
        def on_save():
            name = supplier_name_var.get().strip()
            if not name:
                messagebox.showwarning("⚠️ Внимание", "Название поставщика обязательно", parent=dialog)
                return
            
            if mode == "create":
                try:
                    self.db.execute(
                        "INSERT INTO suppliers(name, phone, created_at) VALUES(?, ?, ?)",
                        (name, supplier_contact_var.get().strip(), created_at_var.get().strip()),
                    )
                except sqlite3.IntegrityError:
                    messagebox.showerror("❌ Ошибка", "Поставщик с таким названием уже существует", parent=dialog)
                    return
            else:
                self.db.execute(
                    "UPDATE suppliers SET name = ?, phone = ? WHERE id = ?",
                    (name, supplier_contact_var.get().strip(), supplier_id),
                )
            
            self.refresh_all()
            dialog.destroy()
        
        # Кнопки
        btn_frame = ttk.Frame(main_frame, style="Card.TFrame")
        btn_frame.pack(fill="x", pady=(20, 0))
        
        ttk.Button(btn_frame, text="❌ Отмена", style="Secondary.TButton",
                  command=dialog.destroy).pack(side="left")
        ttk.Button(btn_frame, text="✅ Сохранить", style="Success.TButton",
                  command=on_save).pack(side="right")

    def delete_selected_supplier(self):
        """Удалить выбранного поставщика"""
        selected = self.suppliers_tree.selection()
        if not selected:
            messagebox.showwarning("⚠️ Внимание", "Выберите поставщика для удаления")
            return
        supplier_id, supplier_name = self.suppliers_tree.item(selected[0], "values")[:2]
        if not messagebox.askyesno("🗑️ Подтверждение", f"Удалить поставщика «{supplier_name}»?"):
            return
        self.db.execute("DELETE FROM suppliers WHERE id = ?", (int(supplier_id),))
        self.refresh_all()

    # ==================== 3PL КЛИЕНТЫ - ДИАЛОГИ ====================

    def _next_client_id(self):
        """Получить следующий ID клиента"""
        row = self.db.query("SELECT COALESCE(MAX(id), 0) + 1 FROM clients")
        return str(row[0][0])

    def _next_client_code(self):
        """Получить следующий код клиента"""
        row = self.db.query("SELECT code FROM clients WHERE code LIKE 'C%' ORDER BY id DESC LIMIT 1")
        if not row or not row[0][0]:
            return "C00001"
        code = row[0][0]
        try:
            return f"C{int(code.replace('C', '')) + 1:05d}"
        except ValueError:
            cnt = self.db.query("SELECT COUNT(*) FROM clients")[0][0] + 1
            return f"C{cnt:05d}"

    def open_create_client_dialog(self):
        """Открыть диалог создания клиента"""
        self._open_client_dialog(mode="create")

    def open_edit_client_dialog(self):
        """Открыть диалог редактирования клиента"""
        selected = self.clients_tree.selection()
        if not selected:
            messagebox.showwarning("⚠️ Внимание", "Выберите клиента для редактирования")
            return
        client_id = int(self.clients_tree.item(selected[0], "values")[0])
        self._open_client_dialog(mode="edit", client_id=client_id)

    def _open_client_dialog(self, mode: str, client_id: int | None = None):
        """Открыть диалог клиента"""
        dialog = tk.Toplevel(self)
        dialog.title("👥 Карточка 3PL клиента")
        dialog.geometry("500x320")
        dialog.configure(bg=self.COLORS["bg_card"])
        dialog.transient(self)
        dialog.grab_set()
        dialog.resizable(False, False)
        
        self.after(10, lambda: self._center_window(dialog))
        
        main_frame = ttk.Frame(dialog, style="Card.TFrame")
        main_frame.pack(fill="both", expand=True, padx=30, pady=30)
        
        title = "Создание 3PL клиента" if mode == "create" else "Редактирование 3PL клиента"
        ttk.Label(main_frame, text=title, style="Heading.TLabel").pack(anchor="w", pady=(0, 20))
        
        # Переменные
        client_name_var = tk.StringVar()
        client_id_var = tk.StringVar(value=self._next_client_id())
        client_contact_var = tk.StringVar()
        created_at_var = tk.StringVar(value=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        
        if mode == "edit" and client_id is not None:
            row = self.db.query("SELECT id, name, contact, created_at FROM clients WHERE id = ?", (client_id,))
            if not row:
                messagebox.showerror("❌ Ошибка", "Клиент не найден", parent=dialog)
                dialog.destroy()
                return
            cid, name, contact, created = row[0]
            client_id_var.set(str(cid))
            client_name_var.set(name or "")
            client_contact_var.set(contact or "")
            created_at_var.set(created or "")
        
        # Форма
        form_frame = ttk.Frame(main_frame, style="Card.TFrame")
        form_frame.pack(fill="x")
        
        fields = [
            ("Название клиента *", client_name_var, False),
            ("ID клиента", client_id_var, True),
            ("Контактный телефон", client_contact_var, False),
            ("Дата создания", created_at_var, True),
        ]
        
        for i, (label, var, readonly) in enumerate(fields):
            ttk.Label(form_frame, text=label, style="Body.TLabel").grid(row=i, column=0, sticky="w", pady=8)
            state = "readonly" if readonly else "normal"
            entry = ttk.Entry(form_frame, textvariable=var, width=35, state=state)
            entry.grid(row=i, column=1, sticky="w", pady=8, padx=(15, 0))
        
        def on_save():
            name = client_name_var.get().strip()
            if not name:
                messagebox.showwarning("⚠️ Внимание", "Название клиента обязательно", parent=dialog)
                return
            
            if mode == "create":
                try:
                    self.db.execute(
                        "INSERT INTO clients(code, name, contact, created_at) VALUES(?, ?, ?, ?)",
                        (self._next_client_code(), name, client_contact_var.get().strip(), created_at_var.get().strip()),
                    )
                except sqlite3.IntegrityError:
                    messagebox.showerror("❌ Ошибка", "Клиент с таким кодом уже существует", parent=dialog)
                    return
            else:
                self.db.execute(
                    "UPDATE clients SET name = ?, contact = ? WHERE id = ?",
                    (name, client_contact_var.get().strip(), client_id),
                )
            
            self.refresh_all()
            dialog.destroy()
        
        # Кнопки
        btn_frame = ttk.Frame(main_frame, style="Card.TFrame")
        btn_frame.pack(fill="x", pady=(20, 0))
        
        ttk.Button(btn_frame, text="❌ Отмена", style="Secondary.TButton",
                  command=dialog.destroy).pack(side="left")
        ttk.Button(btn_frame, text="✅ Сохранить", style="Success.TButton",
                  command=on_save).pack(side="right")

    def delete_selected_client(self):
        """Удалить выбранного клиента"""
        selected = self.clients_tree.selection()
        if not selected:
            messagebox.showwarning("⚠️ Внимание", "Выберите клиента для удаления")
            return
        client_id, client_name = self.clients_tree.item(selected[0], "values")[:2]
        if not messagebox.askyesno("🗑️ Подтверждение", f"Удалить клиента «{client_name}»?"):
            return
        self.db.execute("DELETE FROM clients WHERE id = ?", (int(client_id),))
        self.refresh_all()

    # ==================== КАТЕГОРИИ - МЕТОДЫ ====================

    def create_category(self):
        """Создать категорию"""
        name = self.new_category_var.get().strip()
        if not name:
            messagebox.showwarning("⚠️ Внимание", "Введите название категории")
            return
        try:
            self.db.execute("INSERT INTO categories(name) VALUES(?)", (name,))
        except sqlite3.IntegrityError:
            messagebox.showerror("❌ Ошибка", "Категория с таким названием уже существует")
            return
        self.new_category_var.set("")
        self.refresh_all()

    def create_subcategory(self):
        """Создать подкатегорию"""
        name = self.new_subcategory_var.get().strip()
        cat_token = self.subcategory_parent_var.get().strip()
        if not name or not cat_token:
            messagebox.showwarning("⚠️ Внимание", "Укажите название подкатегории и выберите категорию")
            return
        category_id = int(cat_token.split(" | ")[0])
        try:
            self.db.execute("INSERT INTO subcategories(category_id, name) VALUES(?, ?)", (category_id, name))
        except sqlite3.IntegrityError:
            messagebox.showerror("❌ Ошибка", "Подкатегория с таким названием уже существует в выбранной категории")
            return
        self.new_subcategory_var.set("")
        self.refresh_all()

    # ==================== НОМЕНКЛАТУРА - ДИАЛОГИ ====================

    def _next_article(self):
        """Получить следующий артикул"""
        row = self.db.query("SELECT article FROM products WHERE article LIKE 'ART-%' ORDER BY id DESC LIMIT 1")
        if not row or not row[0][0]:
            return "ART-00001"
        try:
            nxt = int(str(row[0][0]).split('-')[-1]) + 1
        except ValueError:
            nxt = self.db.query("SELECT COUNT(*) FROM products")[0][0] + 1
        return f"ART-{nxt:05d}"

    def search_nomenclature(self):
        """Поиск по номенклатуре"""
        self.nomenclature_has_searched = True
        self.refresh_nomenclature()

    def open_create_product_dialog(self):
        """Открыть диалог создания товара"""
        self._open_product_dialog(mode="create")

    def open_edit_product_dialog(self):
        """Открыть диалог редактирования товара"""
        selected = self.nomenclature_tree.selection()
        if not selected:
            messagebox.showwarning("⚠️ Внимание", "Выберите товар для редактирования")
            return
        self._open_product_dialog(mode="edit", product_id=int(self.nomenclature_tree.item(selected[0], "values")[0]))

    def _open_product_dialog(self, mode: str, product_id: int | None = None):
        """Открыть диалог товара"""
        dialog = self._open_fullscreen_dialog(self, "📦 Карточка товара", "700x650")
        
        main_frame = ttk.Frame(dialog, style="Card.TFrame")
        main_frame.pack(fill="both", expand=True, padx=40, pady=30)
        
        title = "Создание карточки товара" if mode == "create" else "Редактирование карточки товара"
        ttk.Label(main_frame, text=title, style="Heading.TLabel").pack(anchor="w", pady=(0, 25))
        
        # Переменные формы
        brand_var = tk.StringVar()
        supplier_var = tk.StringVar()
        client_var = tk.StringVar()
        unit_var = tk.StringVar(value="Шт")
        volume_var = tk.StringVar()
        weight_var = tk.StringVar()
        barcode_var = tk.StringVar()
        article_var = tk.StringVar(value=self._next_article())
        serial_var = tk.StringVar(value="Нет")
        category_var = tk.StringVar()
        subcategory_var = tk.StringVar()
        product_owner_var = tk.StringVar(value=self.current_user)
        
        suppliers = self._load_reference_dict("suppliers")
        clients = self._load_reference_dict("clients")
        categories = self._load_reference_dict("categories")
        
        # Форма в 2 колонки
        form_frame = ttk.Frame(main_frame, style="Card.TFrame")
        form_frame.pack(fill="both", expand=True)
        
        # Левая колонка
        left_col = ttk.Frame(form_frame, style="Card.TFrame")
        left_col.pack(side="left", fill="both", expand=True, padx=(0, 20))
        
        # Правая колонка
        right_col = ttk.Frame(form_frame, style="Card.TFrame")
        right_col.pack(side="left", fill="both", expand=True)
        
        def add_field(parent, label, widget):
            frame = ttk.Frame(parent, style="Card.TFrame")
            frame.pack(fill="x", pady=8)
            ttk.Label(frame, text=label, style="Body.TLabel").pack(anchor="w")
            widget.pack(fill="x", pady=(5, 0))
        
        # Левая колонка - поля
        add_field(left_col, "Артикул", ttk.Entry(left_col, textvariable=article_var, state="readonly"))
        add_field(left_col, "Марка / Название *", ttk.Entry(left_col, textvariable=brand_var))
        add_field(left_col, "Поставщик *", ttk.Combobox(left_col, textvariable=supplier_var, 
                                                         values=list(suppliers.keys()), state="readonly"))
        add_field(left_col, "3PL клиент *", ttk.Combobox(left_col, textvariable=client_var,
                                                          values=list(clients.keys()), state="readonly"))
        add_field(left_col, "Единица измерения", ttk.Combobox(left_col, textvariable=unit_var,
                                                               values=["Шт", "Палета"], state="readonly"))
        add_field(left_col, "Серийный учёт", ttk.Combobox(left_col, textvariable=serial_var,
                                                          values=["Да", "Нет"], state="readonly"))
        
        # Правая колонка - поля
        add_field(right_col, "Категория *", ttk.Combobox(right_col, textvariable=category_var,
                                                          values=list(categories.keys()), state="readonly"))
        
        subcategory_box = ttk.Combobox(right_col, textvariable=subcategory_var, state="readonly")
        add_field(right_col, "Подкатегория *", subcategory_box)
        
        add_field(right_col, "Объём (м³)", ttk.Entry(right_col, textvariable=volume_var))
        add_field(right_col, "Вес (кг)", ttk.Entry(right_col, textvariable=weight_var))
        add_field(right_col, "Штрих-код", ttk.Entry(right_col, textvariable=barcode_var))
        add_field(right_col, "Владелец карточки", ttk.Entry(right_col, textvariable=product_owner_var, state="readonly"))
        
        def load_all_subcategories():
            rows = self.db.query("SELECT id, name FROM subcategories ORDER BY name")
            options = [f"{r[0]} | {r[1]}" for r in rows]
            subcategory_box["values"] = options
            if options and not subcategory_var.get():
                subcategory_var.set(options[0])
        
        def read_id(token):
            if token and "|" in token:
                return int(token.split(" | ")[0])
            return None
        
        # Загрузка данных при редактировании
        if mode == "edit" and product_id is not None:
            product = self.db.query(
                """
                SELECT article, brand, supplier_id, client_id, unit, volume, weight, barcode, serial_tracking,
                       category_id, subcategory_id, product_owner
                FROM products WHERE id = ?
                """,
                (product_id,),
            )[0]
            article_var.set(product[0] or "")
            brand_var.set(product[1] or "")
            supplier_var.set(next((k for k, v in suppliers.items() if v == product[2]), ""))
            client_var.set(next((k for k, v in clients.items() if v == product[3]), ""))
            unit_var.set(product[4] or "Шт")
            volume_var.set("" if product[5] is None else str(product[5]))
            weight_var.set("" if product[6] is None else str(product[6]))
            barcode_var.set(product[7] or "")
            serial_var.set(product[8] or "Нет")
            category_var.set(next((k for k, v in categories.items() if v == product[9]), ""))
            load_all_subcategories()
            subcategories = self._load_reference_dict("subcategories")
            subcategory_var.set(next((k for k, v in subcategories.items() if v == product[10]), ""))
            product_owner_var.set(product[11] or self.current_user)
        else:
            load_all_subcategories()
        
        def on_save():
            brand = brand_var.get().strip()
            if not brand:
                messagebox.showwarning("⚠️ Внимание", "Марка / Название обязательно", parent=dialog)
                return
            
            supplier_id = read_id(supplier_var.get())
            client_id = read_id(client_var.get())
            if not supplier_id or not client_id:
                messagebox.showwarning("⚠️ Внимание", "Укажите поставщика и 3PL клиента", parent=dialog)
                return
            
            category_id = read_id(category_var.get())
            subcategory_id = read_id(subcategory_var.get())
            if not category_id or not subcategory_id:
                messagebox.showwarning("⚠️ Внимание", "Выберите категорию и подкатегорию", parent=dialog)
                return
            
            try:
                volume = self._normalize_decimal(volume_var.get())
                weight = self._normalize_decimal(weight_var.get())
            except ValueError:
                messagebox.showwarning("⚠️ Внимание", "Объём и вес должны быть числами", parent=dialog)
                return
            
            sub_match = self.db.query("SELECT id FROM subcategories WHERE id = ? AND category_id = ?", 
                                      (subcategory_id, category_id))
            if not sub_match:
                messagebox.showwarning("⚠️ Внимание", "Подкатегория должна принадлежать выбранной категории", parent=dialog)
                return
            
            payload = (
                brand,
                supplier_id,
                client_id,
                unit_var.get().strip(),
                volume,
                weight,
                barcode_var.get().strip(),
                serial_var.get().strip(),
                category_id,
                subcategory_id,
                product_owner_var.get().strip(),
            )
            
            try:
                if mode == "create":
                    self.db.execute(
                        """
                        INSERT INTO products(
                            name, article, brand, supplier_id, client_id, unit,
                            volume, weight, barcode, serial_tracking, category_id, subcategory_id,
                            product_owner, created_at
                        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (brand, article_var.get().strip(),) + payload + (datetime.now().strftime("%Y-%m-%d %H:%M:%S"),),
                    )
                else:
                    self.db.execute(
                        """
                        UPDATE products
                        SET name=?, article=?, brand=?, supplier_id=?, client_id=?, unit=?,
                            volume=?, weight=?, barcode=?, serial_tracking=?, category_id=?, subcategory_id=?,
                            product_owner=?
                        WHERE id=?
                        """,
                        (brand, article_var.get().strip(),) + payload + (product_id,),
                    )
            except sqlite3.IntegrityError as exc:
                messagebox.showerror("❌ Ошибка", f"Не удалось сохранить карточку: {exc}", parent=dialog)
                return
            
            self.refresh_all()
            dialog.destroy()
        
        # Кнопки
        btn_frame = ttk.Frame(main_frame, style="Card.TFrame")
        btn_frame.pack(fill="x", pady=(25, 0))
        
        ttk.Button(btn_frame, text="❌ Отмена", style="Secondary.TButton",
                  command=dialog.destroy).pack(side="left")
        ttk.Button(btn_frame, text="✅ Сохранить", style="Success.TButton",
                  command=on_save).pack(side="right")

    def delete_selected_product(self):
        """Удалить выбранный товар"""
        selected = self.nomenclature_tree.selection()
        if not selected:
            messagebox.showwarning("⚠️ Внимание", "Выберите товар для удаления")
            return
        item = self.nomenclature_tree.item(selected[0], "values")
        product_id, article, brand = int(item[0]), item[1], item[2]
        display_name = brand or article or f"ID: {product_id}"
        if not messagebox.askyesno("🗑️ Подтверждение", f"Удалить товар «{display_name}»?"):
            return
        self.db.execute("DELETE FROM products WHERE id=?", (product_id,))
        self.refresh_all()

        # ==================== ПРИХОДЫ - ДИАЛОГИ ====================

    def search_inbound_orders(self):
        """Поиск приходных заказов"""
        self.inbound_has_searched = True
        self.refresh_inbound_orders()

    def open_selected_inbound_order(self, _event=None):
        """Открыть выбранный приходный заказ"""
        selected = self.inbound_tree.selection()
        if not selected:
            return
        order_number = self.inbound_tree.item(selected[0], "values")[0]
        self.open_inbound_order_dialog(order_number)

    def _next_inbound_order_number(self):
        """Получить следующий номер заказа"""
        row = self.db.query("SELECT order_number FROM inbound_orders ORDER BY id DESC LIMIT 1")
        if not row or not row[0][0]:
            return "IN-00001"
        val = row[0][0]
        try:
            nxt = int(val.split("-")[-1]) + 1
        except ValueError:
            nxt = self.db.query("SELECT COUNT(*) FROM inbound_orders")[0][0] + 1
        return f"IN-{nxt:05d}"

    def open_create_inbound_order_dialog(self):
        """Открыть диалог создания приходного заказа"""
        dialog = self._open_fullscreen_dialog(self, "📥 Создание приходного заказа", "1200x800")
        
        main_frame = ttk.Frame(dialog, style="Card.TFrame")
        main_frame.pack(fill="both", expand=True, padx=30, pady=20)
        
        ttk.Label(main_frame, text="Создание нового приходного заказа", style="Heading.TLabel").pack(anchor="w", pady=(0, 20))
        
        # Переменные заголовка
        order_number_var = tk.StringVar(value=self._next_inbound_order_number())
        created_at_var = tk.StringVar(value=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        status_var = tk.StringVar(value="Новый")
        created_by_var = tk.StringVar(value=self.current_user)
        supplier_var = tk.StringVar()
        client_var = tk.StringVar()
        warehouse_var = tk.StringVar()
        
        suppliers = self._load_reference_dict("suppliers")
        clients = self._load_reference_dict("clients")
        warehouses = self._load_reference_dict("warehouses")
        categories = self._load_reference_dict("categories")
        
        # Заголовок заказа
        header_frame = ttk.Frame(main_frame, style="Card.TFrame")
        header_frame.pack(fill="x", pady=(0, 15))
        
        # Строка 1
        row1 = ttk.Frame(header_frame, style="Card.TFrame")
        row1.pack(fill="x", pady=5)
        
        ttk.Label(row1, text="№ заказа:", style="Body.TLabel").pack(side="left", padx=(0, 5))
        ttk.Entry(row1, textvariable=order_number_var, state="readonly", width=15).pack(side="left", padx=(0, 20))
        
        ttk.Label(row1, text="Дата создания:", style="Body.TLabel").pack(side="left", padx=(0, 5))
        ttk.Entry(row1, textvariable=created_at_var, state="readonly", width=20).pack(side="left", padx=(0, 20))
        
        ttk.Label(row1, text="Статус:", style="Body.TLabel").pack(side="left", padx=(0, 5))
        ttk.Entry(row1, textvariable=status_var, state="readonly", width=12).pack(side="left", padx=(0, 20))
        
        ttk.Label(row1, text="Создал:", style="Body.TLabel").pack(side="left", padx=(0, 5))
        ttk.Entry(row1, textvariable=created_by_var, state="readonly", width=15).pack(side="left")
        
        # Строка 2
        row2 = ttk.Frame(header_frame, style="Card.TFrame")
        row2.pack(fill="x", pady=5)
        
        ttk.Label(row2, text="Поставщик *:", style="Body.TLabel").pack(side="left", padx=(0, 5))
        ttk.Combobox(row2, textvariable=supplier_var, values=list(suppliers.keys()), 
                    state="readonly", width=25).pack(side="left", padx=(0, 20))
        
        ttk.Label(row2, text="3PL клиент *:", style="Body.TLabel").pack(side="left", padx=(0, 5))
        ttk.Combobox(row2, textvariable=client_var, values=list(clients.keys()),
                    state="readonly", width=25).pack(side="left", padx=(0, 20))
        
        ttk.Label(row2, text="Склад *:", style="Body.TLabel").pack(side="left", padx=(0, 5))
        ttk.Combobox(row2, textvariable=warehouse_var, values=list(warehouses.keys()),
                    state="readonly", width=20).pack(side="left")
        
        ttk.Separator(main_frame).pack(fill="x", pady=15)
        
        # Добавление позиций
        ttk.Label(main_frame, text="➕ Добавление позиций в заказ", style="Heading.TLabel").pack(anchor="w", pady=(0, 10))
        
        line_category_var = tk.StringVar()
        line_subcategory_var = tk.StringVar()
        line_product_var = tk.StringVar()
        line_qty_var = tk.StringVar(value="1")
        line_weight_var = tk.StringVar(value="0")
        line_volume_var = tk.StringVar(value="0")
        line_unit_var = tk.StringVar(value="")
        
        # Строка добавления
        add_frame = ttk.Frame(main_frame, style="Card.TFrame")
        add_frame.pack(fill="x", pady=(0, 10))
        
        ttk.Label(add_frame, text="Категория:", style="Body.TLabel").pack(side="left", padx=(0, 5))
        line_category_box = ttk.Combobox(add_frame, textvariable=line_category_var,
                                         values=list(categories.keys()), state="readonly", width=20)
        line_category_box.pack(side="left", padx=(0, 15))
        
        ttk.Label(add_frame, text="Подкатегория:", style="Body.TLabel").pack(side="left", padx=(0, 5))
        line_subcategory_box = ttk.Combobox(add_frame, textvariable=line_subcategory_var, state="readonly", width=20)
        line_subcategory_box.pack(side="left", padx=(0, 15))
        
        ttk.Label(add_frame, text="Товар:", style="Body.TLabel").pack(side="left", padx=(0, 5))
        line_product_box = ttk.Combobox(add_frame, textvariable=line_product_var, state="readonly", width=25)
        line_product_box.pack(side="left", padx=(0, 15))
        
        ttk.Label(add_frame, text="Кол-во:", style="Body.TLabel").pack(side="left", padx=(0, 5))
        ttk.Entry(add_frame, textvariable=line_qty_var, width=8).pack(side="left", padx=(0, 5))
        ttk.Label(add_frame, textvariable=line_unit_var, style="Body.TLabel").pack(side="left", padx=(0, 15))
        
        ttk.Button(add_frame, text="➕ Добавить", style="Success.TButton",
                  command=lambda: add_line()).pack(side="left")
        
        # Список позиций
        order_items = []
        product_catalog = {}
        
        columns = ("category", "subcategory", "article", "product", "unit", "planned_qty", "weight", "volume")
        
        tree_frame = ttk.Frame(main_frame, style="Card.TFrame")
        tree_frame.pack(fill="both", expand=True, pady=(10, 0))
        
        scrollbar = ttk.Scrollbar(tree_frame, orient="vertical")
        scrollbar.pack(side="right", fill="y")
        
        lines_tree = ttk.Treeview(tree_frame, columns=columns, show="headings", 
                                  yscrollcommand=scrollbar.set, height=12)
        lines_tree.pack(fill="both", expand=True)
        scrollbar.config(command=lines_tree.yview)
        
        for col, title, width in [
            ("category", "Категория", 150),
            ("subcategory", "Подкатегория", 170),
            ("article", "Артикул", 120),
            ("product", "Товар", 200),
            ("unit", "Ед.", 80),
            ("planned_qty", "План. кол-во", 120),
            ("weight", "Вес", 100),
            ("volume", "Объём", 100),
        ]:
            lines_tree.heading(col, text=title)
            lines_tree.column(col, width=width, anchor="center" if col in ("unit", "planned_qty", "weight", "volume") else "w")
        
        def refresh_subcategories_for_category(*_):
            cat_token = line_category_var.get().strip()
            if not cat_token or "|" not in cat_token:
                line_subcategory_box["values"] = []
                line_subcategory_var.set("")
                line_product_box["values"] = []
                line_product_var.set("")
                return
            cat_id = int(cat_token.split(" | ")[0])
            rows = self.db.query("SELECT id, name FROM subcategories WHERE category_id = ? ORDER BY name", (cat_id,))
            values = [f"{sid} | {name}" for sid, name in rows]
            line_subcategory_box["values"] = values
            line_subcategory_var.set(values[0] if values else "")
            refresh_products_for_subcategory()
        
        def refresh_products_for_subcategory(*_):
            sub_token = line_subcategory_var.get().strip()
            if not sub_token or "|" not in sub_token:
                line_product_box["values"] = []
                return
            sub_id = int(sub_token.split(" | ")[0])
            cat_token = line_category_var.get().strip()
            if not cat_token:
                return
            cat_id = int(cat_token.split(" | ")[0])
            
            rows = self.db.query(
                """
                SELECT id, article, brand, unit, COALESCE(weight, 0), COALESCE(volume, 0)
                FROM products
                WHERE subcategory_id = ? AND category_id = ?
                ORDER BY brand
                """,
                (sub_id, cat_id),
            )
            product_catalog.clear()
            values = []
            for pid, article, brand, unit, weight, volume in rows:
                token = f"{pid} | {article or ''} | {brand or ''}"
                values.append(token)
                product_catalog[token] = {
                    "id": pid, "article": article or "", "brand": brand or "",
                    "unit": unit or "Шт", "weight": float(weight or 0), "volume": float(volume or 0)
                }
            line_product_box["values"] = values
            if values:
                line_product_var.set(values[0])
                update_calculated_fields()
        
        def update_calculated_fields(*_):
            token = line_product_var.get().strip()
            info = product_catalog.get(token)
            if not info:
                line_unit_var.set("")
                line_weight_var.set("0")
                line_volume_var.set("0")
                return
            line_unit_var.set(info["unit"])
            try:
                qty = float(line_qty_var.get())
            except ValueError:
                qty = 0
            line_weight_var.set(f"{qty * info['weight']:.3f}")
            line_volume_var.set(f"{qty * info['volume']:.3f}")
        
        line_category_var.trace_add("write", refresh_subcategories_for_category)
        line_subcategory_var.trace_add("write", refresh_products_for_subcategory)
        line_product_var.trace_add("write", update_calculated_fields)
        line_qty_var.trace_add("write", update_calculated_fields)
        
        def add_line():
            cat = line_category_var.get().strip()
            sub = line_subcategory_var.get().strip()
            prod = line_product_var.get().strip()
            
            if not cat or not sub or not prod:
                messagebox.showwarning("⚠️ Внимание", "Выберите категорию, подкатегорию и товар", parent=dialog)
                return
            
            try:
                qty = float(line_qty_var.get())
                if qty <= 0:
                    raise ValueError
            except ValueError:
                messagebox.showwarning("⚠️ Внимание", "Количество должно быть положительным числом", parent=dialog)
                return
            
            info = product_catalog.get(prod)
            if not info:
                messagebox.showwarning("⚠️ Внимание", "Выберите корректный товар", parent=dialog)
                return
            
            cat_id = int(cat.split(" | ")[0])
            sub_id = int(sub.split(" | ")[0])
            
            planned_weight = qty * info["weight"]
            planned_volume = qty * info["volume"]
            
            item = {
                "category_id": cat_id,
                "subcategory_id": sub_id,
                "product_id": info["id"],
                "category_name": cat.split(" | ", 1)[1],
                "subcategory_name": sub.split(" | ", 1)[1],
                "article": info["article"],
                "brand": info["brand"],
                "unit": info["unit"],
                "planned_qty": qty,
                "planned_weight": planned_weight,
                "planned_volume": planned_volume,
            }
            order_items.append(item)
            lines_tree.insert(
                "", "end",
                values=(
                    item["category_name"],
                    item["subcategory_name"],
                    item["article"],
                    item["brand"],
                    item["unit"],
                    f"{qty:.3f}",
                    f"{planned_weight:.3f}",
                    f"{planned_volume:.3f}",
                ),
            )
        
        def read_id(token):
            if token and "|" in token:
                return int(token.split(" | ")[0])
            return None
        
        def save_order():
            supplier_id = read_id(supplier_var.get())
            client_id = read_id(client_var.get())
            warehouse_id = read_id(warehouse_var.get())
            
            if not supplier_id or not client_id or not warehouse_id:
                messagebox.showwarning("⚠️ Внимание", "Укажите поставщика, 3PL клиента и склад", parent=dialog)
                return
            
            if not order_items:
                messagebox.showwarning("⚠️ Внимание", "Добавьте хотя бы одну позицию в заказ", parent=dialog)
                return
            
            try:
                order_id = self.db.execute(
                    """
                    INSERT INTO inbound_orders(order_number, created_at, received_at, created_by, supplier_id, client_id, warehouse_id, status)
                    VALUES(?, ?, NULL, ?, ?, ?, ?, ?)
                    """,
                    (
                        order_number_var.get().strip(),
                        created_at_var.get().strip(),
                        created_by_var.get().strip(),
                        supplier_id,
                        client_id,
                        warehouse_id,
                        "Новый",
                    ),
                )
                for item in order_items:
                    self.db.execute(
                        """
                        INSERT INTO inbound_order_items(
                            order_id, category_id, subcategory_id, product_id,
                            planned_qty, actual_qty, actual_filled, planned_weight, planned_volume, serial_numbers
                        ) VALUES(?, ?, ?, ?, ?, 0, 0, ?, ?, '')
                        """,
                        (
                            order_id,
                            item["category_id"],
                            item["subcategory_id"],
                            item["product_id"],
                            item["planned_qty"],
                            item["planned_weight"],
                            item["planned_volume"],
                        ),
                    )
            except sqlite3.IntegrityError as exc:
                messagebox.showerror("❌ Ошибка", f"Не удалось сохранить заказ: {exc}", parent=dialog)
                return
            
            self.refresh_all()
            dialog.destroy()
            messagebox.showinfo("✅ Успешно", f"Заказ {order_number_var.get()} создан")
        
        # Кнопки
        btn_frame = ttk.Frame(main_frame, style="Card.TFrame")
        btn_frame.pack(fill="x", pady=(15, 0))
        
        ttk.Button(btn_frame, text="❌ Отмена", style="Secondary.TButton",
                  command=dialog.destroy).pack(side="left")
        ttk.Button(btn_frame, text="✅ Сохранить заказ", style="Success.TButton",
                  command=save_order).pack(side="right")

    def open_inbound_order_dialog(self, order_number: str):
        """Открыть диалог приёмки заказа"""
        order_rows = self.db.query(
            """
            SELECT o.id, o.order_number, o.created_at, COALESCE(o.received_at, ''),
                   s.name, c.name, w.name, o.status, o.created_by, COALESCE(o.accepted_by, '')
            FROM inbound_orders o
            JOIN suppliers s ON s.id = o.supplier_id
            JOIN clients c ON c.id = o.client_id
            JOIN warehouses w ON w.id = o.warehouse_id
            WHERE o.order_number = ?
            """,
            (order_number,),
        )
        if not order_rows:
            messagebox.showerror("❌ Ошибка", "Заказ не найден")
            return

        order_id, order_no, created_at, received_at, supplier_name, client_name, warehouse_name, status, created_by, accepted_by = order_rows[0]

        dialog = self._open_fullscreen_dialog(self, f"📥 Приёмка заказа {order_no}", "1400x800")

        main_frame = ttk.Frame(dialog, style="Card.TFrame")
        main_frame.pack(fill="both", expand=True, padx=30, pady=20)

        # Заголовок
        header_title = ttk.Frame(main_frame, style="Card.TFrame")
        header_title.pack(fill="x", pady=(0, 15))
        
        ttk.Label(header_title, text=f"Приёмка заказа {order_no}", style="Heading.TLabel").pack(side="left")
        
        # Статус с цветом
        status_colors = {"Новый": self.COLORS["warning"], "Принят": self.COLORS["success"]}
        status_label = tk.Label(header_title, text=f"  {status}  ", 
                               font=self.FONTS["body_bold"],
                               bg=status_colors.get(status, self.COLORS["primary"]),
                               fg="white", padx=10, pady=3)
        status_label.pack(side="right")

        # Информация о заказе
        info_frame = ttk.Frame(main_frame, style="Card.TFrame")
        info_frame.pack(fill="x", pady=(0, 15))
        
        info_data = [
            ("№ заказа:", order_no),
            ("Дата создания:", created_at),
            ("Поставщик:", supplier_name),
            ("3PL клиент:", client_name),
            ("Склад:", warehouse_name),
            ("Создал:", created_by),
            ("Принял:", accepted_by or "—"),
            ("Дата приёма:", received_at or "—"),
        ]
        
        for i, (label, value) in enumerate(info_data):
            row = i // 4
            col = (i % 4) * 2
            
            if not hasattr(self, '_info_grid_frame'):
                self._info_grid_frame = ttk.Frame(info_frame, style="Card.TFrame")
                self._info_grid_frame.pack(fill="x")
            
            lbl = ttk.Label(info_frame, text=label, style="Body.TLabel")
            lbl.grid(row=row, column=col, sticky="w", padx=(0 if col == 0 else 20, 5), pady=3)
            
            val = ttk.Label(info_frame, text=value, style="Body.TLabel", font=self.FONTS["body_bold"])
            val.grid(row=row, column=col + 1, sticky="w", pady=3)

        ttk.Separator(main_frame).pack(fill="x", pady=15)

        # Панель инструментов
        toolbar = ttk.Frame(main_frame, style="Card.TFrame")
        toolbar.pack(fill="x", pady=(0, 10))
        
        ttk.Button(toolbar, text="📝 Ввести фактическое", style="Primary.TButton",
                  command=lambda: edit_selected_line()).pack(side="left", padx=(0, 10))
        ttk.Button(toolbar, text="✏️ Редактировать кол-во", style="Secondary.TButton",
                  command=lambda: edit_selected_qty()).pack(side="left", padx=(0, 10))
        
        if status == "Новый":
            ttk.Button(toolbar, text="✅ Принять заказ", style="Success.TButton",
                      command=lambda: accept_order()).pack(side="left", padx=(0, 10))

        # Таблица позиций
        columns = (
            "line_id", "product_name", "article", "category", "subcategory",
            "unit", "planned_qty", "actual_qty", "weight", "volume", "barcode", "serial_count", "serial_tracking"
        )
        
        tree_frame = ttk.Frame(main_frame, style="Card.TFrame")
        tree_frame.pack(fill="both", expand=True)
        
        scrollbar_y = ttk.Scrollbar(tree_frame, orient="vertical")
        scrollbar_y.pack(side="right", fill="y")
        
        scrollbar_x = ttk.Scrollbar(tree_frame, orient="horizontal")
        scrollbar_x.pack(side="bottom", fill="x")
        
        lines_tree = ttk.Treeview(tree_frame, columns=columns, show="headings",
                                  yscrollcommand=scrollbar_y.set, xscrollcommand=scrollbar_x.set, height=16)
        lines_tree.pack(fill="both", expand=True)
        
        scrollbar_y.config(command=lines_tree.yview)
        scrollbar_x.config(command=lines_tree.xview)
        
        for col, title, width in [
            ("line_id", "ID", 50),
            ("product_name", "Товар", 200),
            ("article", "Артикул", 110),
            ("category", "Категория", 130),
            ("subcategory", "Подкатегория", 140),
            ("unit", "Ед.", 60),
            ("planned_qty", "План. кол-во", 100),
            ("actual_qty", "Факт. кол-во", 100),
            ("weight", "Вес", 80),
            ("volume", "Объём", 80),
            ("barcode", "Штрих-код", 120),
            ("serial_count", "Серий", 80),
            ("serial_tracking", "Серийный учёт", 100),
        ]:
            lines_tree.heading(col, text=title)
            lines_tree.column(col, width=width, anchor="center" if col in ("line_id", "unit", "planned_qty", "actual_qty", "weight", "volume", "serial_count") else "w")
        
        # Цветовые теги
        lines_tree.tag_configure("qty_less", background="#fff3e0")   # Недостача - оранжевый
        lines_tree.tag_configure("qty_more", background="#ffebee")   # Излишек - красный
        lines_tree.tag_configure("qty_ok", background="#e8f5e9")     # Совпадает - зелёный
        lines_tree.tag_configure("not_filled", background="#f5f5f5") # Не заполнено - серый

        def serial_count_text(serials: str):
            serial_list = [x.strip() for x in (serials or "").split(",") if x.strip()]
            return str(len(serial_list))

        def get_qty_tag(planned: float, actual: float, filled: int):
            if filled == 0:
                return "not_filled"
            if actual < planned:
                return "qty_less"
            if actual > planned:
                return "qty_more"
            return "qty_ok"

        def load_lines():
            for item in lines_tree.get_children():
                lines_tree.delete(item)
            rows = self.db.query(
                """
                SELECT i.id,
                       COALESCE(p.name, p.brand, ''),
                       COALESCE(p.article, ''),
                       COALESCE(cat.name, ''),
                       COALESCE(sub.name, ''),
                       COALESCE(p.unit, ''),
                       i.planned_qty,
                       i.actual_qty,
                       COALESCE(p.weight, 0) * i.actual_qty,
                       COALESCE(p.volume, 0) * i.actual_qty,
                       COALESCE(p.barcode, ''),
                       COALESCE(i.serial_numbers, ''),
                       COALESCE(p.serial_tracking, 'Нет'),
                       i.actual_filled
                FROM inbound_order_items i
                JOIN products p ON p.id = i.product_id
                LEFT JOIN categories cat ON cat.id = i.category_id
                LEFT JOIN subcategories sub ON sub.id = i.subcategory_id
                WHERE i.order_id = ?
                ORDER BY i.id
                """,
                (order_id,),
            )
            for rid, pname, article, cat, sub, unit, planned, actual, w, v, barcode, serials, serial_tracking, filled in rows:
                tag = get_qty_tag(float(planned), float(actual), int(filled))
                lines_tree.insert(
                    "", "end",
                    values=(rid, pname, article, cat, sub, unit, planned, actual, 
                           f"{w:.2f}", f"{v:.3f}", barcode, serial_count_text(serials), serial_tracking),
                    tags=(tag,),
                )

        def set_actual_qty(line_id: int, qty: float, serial_numbers: str | None = None):
            if serial_numbers is None:
                self.db.execute("UPDATE inbound_order_items SET actual_qty = ?, actual_filled = 1 WHERE id = ?", (qty, line_id))
            else:
                self.db.execute(
                    "UPDATE inbound_order_items SET actual_qty = ?, actual_filled = 1, serial_numbers = ? WHERE id = ?",
                    (qty, serial_numbers, line_id),
                )

        def edit_serial_line(line_id: int, product_name: str, current_serial: str):
            """Диалог сканирования серийных номеров"""
            serial_dialog = tk.Toplevel(dialog)
            serial_dialog.title(f"🔢 Сканирование серий: {product_name}")
            serial_dialog.geometry("600x500")
            serial_dialog.configure(bg=self.COLORS["bg_card"])
            serial_dialog.transient(dialog)
            serial_dialog.grab_set()
            
            self.after(10, lambda: self._center_window(serial_dialog))

            serial_frame = ttk.Frame(serial_dialog, style="Card.TFrame")
            serial_frame.pack(fill="both", expand=True, padx=20, pady=20)

            ttk.Label(serial_frame, text=f"Товар: {product_name}", style="Heading.TLabel").pack(anchor="w", pady=(0, 15))
            
            # Поле ввода серии
            input_frame = ttk.Frame(serial_frame, style="Card.TFrame")
            input_frame.pack(fill="x", pady=(0, 10))
            
            ttk.Label(input_frame, text="Серийный номер:", style="Body.TLabel").pack(side="left", padx=(0, 10))
            serial_input = tk.StringVar()
            serial_entry = ttk.Entry(input_frame, textvariable=serial_input, width=30)
            serial_entry.pack(side="left", padx=(0, 10))
            serial_entry.focus_set()
            
            ttk.Button(input_frame, text="➕ Добавить", style="Success.TButton",
                      command=lambda: add_serial()).pack(side="left")

            # Список серий
            serials = [x.strip() for x in (current_serial or "").split(",") if x.strip()]
            
            list_frame = ttk.Frame(serial_frame, style="Card.TFrame")
            list_frame.pack(fill="both", expand=True, pady=10)
            
            scrollbar = ttk.Scrollbar(list_frame, orient="vertical")
            scrollbar.pack(side="right", fill="y")
            
            listbox = tk.Listbox(list_frame, font=self.FONTS["body"], height=12,
                                yscrollcommand=scrollbar.set, selectmode="single")
            listbox.pack(fill="both", expand=True)
            scrollbar.config(command=listbox.yview)
            
            for ser in serials:
                listbox.insert("end", ser)

            count_var = tk.StringVar(value=f"📊 Отсканировано: {len(serials)}")
            ttk.Label(serial_frame, textvariable=count_var, style="Body.TLabel").pack(anchor="w", pady=(5, 0))

            def refresh_count():
                count_var.set(f"📊 Отсканировано: {listbox.size()}")

            def add_serial(_event=None):
                val = serial_input.get().strip()
                if not val:
                    return
                existing = [listbox.get(i) for i in range(listbox.size())]
                if val in existing:
                    messagebox.showwarning("⚠️ Внимание", "Эта серия уже добавлена", parent=serial_dialog)
                    return
                listbox.insert("end", val)
                serial_input.set("")
                serial_entry.focus_set()
                refresh_count()

            def remove_selected():
                sel = listbox.curselection()
                if not sel:
                    messagebox.showwarning("⚠️ Внимание", "Выберите серию для удаления", parent=serial_dialog)
                    return
                listbox.delete(sel[0])
                refresh_count()

            def finish_scan():
                out = [listbox.get(i) for i in range(listbox.size())]
                set_actual_qty(line_id, float(len(out)), ", ".join(out))
                serial_dialog.destroy()
                load_lines()
                self.refresh_inbound_orders()

            serial_entry.bind("<Return>", add_serial)

            # Кнопки
            btn_frame = ttk.Frame(serial_frame, style="Card.TFrame")
            btn_frame.pack(fill="x", pady=(15, 0))
            
            ttk.Button(btn_frame, text="🗑️ Удалить выбранную", style="Danger.TButton",
                      command=remove_selected).pack(side="left")
            ttk.Button(btn_frame, text="✅ Завершить", style="Success.TButton",
                      command=finish_scan).pack(side="right")

        def edit_selected_line():
            """Ввод фактического количества"""
            sel = lines_tree.selection()
            if not sel:
                messagebox.showwarning("⚠️ Внимание", "Выберите позицию", parent=dialog)
                return
            values = lines_tree.item(sel[0], "values")
            line_id = int(values[0])
            product_name = values[1]

            row = self.db.query(
                """
                SELECT i.actual_qty, COALESCE(i.serial_numbers, ''), COALESCE(p.serial_tracking, 'Нет')
                FROM inbound_order_items i
                JOIN products p ON p.id = i.product_id
                WHERE i.id = ?
                """,
                (line_id,),
            )[0]
            current_actual, current_serial, serial_tracking_db = float(row[0]), row[1], row[2]

            if serial_tracking_db == "Да":
                edit_serial_line(line_id, product_name, current_serial)
                return

            qty_text = simpledialog.askstring(
                "📝 Ввод количества",
                f"Введите количество для приёмки:\n\nТовар: {product_name}\nТекущее факт. кол-во: {current_actual}",
                initialvalue="1",
                parent=dialog,
            )
            if qty_text is None:
                return
            try:
                delta_qty = float(qty_text)
                if delta_qty <= 0:
                    raise ValueError
            except ValueError:
                messagebox.showwarning("⚠️ Внимание", "Количество должно быть положительным числом", parent=dialog)
                return

            set_actual_qty(line_id, current_actual + delta_qty)
            load_lines()
            self.refresh_inbound_orders()

        def edit_selected_qty():
            """Редактирование фактического количества"""
            sel = lines_tree.selection()
            if not sel:
                messagebox.showwarning("⚠️ Внимание", "Выберите позицию", parent=dialog)
                return
            values = lines_tree.item(sel[0], "values")
            line_id = int(values[0])
            product_name = values[1]

            row = self.db.query(
                """
                SELECT i.actual_qty, COALESCE(i.serial_numbers, ''), COALESCE(p.serial_tracking, 'Нет')
                FROM inbound_order_items i
                JOIN products p ON p.id = i.product_id
                WHERE i.id = ?
                """,
                (line_id,),
            )[0]
            current_actual, current_serial, serial_tracking_db = float(row[0]), row[1], row[2]

            if serial_tracking_db == "Да":
                edit_serial_line(line_id, product_name, current_serial)
                return

            qty_text = simpledialog.askstring(
                "✏️ Редактирование количества",
                f"Введите новое фактическое количество:\n\nТовар: {product_name}",
                initialvalue=str(current_actual),
                parent=dialog,
            )
            if qty_text is None:
                return
            try:
                new_qty = float(qty_text)
                if new_qty < 0:
                    raise ValueError
            except ValueError:
                messagebox.showwarning("⚠️ Внимание", "Количество должно быть числом >= 0", parent=dialog)
                return

            set_actual_qty(line_id, new_qty)
            load_lines()
            self.refresh_inbound_orders()

        def accept_order():
            """Принять заказ"""
            state = self.db.query("SELECT status FROM inbound_orders WHERE id = ?", (order_id,))[0][0]
            if state == "Принят":
                messagebox.showinfo("ℹ️ Информация", "Заказ уже принят", parent=dialog)
                return

            check_rows = self.db.query(
                """
                SELECT i.id, i.planned_qty, i.actual_qty, i.actual_filled, i.product_id,
                       COALESCE(i.serial_numbers, ''), COALESCE(p.serial_tracking, 'Нет')
                FROM inbound_order_items i
                JOIN products p ON p.id = i.product_id
                WHERE i.order_id = ?
                """,
                (order_id,),
            )
            
            if not check_rows:
                messagebox.showwarning("⚠️ Внимание", "В заказе нет позиций", parent=dialog)
                return

            not_filled = [r for r in check_rows if int(r[3]) != 1]
            if not_filled:
                messagebox.showwarning("⚠️ Внимание", 
                                      f"Заполните фактическое количество для всех позиций.\nНе заполнено: {len(not_filled)} позиций", 
                                      parent=dialog)
                return

            # Проверка расхождений
            discrepancies = []
            for _, planned_qty, actual_qty, _, _, serial_numbers, serial_tracking in check_rows:
                planned = float(planned_qty)
                actual = float(actual_qty)
                
                if actual > planned:
                    discrepancies.append(f"Излишек: план {planned}, факт {actual}")
                elif actual < planned:
                    discrepancies.append(f"Недостача: план {planned}, факт {actual}")
                
                if serial_tracking == "Да":
                    serial_count = len([x for x in (serial_numbers or "").split(",") if x.strip()])
                    if int(actual) != serial_count:
                        messagebox.showwarning(
                            "⚠️ Внимание",
                            "Количество серийных номеров должно совпадать с фактическим количеством",
                            parent=dialog,
                        )
                        return

            if discrepancies:
                if not messagebox.askyesno("⚠️ Подтверждение", 
                                          f"Обнаружены расхождения:\n\n" + "\n".join(discrepancies[:5]) + 
                                          ("\n..." if len(discrepancies) > 5 else "") +
                                          "\n\nВсё равно принять заказ?", 
                                          parent=dialog):
                    return

            # Проведение в остаток
            for _, _, actual_qty, _, product_id, _, _ in check_rows:
                qty = float(actual_qty)
                if qty > 0:
                    if abs(qty - int(qty)) > 1e-9:
                        messagebox.showwarning(
                            "⚠️ Внимание",
                            "Для проведения в остаток фактическое количество должно быть целым числом",
                            parent=dialog,
                        )
                        return
                    self.db.execute(
                        """
                        INSERT INTO movements(product_id, movement_type, quantity, reference, moved_at, note)
                        VALUES(?, 'IN', ?, ?, ?, ?)
                        """,
                        (
                            product_id,
                            int(qty),
                            order_no,
                            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            "Приём товара по заказу",
                        ),
                    )

            self.db.execute(
                "UPDATE inbound_orders SET status = 'Принят', received_at = ?, accepted_by = ? WHERE id = ?",
                (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), self.current_user, order_id),
            )
            
            messagebox.showinfo("✅ Успешно", "Заказ принят и проведён в остатки", parent=dialog)
            self.refresh_all()
            dialog.destroy()

        load_lines()

    # ==================== ДВИЖЕНИЯ - МЕТОДЫ ====================

    def add_movement(self):
        """Добавить движение товара"""
        token = self.movement_product.get().strip()
        if not token:
            messagebox.showwarning("⚠️ Внимание", "Выберите товар")
            return
        product_id = int(token.split(" | ")[0])
        movement_type = self.movement_type.get().strip()
        
        try:
            quantity = int(self.movement_qty.get())
            if quantity <= 0:
                raise ValueError
        except ValueError:
            messagebox.showwarning("⚠️ Внимание", "Количество должно быть положительным целым числом")
            return

        if movement_type == "OUT":
            available = self._current_stock_by_product().get(product_id, 0)
            if quantity > available:
                messagebox.showerror("❌ Ошибка", f"Недостаточно остатка.\nДоступно: {available}\nТребуется: {quantity}")
                return

        self.db.execute(
            "INSERT INTO movements(product_id, movement_type, quantity, reference, moved_at, note) VALUES(?, ?, ?, ?, ?, ?)",
            (
                product_id,
                movement_type,
                quantity,
                self.movement_ref.get().strip(),
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                self.movement_note.get().strip(),
            ),
        )

        self.movement_qty.set("1")
        self.movement_ref.set("")
        self.movement_note.set("")
        self.refresh_all()
        messagebox.showinfo("✅ Успешно", f"Движение {movement_type} на {quantity} ед. проведено")

    def _current_stock_by_product(self):
        """Получить текущие остатки по товарам"""
        rows = self.db.query(
            """
            SELECT p.id, COALESCE(SUM(CASE WHEN m.movement_type = 'IN' THEN m.quantity ELSE -m.quantity END), 0)
            FROM products p
            LEFT JOIN movements m ON m.product_id = p.id
            GROUP BY p.id
            """
        )
        return {pid: stock for pid, stock in rows}

        # ==================== ОБНОВЛЕНИЕ ДАННЫХ ====================

    def refresh_all(self):
        """Обновить все данные"""
        self.refresh_suppliers()
        self.refresh_3pl_clients()
        self.refresh_categories_tab()
        self.refresh_nomenclature()
        self.refresh_inbound_orders()
        self.refresh_movements()
        self.refresh_stock()
        self.refresh_metrics()

    def refresh_suppliers(self):
        """Обновить список поставщиков"""
        self._clear_tree(self.suppliers_tree)
        rows = self.db.query(
            "SELECT id, name, COALESCE(phone, ''), COALESCE(created_at, '') FROM suppliers ORDER BY id DESC"
        )
        term = self.suppliers_search_var.get().strip().lower()
        for row in rows:
            if term and term not in (row[1] or '').lower():
                continue
            self.suppliers_tree.insert("", "end", values=row)

    def refresh_3pl_clients(self):
        """Обновить список 3PL клиентов"""
        self._clear_tree(self.clients_tree)
        rows = self.db.query(
            "SELECT id, name, COALESCE(contact, ''), COALESCE(created_at, '') FROM clients ORDER BY id DESC"
        )
        term = self.clients_search_var.get().strip().lower()
        for row in rows:
            if term and term not in (row[1] or '').lower():
                continue
            self.clients_tree.insert("", "end", values=row)

    def refresh_categories_tab(self):
        """Обновить вкладку категорий"""
        if not hasattr(self, "categories_tree"):
            return
        
        self._clear_tree(self.categories_tree)
        self._clear_tree(self.subcategories_tree)

        term = self.categories_filter_var.get().strip().lower()
        
        # Загрузка категорий
        cat_rows = self.db.query("SELECT id, name FROM categories ORDER BY name")
        for row in cat_rows:
            if term and term not in (row[1] or '').lower():
                continue
            self.categories_tree.insert("", "end", values=row)

        # Загрузка подкатегорий
        sub_rows = self.db.query(
            """
            SELECT s.id, s.name, COALESCE(c.name, '')
            FROM subcategories s
            LEFT JOIN categories c ON c.id = s.category_id
            ORDER BY c.name, s.name
            """
        )
        for row in sub_rows:
            full = f"{row[1]} {row[2]}".lower()
            if term and term not in full:
                continue
            self.subcategories_tree.insert("", "end", values=row)

        # Обновление комбобокса родительских категорий
        parent_values = [f"{r[0]} | {r[1]}" for r in cat_rows]
        self.subcategory_parent_box["values"] = parent_values
        if parent_values and not self.subcategory_parent_var.get():
            self.subcategory_parent_var.set(parent_values[0])

    def refresh_nomenclature(self):
        """Обновить справочник номенклатуры"""
        self._clear_tree(self.nomenclature_tree)
        
        if not self.nomenclature_has_searched:
            return
        
        # Фильтры поиска
        search_brand = self.nomenclature_brand_filter.get().strip().lower()
        search_article = self.nomenclature_article_filter.get().strip().lower()
        search_supplier = self.nomenclature_supplier_filter.get().strip().lower()
        search_client = self.nomenclature_client_filter.get().strip().lower()
        
        rows = self.db.query(
            """
            SELECT p.id, COALESCE(p.article, ''), p.brand, s.name, c.name, p.unit, 
                   p.volume, p.weight, p.barcode, p.serial_tracking, cat.name, sub.name, p.product_owner
            FROM products p
            LEFT JOIN suppliers s ON s.id = p.supplier_id
            LEFT JOIN clients c ON c.id = p.client_id
            LEFT JOIN categories cat ON cat.id = p.category_id
            LEFT JOIN subcategories sub ON sub.id = p.subcategory_id
            ORDER BY p.id DESC
            """
        )
        
        for row in rows:
            brand = (row[2] or "").lower()
            article = (row[1] or "").lower()
            supplier = (row[3] or "").lower()
            client = (row[4] or "").lower()
            
            # Применение фильтров
            if search_brand and search_brand not in brand:
                continue
            if search_article and search_article not in article:
                continue
            if search_supplier and search_supplier not in supplier:
                continue
            if search_client and search_client not in client:
                continue
            
            self.nomenclature_tree.insert("", "end", values=row)

        # Обновление комбобокса товаров в движениях
        product_values = [
            f"{r[0]} | {r[1] or ''} | {r[2] or ''}" 
            for r in self.db.query("SELECT id, article, brand FROM products ORDER BY id DESC")
        ]
        self.movement_product_box["values"] = product_values
        if product_values and not self.movement_product.get():
            self.movement_product.set(product_values[0])

    def refresh_inbound_orders(self):
        """Обновить список приходных заказов"""
        self._clear_tree(self.inbound_tree)
        
        if not self.inbound_has_searched:
            return
        
        rows = self.db.query(
            """
            SELECT o.order_number,
                   o.created_at,
                   COALESCE(o.received_at, ''),
                   o.created_by,
                   COALESCE(o.accepted_by, ''),
                   s.name,
                   c.name,
                   w.name,
                   o.status,
                   COALESCE(SUM(i.planned_qty), 0),
                   COALESCE(SUM(i.actual_qty), 0)
            FROM inbound_orders o
            JOIN suppliers s ON s.id = o.supplier_id
            JOIN clients c ON c.id = o.client_id
            JOIN warehouses w ON w.id = o.warehouse_id
            LEFT JOIN inbound_order_items i ON i.order_id = o.id
            GROUP BY o.id, o.order_number, o.created_at, o.received_at, o.created_by, o.accepted_by,
                     s.name, c.name, w.name, o.status
            ORDER BY o.id DESC
            """
        )

        # Получение значений фильтров
        search = self.inbound_order_search_var.get().strip().lower()
        status = self.inbound_status_var.get().strip()
        from_date = self.inbound_from_date_var.get().strip()
        to_date = self.inbound_to_date_var.get().strip()
        created_by = self.inbound_created_by_filter_var.get().strip().lower()
        accepted_by = self.inbound_accepted_by_filter_var.get().strip().lower()
        supplier = self.inbound_supplier_filter_var.get().strip().lower()
        client = self.inbound_client_filter_var.get().strip().lower()

        for row in rows:
            order_number = (row[0] or "").lower()
            created_at = row[1] or ""
            created_day = created_at[:10]
            row_status = row[8] or ""
            
            # Применение фильтров
            if search and search not in order_number:
                continue
            if status and status != "Все" and row_status != status:
                continue
            if from_date and created_day < from_date:
                continue
            if to_date and created_day > to_date:
                continue
            if created_by and created_by not in (row[3] or "").lower():
                continue
            if accepted_by and accepted_by not in (row[4] or "").lower():
                continue
            if supplier and supplier not in (row[5] or "").lower():
                continue
            if client and client not in (row[6] or "").lower():
                continue
            
            # Определение тега для цветовой индикации
            tag = "new" if row_status == "Новый" else "accepted"
            self.inbound_tree.insert("", "end", values=row, tags=(tag,))

    def refresh_movements(self):
        """Обновить журнал движений"""
        self._clear_tree(self.movements_tree)
        
        rows = self.db.query(
            """
            SELECT m.id, p.brand, m.movement_type, m.quantity, m.reference, m.moved_at, m.note
            FROM movements m
            JOIN products p ON p.id = m.product_id
            ORDER BY m.id DESC
            """
        )
        
        for row in rows:
            # Цветовая индикация по типу движения
            tag = "in" if row[2] == "IN" else "out"
            self.movements_tree.insert("", "end", values=row, tags=(tag,))

    def refresh_stock(self):
        """Обновить остатки на складе"""
        self._clear_tree(self.stock_tree)
        
        rows = self.db.query(
            """
            SELECT p.brand, c.name, p.unit,
                   COALESCE(SUM(CASE WHEN m.movement_type = 'IN' THEN m.quantity ELSE -m.quantity END), 0)
            FROM products p
            LEFT JOIN clients c ON c.id = p.client_id
            LEFT JOIN movements m ON m.product_id = p.id
            GROUP BY p.id, p.brand, c.name, p.unit
            ORDER BY p.id DESC
            """
        )
        
        for row in rows:
            stock = row[3]
            # Цветовая индикация по уровню остатка
            if stock == 0:
                tag = "zero"
            elif stock < 10:
                tag = "low"
            else:
                tag = "ok"
            self.stock_tree.insert("", "end", values=row, tags=(tag,))

    def refresh_metrics(self):
        """Обновить метрики на информационной панели"""
        suppliers_count = self.db.query("SELECT COUNT(*) FROM suppliers")[0][0]
        clients_count = self.db.query("SELECT COUNT(*) FROM clients")[0][0]
        products_count = self.db.query("SELECT COUNT(*) FROM products")[0][0]
        inbound_count = self.db.query("SELECT COUNT(*) FROM inbound_orders")[0][0]
        movements_count = self.db.query("SELECT COUNT(*) FROM movements")[0][0]
        
        # Обновление меток с метриками
        self.metric_labels["suppliers"].configure(text=str(suppliers_count))
        self.metric_labels["clients"].configure(text=str(clients_count))
        self.metric_labels["products"].configure(text=str(products_count))
        self.metric_labels["inbound"].configure(text=str(inbound_count))
        self.metric_labels["movements"].configure(text=str(movements_count))

    # ==================== ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ ====================

    def _clear_tree(self, tree):
        """Очистить все строки в таблице Treeview"""
        for row in tree.get_children():
            tree.delete(row)

    def _load_reference_dict(self, table_name):
        """Загрузить справочник в виде словаря {токен: id}"""
        rows = self.db.query(f"SELECT id, name FROM {table_name} ORDER BY name")
        return {f"{r[0]} | {r[1]}": r[0] for r in rows}

    def _current_stock_by_product(self):
        """Получить текущие остатки по всем товарам"""
        rows = self.db.query(
            """
            SELECT p.id, COALESCE(SUM(CASE WHEN m.movement_type = 'IN' THEN m.quantity ELSE -m.quantity END), 0)
            FROM products p
            LEFT JOIN movements m ON m.product_id = p.id
            GROUP BY p.id
            """
        )
        return {pid: stock for pid, stock in rows}

    def _normalize_decimal(self, raw: str):
        """Нормализовать строку в десятичное число"""
        value = (raw or "").strip().replace(",", ".")
        if not value:
            return None
        if not re.fullmatch(r"\d+(?:\.\d{1,4})?", value):
            raise ValueError("Некорректный формат числа")
        return float(value)

    def _center_window(self, window):
        """Центрировать окно на экране"""
        window.update_idletasks()
        width = window.winfo_width()
        height = window.winfo_height()
        screen_width = window.winfo_screenwidth()
        screen_height = window.winfo_screenheight()
        x = (screen_width - width) // 2
        y = (screen_height - height) // 2
        window.geometry(f"{width}x{height}+{x}+{y}")

    def _maximize_window(self, win):
        """Развернуть окно на весь экран"""
        try:
            win.state("zoomed")
        except tk.TclError:
            try:
                win.attributes("-fullscreen", True)
            except tk.TclError:
                pass

    def _open_fullscreen_dialog(self, parent, title: str, geometry: str | None = None):
        """Открыть диалоговое окно с возможностью разворота"""
        dialog = tk.Toplevel(parent)
        dialog.title(title)
        dialog.configure(bg=self.COLORS["bg_main"])
        
        if geometry:
            dialog.geometry(geometry)
        
        dialog.transient(parent)
        dialog.grab_set()
        
        # Развернуть на весь экран после небольшой задержки
        self.after(100, lambda: self._maximize_window(dialog))
        
        return dialog

    def _open_date_picker(self, target_var):
        """Открыть диалог выбора даты"""
        picker = tk.Toplevel(self)
        picker.title("📅 Выбор даты")
        picker.transient(self)
        picker.grab_set()
        picker.configure(bg=self.COLORS["bg_card"])
        picker.geometry("340x350")
        picker.resizable(False, False)
        
        # Центрируем окно
        self.after(10, lambda: self._center_window(picker))
        
        today = datetime.now()
        year_var = tk.IntVar(value=today.year)
        month_var = tk.IntVar(value=today.month)
        
        main_frame = ttk.Frame(picker, style="Card.TFrame")
        main_frame.pack(fill="both", expand=True, padx=15, pady=15)
        
        # Названия месяцев на русском
        month_names_ru = [
            "", "Январь", "Февраль", "Март", "Апрель", "Май", "Июнь",
            "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь"
        ]
        
        # Навигация по месяцам
        nav_frame = ttk.Frame(main_frame, style="Card.TFrame")
        nav_frame.pack(fill="x", pady=(0, 15))
        
        def shift_month(delta):
            m = month_var.get() + delta
            y = year_var.get()
            if m < 1:
                m, y = 12, y - 1
            elif m > 12:
                m, y = 1, y + 1
            month_var.set(m)
            year_var.set(y)
            build_days()
        
        ttk.Button(nav_frame, text="◀", style="Secondary.TButton", width=3,
                  command=lambda: shift_month(-1)).pack(side="left")
        
        month_label = ttk.Label(nav_frame, text="", style="Heading.TLabel")
        month_label.pack(side="left", expand=True)
        
        ttk.Button(nav_frame, text="▶", style="Secondary.TButton", width=3,
                  command=lambda: shift_month(1)).pack(side="right")
        
        # Фрейм для дней
        days_frame = ttk.Frame(main_frame, style="Card.TFrame")
        days_frame.pack(fill="both", expand=True)
        
        def select_day(day):
            target_var.set(f"{year_var.get():04d}-{month_var.get():02d}-{day:02d}")
            picker.destroy()
        
        def build_days():
            # Очистка предыдущих элементов
            for child in days_frame.winfo_children():
                child.destroy()
            
            y, m = year_var.get(), month_var.get()
            month_label.configure(text=f"{month_names_ru[m]} {y}")
            
            # Заголовки дней недели
            days_of_week = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
            for c, day_name in enumerate(days_of_week):
                lbl = ttk.Label(days_frame, text=day_name, style="Body.TLabel", width=4, anchor="center")
                lbl.grid(row=0, column=c, padx=2, pady=5)
            
            # Дни месяца
            for r, week in enumerate(calendar.monthcalendar(y, m)):
                for c, day in enumerate(week):
                    if day == 0:
                        ttk.Label(days_frame, text="", width=4).grid(row=r+1, column=c, padx=2, pady=2)
                    else:
                        # Подсветка текущего дня
                        is_today = (day == today.day and m == today.month and y == today.year)
                        style = "Accent.TButton" if is_today else "Secondary.TButton"
                        btn = ttk.Button(days_frame, text=str(day), width=4, style=style,
                                        command=lambda d=day: select_day(d))
                        btn.grid(row=r+1, column=c, padx=2, pady=2)
        
        # Кнопки внизу
        btn_frame = ttk.Frame(main_frame, style="Card.TFrame")
        btn_frame.pack(fill="x", pady=(15, 0))
        
        def clear_date():
            target_var.set("")
            picker.destroy()
        
        def set_today():
            target_var.set(datetime.now().strftime("%Y-%m-%d"))
            picker.destroy()
        
        ttk.Button(btn_frame, text="✖ Очистить", style="Secondary.TButton",
                  command=clear_date).pack(side="left")
        ttk.Button(btn_frame, text="📅 Сегодня", style="Primary.TButton",
                  command=set_today).pack(side="right")
        
        build_days()

    def copy_selected_value(self):
        """Скопировать выбранное значение в буфер обмена"""
        self.clipboard_clear()
        self.clipboard_append(str(self.selected_copy_value))
        
    def show_copy_menu(self, event):
        """Показать контекстное меню для номенклатуры"""
        row = self.nomenclature_tree.identify_row(event.y)
        col = self.nomenclature_tree.identify_column(event.x)
        if not row:
            return
        self.nomenclature_tree.selection_set(row)
        values = self.nomenclature_tree.item(row, "values")
        col_index = int(col.replace("#", "")) - 1 if col else 0
        self.selected_copy_value = values[col_index] if 0 <= col_index < len(values) else ""
        self.copy_menu.tk_popup(event.x_root, event.y_root)

    def show_suppliers_copy_menu(self, event):
        """Показать контекстное меню для поставщиков"""
        row = self.suppliers_tree.identify_row(event.y)
        col = self.suppliers_tree.identify_column(event.x)
        if not row:
            return
        self.suppliers_tree.selection_set(row)
        values = self.suppliers_tree.item(row, "values")
        col_index = int(col.replace("#", "")) - 1 if col else 0
        self.selected_copy_value = values[col_index] if 0 <= col_index < len(values) else ""
        self.suppliers_copy_menu.tk_popup(event.x_root, event.y_root)

    def show_clients_copy_menu(self, event):
        """Показать контекстное меню для клиентов"""
        row = self.clients_tree.identify_row(event.y)
        col = self.clients_tree.identify_column(event.x)
        if not row:
            return
        self.clients_tree.selection_set(row)
        values = self.clients_tree.item(row, "values")
        col_index = int(col.replace("#", "")) - 1 if col else 0
        self.selected_copy_value = values[col_index] if 0 <= col_index < len(values) else ""
        self.clients_copy_menu.tk_popup(event.x_root, event.y_root)

    # ==================== ОБРАБОТЧИК ЗАКРЫТИЯ ====================

    def on_close(self):
        """Обработчик закрытия приложения"""
        if messagebox.askyesno("🚪 Выход", "Вы уверены, что хотите выйти из приложения?"):
            self.db.close()
            self.destroy()


# ==================== ТОЧКА ВХОДА ====================

if __name__ == "__main__":
    app = WMSApp()
    app.mainloop()
